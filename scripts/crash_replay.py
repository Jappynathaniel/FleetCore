"""Crash PostgreSQL after a fixture row is inserted but before its batch commits."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def compose(*args, sql=None, timeout=20):
    return subprocess.run(
        ["docker", "compose", *args], input=sql, text=True,
        stdout=subprocess.PIPE, check=True, timeout=timeout,
    ).stdout.strip()


def query(sql):
    return compose(
        "exec", "-T", "db", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
        "-U", "fleetcore", "-d", "fleetcore", sql=sql,
    )


def snapshot():
    return json.loads(query("SELECT COALESCE(json_agg(t), '[]') FROM telemetry AS t;"))


def key(row):
    return row["tenant_id"], row["device_id"], row["event_id"]


def reconcile(expected, phase):
    rows = snapshot()
    actual = {key(row): row for row in rows}
    if len(actual) != len(rows):
        raise RuntimeError(f"{phase}: duplicate stored identities")
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    changed = [identity for identity in expected.keys() & actual.keys()
               if expected[identity] != actual[identity]]
    if missing or extra or changed:
        raise RuntimeError(
            f"{phase}: missing={sorted(missing)[:5]}, extra={sorted(extra)[:5]}, "
            f"payload mismatches={sorted(changed)[:5]}"
        )


def send(url, payload):
    request = Request(url, data=payload, headers={
        "Authorization": "Bearer north-van-demo", "Content-Type": "application/json",
    })
    try:
        with urlopen(request, timeout=40) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        with error:
            return error.code, None


def check_receipt(url, payload, batch, inserted):
    status, receipt = send(url, payload)
    expected = {
        "batch_id": batch["batch_id"], "outcome": "committed",
        "inserted": inserted, "duplicates": len(batch["events"]) - inserted,
    }
    if status != 200 or receipt != expected:
        raise RuntimeError(f"Unexpected receipt: HTTP {status}, {receipt}")


def main():
    # Keep the fault injection confined to the disposable projects owned by CI.
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    if project not in {
        f"fleetcore-ci-{writer}-{admission}"
        for writer in ("unnest", "copy") for admission in ("false", "true")
    }:
        raise SystemExit("Run this check through scripts/ci_acceptance.sh")

    url = "http://" + compose("port", "api", "8080") + "/v1/telemetry/batches"
    device = json.loads(query("""
        SELECT json_build_object('tenant_id', tenant_id, 'device_id', id)
        FROM devices WHERE id = 'north-van';
    """))
    before = {key(row): row for row in snapshot()}
    batch = {"batch_id": str(uuid4()), "events": [
        {"event_id": str(uuid4()), "observed_at_ms": 1_700_000_000_000 + i,
         "latitude_e6": -5000 + i, "longitude_e6": 6000 - i, "speed_cm_s": i}
        for i in range(32)
    ]}
    # The fixture is saved before the first send and reopened after the crash.
    with tempfile.TemporaryDirectory(prefix="fleetcore-crash-") as directory:
        manifest = Path(directory) / "batch.json"
        manifest.write_text(json.dumps(batch), encoding="utf-8")
        payload = manifest.read_bytes()
        first_id = batch["events"][0]["event_id"]
        query(f"""
            CREATE FUNCTION ci_pause_telemetry_insert() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_sleep(90);
                RETURN NEW;
            END;
            $$;
            CREATE TRIGGER ci_pause_telemetry_insert AFTER INSERT ON telemetry
            FOR EACH ROW WHEN (NEW.event_id = '{first_id}'::uuid)
            EXECUTE FUNCTION ci_pause_telemetry_insert();
        """)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                attempt = executor.submit(send, url, payload)
                deadline = time.monotonic() + 15
                while True:
                    if attempt.done() or time.monotonic() >= deadline:
                        raise RuntimeError("Batch never entered the PostgreSQL insert barrier")
                    waiting = query("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database() AND wait_event = 'PgSleep'
                        AND query LIKE '%INSERT INTO telemetry%';
                    """)
                    if waiting == "1":
                        break
                    time.sleep(0.2)
                print("Observed in-flight insert. Killing PostgreSQL before commit.", flush=True)
                compose("kill", "-s", "SIGKILL", "db")
                status, _ = attempt.result(timeout=35)
                if status != 503:
                    raise RuntimeError(f"Interrupted batch returned HTTP {status}; expected 503")
        finally:
            compose("up", "-d", "--wait", "--wait-timeout", "60", "--no-build", "api",
                    timeout=75)
            query("DROP TRIGGER ci_pause_telemetry_insert ON telemetry; "
                  "DROP FUNCTION ci_pause_telemetry_insert();")

        reconcile(before, "after crash, before replay")
        payload = manifest.read_bytes()
        batch = json.loads(payload)
        expected = before.copy()
        for event in batch["events"]:
            row = device | event
            expected[key(row)] = row
        check_receipt(url, payload, batch, inserted=len(batch["events"]))
        reconcile(expected, "after replay")
        check_receipt(url, payload, batch, inserted=0)
        reconcile(expected, "after duplicate replay")
        print(f"PASS: interrupted batch rolled back; {len(batch['events'])} events "
              "reconciled with no missing, extra, duplicate or altered rows", flush=True)


if __name__ == "__main__":
    main()
