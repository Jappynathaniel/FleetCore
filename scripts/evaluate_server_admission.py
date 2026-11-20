"""Fail closed on absent, failed, or mismatched experiment cells."""
import json
import math
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])


def require(condition, label):
    if not condition:
        raise SystemExit(f"Incomplete or mismatched experiment cell: {label}")


def load(label, concurrency, events=360000, batches=1000, spread=0, cohort=None):
    require((root / f"{label}.status").read_text().strip() == "completed", label)
    manifest = root / f"{label}.server.json"
    if manifest.exists():
        config = json.loads(manifest.read_text())[0]["Config"]
        env = dict(value.split("=", 1) for value in config["Env"] if "=" in value)
        enabled = label.startswith(("candidate-", "holdout-candidate-", "quiet-", "noisy-"))
        require(env.get("TENANT_ADMISSION") == str(enabled).lower()
                and env.get("ADMISSION_GLOBAL") == "16"
                and env.get("ADMISSION_PER_TENANT") == "8"
                and env.get("INGEST_QUEUE") == "64", label)
    records = [json.loads(line) for line in (root / f"{label}.jsonl").read_text().splitlines() if line.strip()]
    require(len(records) == 3, label)
    metadata = records[0]
    require(metadata["arrival_spread_ms"] == spread, label)
    require(metadata["noisy_neighbor"] == (cohort == "noisy"), label)
    require(metadata["quiet_only"] == (cohort == "quiet"), label)
    require(metadata["http_concurrency"] == concurrency and metadata["measured_trials"] == 1
            and metadata["query_rate"] == 0 and metadata["sustain_seconds"] == 0
            and metadata["synchronous_commit"] == "on"
            and metadata["devices_override"] is None
            and metadata["events_per_device_override"] == (240 if spread else None), label)
    require(records[1]["warmup"] is True and records[1]["trial"] == 0, label)
    rows = [r for r in records[1:] if not r["warmup"]]
    require(len(rows) == 1, label)
    row = rows[0]
    require(row["integrity_verified"] is True and row["trial"] == 1, label)
    require((row["events"], row["batches"], row["concurrency"]) == (events, batches, concurrency), label)
    require(row["layer"] == "http" and row["scenario"] == "reconnect" and row["mode"] == "unnest", label)
    require(all(math.isfinite(row[key]) and row[key] >= 0 for key in
                ("elapsed_seconds", "scheduled_p99_ms", "events_per_second", "retries"))
            and row["elapsed_seconds"] > 0 and type(row["retries"]) is int, label)
    tenants = {"benchmark-noisy": 1000, "benchmark-quiet": 100} if cohort == "noisy" else {"benchmark-quiet": 100} if cohort == "quiet" else {"benchmark": 1000}
    require(set(row["tenant_results"]) == set(tenants), label)
    for tenant, count in tenants.items():
        result = row["tenant_results"][tenant]
        require(result["batches"] == count and all(math.isfinite(result[key]) and result[key] >= 0
                for key in ("scheduled_p99_ms", "completed_at_seconds")), label)
    return row


def summary(rows):
    return {key: statistics.median(r[key] for r in rows) for key in
            ("elapsed_seconds", "scheduled_p99_ms", "retries", "events_per_second")}


groups = {name: [load(f"{name}-{i}", 16 if name == "aggregate" else 1000) for i in range(1, 6)]
          for name in ("aggregate", "independent", "candidate")}
medians = {name: summary(rows) for name, rows in groups.items()}
a, b, c = (medians[k] for k in ("aggregate", "independent", "candidate"))
gates = {
    "strict_aggregate_drain": c["elapsed_seconds"] <= a["elapsed_seconds"],
    "strict_aggregate_retries": c["retries"] <= a["retries"],
    "independent_drain_10_percent": c["elapsed_seconds"] <= b["elapsed_seconds"] * .9,
    "independent_scheduled_p99_10_percent": c["scheduled_p99_ms"] <= b["scheduled_p99_ms"] * .9,
    "independent_throughput": c["events_per_second"] >= b["events_per_second"] * .95,
}
neighbors = []
for i in range(1, 4):
    quiet = load(f"quiet-{i}", 100, 36000, 100, cohort="quiet")
    noisy = load(f"noisy-{i}", 1100, 396000, 1100, cohort="noisy")
    q = noisy["tenant_results"]["benchmark-quiet"]
    n = noisy["tenant_results"]["benchmark-noisy"]
    passed = q["scheduled_p99_ms"] < 10000 and q["scheduled_p99_ms"] <= 2 * quiet["scheduled_p99_ms"] + 1000 and q["completed_at_seconds"] < n["completed_at_seconds"]
    neighbors.append({"quiet_p99_ms": q["scheduled_p99_ms"], "quiet_only_p99_ms": quiet["scheduled_p99_ms"], "passed": passed})
gates["noisy_neighbor_all_repetitions"] = all(r["passed"] for r in neighbors)
holdout = {name: summary([load(f"holdout-{name}-{i}", 1000, 240000, 1000, 2000) for i in range(1, 4)]) for name in ("independent", "candidate")}
h, k = holdout["independent"], holdout["candidate"]
gates["holdout_drain_10_percent"] = k["elapsed_seconds"] <= h["elapsed_seconds"] * .9
gates["holdout_scheduled_p99_10_percent"] = k["scheduled_p99_ms"] <= h["scheduled_p99_ms"] * .9
print(json.dumps({"medians": medians, "noisy_neighbor": neighbors, "holdout": holdout, "gates": gates, "promote": all(gates.values())}, indent=2))
