"""Independently reconcile MQTT expected payloads with a PostgreSQL JSON export."""
import json
import sys

expected = {"expected": []}
for path in [sys.argv[1], *sys.argv[3:]]:
    expected["expected"].extend(json.load(open(path))["expected"])
actual = json.load(open(sys.argv[2])) or []
rows = {}
for row in actual:
    key = (row["tenant_id"], row["device_id"], row["event_id"])
    if key in rows:
        raise SystemExit(f"duplicate persisted identity {key}")
    rows[key] = row
expected_keys = set()
for item in expected["expected"]:
    event = item["event"]
    key = ("benchmark", item["device"], event["event_id"])
    if key in expected_keys:
        raise SystemExit(f"duplicate expected identity {key}")
    expected_keys.add(key)
    stored = rows.get(key)
    if stored is None:
        raise SystemExit(f"missing event {key}")
    if {name: stored[name] for name in event} != event:
        raise SystemExit(f"payload mismatch {key}")
if len(sys.argv) > 3:
    if expected_keys != set(rows):
        raise SystemExit("Unexpected or missing persisted event identities")
print(json.dumps({"verified_events": len(expected["expected"]), "all_fields_match": True}))
