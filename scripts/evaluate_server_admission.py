"""Fail closed on absent, failed, or mismatched experiment cells."""
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])


def load(label, concurrency, events=360000, batches=1000, spread=0, cohort=None):
    assert (root / f"{label}.status").read_text().strip() == "completed", label
    records = [json.loads(line) for line in (root / f"{label}.jsonl").read_text().splitlines() if line.strip()]
    metadata = records[0]
    assert metadata["arrival_spread_ms"] == spread, label
    assert metadata["noisy_neighbor"] == (cohort == "noisy"), label
    assert metadata["quiet_only"] == (cohort == "quiet"), label
    rows = [r for r in records[1:] if not r["warmup"]]
    assert len(rows) == 1, label
    row = rows[0]
    assert row["integrity_verified"] is True, label
    assert (row["events"], row["batches"], row["concurrency"]) == (events, batches, concurrency), label
    assert row["layer"] == "http" and row["scenario"] == "reconnect" and row["mode"] == "unnest", label
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
