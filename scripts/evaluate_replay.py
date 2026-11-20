"""Paired bootstrap of complete trials. The output is an uncertainty estimate on this host."""
from pathlib import Path
import json
import random
import statistics
import sys

root = Path(sys.argv[1])
prefix = sys.argv[2] if len(sys.argv) > 2 else "concurrency"
trials = int(sys.argv[3]) if len(sys.argv) > 3 else 10
baseline, candidate = [], []
for trial in range(1, trials + 1):
    paired_metadata = []
    for concurrency, output in ((96, baseline), (16, candidate)):
        rows = [json.loads(line) for line in (root / f"{prefix}-{concurrency}-{trial}.jsonl").read_text().splitlines()]
        valid = [row for row in rows if row.get("mode") and not row["warmup"]]
        if len(valid) != 1 or not valid[0]["integrity_verified"]:
            raise SystemExit("Incomplete or invalid trial")
        row, metadata = valid[0], rows[0]
        paired_metadata.append(metadata)
        devices = metadata.get("devices_override") or 1000
        per_device = metadata.get("events_per_device_override") or 360
        if (row.get("concurrency") != concurrency
                or metadata.get("http_concurrency") != concurrency
                or row.get("layer") != "http" or row.get("scenario") != "reconnect"
                or row.get("mode") != "unnest"
                or row.get("batches") != devices
                or row.get("events") != devices * per_device):
            raise SystemExit("Mismatched workload or concurrency metadata")
        output.append(row)
    if any(baseline[-1][key] != candidate[-1][key] for key in ("events", "batches", "scenario", "layer", "mode")):
        raise SystemExit("Paired workloads differ")
    if any(paired_metadata[0].get(key) != paired_metadata[1].get(key) for key in
           ("arrival_spread_ms", "query_rate", "sustain_seconds", "event_seed",
            "devices_override", "events_per_device_override", "noisy_neighbor", "quiet_only")):
        raise SystemExit("Paired arrival schedules or workload settings differ")

rng = random.Random(20261119)
def median(rows, key):
    return statistics.median(row[key] for row in rows)
def difference(a, b, key):
    return 100 * (median(b, key) / median(a, key) - 1)
results = {}
for key in ("elapsed_seconds", "events_per_second", "confirmation_p99_ms", "scheduled_p99_ms", "retries"):
    samples = []
    for _ in range(10000):
        ids = [rng.randrange(trials) for _ in range(trials)]
        a = [baseline[i] for i in ids]
        b = [candidate[i] for i in ids]
        if median(a, key) == 0:
            continue
        samples.append(difference(a, b, key))
    samples.sort()
    results[key] = {
        "baseline_median": median(baseline, key), "candidate_median": median(candidate, key),
        "change_percent": difference(baseline, candidate, key) if median(baseline, key) else None,
        "paired_bootstrap_95_percent_interval": [samples[249], samples[9749]] if samples else None,
    }
gates = {
    "first_attempt_p99_halved": results["confirmation_p99_ms"]["change_percent"] <= -50,
    "throughput_regression_at_most_5_percent": results["events_per_second"]["change_percent"] >= -5,
    "scheduled_p99_regression_at_most_5_percent": results["scheduled_p99_ms"]["change_percent"] <= 5,
    "all_event_integrity_checks": True,
}
print(json.dumps({"paired_trials": trials, "resamples": 10000, "metrics": results, "gates": gates,
                  "point_estimate_gates_pass": all(gates.values())}, indent=2))
