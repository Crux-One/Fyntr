#!/usr/bin/env python3
"""Compare aggregate results from old/new fyntr-bench JSON trials."""

import argparse
import json
import statistics
from pathlib import Path


FIELDS = (
    ("completed_without_protocol_error", "successful trial rate"),
    ("protocol_failures", "protocol failures"),
    ("throughput_bytes_per_second", "throughput B/s"),
    ("transaction_rate_per_second", "transactions/s"),
    ("p50_latency_ms", "p50 latency ms"),
    ("p99_latency_ms", "p99 latency ms"),
    ("elephant_throughput_bytes_per_second", "elephant throughput B/s"),
    ("mouse_transaction_rate_per_second", "mouse transactions/s"),
    ("fyntr_cpu_ns_per_transferred_byte", "Fyntr CPU ns/B"),
    ("fyntr_peak_rss_bytes", "Fyntr peak RSS bytes"),
)

RATE_FIELDS = {
    "completed_without_protocol_error",
    "recovered",
    "client_backpressure_observed",
}
SUM_FIELDS = {"protocol_failures"}
S7_FIELDS = (
    ("recovered", "S7 recovery rate"),
    ("recovery_time_ms", "S7 recovery time ms"),
    ("generated_records", "S7 generated records"),
    ("acknowledged_records", "S7 acknowledged records"),
    ("client_backpressure_observed", "S7 backpressure rate"),
    ("maximum_record_write_ms", "S7 max write ms"),
)


def json_files(path):
    path = Path(path)
    return sorted(path.glob("*.json")) if path.is_dir() else [path]


def load(path):
    return [json.loads(item.read_text()) for item in json_files(path)]


def group_key(row):
    settings = row["settings"]
    return (
        row["scenario"],
        settings["flows"],
        settings["bulk_payload_bytes"],
        settings["bulk_pipeline_records"],
        settings["mouse_payload_bytes"],
        settings["warmup_secs"],
        settings["measurement_secs"],
        settings["pause_secs"],
        settings["recovery_timeout_secs"],
        settings["max_connections"],
        settings["rss_sample_interval_ms"],
        settings["backpressure_threshold_ms"],
        row["schema_version"],
    )


def groups(rows):
    result = {}
    for row in rows:
        result.setdefault(group_key(row), []).append(row)
    return result


def aggregate(rows, key, nested=None):
    values = []
    for row in rows:
        value = row.get(nested, {}).get(key) if nested else row.get(key)
        if value is not None:
            values.append(float(value))
    if not values:
        return None
    if key in RATE_FIELDS:
        return sum(values) / len(values)
    if key in SUM_FIELDS:
        return sum(values)
    return statistics.median(values)


def display(value):
    return "-" if value is None else f"{value:.3f}"


def delta(old, new):
    if old in (None, 0) or new is None:
        return "-"
    return f"{(new - old) / old * 100:+.2f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("old", help="old JSON file or directory")
    parser.add_argument("new", help="new JSON file or directory")
    args = parser.parse_args()
    old_groups = groups(load(args.old))
    new_groups = groups(load(args.new))

    common = sorted(set(old_groups) & set(new_groups), key=str)
    if not common:
        raise SystemExit("no comparable old/new scenario settings")

    for key in common:
        print(f"\n{key[0]} flows={key[1]} bulk={key[2]}B")
        print(f"{'metric':30} {'old':>16} {'new':>16} {'delta':>12}")
        fields = list(FIELDS)
        if key[0] == "S7":
            fields.extend(S7_FIELDS)
        for field, label in fields:
            nested = "s7" if (field, label) in S7_FIELDS else None
            old = aggregate(old_groups[key], field, nested)
            new = aggregate(new_groups[key], field, nested)
            if old is None and new is None:
                continue
            print(f"{label:30} {display(old):>16} {display(new):>16} {delta(old, new):>12}")


if __name__ == "__main__":
    main()
