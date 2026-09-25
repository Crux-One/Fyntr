import sys
import unittest
from unittest import mock

import plot


def row(scenario="S1", flows=1, value=100, *, recovered=None, trial=1):
    result = {
        "scenario": scenario,
        "schema_version": 1,
        "trial": trial,
        "settings": {
            "flows": flows,
            "bulk_payload_bytes": 8176,
            "bulk_pipeline_records": 8,
            "mouse_payload_bytes": 16384,
            "warmup_secs": 3,
            "measurement_secs": 15,
            "pause_secs": 10,
            "recovery_timeout_secs": 10,
            "max_connections": 17,
            "rss_sample_interval_ms": 50,
            "backpressure_threshold_ms": 100,
        },
        "throughput_bytes_per_second": value,
    }
    if recovered is not None:
        result["s7"] = {"recovered": recovered}
    return result


class PlotDataTests(unittest.TestCase):
    def test_normalization_uses_matching_old_median(self):
        old = [row(value=10, trial=1), row(value=30, trial=2), row(value=20, trial=3)]
        new = [row(value=40, trial=1), row(value=10, trial=2)]

        normalized = plot.normalized_pair(old, new, "throughput_bytes_per_second")

        self.assertEqual(normalized, ([50.0, 150.0, 100.0], [200.0, 50.0]))

    def test_zero_old_median_has_actionable_error(self):
        with self.assertRaisesRegex(ValueError, "S1.*throughput_bytes_per_second.*zero"):
            plot.normalized_pair([row(value=0)], [row(value=1)], "throughput_bytes_per_second", "S1")

    def test_recovery_count_counts_successes_not_median(self):
        rows = [row("S7", recovered=True), row("S7", recovered=False), row("S7", recovered=True)]

        self.assertEqual(plot.recovery_count(rows), 2)

    def test_groups_with_metric_omits_unmeasured_scenarios(self):
        measured = row("S3", 1000, 10)
        measured["p99_latency_ms"] = 12
        missing = row("S1", 1, 10)
        groups = [
            (plot.compare.group_key(measured), ([measured], [measured])),
            (plot.compare.group_key(missing), ([missing], [missing])),
        ]

        available = plot.groups_with_metric(groups, "p99_latency_ms")

        self.assertEqual([key[0] for key, _, _ in available], ["S3"])

    def test_group_validation_rejects_mismatched_settings(self):
        old = {plot.compare.group_key(row("S1", 1)): [row("S1", 1)]}
        new = {plot.compare.group_key(row("S1", 2)): [row("S1", 2)]}

        with self.assertRaisesRegex(ValueError, "only in old.*only in new"):
            plot.validate_group_sets(old, new, "old-results", "new-results")

    def test_trial_validation_rejects_same_count_with_different_ids(self):
        key = plot.compare.group_key(row())
        old = [row(trial=1), row(trial=2)]
        new = [row(trial=1), row(trial=3)]

        with self.assertRaisesRegex(ValueError, r"old trial IDs \[1, 2\].*new trial IDs \[1, 3\]"):
            plot.validate_trial_ids(old, new, key, "old-results", "new-results")

    def test_trial_validation_rejects_duplicates(self):
        key = plot.compare.group_key(row())
        old = [row(trial=1), row(trial=1)]
        new = [row(trial=1), row(trial=2)]

        with self.assertRaisesRegex(ValueError, r"old trial IDs are not unique: \[1\]"):
            plot.validate_trial_ids(old, new, key)

    def test_trial_validation_rejects_missing_ids(self):
        key = plot.compare.group_key(row())
        old = row()
        old.pop("trial")

        with self.assertRaisesRegex(ValueError, "old trial IDs are missing"):
            plot.validate_trial_ids([old], [row()], key)

    def test_main_turns_data_validation_into_system_exit(self):
        with (
            mock.patch.object(plot, "paired_groups", side_effect=ValueError("bad trial data")),
            mock.patch.object(sys, "argv", ["plot.py", "--pair", "old", "new", "--output-dir", "out"]),
            self.assertRaisesRegex(SystemExit, "bad trial data"),
        ):
            plot.main()


if __name__ == "__main__":
    unittest.main()
