import unittest

import compare


class AggregateTests(unittest.TestCase):
    def test_success_rate_counts_every_trial(self):
        rows = [
            {"completed_without_protocol_error": True},
            {"completed_without_protocol_error": True},
            {"completed_without_protocol_error": False},
        ]

        self.assertEqual(
            compare.aggregate(rows, "completed_without_protocol_error"),
            2 / 3,
        )

    def test_protocol_failures_are_summed(self):
        rows = [{"protocol_failures": 0}, {"protocol_failures": 1000}]

        self.assertEqual(compare.aggregate(rows, "protocol_failures"), 1000)

    def test_measurements_still_use_the_median(self):
        rows = [
            {"throughput_bytes_per_second": 10},
            {"throughput_bytes_per_second": 30},
            {"throughput_bytes_per_second": 20},
        ]

        self.assertEqual(compare.aggregate(rows, "throughput_bytes_per_second"), 20)

    def test_nested_boolean_uses_a_rate(self):
        rows = [
            {"s7": {"recovered": True}},
            {"s7": {"recovered": False}},
        ]

        self.assertEqual(compare.aggregate(rows, "recovered", "s7"), 0.5)


if __name__ == "__main__":
    unittest.main()
