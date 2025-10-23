const UNITS: &[(&str, f64)] = &[
    ("GB", 1_000_000_000.0),
    ("MB", 1_000_000.0),
    ("KB", 1_000.0),
    ("B", 1.0),
];

/// Formats a byte count into a human-readable value/unit pair.
pub(crate) fn format_bytes(bytes: u64) -> (f64, &'static str) {
    pick_unit(bytes as f64)
}

/// Formats a throughput (bytes over seconds) into a human-readable rate.
pub(crate) fn format_rate(bytes: u64, seconds: f64) -> Option<(f64, &'static str)> {
    if seconds <= 0.0 {
        return None;
    }

    let (value, unit) = pick_unit((bytes as f64) / seconds);
    let rate_unit = match unit {
        "GB" => "GB/s",
        "MB" => "MB/s",
        "KB" => "KB/s",
        _ => "B/s",
    };

    Some((value, rate_unit))
}

fn pick_unit(value: f64) -> (f64, &'static str) {
    UNITS
        .iter()
        .find(|(_, factor)| value >= *factor)
        .map(|(unit, factor)| (value / *factor, *unit))
        .unwrap_or((value, "B"))
}

#[cfg(test)]
mod tests {
    use super::{format_bytes, format_rate};
    use quickcheck::quickcheck;

    #[test]
    fn format_bytes_scales_units() {
        assert_eq!(format_bytes(0), (0.0, "B"));
        assert_eq!(format_bytes(512), (512.0, "B"));
        assert_eq!(format_bytes(2_000), (2.0, "KB"));
        assert_eq!(format_bytes(3_000_000), (3.0, "MB"));
        assert_eq!(format_bytes(5_000_000_000), (5.0, "GB"));
    }

    #[test]
    fn format_rate_handles_zero_duration() {
        assert!(format_rate(1_000, 0.0).is_none());
        assert!(format_rate(1_000, -1.0).is_none());
    }

    #[test]
    fn format_rate_scales_units() {
        assert_eq!(format_rate(1_000, 1.0), Some((1.0, "KB/s")));
        assert_eq!(format_rate(2_000_000, 1.0), Some((2.0, "MB/s")));
        assert_eq!(format_rate(3_000_000_000, 1.0), Some((3.0, "GB/s")));
    }

    #[test]
    fn format_rate_reports_bytes_for_small_values() {
        assert_eq!(format_rate(500, 1.0), Some((500.0, "B/s")));
    }

    quickcheck! {
        fn format_bytes_unit_threshold(bytes: u64) -> bool {
            let (_, unit) = format_bytes(bytes);
            match unit {
                "B" => bytes < 1_000,
                "KB" => (1_000..1_000_000).contains(&bytes),
                "MB" => (1_000_000..1_000_000_000).contains(&bytes),
                "GB" => bytes >= 1_000_000_000,
                _ => false,
            }
        }
    }

    quickcheck! {
        fn format_rate_unit_threshold(bytes: u64, millis: u32) -> bool {
            let seconds = (millis as f64) / 1_000.0 + 0.001;
            let rate = (bytes as f64) / seconds;
            if let Some((_, unit)) = format_rate(bytes, seconds) {
                match unit {
                    "GB/s" => rate >= 1_000_000_000.0,
                    "MB/s" => rate >= 1_000_000.0 && rate < 1_000_000_000.0,
                    "KB/s" => rate >= 1_000.0 && rate < 1_000_000.0,
                    "B/s" => rate < 1_000.0,
                    _ => false,
                }
            } else {
                false
            }
        }
    }
}
