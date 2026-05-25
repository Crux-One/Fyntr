use tokio::time::Instant;

/// Tracks statistics for a flow, including bytes sent, packets sent, and average packet size using a low-pass filter.
///
/// The average packet size is maintained using an exponential moving average (EMA) with a smoothing factor.
/// This helps smooth out short-term fluctuations while responding to longer-term trends in packet sizes.
#[derive(Debug)]
pub(super) struct FlowStats {
    bytes_sent: u64,
    packets_sent: u64,
    start_time: Option<Instant>,
    avg_packet_size_lpf: Option<f64>,
}

impl FlowStats {
    pub(super) fn new() -> Self {
        Self {
            bytes_sent: 0,
            packets_sent: 0,
            start_time: None,
            avg_packet_size_lpf: None,
        }
    }

    /// Updates the flow statistics with a new packet size sample.
    ///
    /// Uses an exponential moving average to compute the average packet size, reducing noise from individual packet variations.
    /// The smoothing factor ALPHA = 0.2 provides a balance: 20% weight to the current sample and 80% to the previous average,
    /// allowing the average to adapt to changes while filtering out transient spikes.
    pub(super) fn update(&mut self, bytes: usize) {
        const ALPHA: f64 = 0.2;

        self.bytes_sent += bytes as u64;
        self.packets_sent += 1;
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }

        let sample = bytes as f64;
        self.avg_packet_size_lpf = Some(match self.avg_packet_size_lpf {
            Some(prev) => prev + ALPHA * (sample - prev),
            None => sample,
        });
    }

    pub(super) fn avg_packet_size(&self) -> Option<usize> {
        self.avg_packet_size_lpf.map(|avg| avg.round() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::FlowStats;

    #[test]
    fn test_flow_stats_ema() {
        let mut stats = FlowStats::new();

        // Initial state
        assert!(stats.avg_packet_size().is_none());

        // First packet: initializes average
        stats.update(1000);
        assert_eq!(stats.avg_packet_size(), Some(1000));

        // Second packet: 2000 bytes
        // ALPHA = 0.2
        // avg = 1000 + 0.2 * (2000 - 1000) = 1000 + 200 = 1200
        stats.update(2000);
        assert_eq!(stats.avg_packet_size(), Some(1200));

        // Third packet: 500 bytes
        // avg = 1200 + 0.2 * (500 - 1200) = 1200 + 0.2 * (-700) = 1200 - 140 = 1060
        stats.update(500);
        assert_eq!(stats.avg_packet_size(), Some(1060));

        // Convergence test: constant stream of 2000 bytes
        // It should approach 2000.
        for _ in 0..50 {
            stats.update(2000);
        }
        // After many updates, it should be very close to 2000.
        let avg = stats.avg_packet_size().unwrap();
        assert!(
            (1900..=2000).contains(&avg),
            "Average should converge to 2000, got {}",
            avg
        );
    }
}
