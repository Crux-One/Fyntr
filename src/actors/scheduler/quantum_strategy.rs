#[derive(Clone, Copy, Debug)]
struct QuantumConfig {
    min_quantum: usize,
    max_quantum: usize,
    target_burst_packets: usize,
    small_packet_threshold: usize,
}

/// Tuning defaults chosen to balance small-packet latency against large-packet throughput.
///
/// * `min_quantum (1500 bytes)`: Standard Ethernet MTU, ensuring even latency-sensitive flows get
///   at least one full-sized packet per scheduling turn.
/// * `max_quantum (16 KiB)`: Prevents any single flow from monopolizing airtime while still
///   amortizing scheduler overhead for bulk traffic.
/// * `target_burst_packets (10)`: Empirically smooths throughput without making interactive
///   traffic wait excessively between turns.
/// * `small_packet_threshold (200 bytes)`: Heuristic cutoff for “chatty” or control-plane flows
///   (e.g., TCP ACKs, SSH, VoIP) that benefit from shorter quanta to reduce jitter.
const DEFAULT_QUANTUM_CONFIG: QuantumConfig = QuantumConfig {
    min_quantum: 1_500,
    max_quantum: 16 * 1024,
    target_burst_packets: 10,
    small_packet_threshold: 200,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct DrrQuantumStrategy {
    config: QuantumConfig,
}

/// Immutable default strategy reused by every flow to avoid repeatedly constructing the same tuning profile.
pub(super) const DEFAULT_DRR_QUANTUM_STRATEGY: DrrQuantumStrategy = DrrQuantumStrategy {
    config: DEFAULT_QUANTUM_CONFIG,
};

impl DrrQuantumStrategy {
    /// Calculates the optimal Deficit Round Robin (DRR) quantum based on historical packet
    /// statistics.
    ///
    /// Strategy overview:
    /// - If we have no packet history, fall back to the caller-provided default to avoid guessing.
    /// - Small packets (below `small_packet_threshold`) get the minimum quantum to keep the
    ///   scheduler cycling quickly and minimize jitter for interactive traffic.
    /// - Otherwise we scale linearly to target `target_burst_packets` per turn and clamp the result
    ///   between `min_quantum` and `max_quantum` so bulk flows gain efficiency without starving
    ///   others.
    pub(super) fn recommended_quantum(
        &self,
        avg_packet_size: Option<usize>,
        default_quantum: usize,
    ) -> usize {
        match avg_packet_size {
            Some(avg) if avg < self.config.small_packet_threshold => self.config.min_quantum,
            Some(avg) => {
                let target = avg.saturating_mul(self.config.target_burst_packets);
                target.clamp(self.config.min_quantum, self.config.max_quantum)
            }
            None => default_quantum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_DRR_QUANTUM_STRATEGY;

    #[test]
    fn test_drr_quantum_strategy_cases() {
        let strategy = DEFAULT_DRR_QUANTUM_STRATEGY;
        let config = strategy.config;
        let default_quantum = 4096;

        // Validate DRR quantum selection across key regimes: no stats, small packets,
        // burst-scaled packets, and clamped large packets.
        let cases = vec![
            (None, default_quantum, "no stats uses default"),
            (
                Some(config.small_packet_threshold - 1),
                config.min_quantum,
                "small packets get minimum quantum",
            ),
            (
                Some(config.small_packet_threshold + 50),
                (config.small_packet_threshold + 50)
                    .saturating_mul(config.target_burst_packets)
                    .clamp(config.min_quantum, config.max_quantum),
                "moderate packets scale by burst target",
            ),
            (
                Some(config.max_quantum + 1),
                config.max_quantum,
                "large packets clamp at maximum",
            ),
        ];

        for (avg, expected, label) in cases {
            assert_eq!(
                strategy.recommended_quantum(avg, default_quantum),
                expected,
                "{}",
                label
            );
        }
    }
}
