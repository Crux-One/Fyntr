# Fyntr pre-P5 benchmark

This is an opt-in black-box harness for comparing pre-P5 and post-P5 Fyntr binaries on macOS. It is an independent Cargo package, so the root `cargo test --workspace` does not run the load tests.

The load generator, Fyntr, and the verifying backend run as three separate processes on loopback. Every record carries a per-flow sequence number and payload length. The backend validates the complete deterministic payload before returning the matching ACK; reported transferred bytes are ACK-confirmed, not attempted writes.

## Build and run

Build the old binary from `a0e8708`, preserve it at a stable path, and build the harness:

```sh
git switch --detach a0e8708
cargo build --release
cp target/release/fyntr /absolute/path/to/fyntr-old-a0e8708
git switch -
cargo build --release --manifest-path bench/Cargo.toml
```

Run one scenario against either binary path:

```sh
bench/target/release/fyntr-bench \
  --scenario s1 \
  --fyntr-bin /absolute/path/to/fyntr-old-a0e8708 \
  --label old \
  --output-dir bench/results/old
```

Supported scenarios are `s1`, `s2`, `s3`, `s5`, and `s7`. For S2, pass `--flows 100` or `--flows 1000`. S3 always uses 1000 persistent flows; S5 uses one elephant and 100 mice. Runs with 1000 flows start Fyntr with `--max-connections 1200`.

Defaults match the measurement plan:

- S1/S2/S3/S5: 3 trials, 3 s warmup, 15 s measurement, release binaries.
- S3/S5 mice: exactly 16 KiB and at most one outstanding record per flow. Latency begins immediately before sending and ends on the corresponding ACK.
- Bulk: a continuous 8-record pipeline. The default application payload is 8,176 bytes, producing an exact 8 KiB wire record after the 16-byte header (64 KiB outstanding per flow). Override payload size with `--bulk-payload-bytes`; both settings are retained in JSON.
- S7: one flow, backend reads paused for 10 s, then up to 10 s to drain every generated record.
- Fyntr: `RUST_LOG=error`; CPU and RSS are sampled from only the Fyntr PID. RSS is sampled every 50 ms by default.

Each trial is a separate JSON file. A shortened example is:

```json
{
  "schema_version": 1,
  "label": "old",
  "fyntr_binary": "/absolute/path/to/fyntr-old-a0e8708",
  "scenario": "S3",
  "trial": 1,
  "completed_without_protocol_error": true,
  "protocol_failures": 0,
  "backend_confirmed_payload_bytes": 123456789,
  "throughput_bytes_per_second": 8230452.6,
  "transaction_rate_per_second": 502.3,
  "p50_latency_ms": 12.4,
  "p99_latency_ms": 31.8,
  "fyntr_cpu_time_ns": 456789000,
  "fyntr_cpu_ns_per_transferred_byte": 3.70,
  "fyntr_peak_rss_bytes": 73400320
}
```

Compare medians for matching scenario settings:

```sh
python3 bench/report/compare.py bench/results/old bench/results/new
```

Example table:

```text
metric                                      old              new        delta
throughput B/s                      1560000.000      2010000.000      +28.85%
p99 latency ms                           18.200            8.700      -52.20%
Fyntr peak RSS bytes              440401920.000     78643200.000      -82.14%
```

## Smoke tests

The unit tests are short and do not launch a benchmark:

```sh
cargo test --manifest-path bench/Cargo.toml
```

Run short end-to-end checks with an already-built Fyntr binary:

```sh
bench/target/release/fyntr-bench \
  --scenario s1 --fyntr-bin /absolute/path/to/fyntr \
  --trials 1 --warmup-secs 0 --measurement-secs 1 \
  --output-dir /tmp/fyntr-bench-smoke-s1

bench/target/release/fyntr-bench \
  --scenario s7 --fyntr-bin /absolute/path/to/fyntr \
  --trials 1 --pause-secs 1 --recovery-timeout-secs 2 \
  --output-dir /tmp/fyntr-bench-smoke-s7
```

S7 supplies black-box evidence about RSS, client backpressure, ordering, and recovery. It does not prove the internal `queue buffered bytes + writer pending bytes <= 16 MiB` invariant. Likewise, about 13.1 Mb/s is a hypothesis to observe in S1, not an established result.
