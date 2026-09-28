# Fyntr release benchmark

This is an opt-in black-box harness for comparing Fyntr release or commit binaries on macOS. Its initial use is to compare the v0.4.7-based baseline at `a0e8708` with the v0.4.8 candidate that bounds per-flow outbound buffering. It can also compare any two binaries supplied by path. It is an independent Cargo package, so the root `cargo test --workspace` does not run the load tests.

The load generator, Fyntr, and the verifying backend run as three separate processes on loopback. Every record carries a per-flow sequence number and payload length. The backend validates the complete deterministic payload before returning the matching ACK; reported transferred bytes are ACK-confirmed, not attempted writes.

## Build and run

Build both release binaries from explicit refs, preserve them at stable paths, and build the harness. The `a0e8708` baseline contains the v0.4.7 code plus the backend write-error fix that preceded the v0.4.8 buffering change; `b22c074` is the corresponding v0.4.8 candidate. Replace these commits with release tags or other commits when comparing a different pair:

```sh
cargo build --release --manifest-path bench/Cargo.toml

git switch --detach a0e8708
cargo build --release
cp target/release/fyntr /absolute/path/to/fyntr-v0.4.7-a0e8708
git switch -

git switch --detach b22c074
cargo build --release
cp target/release/fyntr /absolute/path/to/fyntr-v0.4.8-b22c074
git switch -
```

Run the same scenario and settings against both binaries:

```sh
bench/target/release/fyntr-bench \
  --scenario s1 \
  --fyntr-bin /absolute/path/to/fyntr-v0.4.7-a0e8708 \
  --label v0.4.7 \
  --output-dir bench/results/v0.4.7

bench/target/release/fyntr-bench \
  --scenario s1 \
  --fyntr-bin /absolute/path/to/fyntr-v0.4.8-b22c074 \
  --label v0.4.8 \
  --output-dir bench/results/v0.4.8
```

Supported scenarios are `s1`, `s2`, `s3`, `s5`, and `s7`. For S2, pass `--flows 100` or `--flows 1000`. S3 always uses 1000 persistent flows; S5 uses one elephant and 100 mice. Runs with 1000 flows start Fyntr with `--max-connections 1200`.

Defaults match the measurement plan:

- S1/S2/S3/S5: 3 trials, 3 s warmup, 15 s measurement, release binaries.
- S3/S5 mice: exactly 16 KiB and at most one outstanding record per flow. Latency begins immediately before sending and ends on the corresponding ACK.
- Bulk: a continuous 8-record pipeline. The default application payload is 8,176 bytes, producing an exact 8 KiB wire record after the 16-byte header (64 KiB outstanding per flow). Override payload size with `--bulk-payload-bytes`; both settings are retained in JSON.
- S7: one flow, backend reads paused for 10 s, then up to 10 s to drain every generated record.
- S7 `recovery_time_ms`: time from backend resume to the first ACK, not the time to drain every record. `recovered` reports whether every generated record was acknowledged before the deadline.
- Fyntr: `RUST_LOG=error`; CPU and RSS are sampled from only the Fyntr PID. RSS is sampled every 50 ms by default.

Each trial is a separate JSON file. A shortened example is:

```json
{
  "schema_version": 1,
  "label": "v0.4.7",
  "fyntr_binary": "/absolute/path/to/fyntr-v0.4.7-a0e8708",
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

Compare matching scenario settings. Measurement fields use medians, boolean
outcomes use the fraction of trials where the outcome is true, and protocol
failures are summed so a minority of failed trials cannot be hidden by the
median:

```sh
python3 bench/report/compare.py bench/results/v0.4.7 bench/results/v0.4.8
```

Example table:

```text
metric                                      old              new        delta
throughput B/s                      1560000.000      2010000.000      +28.85%
p99 latency ms                           18.200            8.700      -52.20%
Fyntr peak RSS bytes              440401920.000     78643200.000      -82.14%
```

## Charts

`plot.py` reads the same trial JSON without changing it and writes both SVG and
PNG. Matplotlib is only needed while rendering, so use `uv` to provide it
without adding a normal project dependency. Pair matching old/new result
directories for each scenario; the final 10-trial run can be rendered as
follows:

```sh
uv run --with matplotlib python bench/report/plot.py \
  --old-label v0.4.7 --new-label v0.4.8 \
  --pair bench/results/s1-rerun-old bench/results/s1-rerun-new \
  --pair bench/results/s2-100-rerun-old bench/results/s2-100-rerun-new \
  --pair bench/results/s2-1000-rerun-old bench/results/s2-1000-rerun-new \
  --pair bench/results/s3-rerun-old bench/results/s3-rerun-new \
  --pair bench/results/s7-rerun-old bench/results/s7-rerun-new \
  --output-dir bench/results/charts
```

This creates `normal-load.svg`/`.png` and `s7.svg`/`.png`. The normal-load
chart shows each trial as a point and its median as a short line. Each metric
is separately normalized to the matching old median (100), so it compares
change within a metric, not the units of throughput, CPU, RSS, and latency to
one another. Panels omit scenarios where that metric was not measured. The S7
chart uses absolute values for RSS, shows generated and acknowledged record
counts together, and labels recovery as successful trials over all trials in a
separate panel. Its write-duration view is supporting backpressure evidence;
`recovery_time_ms` is intentionally not a headline chart because it only
measures the first ACK after resume.

## Smoke tests

The unit tests are short and do not launch a benchmark:

```sh
cargo test --manifest-path bench/Cargo.toml
python3 -m unittest discover -s bench/report -p 'test_*.py'
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

S7 supplies black-box evidence about RSS, client backpressure, ordering, and recovery. It does not by itself prove the internal `queue buffered bytes + writer pending bytes <= 16 MiB` invariant; normal unit and integration tests cover that invariant. Likewise, report the rate observed in S1 rather than treating an estimated limit as an established result.
