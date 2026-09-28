use std::{
    fs,
    io::{BufRead, BufReader, Write as _},
    net::{Ipv4Addr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, sleep, sleep_until, timeout, timeout_at},
};

const MAGIC: [u8; 4] = *b"FYB1";
const RECORD_HEADER_BYTES: usize = 16;
const DEFAULT_BULK_PAYLOAD_BYTES: usize = 8 * 1024 - RECORD_HEADER_BYTES;
const MOUSE_PAYLOAD_BYTES: usize = 16 * 1024;
const BACKPRESSURE_THRESHOLD: Duration = Duration::from_millis(100);
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const BULK_PIPELINE_RECORDS: u64 = 8;

#[derive(Parser, Debug)]
#[command(about = "Manual black-box benchmark for Fyntr (not part of the root workspace)")]
struct Cli {
    #[command(subcommand)]
    command: Option<InternalCommand>,
    #[arg(long, value_enum)]
    scenario: Option<Scenario>,
    #[arg(long)]
    fyntr_bin: Option<PathBuf>,
    #[arg(long, default_value = "old")]
    label: String,
    #[arg(long, default_value_t = 3)]
    trials: u32,
    #[arg(long, default_value = "results")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 15)]
    measurement_secs: u64,
    #[arg(long, default_value_t = 10)]
    pause_secs: u64,
    #[arg(long, default_value_t = 10)]
    recovery_timeout_secs: u64,
    #[arg(long, default_value_t = 50)]
    rss_sample_ms: u64,
    #[arg(long, default_value_t = DEFAULT_BULK_PAYLOAD_BYTES)]
    bulk_payload_bytes: usize,
    #[arg(long, default_value_t = 100)]
    flows: usize,
}

#[derive(Subcommand, Debug)]
enum InternalCommand {
    #[command(hide = true)]
    Backend(BackendArgs),
}

#[derive(clap::Args, Debug)]
struct BackendArgs {
    #[arg(long)]
    port: u16,
    #[arg(long)]
    pause_secs: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize, PartialEq, Eq)]
enum Scenario {
    S1,
    S2,
    S3,
    S5,
    S7,
}

#[derive(Serialize)]
struct TrialResult {
    schema_version: u32,
    label: String,
    fyntr_binary: String,
    scenario: Scenario,
    trial: u32,
    settings: Settings,
    observation_seconds: f64,
    completed_without_protocol_error: bool,
    protocol_failures: u64,
    backend_confirmed_payload_bytes: u64,
    throughput_bytes_per_second: f64,
    transaction_count: Option<u64>,
    transaction_rate_per_second: Option<f64>,
    p50_latency_ms: Option<f64>,
    p99_latency_ms: Option<f64>,
    elephant_confirmed_payload_bytes: Option<u64>,
    elephant_throughput_bytes_per_second: Option<f64>,
    mouse_transaction_count: Option<u64>,
    mouse_transaction_rate_per_second: Option<f64>,
    fyntr_cpu_time_ns: u64,
    fyntr_cpu_ns_per_transferred_byte: Option<f64>,
    fyntr_peak_rss_bytes: u64,
    s7: Option<S7Result>,
}

#[derive(Serialize)]
struct Settings {
    warmup_secs: u64,
    measurement_secs: u64,
    pause_secs: u64,
    recovery_timeout_secs: u64,
    flows: usize,
    bulk_payload_bytes: usize,
    bulk_pipeline_records: u64,
    mouse_payload_bytes: usize,
    max_connections: usize,
    rss_sample_interval_ms: u64,
    backpressure_threshold_ms: u64,
}

#[derive(Serialize)]
struct S7Result {
    recovered: bool,
    recovery_time_ms: Option<f64>,
    generated_records: u64,
    acknowledged_records: u64,
    protocol_failures: u64,
    client_backpressure_observed: bool,
    maximum_record_write_ms: f64,
    note: &'static str,
}

#[derive(Default)]
struct Stats {
    confirmed_bytes: AtomicU64,
    transactions: AtomicU64,
    elephant_bytes: AtomicU64,
    mouse_transactions: AtomicU64,
    generated: AtomicU64,
    acknowledged: AtomicU64,
    failures: AtomicU64,
    maximum_write_ns: AtomicU64,
    latencies_ns: Mutex<Vec<u64>>,
    first_recovery_ns: AtomicU64,
}

struct Usage {
    cpu_ns: u64,
    rss_bytes: u64,
}

struct ChildGuard {
    name: &'static str,
    child: Child,
    stdout: Option<BufReader<std::process::ChildStdout>>,
}

impl ChildGuard {
    fn new(name: &'static str, mut child: Child) -> Self {
        let stdout = child.stdout.take().map(BufReader::new);
        Self {
            name,
            child,
            stdout,
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn stdin_mut(&mut self) -> Result<&mut std::process::ChildStdin> {
        self.child
            .stdin
            .as_mut()
            .with_context(|| format!("{} stdin is unavailable", self.name))
    }

    fn read_stdout_line(&mut self) -> Result<String> {
        let stdout = self
            .stdout
            .as_mut()
            .with_context(|| format!("{} stdout is unavailable", self.name))?;
        let mut line = String::new();
        stdout.read_line(&mut line)?;
        Ok(line)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(InternalCommand::Backend(args)) = cli.command {
        return run_backend(args).await;
    }

    let scenario = cli.scenario.context("--scenario is required")?;
    validate(&cli, scenario)?;
    raise_file_limit()?;

    let fyntr_binary = cli
        .fyntr_bin
        .as_ref()
        .context("--fyntr-bin is required")?
        .canonicalize()
        .context("canonicalize --fyntr-bin")?;
    fs::create_dir_all(&cli.output_dir)?;

    for trial in 1..=cli.trials {
        let result = run_trial(&cli, scenario, &fyntr_binary, trial).await?;
        let scenario_name = if scenario == Scenario::S2 {
            format!("S2-{}", cli.flows)
        } else {
            format!("{scenario:?}")
        };
        let name = format!("{}-{scenario_name}-trial{trial}.json", cli.label);
        let path = cli.output_dir.join(name);
        fs::write(&path, serde_json::to_vec_pretty(&result)?)?;
        println!("wrote {}", path.display());
    }
    Ok(())
}

fn validate(cli: &Cli, scenario: Scenario) -> Result<()> {
    if cli.trials == 0
        || cli.rss_sample_ms == 0
        || cli.bulk_payload_bytes == 0
        || cli.bulk_payload_bytes > MAX_RECORD_BYTES
    {
        bail!("trials, RSS interval, and bulk payload must be non-zero; payload must be <= 1 MiB")
    }
    if scenario != Scenario::S7 && cli.measurement_secs == 0 {
        bail!("--measurement-secs must be non-zero")
    }
    if scenario == Scenario::S2 && !matches!(cli.flows, 100 | 1000) {
        bail!("S2 --flows must be 100 or 1000")
    }
    Ok(())
}

async fn run_trial(
    cli: &Cli,
    scenario: Scenario,
    fyntr_binary: &Path,
    trial: u32,
) -> Result<TrialResult> {
    let backend_port = free_port()?;
    let proxy_port = free_port()?;
    let flows = scenario_flow_count(cli, scenario);
    let max_connections = if flows >= 1000 { 1200 } else { flows + 16 };

    let executable = std::env::current_exe()?;
    let backend_child = Command::new(executable)
        .arg("backend")
        .arg("--port")
        .arg(backend_port.to_string())
        .arg("--pause-secs")
        .arg(
            if scenario == Scenario::S7 {
                cli.pause_secs
            } else {
                0
            }
            .to_string(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn benchmark backend")?;
    let mut backend = ChildGuard::new("backend", backend_child);
    wait_backend_ready(&mut backend)?;

    let fyntr_child = Command::new(fyntr_binary)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &proxy_port.to_string(),
            "--max-connections",
            &max_connections.to_string(),
            "--idle-timeout",
            "0",
            "--allow-port",
            &backend_port.to_string(),
            "--allow-cidr",
            "127.0.0.0/8",
        ])
        .env("RUST_LOG", "error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn Fyntr")?;
    let fyntr = ChildGuard::new("Fyntr", fyntr_child);
    wait_for_listener(proxy_port).await?;

    let streams = connect_all_flows(proxy_port, backend_port, flows).await?;
    let stats = Arc::new(Stats::default());

    let measured = if scenario == Scenario::S7 {
        run_s7(cli, streams, &mut backend, fyntr.id() as i32, stats.clone()).await?
    } else {
        run_timed(
            cli,
            scenario,
            streams,
            &mut backend,
            fyntr.id() as i32,
            stats.clone(),
        )
        .await?
    };

    let confirmed_bytes = stats.confirmed_bytes.load(Ordering::Relaxed);
    let transactions = stats.transactions.load(Ordering::Relaxed);
    let elephant_bytes = stats.elephant_bytes.load(Ordering::Relaxed);
    let mouse_transactions = stats.mouse_transactions.load(Ordering::Relaxed);
    let mut latencies = stats.latencies_ns.lock().expect("latency mutex").clone();
    latencies.sort_unstable();

    Ok(TrialResult {
        schema_version: 1,
        label: cli.label.clone(),
        fyntr_binary: fyntr_binary.display().to_string(),
        scenario,
        trial,
        settings: Settings {
            warmup_secs: cli.warmup_secs,
            measurement_secs: cli.measurement_secs,
            pause_secs: cli.pause_secs,
            recovery_timeout_secs: cli.recovery_timeout_secs,
            flows,
            bulk_payload_bytes: cli.bulk_payload_bytes,
            bulk_pipeline_records: BULK_PIPELINE_RECORDS,
            mouse_payload_bytes: MOUSE_PAYLOAD_BYTES,
            max_connections,
            rss_sample_interval_ms: cli.rss_sample_ms,
            backpressure_threshold_ms: BACKPRESSURE_THRESHOLD.as_millis() as u64,
        },
        observation_seconds: measured.elapsed.as_secs_f64(),
        completed_without_protocol_error: stats.failures.load(Ordering::Relaxed) == 0,
        protocol_failures: stats.failures.load(Ordering::Relaxed),
        backend_confirmed_payload_bytes: confirmed_bytes,
        throughput_bytes_per_second: rate(confirmed_bytes, measured.elapsed),
        transaction_count: matches!(scenario, Scenario::S3).then_some(transactions),
        transaction_rate_per_second: matches!(scenario, Scenario::S3)
            .then_some(rate(transactions, measured.elapsed)),
        p50_latency_ms: percentile_ms(&latencies, 50),
        p99_latency_ms: percentile_ms(&latencies, 99),
        elephant_confirmed_payload_bytes: (scenario == Scenario::S5).then_some(elephant_bytes),
        elephant_throughput_bytes_per_second: (scenario == Scenario::S5)
            .then_some(rate(elephant_bytes, measured.elapsed)),
        mouse_transaction_count: (scenario == Scenario::S5).then_some(mouse_transactions),
        mouse_transaction_rate_per_second: (scenario == Scenario::S5)
            .then_some(rate(mouse_transactions, measured.elapsed)),
        fyntr_cpu_time_ns: measured.cpu_ns,
        fyntr_cpu_ns_per_transferred_byte: (confirmed_bytes > 0)
            .then_some(measured.cpu_ns as f64 / confirmed_bytes as f64),
        fyntr_peak_rss_bytes: measured.peak_rss,
        s7: (scenario == Scenario::S7).then(|| {
            let generated = stats.generated.load(Ordering::Relaxed);
            let acknowledged = stats.acknowledged.load(Ordering::Relaxed);
            let failures = stats.failures.load(Ordering::Relaxed);
            let recovery_ns = stats.first_recovery_ns.load(Ordering::Relaxed);
            S7Result {
                recovered: generated > 0 && generated == acknowledged && failures == 0,
                recovery_time_ms: (recovery_ns > 0).then_some(recovery_ns as f64 / 1_000_000.0),
                generated_records: generated,
                acknowledged_records: acknowledged,
                protocol_failures: failures,
                client_backpressure_observed: stats.maximum_write_ns.load(Ordering::Relaxed)
                    >= BACKPRESSURE_THRESHOLD.as_nanos() as u64,
                maximum_record_write_ms: stats.maximum_write_ns.load(Ordering::Relaxed) as f64
                    / 1_000_000.0,
                note: "black-box evidence only; this does not prove the 16 MiB internal invariant",
            }
        }),
    })
}

fn scenario_flow_count(cli: &Cli, scenario: Scenario) -> usize {
    match scenario {
        Scenario::S1 | Scenario::S7 => 1,
        Scenario::S2 => cli.flows,
        Scenario::S3 => 1000,
        Scenario::S5 => 101,
    }
}

struct Measurement {
    elapsed: Duration,
    cpu_ns: u64,
    peak_rss: u64,
}

async fn run_timed(
    cli: &Cli,
    scenario: Scenario,
    streams: Vec<TcpStream>,
    backend: &mut ChildGuard,
    fyntr_pid: i32,
    stats: Arc<Stats>,
) -> Result<Measurement> {
    let phase = Arc::new(AtomicU8::new(0));
    let mut workers = JoinSet::new();
    for (flow, stream) in streams.into_iter().enumerate() {
        let stats = stats.clone();
        let phase = phase.clone();
        let bulk_payload = cli.bulk_payload_bytes;
        workers.spawn(async move {
            let result = match scenario {
                Scenario::S1 | Scenario::S2 => {
                    run_bulk_flow(stream, bulk_payload, false, phase, stats.clone()).await
                }
                Scenario::S3 => run_mouse_flow(stream, phase, stats.clone()).await,
                Scenario::S5 if flow == 0 => {
                    run_bulk_flow(stream, bulk_payload, true, phase, stats.clone()).await
                }
                Scenario::S5 => run_mouse_flow(stream, phase, stats.clone()).await,
                Scenario::S7 => unreachable!(),
            };
            if result.is_err() {
                stats.failures.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    let _ = start_backend(backend)?;
    sleep(Duration::from_secs(cli.warmup_secs)).await;

    let start_usage = usage(fyntr_pid)?;
    let (stop_sampling, sampler) =
        start_rss_sampler(fyntr_pid, cli.rss_sample_ms, start_usage.rss_bytes);
    let started = Instant::now();
    phase.store(1, Ordering::Release);
    sleep(Duration::from_secs(cli.measurement_secs)).await;
    phase.store(2, Ordering::Release);
    let elapsed = started.elapsed();
    let end_usage = usage(fyntr_pid)?;
    stop_sampling.store(true, Ordering::Release);
    let peak_rss = sampler
        .await
        .unwrap_or(start_usage.rss_bytes)
        .max(end_usage.rss_bytes);

    let drained = timeout(Duration::from_secs(5), async {
        while let Some(result) = workers.join_next().await {
            result.context("benchmark worker task")?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    match drained {
        Ok(result) => result?,
        Err(_) => {
            workers.abort_all();
            while workers.join_next().await.is_some() {}
            bail!("benchmark workers did not drain within 5 seconds")
        }
    }

    Ok(Measurement {
        elapsed,
        cpu_ns: end_usage.cpu_ns.saturating_sub(start_usage.cpu_ns),
        peak_rss,
    })
}

async fn run_s7(
    cli: &Cli,
    mut streams: Vec<TcpStream>,
    backend: &mut ChildGuard,
    fyntr_pid: i32,
    stats: Arc<Stats>,
) -> Result<Measurement> {
    let stream = streams.pop().context("S7 flow")?;
    let (read_half, write_half) = stream.into_split();
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(read_ack_stream(read_half, ack_tx));

    let start_usage = usage(fyntr_pid)?;
    let (stop_sampling, sampler) =
        start_rss_sampler(fyntr_pid, cli.rss_sample_ms, start_usage.rss_bytes);
    let started = Instant::now();
    let resume_at = start_backend(backend)?;

    let writer_stats = stats.clone();
    let payload_bytes = cli.bulk_payload_bytes;
    let mut writer = tokio::spawn(async move {
        write_s7_records(write_half, payload_bytes, resume_at, writer_stats).await
    });

    let deadline = resume_at + Duration::from_secs(cli.recovery_timeout_secs);
    let (generated, _write_guard) = match timeout_at(deadline.into(), &mut writer).await {
        Ok(Ok(Ok((generated, writer)))) => (generated, Some(writer)),
        Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
            writer.abort();
            stats.failures.fetch_add(1, Ordering::Relaxed);
            (stats.generated.load(Ordering::Relaxed), None)
        }
    };
    let mut expected = 1_u64;
    let mut acknowledged = 0_u64;
    while acknowledged < generated {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        match timeout(deadline - now, ack_rx.recv()).await {
            Ok(Some(Ok(sequence))) if sequence == expected => {
                if stats.first_recovery_ns.load(Ordering::Relaxed) == 0 {
                    let recovery = Instant::now().saturating_duration_since(resume_at);
                    let value = recovery.as_nanos().max(1) as u64;
                    let _ = stats.first_recovery_ns.compare_exchange(
                        0,
                        value,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                }
                expected += 1;
                acknowledged += 1;
                stats
                    .confirmed_bytes
                    .fetch_add(payload_bytes as u64, Ordering::Relaxed);
                stats.acknowledged.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Some(Ok(_))) | Ok(Some(Err(_))) | Ok(None) => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                break;
            }
            Err(_) => break,
        }
    }
    reader.abort();
    let elapsed = started.elapsed();
    let end_usage = usage(fyntr_pid)?;
    stop_sampling.store(true, Ordering::Release);
    let peak_rss = sampler
        .await
        .unwrap_or(start_usage.rss_bytes)
        .max(end_usage.rss_bytes);

    Ok(Measurement {
        elapsed,
        cpu_ns: end_usage.cpu_ns.saturating_sub(start_usage.cpu_ns),
        peak_rss,
    })
}

async fn run_bulk_flow(
    stream: TcpStream,
    payload_bytes: usize,
    elephant: bool,
    phase: Arc<AtomicU8>,
    stats: Arc<Stats>,
) -> Result<()> {
    let payload = Arc::new(payload(payload_bytes));
    let (mut reader, mut writer) = stream.into_split();
    let sent = Arc::new(AtomicU64::new(0));
    let acknowledged = Arc::new(AtomicU64::new(0));
    let writer_sent = sent.clone();
    let writer_acknowledged = acknowledged.clone();
    let writer_phase = phase.clone();
    let writer_payload = payload.clone();
    let mut writer_task = tokio::spawn(async move {
        let mut sequence = 0_u64;
        'sending: while writer_phase.load(Ordering::Acquire) < 2 {
            while sequence.saturating_sub(writer_acknowledged.load(Ordering::Acquire))
                >= BULK_PIPELINE_RECORDS
            {
                if writer_phase.load(Ordering::Acquire) >= 2 {
                    break 'sending;
                }
                sleep(Duration::from_micros(50)).await;
            }
            sequence += 1;
            write_record(&mut writer, sequence, &writer_payload).await?;
            writer_sent.store(sequence, Ordering::Release);
        }
        writer.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    });

    let mut expected = 1_u64;
    let mut writer_finished = false;
    loop {
        if phase.load(Ordering::Acquire) >= 2 && expected > sent.load(Ordering::Acquire) {
            if !writer_finished {
                (&mut writer_task).await??;
                writer_finished = true;
                continue;
            }
            break;
        }

        let sequence = match read_ack(&mut reader).await {
            Ok(sequence) => sequence,
            Err(error)
                if phase.load(Ordering::Acquire) >= 2
                    && expected > sent.load(Ordering::Acquire) =>
            {
                if !writer_finished {
                    (&mut writer_task).await??;
                }
                if expected > sent.load(Ordering::Acquire) {
                    break;
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if sequence != expected {
            bail!("ACK order mismatch: expected {expected}, received {sequence}")
        }
        acknowledged.store(sequence, Ordering::Release);
        expected += 1;
        if phase.load(Ordering::Acquire) == 1 {
            stats
                .confirmed_bytes
                .fetch_add(payload_bytes as u64, Ordering::Relaxed);
            if elephant {
                stats
                    .elephant_bytes
                    .fetch_add(payload_bytes as u64, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn run_mouse_flow(
    mut stream: TcpStream,
    phase: Arc<AtomicU8>,
    stats: Arc<Stats>,
) -> Result<()> {
    let payload = payload(MOUSE_PAYLOAD_BYTES);
    let mut sequence = 0_u64;
    while phase.load(Ordering::Acquire) < 2 {
        sequence += 1;
        let sample_phase = phase.load(Ordering::Acquire);
        let started = Instant::now();
        write_record(&mut stream, sequence, &payload).await?;
        let ack = read_ack(&mut stream).await?;
        if ack != sequence {
            bail!("ACK order mismatch: expected {sequence}, received {ack}")
        }
        if sample_phase == 1 && phase.load(Ordering::Acquire) == 1 {
            let latency = started.elapsed().as_nanos() as u64;
            stats
                .confirmed_bytes
                .fetch_add(MOUSE_PAYLOAD_BYTES as u64, Ordering::Relaxed);
            stats.transactions.fetch_add(1, Ordering::Relaxed);
            stats.mouse_transactions.fetch_add(1, Ordering::Relaxed);
            stats
                .latencies_ns
                .lock()
                .expect("latency mutex")
                .push(latency);
        }
    }
    Ok(())
}

async fn write_s7_records(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    payload_bytes: usize,
    stop_at: Instant,
    stats: Arc<Stats>,
) -> Result<(u64, tokio::net::tcp::OwnedWriteHalf)> {
    let payload = payload(payload_bytes);
    let mut sequence = 0_u64;
    while Instant::now() < stop_at {
        sequence += 1;
        stats.generated.store(sequence, Ordering::Relaxed);
        let started = Instant::now();
        write_record(&mut writer, sequence, &payload).await?;
        stats
            .maximum_write_ns
            .fetch_max(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    Ok((sequence, writer))
}

async fn read_ack_stream(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::UnboundedSender<Result<u64, String>>,
) {
    loop {
        match read_ack(&mut reader).await {
            Ok(sequence) => {
                if tx.send(Ok(sequence)).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = tx.send(Err(error.to_string()));
                return;
            }
        }
    }
}

async fn connect_all_flows(
    proxy_port: u16,
    backend_port: u16,
    count: usize,
) -> Result<Vec<TcpStream>> {
    let mut connections = JoinSet::new();
    let mut streams = Vec::with_capacity(count);
    for flow in 0..count {
        connections.spawn(async move {
            connect_flow(proxy_port, backend_port)
                .await
                .with_context(|| format!("establish flow {flow}"))
                .map(|stream| (flow, stream))
        });
        if connections.len() >= 128 {
            let result = connections
                .join_next()
                .await
                .context("missing flow connection task")?;
            streams.push(result.context("flow connection task")??);
        }
    }
    while let Some(result) = connections.join_next().await {
        streams.push(result.context("flow connection task")??);
    }
    streams.sort_unstable_by_key(|(flow, _)| *flow);
    Ok(streams.into_iter().map(|(_, stream)| stream).collect())
}

async fn connect_flow(proxy_port: u16, backend_port: u16) -> Result<TcpStream> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, proxy_port)).await?;
    stream.set_nodelay(true)?;
    let request = format!(
        "CONNECT 127.0.0.1:{backend_port} HTTP/1.1\r\nHost: 127.0.0.1:{backend_port}\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::with_capacity(128);
    let mut byte = [0_u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await?;
        response.push(byte[0]);
        if response.len() > 8192 {
            bail!("oversized CONNECT response")
        }
    }
    let response = std::str::from_utf8(&response)?;
    if !response.starts_with("HTTP/1.1 200 ") {
        bail!(
            "CONNECT failed: {}",
            response.lines().next().unwrap_or(response)
        )
    }
    Ok(stream)
}

async fn write_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    sequence: u64,
    payload: &[u8],
) -> Result<()> {
    let mut record = Vec::with_capacity(RECORD_HEADER_BYTES + payload.len());
    record.extend_from_slice(&MAGIC);
    record.extend_from_slice(&sequence.to_be_bytes());
    record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    record.extend_from_slice(payload);
    writer.write_all(&record).await?;
    Ok(())
}

async fn read_ack<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64> {
    let mut ack = [0_u8; 8];
    reader.read_exact(&mut ack).await?;
    Ok(u64::from_be_bytes(ack))
}

fn payload(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

async fn run_backend(args: BackendArgs) -> Result<()> {
    raise_file_limit()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, args.port)).await?;
    println!("READY");
    std::io::stdout().flush()?;

    let (resume_tx, resume_rx) = watch::channel(None::<tokio::time::Instant>);
    let pause = Duration::from_secs(args.pause_secs);
    tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        let result = std::io::stdin().lock().read_line(&mut line);
        match result {
            Ok(_) if line.trim() == "GO" => {
                let resume_at = tokio::time::Instant::now() + pause;
                let resume_unix_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time before Unix epoch")
                    .as_nanos()
                    + pause.as_nanos();
                let _ = resume_tx.send(Some(resume_at));
                println!("RESUME_UNIX_NS {resume_unix_ns}");
                let _ = std::io::stdout().flush();
            }
            _ => std::process::exit(2),
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let receiver = resume_rx.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_backend_flow(stream, receiver).await {
                if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::UnexpectedEof
                    )
                }) {
                    return;
                }
                eprintln!("backend flow failed: {error:#}");
                std::process::exit(2);
            }
        });
    }
}

async fn serve_backend_flow(
    mut stream: TcpStream,
    mut resume_rx: watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<()> {
    let resume = loop {
        if let Some(resume) = *resume_rx.borrow_and_update() {
            break resume;
        }
        resume_rx.changed().await?;
    };
    sleep_until(resume).await;

    let mut expected = 1_u64;
    loop {
        let mut header = [0_u8; RECORD_HEADER_BYTES];
        match stream.read(&mut header[..1]).await {
            Ok(0) => return Ok(()),
            Ok(_) => {
                stream.read_exact(&mut header[1..]).await?;
            }
            Err(error) => return Err(error.into()),
        }
        if header[..4] != MAGIC {
            bail!(
                "invalid record magic at sequence {expected}: {:02x?}",
                &header[..4]
            )
        }
        let sequence = u64::from_be_bytes(header[4..12].try_into().expect("sequence bytes"));
        if sequence != expected {
            bail!("record order mismatch: expected {expected}, received {sequence}")
        }
        let length = u32::from_be_bytes(header[12..].try_into().expect("length bytes")) as usize;
        if length == 0 || length > MAX_RECORD_BYTES {
            bail!("invalid record payload length {length}")
        }
        let mut body = vec![0_u8; length];
        stream.read_exact(&mut body).await?;
        if body
            .iter()
            .enumerate()
            .any(|(index, byte)| *byte != (index % 251) as u8)
        {
            bail!("payload verification failed for sequence {sequence}")
        }
        stream.write_all(&sequence.to_be_bytes()).await?;
        expected += 1;
    }
}

fn start_backend(backend: &mut ChildGuard) -> Result<Instant> {
    let stdin = backend.stdin_mut()?;
    stdin.write_all(b"GO\n")?;
    stdin.flush()?;
    let line = backend.read_stdout_line()?;
    let target_ns: u128 = line
        .trim()
        .strip_prefix("RESUME_UNIX_NS ")
        .context("backend did not report resume deadline")?
        .parse()
        .context("parse backend resume deadline")?;
    let now_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let remaining_ns = target_ns.saturating_sub(now_ns).min(u64::MAX as u128) as u64;
    Ok(Instant::now() + Duration::from_nanos(remaining_ns))
}

fn wait_backend_ready(backend: &mut ChildGuard) -> Result<()> {
    let line = backend.read_stdout_line()?;
    if line.trim() != "READY" {
        bail!("backend did not become ready: {line:?}")
    }
    Ok(())
}

async fn wait_for_listener(port: u16) -> Result<()> {
    for _ in 0..100 {
        if TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }
    bail!("Fyntr did not listen on port {port}")
}

fn free_port() -> Result<u16> {
    Ok(StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port())
}

fn start_rss_sampler(
    pid: i32,
    interval_ms: u64,
    initial_rss: u64,
) -> (Arc<AtomicBool>, JoinHandle<u64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let task_stop = stop.clone();
    let task = tokio::spawn(async move {
        let mut peak = initial_rss;
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while !task_stop.load(Ordering::Acquire) {
            ticker.tick().await;
            if task_stop.load(Ordering::Acquire) {
                break;
            }
            if let Ok(current) = usage(pid) {
                peak = peak.max(current.rss_bytes);
            }
        }
        peak
    });
    (stop, task)
}

fn rate(value: u64, elapsed: Duration) -> f64 {
    value as f64 / elapsed.as_secs_f64()
}

fn percentile_ms(sorted_ns: &[u64], percentile: usize) -> Option<f64> {
    if sorted_ns.is_empty() {
        return None;
    }
    let rank = (percentile * sorted_ns.len()).div_ceil(100).max(1);
    Some(sorted_ns[rank - 1] as f64 / 1_000_000.0)
}

#[cfg(unix)]
fn raise_file_limit() -> Result<()> {
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error()).context("getrlimit(RLIMIT_NOFILE)");
    }
    let desired = 4096.min(limit.rlim_max);
    if limit.rlim_cur < desired {
        limit.rlim_cur = desired;
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
            return Err(std::io::Error::last_os_error()).context("setrlimit(RLIMIT_NOFILE)");
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn usage(pid: i32) -> Result<Usage> {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    let status = unsafe {
        libc::proc_pid_rusage(
            pid,
            libc::RUSAGE_INFO_V2,
            &mut info as *mut _ as *mut libc::rusage_info_t,
        )
    };
    if status != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("proc_pid_rusage({pid})"));
    }
    Ok(Usage {
        cpu_ns: info.ri_user_time + info.ri_system_time,
        rss_bytes: info.ri_resident_size,
    })
}

#[cfg(not(target_os = "macos"))]
fn usage(_pid: i32) -> Result<Usage> {
    bail!("Fyntr CPU/RSS measurement is supported only on macOS")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_nearest_rank_boundary() {
        let values = [1_000_000, 2_000_000, 3_000_000, 4_000_000];
        assert_eq!(percentile_ms(&values, 50), Some(2.0));
        assert_eq!(percentile_ms(&values, 99), Some(4.0));
        assert_eq!(percentile_ms(&[], 99), None);
    }

    #[test]
    fn validates_s2_flow_counts() {
        let cli = Cli::parse_from([
            "fyntr-bench",
            "--scenario",
            "s2",
            "--fyntr-bin",
            "fyntr",
            "--flows",
            "99",
        ]);
        assert!(validate(&cli, Scenario::S2).is_err());
    }

    #[tokio::test]
    async fn record_round_trip_preserves_sequence_and_payload() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let body = payload(128);
        let send = tokio::spawn(async move { write_record(&mut writer, 7, &body).await });

        let mut header = [0_u8; RECORD_HEADER_BYTES];
        reader.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..4], &MAGIC);
        assert_eq!(u64::from_be_bytes(header[4..12].try_into().unwrap()), 7);
        let mut received = [0_u8; 128];
        reader.read_exact(&mut received).await.unwrap();
        assert_eq!(received.to_vec(), payload(128));
        send.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bulk_flow_reports_missing_ack_after_measurement() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut backend, _) = listener.accept().await.unwrap();
        let phase = Arc::new(AtomicU8::new(1));
        let flow = tokio::spawn(run_bulk_flow(
            client,
            1,
            false,
            phase.clone(),
            Arc::new(Stats::default()),
        ));

        let mut record = [0_u8; RECORD_HEADER_BYTES + 1];
        backend.read_exact(&mut record).await.unwrap();
        backend.read_exact(&mut record).await.unwrap();
        phase.store(2, Ordering::Release);
        drop(backend);

        let result = timeout(Duration::from_secs(2), flow)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err(), "missing ACKs must fail the bulk flow");
    }

    #[tokio::test]
    async fn bulk_flow_finishes_after_all_acks() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut backend, _) = listener.accept().await.unwrap();
        let phase = Arc::new(AtomicU8::new(1));
        let flow = tokio::spawn(run_bulk_flow(
            client,
            1,
            false,
            phase.clone(),
            Arc::new(Stats::default()),
        ));

        let mut header = [0_u8; RECORD_HEADER_BYTES];
        let mut body = [0_u8; 1];
        for _ in 0..2 {
            backend.read_exact(&mut header).await.unwrap();
            backend.read_exact(&mut body).await.unwrap();
            backend.write_all(&header[4..12]).await.unwrap();
        }
        phase.store(2, Ordering::Release);

        while backend.read(&mut header[..1]).await.unwrap() != 0 {
            backend.read_exact(&mut header[1..]).await.unwrap();
            backend.read_exact(&mut body).await.unwrap();
            backend.write_all(&header[4..12]).await.unwrap();
        }
        drop(backend);

        let result = timeout(Duration::from_secs(2), flow)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok(), "all ACKed records should drain cleanly");
    }
}
