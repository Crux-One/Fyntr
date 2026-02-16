<div align="center">
  <p>
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Crux-One/Fyntr/main/assets/fyntr.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/Crux-One/Fyntr/main/assets/fyntr.png">
        <img class="logo-img" alt="Fyntr" width="30%" src="https://raw.githubusercontent.com/Crux-One/Fyntr/main/assets/fyntr.png">
    </picture>
  </p>

  <h1>
    Fyntr
  </h1>
  <p>
    A minimal forward proxy to tame bursty outbound traffic.

[![GitHub License](https://img.shields.io/github/license/Crux-One/Fyntr?logo=github&style=for-the-badge)](https://github.com/Crux-One/Fyntr)
[![Release Version](https://img.shields.io/github/v/release/Crux-One/Fyntr?include_prereleases&logo=github&style=for-the-badge)](https://github.com/Crux-One/Fyntr/releases/latest)
[![Crates.io Version](https://img.shields.io/crates/v/fyntr?style=for-the-badge&logo=rust&color=yellow)](https://crates.io/crates/fyntr)

  </p>
</div>

## About
Fyntr *(/ˈfɪn.tər/)* is a minimal forward proxy that smooths bursts of outbound TLS traffic, stabilizing connections on constrained networks.
No server-side changes required, no auth, no inspection.

Fyntr starts with a small memory profile right after startup (~1-2MB peak memory footprint on macOS via `/usr/bin/time -l`, and ~1MB private memory on Windows) and uses an actor-driven scheduler to relay traffic transparently, making bursty workloads more stable and reliable without terminating TLS.

## Internals
- Traffic shaping: Prevents burst congestion by interleaving packets via Deficit Round-Robin (DRR) scheduling.
- Adaptive quantum tuning: Optimizes quantum size via packet size statistics to reduce latency spikes and improve throughput.
- FD limit guard: Validates file descriptor limits against max connection settings.

## Quick Start

1. Install and run Fyntr:

    Install the crates.io release and run it locally (defaults to port 9999).

    ```bash
    cargo install fyntr
    fyntr
    ```

    Or build from source:

    ```bash
    cargo run --release
    ```

    Override defaults via CLI flags or env vars:

    ```bash
    cargo run --release -- --port 8080 --max-connections 512

    # Or use environment variables
    FYNTR_PORT=8080 FYNTR_MAX_CONNECTIONS=512 cargo run --release
    ```

    By default, Fyntr caps concurrent connections at 1000 (set `0` for unlimited).

2. Configure Your Environment:

    Export the following environment variables in a separate terminal.

    ```bash
    export HTTPS_PROXY=http://127.0.0.1:9999 
    ```

    This configuration affects not only `aws-cli` but also various tools that use `libcurl`, including `git`, `brew`, `wget`, and more. 

3. Verify It Works:

    You can test the connection with a simple `curl` command.

    ```bash
    curl https://ifconfig.me
    ```

## Library Usage

Requires [`actix-rt`][10] and [`anyhow`][11] in your application's dependencies. For logging, add [`env_logger`][12] (optional but recommended).

```rust
use fyntr::run;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    // Optional: enables logs via RUST_LOG (e.g., RUST_LOG=info).
    env_logger::init();

    let handle = run::server()
        .bind("127.0.0.1")
        .port(0) // 0 lets the OS pick an available port
        .max_connections(512)
        .background()
        .await?;

    println!("Fyntr listening on {}", handle.listen_addr());

    // ... run your app ...

    handle.shutdown().await?;
    Ok(())
}
```

[10]:https://docs.rs/crate/actix-rt/latest
[11]:https://docs.rs/crate/anyhow/latest
[12]:https://docs.rs/crate/env_logger/latest

## CLI Options

| Option | Env var | Default | Description |
| --- | --- | --- | --- |
| `--bind <IP>` | `FYNTR_BIND` | `127.0.0.1` | Address to bind on. Binding to non-loopback interfaces (e.g. `0.0.0.0`) without auth can expose the proxy on the network. |
| `--port <PORT>` | `FYNTR_PORT` | `9999` | Port to listen on (use `0` to auto-select an available port). |
| `--max-connections <MAX_CONNECTIONS>` | `FYNTR_MAX_CONNECTIONS` | `1000` | Maximum number of concurrent connections allowed (set `0` for unlimited). |

## Why Fyntr?
Cloud automation tools such as Terraform spawn bursts of TCP connections that rapidly open and close.
The simultaneous transmission of data from these flows often causes micro-bursts that choke routers with limited capacity, particularly on consumer-grade NAT devices, which can lead to unresponsive networks due to overwhelming CPU interrupt loads.

Rather than relying on connection pooling, Fyntr regulates the traffic itself.
Its scheduler uses DRR to distribute sending opportunities across active flows fairly,
so packet bursts from many parallel flows get interleaved instead of letting them fire all at once.

This smoothing reduces CPU pressure on routers during connection storms.
This effect is most critical when scheduling overhead, rather than bandwidth, is the primary bottleneck.

## Usage with Terraform

### Example: AWS Provider
    
```bash
# Set environment variables
export HTTPS_PROXY=http://127.0.0.1:9999

# Standard usage
terraform apply

# Or use aws-vault wrapper
aws-vault exec my-profile -- terraform apply
```
