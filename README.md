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

[![CI](https://img.shields.io/github/actions/workflow/status/Crux-One/Fyntr/ci.yml?logo=githubactions&style=for-the-badge)](https://github.com/Crux-One/Fyntr/actions/workflows/ci.yml)
[![GitHub License](https://img.shields.io/github/license/Crux-One/Fyntr?logo=github&style=for-the-badge)](https://github.com/Crux-One/Fyntr)
[![Release Version](https://img.shields.io/github/v/release/Crux-One/Fyntr?include_prereleases&logo=github&style=for-the-badge)](https://github.com/Crux-One/Fyntr/releases/latest)
[![Crates.io Version](https://img.shields.io/crates/v/fyntr?style=for-the-badge&logo=rust&color=yellow)](https://crates.io/crates/fyntr)

  </p>
</div>

## About
Fyntr *(/ˈfɪn.tər/)* is a minimal forward proxy for constrained networks that keeps them responsive under bursty outbound TLS traffic.
No server-side configuration, no inspection, and low baseline memory use.

## Internals
- Transparent CONNECT relay: Forwards TLS traffic E2E without termination or inspection.
- Traffic shaping: Interleaves packets across active flows using Deficit Round-Robin (DRR).
- Adaptive quantum tuning: Adjusts DRR quantum from observed packet-size statistics.
- Threat detection: Checks CONNECT hosts and resolved addresses against local domain/IP feeds loaded at startup.
- DoS guardrails: Caps request line/header sizes and per-flow queue buffering.

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

2. Configure Your Environment:

    Export the following environment variables in a separate terminal.

    ```bash
    export HTTPS_PROXY=http://127.0.0.1:9999
    ```

    This configuration affects not only `aws-cli` but also various tools that use `libcurl`, including `git`, `brew`, `wget`, and more.

3. Verify the proxy:

    Send an `HTTPS` request through the proxy and confirm that it succeeds:

    ```bash
    curl https://example.com
    ```

    If logging is enabled, you should also see a log entry showing the `CONNECT` target for that request.

## Library Usage

Requires [`actix-rt`][10] and [`anyhow`][11] in your application's dependencies. For logging, add [`env_logger`][12] (optional but recommended).

```rust
use fyntr::run;

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    // Optional: to enable logging, set RUST_LOG (for example, RUST_LOG=info)
    // and uncomment the line below to initialize env_logger:
    // env_logger::init();

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

## Configuration Examples

These examples assume you installed the `fyntr` binary.
If you are running from source, replace `fyntr ...` with `cargo run --release -- ...`.

### Set a higher connection limit:

```bash
# CLI flags
fyntr --max-connections 2048

# Equivalent via environment variables
FYNTR_MAX_CONNECTIONS=2048 \
fyntr
```

### Allow only explicit `CONNECT` ports:

```bash
# CLI flags
fyntr --no-default-allow-port --allow-port 8443

# Equivalent via environment variables
FYNTR_NO_DEFAULT_ALLOW_PORT=true \
FYNTR_ALLOW_PORT=8443 \
fyntr
```

### Detect `CONNECT` targets with threat feeds:

```bash
fyntr \
  --threat-feed-file ./phishing-domains.txt \
  --threat-feed-file ./malicious-ips.txt \
  --threat-action block
```

Feeds can contain plain domain/IP lines or AdGuard-style rules such as `||example.com^` and `||1.2.3.4^`.
Unsupported rules are skipped and reported in startup logs. Fyntr fails to start if no supported entries can be loaded.

## CLI Options

### Server

| Option | Env var | Default | Description |
| --- | --- | --- | --- |
| `--bind <ADDR>` | `FYNTR_BIND` | `127.0.0.1` | Address/hostname to bind on (e.g. `127.0.0.1`, `::1`, `localhost`, `0.0.0.0`). Supports both IPv4 and IPv6. Binding to non-loopback interfaces without auth can expose the proxy on the network. |
| `--port <PORT>` | `FYNTR_PORT` | `9999` | Port to listen on (use `0` to auto-select an available port). |
| `--max-connections <MAX_CONNECTIONS>` | `FYNTR_MAX_CONNECTIONS` | `1000` | Maximum number of concurrent connections allowed (set `0` for unlimited). |

### CONNECT Policy

| Option | Env var | Default | Description |
| --- | --- | --- | --- |
| `--no-default-allow-port` | `FYNTR_NO_DEFAULT_ALLOW_PORT` | `false` | Disable implicit `443` allowance. Only explicitly configured `--allow-port` values are permitted. |
| `--allow-port <PORT>` | `FYNTR_ALLOW_PORT` | implicit `443` unless `--no-default-allow-port` | Allowed destination port for `CONNECT` in the range `1-65535` (repeat flag or comma-separate to add more). |
| `--deny-cidr <CIDR>` | `FYNTR_DENY_CIDR` | Internal ranges | CIDR ranges denied for `CONNECT` destination IPs (repeat flag or comma-separate). |
| `--allow-cidr <CIDR>` | `FYNTR_ALLOW_CIDR` | none | CIDR exceptions that are allowed even if they match denied internal ranges. |
| `--allow-domain <DOMAIN>` | `FYNTR_ALLOW_DOMAIN` | none | Domain/suffix allowlist for `CONNECT` targets. When a domain matches, addresses blocked by deny CIDRs are filtered out rather than causing the entire connection to fail. If all resolved addresses are blocked, the connection is denied. |
| `--threat-feed-file <PATH>` | `FYNTR_THREAT_FEED_FILE` | none | Local threat feed file with domain/IP entries to warn on or block (repeat flag or comma-separate to load multiple feeds). The feed is loaded once at startup into an immutable in-memory index. |
| `--threat-action <warn\|block>` | `FYNTR_THREAT_ACTION` | `warn` | Warn on matching `CONNECT` targets, or reject them with `403 Forbidden` when set to `block`. |

> [!NOTE]
> `--allow-domain` applies only to CONNECT CIDR policy exceptions. It does not override threat feed matches.

## Why Fyntr?
Cloud automation tools such as Terraform can spawn bursts of TCP connections that rapidly open and close, especially when managing many resources in parallel.

When many flows send data simultaneously, they can create short traffic spikes that overwhelm low-capacity routers, particularly consumer NAT devices. This can push CPU interrupt load too high and make the network feel unresponsive.

Rather than relying on connection pooling, Fyntr regulates traffic at the application layer.

Its scheduler uses DRR to distribute sending opportunities fairly across active flows,
so bursts from many parallel connections get interleaved as queued chunks instead of firing all at once.
As a result, Fyntr reduces bufferbloat-like queue buildup and thundering-herd-like traffic patterns.

This smoothing reduces CPU pressure on routers during connection storms.
This matters most when scheduling overhead, rather than bandwidth, is the primary bottleneck.

## Limitations
1. In certain environments, DRR scheduling can reduce upload throughput, especially on low-spec hardware, as a trade-off for more stable responsiveness.
2. Currently, Fyntr supports only HTTP CONNECT tunneling (commonly used for HTTPS) and does not support plain HTTP proxying.
3. Fyntr has no built-in authentication. Exposing a public bind address can allow unauthorized proxy use.
