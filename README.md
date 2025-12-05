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
Fyntr *(/ˈfɪn.tər/)* is a minimal forward proxy that smooths bursts of outbound TLS traffic.
It needs no config to launch and stays out of the way: no auth, no inspection, and a tiny runtime memory footprint (typically ~14MB RSS on macOS).
Its internal actor-driven scheduler relays encrypted traffic transparently without terminating TLS, making bursty workloads more predictable and robust.

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

    Override the listener port or connection cap via CLI flags or env vars:

    ```bash
    cargo run --release -- --port 8080 --max-connections 512
    # or
    FYNTR_PORT=8080 FYNTR_MAX_CONNECTIONS=512 cargo run --release
    ```

    By default, Fyntr caps concurrent connections at 1000 (set to `0` to disable).

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

## Why Fyntr?
When managing cloud operations using tools like Terraform, you might spawn bursts of short-lived TCP connections constantly opening and closing.
These can lead to issues such as exhausting available ephemeral ports due to `TIME_WAIT` sockets, or overloading the NAT table on routers with limited capacity, particularly on consumer-grade NAT devices, which can freeze things up or become unresponsive.

Fyntr takes a simpler approach. Instead of pooling connections, it evens out how active flows are serviced.
The scheduler uses Deficit Round-Robin (DRR) to distribute sending opportunities across flows fairly,
so bursts from many parallel flows get interleaved instead of firing all at once.

This smoothing of peaks makes it less likely for small routers to choke their CPU during bursts of simultaneous connections, even though the connection count and total throughput remain the same.

## Usage with Terraform

### Example: AWS Provider
    
```bash
# Set environment variables
export HTTPS_PROXY=http://127.0.0.1:9999

# Assuming your AWS credentials are managed by aws-vault
aws-vault exec my-profile -- terraform apply
```
