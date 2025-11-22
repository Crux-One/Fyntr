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

[![GitHub license](https://img.shields.io/github/license/Crux-One/Fyntr?label=License&logo=github)](https://github.com/Crux-One/Fyntr "Click to view the repo on GitHub")
[![Release Version](https://img.shields.io/github/v/release/Crux-One/Fyntr?include_prereleases&label=Release&logo=github)](https://github.com/Crux-One/Fyntr/releases/latest "Click to view the repo on GitHub")
  </p>
</div>

## About
Fyntr *(/ˈfɪn.tər/)* is a minimal forward proxy that smooths bursts of outbound TLS traffic.
It needs no config to launch and stays out of the way: no auth, no inspection, tiny footprint (~12MB on macOS).
Its internal actor-driven scheduler relays encrypted traffic transparently without terminating TLS, making bursty workloads more predictable and stable.

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
Managing cloud infrastructure with tools like Terraform often spawns a torrent of short-lived TCP connections.
These can lead to issues, including `TIME_WAIT` socket exhaustion or NAT table saturation on routers with limited NAT table capacity, particularly on consumer-grade models, which can eventually stall operations or cause timeouts.

Fyntr takes a simpler approach. It doesn't pool connections; it smooths them.
By pacing each flow through its scheduler, it prevents simultaneous bursts that could overwhelm routers,
resulting in fewer network spikes and reduced traffic congestion.

Under the hood, the internal scheduler—built on an actor-based concurrency model and a Deficit Round-Robin (DRR) algorithm—ensures every flow is handled fairly, even under heavy parallel load.

## Usage with Terraform

### Example: AWS Provider
    
```bash
# Set environment variables
export HTTPS_PROXY=http://127.0.0.1:9999

# Assuming your AWS credentials are managed by aws-vault
aws-vault exec my-profile -- terraform apply
```
