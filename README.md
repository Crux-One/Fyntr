<div align="center">
  <p>
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="./assets/fyntr.png">
      <source media="(prefers-color-scheme: light)" srcset="./assets/fyntr.png">
        <img class="logo-img" alt="Fyntr" width="30%">
    </picture>
  </p>

  <h1>
    Fyntr
  </h1>
  <p>
    A minimal forward proxy with transparent TLS passthrough.

[![GitHub license](https://img.shields.io/github/license/Crux-One/Fyntr?label=License&logo=github)](https://github.com/Crux-One/Fyntr "Click to view the repo on GitHub")
[![Release Version](https://img.shields.io/github/release/Crux-One/Fyntr?include_prereleases&label=Release&logo=github)](https://github.com/Crux-One/Fyntr/releases/latest "Click to view the repo on GitHub")
[![Repo Size](https://img.shields.io/github/repo-size/Crux-One/Fyntr?label=Size&logo=github)](https://github.com/Crux-One/Fyntr "Click to view the repo on GitHub")
  </p>
</div>

## About
Fyntr *(/ˈfɪn.tɚ/)* is a minimal forward proxy with TLS passthrough, engineered in Rust, designed for efficiency and simplicity.
It includes no authentication or inspection capabilities.
It was created to make bursty network workloads predictable and stable.
Its internal scheduler relays encrypted traffic transparently without terminating TLS.

## Why Fyntr?
Managing cloud infrastructure with tools like Terraform often spawns a torrent of short-lived TCP connections.
These can lead to issues, including `TIME_WAIT` socket exhaustion or NAT table saturation on routers,
which can eventually stall operations or cause timeouts.

Fyntr takes a simpler approach. It doesn't pool connections; it smooths them.
By pacing each flow through its scheduler, it prevents simultaneous bursts that could overwhelm routers,
resulting in fewer network spikes and reduced traffic congestion.

Under the hood, the internal scheduler—built on an actor-based concurrency model and a Deficit Round-Robin (DRR) algorithm—ensures every flow is handled fairly, even under heavy parallel load.

## Quick Start

1.  Run Fyntr:

    Fyntr listens on port 9999 by default.

    ```bash
    cargo run --release
    ```

2.  Configure Your Environment:

    Export the following environment variables in a separate terminal.

    ```bash
    export HTTPS_PROXY=http://127.0.0.1:9999 
    ```

3.  Verify It Works:

    You can test the connection with a simple `curl` command.

    ```bash
    curl https://ifconfig.me
    ```

## Usage with Terraform

- Example: AWS Provider
    
    ```bash
    # Set environment variables
    export HTTPS_PROXY=http://127.0.0.1:9999
    export NO_PROXY=169.254.169.254,localhost,127.0.0.1

    # Assuming your AWS credentials are managed by aws-vault
    aws-vault exec my-profile -- terraform apply
    ```

## Status

Fyntr is currently in an alpha stage and under development.
Feedback, bug reports, and contributions are highly welcome.
