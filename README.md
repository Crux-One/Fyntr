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
It runs as an HTTP CONNECT proxy with Deficit Round-Robin traffic shaping and optional threat-feed blocking.
No server-side configuration, no inspection, and low baseline memory use.

## Quick Start

  Install and run the crates.io release:

  ```bash
  cargo install fyntr
  fyntr
  ```

Then configure your environment:

  ```bash
  export HTTPS_PROXY=http://127.0.0.1:9999
  ```

Verify the proxy:

  ```bash
  curl https://example.com
  ```

## Documentation
[Fyntr Docs][10]

[10]:https://defrag-0am.blog/projects/fyntr/

## Examples

These examples assume you installed the `fyntr` binary.
If you are running from source, replace `fyntr ...` with `cargo run --release -- ...`.

### Set a higher connection limit

```bash
fyntr --max-connections 2048

# Equivalent via environment variables
FYNTR_MAX_CONNECTIONS=2048 \
fyntr
```

### Allow only explicit `CONNECT` ports

```bash
fyntr --no-default-allow-port --allow-port 8443
```

### Detect `CONNECT` targets with threat feeds

```bash
fyntr \
  --threat-feed-file ./phishing-domains.txt \
  --threat-feed-file ./malicious-ips.txt \
  --threat-action block
```

### Enable a SOCKS5 listener:

```bash
fyntr --socks5-port 1080
```

## Limitations

1. In certain environments, DRR scheduling can reduce upload throughput, especially on low-spec hardware, as a trade-off for more stable responsiveness.
2. Fyntr supports HTTP CONNECT tunneling and optional SOCKS5 CONNECT tunneling, but does not support plain HTTP proxying, SOCKS5 UDP ASSOCIATE, or SOCKS5 BIND.
3. Fyntr has no built-in authentication. Exposing a public bind address can allow unauthorized proxy use.
