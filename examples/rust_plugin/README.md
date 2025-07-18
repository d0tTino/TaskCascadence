# Rust Cronyx Plugin

This directory contains a minimal Rust plugin that exposes tasks via
CronyxServer. The `Cargo.toml` includes the necessary metadata and a simple
`main.rs` that registers a task and serves it over HTTP for discovery.

## Building

Compile the plugin using Cargo:

```bash
cargo build --release
```

## Serving

Start the server binary and set ``CRONYX_BASE_URL`` so Cascadence can fetch the
advertised tasks:

```bash
./target/release/rustplugin &
export CRONYX_BASE_URL=http://localhost:8000
# start Cascadence as usual
```
