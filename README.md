# Aura

State-in-one distributed Actor engine. Design: [aura-architecture.md](../../.hermes/wiki/aura-architecture.md) (wiki).

Compute-storage-unified modern distributed Actor engine: no SQL, no external cache lock, no multi-DB sync. State managed by Actors automatically — modify context in memory, the Rust runtime handles concurrency and multi-machine backup.

- Single binary, zero runtime dependencies (no Docker, no etcd, no database)
- Rust shell + Steel/Python/Wasm embedded (Polyglot Bridge, zero IPC)
- Definitions and engine metadata per-node (single okm instance, ADR-0025; control-plane single-writer, no consensus); Actor state in Fjall (local) or SlateDB + S3 (cloud-native)
- Single-node start; federation via well-known protocol authentication, user data stays on its home node

Agent components on top: Gravity (turn executor, Actor type), Probe (execution base / remote actuator), Prism (entry: WS gateway + CLI over WS).

## Running tests

The test suite is feature-gated: script carriers (steel/python/nushell/wasmtime) and storage engines (fjall) compile in via Cargo features. A bare `cargo test` compiles NO carriers — script-actor tests fail with `language not resident-carried by this probe build`. Always run:

```sh
cargo test --all-features --no-fail-fast
```

Single acceptance file (still needs `--all-features`):

```sh
cargo test -p aura-engine --test timer_reclaim --all-features
```

Note: full compile with all features takes several minutes; `cargo check --all-features --workspace` is the fast loop.
