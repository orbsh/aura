# Aura

State-in-one distributed Actor engine. Design: [aura-architecture.md](../../.hermes/wiki/aura-architecture.md) (wiki).

Compute-storage-unified modern distributed Actor engine: no SQL, no external cache lock, no multi-DB sync. State managed by Actors automatically — modify context in memory, the Rust runtime handles concurrency and multi-machine backup.

- Single binary, zero runtime dependencies (no Docker, no etcd, no database)
- Rust shell + Steel/Python/Wasm embedded carriers, BGI/Exec out-of-process — all imported from the effector runtime (one carrier set serves embedded booths and the remote effector)
- Definitions and engine metadata per-node (single okm instance, ADR-0025; control-plane single-writer, no consensus); booth state in type-scoped okm collections (ADR-0026) on Fjall (local) or SlateDB + S3 (cloud-native) — keyspace map: [docs/design/ns-layout.md](docs/design/ns-layout.md)
- Single-node start; federation via well-known protocol authentication, user data stays on its home node

Agent components on top: Gravity (turn executor, Actor type), Effector (execution base / remote effector), Prism (entry: WS gateway + CLI over WS).

## Running tests

The test suite is feature-gated: script carriers (steel/python/wasmtime) and
storage engines (fjall) compile in via Cargo features — the `nushell` feature
retired with the PTY carrier (nu booths ride `bgi`). A bare `cargo test`
compiles NO carriers — script-booth tests fail with `language not
resident-carried by this effector build`. Run the default combo:

```sh
cargo test -p aura-engine -p aura-realm --features steel,python,wasmtime,fjall
```

Single acceptance file (pass the same feature combo):

```sh
cargo test -p aura-engine --test timer_reclaim --features steel,python,wasmtime,fjall
```

Note: full compile with all features takes several minutes; `cargo check --all-features --workspace` is the fast loop.
