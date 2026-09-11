# Aura

State-in-one distributed Actor engine. Design: [aura-architecture.md](../../.hermes/wiki/aura-architecture.md) (wiki).

Compute-storage-unified modern distributed Actor engine: no SQL, no external cache lock, no multi-DB sync. State managed by Actors automatically — modify context in memory, the Rust runtime handles concurrency and multi-machine backup.

- Single binary, zero runtime dependencies (no Docker, no etcd, no database)
- Rust shell + Steel/Python/Wasm embedded (Polyglot Bridge, zero IPC)
- Metadata consistency via Openraft; Actor state in Fjall (local) or SlateDB + S3 (cloud-native)
- Single-node start, multi-machine with one `--raft-nodes` line

Agent components on top: Gravity (turn executor, Actor type), Probe (execution base / remote actuator), Prism (entry: WS gateway + CLI over WS).
