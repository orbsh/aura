# okm Keyspace Layout (merged into the Event Flow document)

> **Languages:** [English](ns-layout-en.md) (primary) · [中文](ns-layout.md)

This page merged into **[Event Flow §7 Keyspace layout](event-flow-en.md)** —
the layout now reads in its mechanic context (which table, on which path,
written and read by whom) instead of standing alone.

The authoritative definitions still live in the code's `#[ok_ns]` /
`#[ok_partition]` annotations (`crates/realm/src/mq.rs`,
`crates/realm/src/meta.rs`); annotation changes sync Event Flow §7 (both
languages).
