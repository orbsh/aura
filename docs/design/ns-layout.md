# okm 键空间布局（已并入事件流转文档）

> **语言：** [English](ns-layout-en.md) · [中文](ns-layout.md)

本页已并入 **[事件流转（emit / on 全机制）§7 键空间布局](event-flow.md)**——键空间图
按机制语境（哪张表在哪条路径里被谁读写）呈现，不再独立维护。

> **Languages:** [English](ns-layout-en.md) · [中文](ns-layout.md)

This page merged into **[Event Flow §7 Keyspace layout](event-flow-en.md)**.

权威定义仍住代码的 `#[ok_ns]` / `#[ok_partition]` 注解（`crates/realm/src/mq.rs`、
`crates/realm/src/meta.rs`）；改动注解时同步 event-flow 双语 §7。
