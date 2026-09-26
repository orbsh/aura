# 0029 — Realm 拆分：realm/src/lib.rs 的文件级分解

> **Languages:** [English](0029-realm-split.md) (primary) · [中文](0029-realm-split.zh-CN.md)

**Status:** Accepted (2026-09-26) — design; implementation pending, see Consequences

## Context

`crates/realm/src/lib.rs` 有 1,257 行，28 个方法挂在一个 17 字段的 `Realm`
结构体上。这些字段并不构成一个关注点，而是六个各自独立的职责共用一个宿主：

- **Registry 平面**：`types`、`store_plans`、`persisted_schemas`（+
  `register_type`、introspection 辅助函数）。
- **实例生命周期**：`instances`、`queue_capacity`、evictor（`evict_idle`、
  `evict_instance`、`spawn_evictor`）。
- **Call-slot 平面**：`call_specs`、`pending_calls`、`call_seq`（`call`、
  `dispatch_call`、`resolve_call`、deadline 扫描）。
- **Event + MQ 平面**：`mq`、`router`、`dead_events`（`emit`、队列压缩、
  cursor 消费胶水）。
- **Remote-probe 平面**：`probes`、`pending_remote`、`code_base_url`
  （`RemotePending`、`ProbeConn`）。
- **Ctx 桥**：`ctx_for`、`host_bridge_for`——为驻留会话组装每次调用的
  host 闭包的接缝。

函数长度分布说明同一件事：`run_job` 145 行、`instance` 108、`host_bridge_for` 96、
`call` 88、`emit` 88、`register_type` 69。一个 1,257 行的文件里，最大的几个函数各自
服务不同的平面——这不是一个模块，是六个模块叠在一个文件里。

本次评审的第二个动机：lib.rs 携带 98 处 `.clone()`，是 workspace 内密度最高的文件。
如实分析（记录于此，避免目标被误读）：这些 clone 分三类——闭包/spawn 捕获
（`async move` 要求所有权）、注册表双向写入（`HashMap` key 所有权）、廉价句柄分发
（`self.mq.clone()`）。**文件拆分不消除第 1、2 类。** 拆分改变的是所有权*可见性*：
今天很多 clone 存在，是因为所有平面都挂在同一个 `self` 上、没有哪个模块拥有一个值
——辅助函数借到 `self`，然后把需要的东西 clone 进闭包。一旦每个平面住在自己的模块
里、有自己的状态拥有者，数据就可以在拥有者之间 move，而不是每次借用都重新 clone。
现实的结果是 clone 部分下降（闭包捕获类不变），结构性清晰才是主要收益。如果拆完
之后所有状态仍然挂在同一个胖结构体上，clone 一处也不会少——正是下面的模块边界让
改进成为可能。

## Decision

1. **同一 crate 内的文件级拆分——不建新 crate、不改 API。** Rust 允许同一 crate
   内跨文件写多个 `impl Realm` 块，所以方法移入模块，而 `Realm` 保持为唯一的组合
   结构体，字段 `pub(crate)`。消费者（`aura-engine`、测试）看不到任何签名变化。

   目标布局：

   - `registry.rs` — registry 平面：`types`、`store_plans`、
     `persisted_schemas`、`register_type`/`register_inner` 管线、introspection
     辅助函数。
   - `instance.rs` — 实例生命周期：`Instance`、instances 映射、`instance`、
     `submit`、`run_job`、`run_job_queued`、evictor 三件套。
   - `call_slot.rs` — call slot：`CallSpec`、`PendingEntry`、`call_seq`、
     `call`、`dispatch_call`、`resolve_call`、`declare_call`、deadline 扫描。
   - `events.rs` — event + MQ 平面：`emit`、路由辅助、队列压缩、dead-event
     胶水。（`mq.rs` 本身保持不动——它是 store，不是 realm 胶水。）
   - `remote.rs` — probe 平面：`ProbeConn`、`RemotePending`、`probes`、
     `pending_remote`、`code_base_url`、remote 分发臂。
   - `ctx.rs` — ctx 桥：`ctx_for`、`host_bridge_for`、目前在 lib.rs 顶部的
     JSON 参数辅助函数。
   - `lib.rs` 保留：`Realm` 结构体定义、构造函数（`with_mq`、
     `with_code_base_url`）、`Default`、re-export。目标体量 ≈ 150–250 行。

2. **跨平面访问走 `pub(crate)` 字段读取，不做访问器方法。** 平面之间真实交织
   （remote 调用解析 call slot、call slot 交付进实例）；在它们之间发明 trait 或
   消息层是为结构而结构。拆分的目的是文件级的所有权清晰；`pub(crate)` 保持交织
   可表达，同时为将来更硬的边界留出选项，而现在不预先承诺。

3. **`SharedRealm`/`Weak` 纪律不变。** 任何活得比单次 job 更久的闭包或任务继续
   持有 realm `Weak`（既有的 retain-cycle 规则）。拆分不得创造第二种捕获模式；
   模块抽取先把代码原样搬移，任何所有权简化（以 move 代 clone）都是独立的、逐点
   评审的后续改动。

## Honest semantic cost

- clone 数量不会从 98 降到很小。闭包捕获和 `HashMap` key 所有权在任何分解下都
  存在；只有「借 self、把闭包需要的东西 clone 走」这一类获得了 move 路径。本
  ADR 的目标是所有权清晰；clone 减少是部分的、附带的。
- 共享一个结构体的六个文件可能漂移回巨石：新字段不加分辨地追加到 `Realm` 上。
  缓解靠评审纪律而非代码：新字段落在拥有其关注点的模块里，否则提案须说明它为
  何是横切的。
- 多 `impl` 块用文件局部性换模块局部性——读一个平面不再同时看到整个结构体。
  这正是想要的取舍：17 个字段本来就已经多到无法一眼容纳。

## Consequences

- 实现是一个纯搬移提交：函数和字段注释迁入上述模块，`use` 语句重新接线，无
  行为变化，全测试套件绿（按 feature 转发规则跑 `cargo test -p aura-realm
  --features "fjall,steel,nushell"`），另跑 `cargo test -p aura-engine
  --features "fjall,nushell,steel"`（engine 测试覆盖 realm 表面）。
- 后续所有权简化（逐点以 move 代 clone）是独立工作项，各自在自己的调用点说理
  ——不与搬移捆绑。
- 不建 PLAN phase；这是内部结构，不是能力。PLAN 会话记录记录落地。
