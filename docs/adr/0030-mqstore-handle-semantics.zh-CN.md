# 0030 — MqStore 句柄语义：共享一个 engine，没有 Mutex

> **Languages:** [English](0030-mqstore-handle-semantics.md) (primary) · [中文](0030-mqstore-handle-semantics.zh-CN.md)

**Status:** Accepted (2026-09-26) — 同日修订：step 1 被取代，okm 的 trait 放宽
（okm ADR-0026）先落地，终态形状直接实现

## Context

`realm/src/mq.rs` 定义了所有 mq 表（以及经 `ns_raw` 的所有 wasm 存储平面）绑定的
store：

```rust
pub struct MqStore {
    prefix: Vec<u8>,
    inner: Arc<Mutex<MqEngine>>,
}
```

两个发现塑造了本 ADR。

**发现一：派生句柄悄悄改变了锁边界。**
`MqStore::for_realm` 和 `MqStore::ns_raw` 并不是克隆句柄——它们分解再重包：

```rust
Self { prefix: p, inner: Arc::new(Mutex::new(inner.inner.lock().unwrap().clone())) }
```

每个派生的 realm/type 句柄因此携带**自己的** Mutex。基底 `MqStore` 与它的
`for_realm` 子句柄之间互不串行化；真正做串行化的只有内部的 engine（fjall 自己的
内部同步）。今天没有东西坏——每个 `VirtualStorage` 操作都是单次 put/get/scan，
不存在跨句柄 batch——但代码读起来像 `Arc<Mutex<>>` 是一个共享协调点，而它不是。
未来的读者若在这个 Mutex 上添加多操作不变量，在恰好由上述两个构造器创建的句柄上
就是错的。

**发现二：`Arc<Mutex<>>` 之所以存在，只因 okm 的 trait 写了 `&mut self`。**
okm-core 的 `VirtualStorage` 声明 `put(&mut self, …)` 和 `del(&mut self, …)`
（storage.rs:28–31）。然而 trait 背后的每个真实引擎都已经是 Arc-inner 的廉价克隆
句柄，自带并发管理：

- `FjallStore` — `#[derive(Clone)]`，注释明言 "Clone IS a shared handle
  (Arc-inner)"；fjall 的 `Database`/`Keyspace` 内部自持同步。
- `TestStore` — 各臂包裹 `Arc<SlatedbSync>` / 持有 `FjallStore` /
  `RedbStore`（后者自己就是 Arc 包裹："clones share the underlying keyspace"）。
- `SlatedbSync` — 字段是 `Arc<Runtime>` + `Db`（都是共享句柄类型）。

`put`/`del` 上的 `&mut` 因此是*过度规约*：它没有在数据层换来独占（engine 已有），
却强迫每个消费者自己拥有 `&mut` 路径——这正是 `MqStore` 用 `Arc<Mutex<>>` 包住
engine 的原因，也是 aura 的调用点要 `self.mq.clone()` 出局部量来获取 `&mut` 的
原因。trait 在上面一层其实已经承认了共享句柄语义：
`SharedVirtualStorage::shared_handle()` 返回 "a handle to the same physical
engine. Cheap; shares all state."。契约的两处表述互相矛盾：句柄是共享的，操作却
假装它不是。

## Decision

1. **okm 的 trait 先放宽（okm ADR-0026）：`put`/`del` 取 `&self`。** 下述理由
   （原草案 §3）即 okm ADR 采纳的论据：所有已发布的 engine 都已是自同步的共享
   句柄，`&mut` 传达了虚假的独占要求，并让消费者背上外层 Mutex 的税；
   `SharedVirtualStorage::shared_handle()` 已经陈述了真实语义。一个反面论证存
   在且在那里被驳回：「`&mut` 让有状态的 engine 用更廉价的内部路径（跨写入复
   用缓冲）」。没有已发布的 engine 这样做；且 engine 真正用 `&mut` 的唯一之
   处——redb 的首次写入建表——通过内部可变性同样可达（redb 本来就要求它）。
   若未来某个 engine 真需要 `&mut`，它在内部自己包 Mutex——独占属于需要它的
   engine，不属于强加给所有实现的契约。

2. **MqStore 整个去掉 Mutex（终态形状，已实现）。** 在放宽后的 trait 下，
   `MqStore` 化简为 `{ prefix: Vec<u8>, inner: MqEngine }`——clone 是纯廉价句柄
   （engine 内部 Arc +1 + 几个前缀字节），Mutex 层与每句柄锁边界分歧一起消失，
   `ns_raw`/`for_realm` 变成对 `inner.inner.clone()` 的纯前缀拼装。原计划的
   step 1（共享同一把 `Arc<Mutex<>>` 作为过渡修法）被取代（SUPERSEDED）——它
   在旧 trait 下是正确的，但先落地它会碰同一批构造器两次，而这个形状又在同一
   个改动里死掉；按跨仓库规则 sibling 先落地，aura 一轮采纳终态形状。

## Honest semantic cost

- 锁边界分歧靠删除而非统一消除：外层 Mutex 完全不存在之后，串行化只住在
  engine 内部——它一直真正居住的地方。一个 realm 句柄上的长 scan 现在阻塞另
  一个句柄上的 put，仅到 engine 的内部程度（fjall 自己的 keyspace 同步）；
  现有代码不依赖更强的行为（scan 返回 owned `Vec`，旧的每句柄 Mutex 本来也
  不共享状态）。
- 放宽是带真实迁移尾部的跨仓库改动：okm 的 trait 改动触及 okm-core 的每个
  `VirtualStorage` impl（7 个）和每个下游消费者 impl——本仓库的 aura 两个，
  prism 的 echo 平面在其 workspace 消费新 okm 时跟进。`KvBatch` 保持
  `&mut`——batch 累积真正是有状态的。
- 移除防御性签名扩大了句柄允许的操作（从共享引用写入）。engine 的运行时行
  为没有任何改变；内部锁原样不动。

## Consequences

- 已在 `crates/realm/src/mq.rs` 实现：`MqStore { prefix, inner: MqEngine }`、
  无 Mutex 的 impl、`ns_raw`/`for_realm` 为前缀拼装 + engine 句柄 clone、共享
  句柄语义在构造器处注释。经临时 `[patch]` 节对本地 okm 验证：
  `cargo test -p aura-realm --features "fjall,steel,nushell"`（11 通过）与
  `cargo test -p aura-engine --features "fjall,nushell,steel,wasmtime"`
  （41 通过）。
- okm 先落地它的 ADR-0026（trait 放宽 + 7 个 impl）；本仓库工作树经 path
  patch 对放宽后的 trait 编译，在 sibling 的提交 push 且
  `cargo update -p okm-core` 拾取的那一刻对 pinned okm 变绿（届时移除
  Cargo.toml 的临时 `[patch]` 节）。
- mq.rs 自己的辅助函数中的 `&mut` 形状管线（`resolve_event_id(store: &mut
  MqStore, …)` 及同族）机械收窄为 `&MqStore`；lib.rs 的
  `routes_drop_booth(&mut self.mq.clone(), …)` 类调用点随之简化。那是本 ADR
  的机械跟进，不是独立设计决策。
