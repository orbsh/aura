# 0042 — 实例身份结构化：单例哨兵退役（关闭 ADR-0038 的残余）

> **语言：** [English](0042-structural-instance-identity.md)（主文档） · [中文](0042-structural-instance-identity.zh-CN.md)

**状态：** Accepted（2026-10-08）—— **已落地（2026-10-09，随 Phase 4.13）**
（commits `33ef84e` + `e3690ac`：`InstanceId.key` 已是
`aura_booth::InstanceKey { Singleton, Named }` 枚举，哨兵退役，路由行形状
同批迁移——两者动同一张 EventRoute 行，分开做等于两次迁移）。
用户裁决（2026-10-08）：趁 4.13 的窗口把实例身份结构化。

## 上下文

ADR-0038 的残余注记记录了这个 bug：实例键空间仍用哨兵字符串 `__singleton__`
（`aura_booth::InstanceId { key: ... }`），所以 payload 的 key 字段字面等于该串时，
会别名到单例**实例**——与 ADR-0039 修掉的分区别名不是同一个 bug（切片 id 0 已构造性
不可达；实例键仍是被魔法值比较的字符串）。

哨兵今天真正住在哪里（全部调用点已核实）：

1. `events.rs` —— `instance_of(&InstanceKey)`：`Singleton` → `mq::SINGLETON`
   字符串，用作投递目标的 `InstanceId.key`。
2. `instance.rs` —— 消费循环只在 `id.key == mq::SINGLETON` 时为实例绑定无键路由。
3. `mq.rs::bound_instance_key` —— 仅当调用者的实例键等于哨兵时返回 `Singleton`。
4. `instance.rs` —— probe 会话身份是字符串 `"{booth_type}/{key}"`；单例实例的键
   把 `__singleton__` 贡献给它。
5. `ctx.rs` —— 泄压阀 fn 把 `self_id.key`（单例实例即哨兵）传回
   `bound_instance_key`。

已核实**不受影响**：状态面。`store_exec::execute` 用脚本提供的 key 对**类型的 ns**
键控文档——实例键从不进入状态键，所以本裁决不携带任何状态迁移。

bug 的形状：`InstanceId.key` 是开放字符串空间，而框架在未声明保留的情况下保留其中
一个值。任何等于 `__singleton__` 的 payload key 都无法按名投递：对该值的键位 emit
解析出独立的 `Named("__singleton__")` 切片，但它的投递目标与该类型的单例实例相撞——
两个逻辑实例共享一个 `InstanceId`。

## 裁决

**裁决——`InstanceId.key` 变为结构化枚举；哨兵字符串从框架整体退役。**

```rust
pub enum InstanceKey {
    /// 该类型的单例实例（至多一个；不需要名字）。
    Singleton,
    /// 具名实例——payload 携带的路由值。
    Named(String),
}

pub struct InstanceId {
    pub booth_type: String,
    pub key: InstanceKey,
}
```

- 单例是一个 **variant**，不是字符串值。字面别名在结构上不可达——与 ADR-0039 给
  切片 id 0 的待遇相同，向上移一个平面。
- `Named(key)` 来自 payload 且无保留：没有任何字符串是特殊的，没有要文档化的保留
  值，没有要防范的碰撞。`SINGLETON`/`__singleton__` 从 `mq.rs` 退役；一个键文本恰好
  等于旧哨兵的具名实例就只是叫那个名字的实例。
- **会话身份**（`instance.rs`）：probe 会话键将 `Singleton` 格式化为 `{type}/`
  （空键段——会话按类型计），`Named(k)` 为 `{type}/{k}`。空键段无歧义：路由规则要求
  `Named` 的键非空（payload 的 key 字段必须产出非空字符串；空 = 畸形，
  `MissingKeyField` 类）。
- **Ctx 面**：脚本继续看到字符串键（等价于 `ctx.self_id.key`）。单例实例的键渲染为
  空字符串——这是诚实的：它没有名字。依赖字面哨兵做分支的脚本是在依赖一条未文档化
  的保留；空字符串才是有文档的渲染。
- **`bound_instance_key`** 改收 `&InstanceKey` 而非 `&str`；单例对键位的比较变成
  variant 匹配，不是字符串比较。
- **Events 接线**：`instance_of` 消失——目标的键就是为切片解析出的那个
  `InstanceKey` variant，不再有字符串往返。

### 什么不变

- MQ 键空间不动：切片段已经是代理 id（ADR-0039 §1）；哨兵从不住在存储键里。
- 路由注册表（ns 25）、各字典、游标键——全部按 id 键控，不受影响。
- `InstanceKey`（MQ 面的切片枚举，`mq::InstanceKey`）保留其名；新枚举是
  `aura_booth::InstanceKey`（实例平面的孪生）。两者刻意同构——切片值就是实例键——
  但它们是不同的类型：一个属于 MQ 面，一个属于 call model。

## 落地连带

- **代码**：`aura_booth::InstanceId.key` 的类型变更波及测试与 CLI 中每一处
  `InstanceId { key: "..." }` 字面量（机械改：`Named("k".into())`）、上面四个框架
  调用点、`dispatch_call` 与会话记账。**无存储迁移**——没有任何持久化数据携带哨兵。
- **Phase 配对**：随 Phase 4.13 落地——两者都在改 EventRoute 行的解析契约，且 4.13
  的 `resolve` 终态本来就按扫描产出实例键，variant 顺着那里自然流入。
- **ADR-0038 的残余注记由本裁决关闭**（按日期记录规矩，原文保留、加被取代指针）。
- **文档**：`event-flow.md`/`-en.md` §1（词汇：实例键获得 variant 形态）、§6.1
  （目标行）、§6.2（绑定规则）、§8.3（残余段关闭）。
- **Probe 缝**：会话键格式的变化只在 probe 的会话目录命名上可观察；probe 不依赖
  哨兵字面量（grep 已核实），所以本裁决不携带 probe 改动。
