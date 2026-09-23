# 0025 — meta 平面的命运：现在并入数据面，完全外置推迟到 Prism

> **Languages:** [English](0025-meta-plane-fate.md) (primary) · [中文](0025-meta-plane-fate.zh-CN.md)

**Status:** Accepted (2026-09-22) — 方案 A 已实现；方案 B 记录在案，推迟裁决

## Context

ADR-0018 的无 JSON 裁决把 meta 平面（actor 定义）迁到了独立的 okm 实例。随后的一个追问瓦解了这个前提：meta 平面凭什么作为**独立实例**存在？两股压力指向更远：

1. **actor 定义就是数据。** 一份定义（source、language、entry、TTL、schema）与 mq 行、state 文档没有结构差异——都是 actor 的数据。为一张表开一个专用实例，多一个目录、多一次引擎选择、多一份配置面（`meta_engine`/`meta_dir`），什么都没换来。
2. **aura 作为纯执行环境。** 如果 aura 是这个技术栈的无状态执行器，「定义在重启后存活」根本不是 aura 的关切——定义属于**外层**（prism/gravity），由外层推送给节点。分布式问题（副本、可用性、真相在哪）是控制平面的；aura 不知道也不需要知道。

由此得出两个终态。两个都记录在案——一个现在实现，一个带着触发条件推迟。

## 方案 A（已实现）：单一存储面，定义作为一张表

**meta 实例删除。actor 定义成为数据面 okm 实例里的一张 `ActorDef` 表**（`realm/src/meta.rs`，ns 41，与 mq 表、state 文档并列）。

- `Engine.meta_store` 没有了；`meta_engine`/`meta_dir` 配置面没有了；一个引擎、一个目录、一次引擎选择。
- `register` 仍持久化定义、boot 仍重载（`load_all` → `engine.register`）——但走的是 mq 表所在的同一实例。4.5b 的**语义**（upload 是独立生命周期；只内省一次；执行路径 schema-free）不变，只有物理位置塌缩了。
- 身份模型保持平面内 registry 模式（TypeName registry + MAX watermark reduce；id 永不复用）——与 ActorName、state 表的身份解析同构。一个模式，三处使用。
- `aura-storage` crate 已删除（aura 内不再有任何 JSON 存储）；本决策不改变 crate 数量。
- 内省 schema 继续按原文 JSON 文本携带——接口产物（LLM/脚本侧契约），不是存储编码。

**为什么不同时裁决 B：** 方案 B 所需的外层尚不存在（prism 已设计、未实现）。在外层能接手之前删掉 aura 的持久化，中间态整栈不可运行——Milestone A 的零依赖启动性质平白丢失。方案 A 是 B 终态的严格子集：B 落地时，剩下这唯一实例被远程句柄替换、boot reload 被删除。

## 方案 B（记录在案，推迟）：aura 完全不持存储

**aura 变成无状态执行器：无本地引擎、无目录、无 boot reload。** 定义、ctx state、mq 全部住在外层的存储里（prism/gravity 的 okm 实例）；aura 经 okm 的远程路径挂载——`NestStorage`/`RemoteStore` over the wire（okm ADR-0010/0021：sender/receiver 框架和流式扫描契约均已落地；aura 侧成为 receiver 宿主）。

- **变什么：** realm 的 mq/state 同步本地引擎调用变成 wire 往返；`Engine::start` 不再打开任何引擎；boot reload 删除——控制平面在节点启动时和定义变更时推送定义（`engine.register` 保留签名，失去持久化副作用）。
- **不变什么：** 节点仍承担自己数据上的引擎邻接职责（写路径执行、watermark 压缩、timer wheel）——「无状态执行器」指不持有**存储所有权**，不是不做计算。okm ADR-0010 的 receiver 契约划定的正是这个形状。
- **触发条件：** prism 的连接面存在且需要一个节点来宿主它的存储（ADR-0017 §1：prism 是引擎所在宿主）。远程挂载重构作为 prism 实现的一部分落地——不提前，不延后。
- **推迟的代价：** 一个 aura 本地持久化的过渡形态。接受：它是当前可运行的系统，且方案 A 已把它塌缩到 B 将整体替换的最小表面（一个实例）。
- **分布式问题向外消解：** 副本、可用性、真相的位置成为控制平面的部署选择（单节点文件、副本化存储、prism 的运营者选什么就是什么）——aura 不再有立场。这正是用户的表述：「即便真的有分布式的问题，它也不需要管，外层的 prism 管」。

## 诚实的语义代价

- **方案 A 保留了一个 aura 将来要卸掉的持久化义务。** 在 A 与 B 之间，节点重启仍从本地目录恢复定义。若控制平面的定义副本与 aura 本地副本漂移，B 落地前 aura 本地副本获胜——定义上存在一个暂时的双主窗口。接受，理由是定义是幂等重推（按 type 名内容寻址、最新版本获胜）且窗口随 B 落地终结。
- **方案 B 用延迟局部性换架构纯粹。** 每次 ctx state 写、每条 mq append 都变成 wire 往返。热循环工作（Phase 6.5 的 resident executor）必须按**远程世界**定价，不能按本地——B 落地时那个每 turn 的成本要实测，不能想当然。本地引擎可以保留为单节点开发的**部署形态**（Milestone A 的零依赖性质）——B 约束的是生产拓扑，不是测试装置。
- **registry reduce 的 group 哨兵变通**（`global` 常量字段，因 okm derive 拒绝空 group 列表）在两个方案下都存活；okm ADR-0023/0024 的组合子落地后吸收。

## Consequences

- 现在：aura 一个 okm 实例；`meta_engine`/`meta_dir` 从配置移除；`actor_defs` 与 mq/state 并列；全仓零 JSON 存储。
- prism 时刻：实现 B——远程挂载这唯一实例、删除 boot reload、定义由控制平面推送。ADR-0017 的修订（§3/§5/§7：身份表住 prism；投递载荷携带 sender 元数据，aura 的 Ctx 不变）与本 ADR 的 B 条款一起执行。
- 先例成立：aura 拥有计算与投递；外层拥有身份、定义、真相的位置。
