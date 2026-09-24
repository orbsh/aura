# 0026 — 类型级 actor 存储：每 actor 类型一个真实 ns，ctx 操作升到 okm 能力级

> **Languages:** [English](0026-type-scoped-actor-storage.md)（主文档）· [中文](0026-type-scoped-actor-storage.zh-CN.md)

**Status:** Accepted (2026-09-24) — design; implementation pending, see Consequences

## Context

当前存储模型给每个 actor **实例**一个扁平状态 document（`InstanceState`，`realm/src/state.rs`），经 `ctx.store.get/set/delete` 寻址——按实例自身 document 的字段级点读写。instance key（`actor_type, key`）曾同时回答三个问题：消息串行化、存储隔离、恢复粒度。

三股压力暴露出存储轴上的过度隔离：

1. **点接口表达不了真实模型。** 单 document 字段上的 `get/set` 没有扫描、没有索引利用、没有 reduce。复杂逻辑（按类型聚合、二级视图、复合键）在单个 actor 内无法表达，被迫外移——拆成更多 actor 类型，或拆成只为补偿接口弱点的 projection actor。
2. **隔离强于威胁模型。** actor 类型的代码是上传的、可信的逻辑——不是任意租户的查询面。类型之间的结构性隔离是应当的（一个类型不得触及另一个类型的数据）；同类型实例之间的结构性隔离不是——类型自己的代码理应看见跨实例的全貌（这正是它的 ns 存在的意义）。
3. **k10r 形态。** krystallizer 类应用是全局单例，其内部分区方式无法也无需提前规划。在当前模型下它要么把一切都塞进一个实例的扁平字段，要么提前围绕 `user_id` 分区——两者都是让模型迁就接口。

修正后的分工：**instance key 只回答一个问题——「这条消息该由谁串行处理」（路由/串行化/恢复）；不再决定存储隔离。** 存储隔离上移到类型级。

## Decision

### 1. 每个 actor TYPE 占据一个真实 okm ns

actor 类型是**声明的、部署级规模**的词汇——每个类型都由应用作者亲手写出，数量按设计有界（每部署数十个，非按用户增长）。这满足既定裁决：封闭、有界的词汇可以占真实 ns 分配；开放、无界的不行（事件/分区仍走 registry + 哈希形态，ADR-0014 的 mq 块）。

- 低位 ns 块保留给 aura 自身：mq 表（30–35）、meta/state（40–41）、未来框架平面。actor 类型从保留块之上的固定基址起分配。
- 分配发生在 `register_type`，作为类型注册表的副作用（即今天已分配 `type_id` 的同一张注册表——本设计需要的动态 actor→ns 注册表已具雏形）；ns id 在节点生命周期内永不复用。
- 用户 namespace 隔离（Phase 3.6 的 `PrefixStore` 包装，每个用户 namespace 一个前缀）是正交的，保留其**机制**：它前缀整个引擎，actor 类型的 ns 活在其下。绑定维度降级为应用决定（PLAN Phase 4.10）——gravity 可绑用户，无用户应用可不绑；「probe 注册凭据 = 用户凭据 → 推导 namespace」被取代。

### 2. 实例是 ns 内的 document，不是隔离单位

`InstanceState` 模型把一个 document 给到每个**实例**、作为它的全部世界——状态 document 本身就是隔离单位。这个形状被取代。按 okm 现行词汇（collection/document，取代表/行），新的可见性单位是 ns：actor 类型的 ns 扮演 SQL schema 在数据库中的角色——类型在其中声明自己的 collection（经其 schema，§4），实例是其中的 document。「每实例一个 document」变为「每类型多个 collection，每（collection, 实例, 键）一个 document」。全局单例类型是单实例的退化形态；单类型内的跨实例聚合就是对类型自身 ns 的普通 scan/reduce——**projection actor 在同类型聚合场景退场**（它的存在是为了补偿点接口），只保留给跨类型聚合（对若干类型数据的预计算）。

串行化语义不变：同 `(type, key)` 经实例队列串行，异 key 并行。改变的只是存储布局所隔离的东西。

### 3. ctx 存储操作升到 okm 能力级

`ctx.store` 获得类型所声明 collection 之上的 okm 操作集——put / get / scan / reduce（确切操作名在实现时定；接口面即 okm 的 Collection API 在动态 document 上的投影）。字段级 `get/set/delete` 点模型被该接口面取代。

- **绑定是结构性的**：每个 ctx 存储句柄在注册时即构造为绑定所属类型的 ns——跨类型访问不可表达，与今天 mq namespace 前缀的构造期保证同型。ns 内部，类型自己的代码以完整接口面受信。
- **脚本侧协议：一个 `emit`，okm 指令载荷**（确立 §1 的 emit 命名在此处的角色）：`ctx.store` 只暴露一个接口——`ctx.store.emit(op)`，op 是一条 okm 指令（collection 名 + 操作 + 参数，DynamicValue 载荷），返回值同为 DynamicValue。语言侧的形态分两档：
  - **python**：脚本内实现 okm 的 `VirtualStorage` 适配器，内部把每个引擎调用翻译成一次 `ctx.store.emit`——此后脚本里直接用 `Collection` API（put/get/scan/… 的类型化门面），桥接成本只在适配器写一次。
  - **steel / nushell**：无适配器，直接 `emit` 单条指令（指令集 = op 收窄项定稿的集合；op 停留在 Collection 语义层，不下沉 VirtualStorage 原语）。
  - **wasm（Rust 源码）是完全体路径**：okm 本体编译进模块——脚本在 `aura_host` imports 之上实现 `VirtualStorage`（每次引擎调用 = 一次 emit op 过 host 桥），模块内运行真正的 `Collection` API。静态 derive 宏在 wasm 构建期生效；动态指令路径是给 actor 用的，不是给 wasm 用的。此时 wasm actor 的存储代码与原生 Rust actor 无差别：同一套 derive、同一套不变量、编译期校验。
  - 归属：`emit` 沿用事件语义的投递形状（指令即事件载荷进 host 桥），与协议层 `ev` 同源——一个动词，两处出现（线上投递、存储指令），语义一致：向接收方递送一条待执行的事实。
- **嵌套 invoke 寻址**：actor A invoke actor B 时，B 的 handler 在 B 的 ns 内操作——解析经类型注册表（类型名 → ns），绝不经调用方提供的键。

### 4. 动态 schema 经 interface_schema 声明

collection 及其键/索引声明搭乘既有的 Phase 4.5b upload 生命周期：

- **python**：脚本内的 okm 类型定义套装饰器派生 schema；introspection 将其合并进 `interface_schema`（与 `@on` 元数据同一套 implicit+explicit 合并，ADR-0014 step 1）。单一声明面，派生式。
- **steel / nushell**：无 AOP——schema 是 `interface_schema` 内的手写字面量（数据，非派生）；handler 直接调用 ctx store 函数。nushell 的 PTY carrier 另无 host bridge，位置不变：在该桥落地前只有内存态。
- **wasm**（Rust 源码）：okm 编译进模块——schema 声明就是构建期的 derive 宏，存储面不需要动态 interface_schema 形态；交付的 `.wasm` 工件在代码里携带自己的 schema，upload 时按 4.5b 生命周期内省读出。
- 合并后的 schema 在注册时随 ActorDef 持久化（dynamic segment）；**执行路径从不重新生成 schema**——类型→schema 的开销只存在于 upload 一次。`ctx.interface_schema` 读持久化副本（便宜），供 handler 对自身声明形态做反射；开发期补全由 LLM 接收同一 interface_schema 服务。

## 命名：全链路单一 event 语义

协议层与概念层只携带 `event`。线上字段是 `ev`——双向皆然；协议不编码方向。`emit`/`on` 是各端实现细节：aura 侧是 actor 的 `@on` 声明与 `emit` 调用；prism 客户端侧是 `ws.send` / `ws.on`。prism 是 aura 的 event 语义到用户端的自然延伸——不存在需要翻译的「action→服务端、type→客户端」独立词汇。凡出现「客户端发 action、服务端发 type 帧」的早期表述，一律由本节取代；server→client 帧字段裁决（双向不复用 `action`）被更强地满足：字段是 `ev`，方向在协议层不存在。

## Honest semantic cost

- **ns 空间花在 actor 类型上。** 这正是有界词汇裁决的用意——但它使保留真实化：反复注册抛弃型类型名的部署会烧 ns id（永不复用）。注册侧去重（同源码哈希 → 同类型）是缓解手段；预算是 u16 宽度，数千个类型。
- **每实例一 document 的状态模型是破坏性替换。** 既有实例状态字节废弃，不做迁移（ADR-0018 先例）。针对 `ctx.store.get/set` 字段语义写的测试改写到 collection 接口面上。
- **ns 内受信是真实受信。** 有 bug（非恶意）的 handler 现在能扫全类型的数据——脚本 bug 的爆炸半径从一个实例的 document 扩大到类型的键空间。接受：代码是上传且受信的；重要的边界（类型 vs 类型、用户 vs 用户）保持结构性。emit 直通 okm 原语同理：开发者对存储有完全控制，绕过 Collection 不变量（如绕过索引/reduce 补偿直接 put）等同于自己坑自己，不设防线。

## Consequences

- 已落地（2026-09-24，probe bbefac8 + aura 7828031）：退役完成——`realm/src/state.rs`（`StateDocumentStore`/`InstanceState`）整体删除；aura-actor 的 `StateStore` trait、`SharedStore`、`Ctx.state` 移除；`ctx_state_get/set/delete` host fns 及其 wire 臂（`HostOp::State`）从 engine 与 probe-protocol 删除。`ctx.store` 恰好暴露 `ctx.store.emit(op)` + `ctx.interface_schema`，作用于声明的 collections。**不留向后兼容糖**：退役的理由是模型裁决（点 document 被 collection 接口面取代），不是迁移成本核算——"已有外部用户"与"还没有外部用户"一样，都不是保留旧接口面的论据；模型正确性是唯一输入。`register_in` 与 `register` 走同一条内省+持久化路径，namespaced 类型得以解析出 ctx.store plan。Remote-probe actor 在 wire 上只保留 invoke（执行节点不持状态；`ctx_store_emit` 需要已解析的 plan = 4.5b）。Phase 4.9 余项：各语言 schema 声明、docs/wiki 清扫。
- 串行化、instance 路由、timer、mq 语义均不动——本裁决只移动存储隔离。
- projection actor 恰好保留在它一贯正当的位置：跨类型预计算。同类型聚合成为类型 ns 内的普通 scan。
- prism 协议命名（`ev`、无方向）随 prism 侧连接平面工作落地（Phase 8）；aura 侧仅是文档。
- wiki 同步（`~/.hermes/wiki/aura-architecture.md`）随实现落地时执行，连同 storage.md/partitioning.md 重写——本 ADR 取代其中「实例状态是一个 document」的段落。
