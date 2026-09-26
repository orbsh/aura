# 0032 — 摊位术语：参与者由 actor 更名为 booth（中文：摊位）

> **语言：** [English](0032-booth-terminology.md)（主文档） · [中文](0032-booth-terminology.zh-CN.md)

**状态：** Accepted（2026-09-26）——命名裁决；代码改名 + 文档清扫随本 ADR 落地，
跨仓对齐（probe/prism/gravity/okm/wiki）同批落地。

## 背景

"actor" 这个词承诺了两件 Aura 并不做的事情。

1. **一个它已经退役的通讯形态。** 经典 actor 的定义性机制是 per-actor
   mailbox——actor 就是它的邮箱（CARB：Computation + Asynchronous RPC +
   Mailbox/Behavior）。PLAN 4.5c 用 per-(event, partition) 队列 +
   per-subscriber cursor 取代了 per-摊位 mailbox（ADR-0014）；投递是 MQ
   形态——按名字队列、一对多、零扇出复制。ADR-0014 自己的理由就是 mailbox
   模型"把'事件属于某个 actor'焊死了——对一对多错了"。actor 概念剩下的
   纯粹是调度：按实例键串行、状态按类型隔离、失败是值。
2. **一个它从来不具备的调用形态。** `ctx.invoke` 是对入口函数返回值的
   请求-响应（reply_to 关联、Phase 3.5 CallSpec 分层）——RPC 语义，不是
   任何经典意义上的 actor `ask`。actor 模型的 ask/tell 是 ActorRef 直发的
   寻址故事；Aura 通过场域按 (type, instance key) 寻址。

分歧已经长到定义脚注压不住的程度：ADR-0031 把参与者与远程服务、浏览器会话
放进了同一个框架，此时 "actor" 还要额外撞上读者从 Erlang/Akka 联邦叙事里
带来的 "remote actor" 词义。

## 决定

**realm 参与者命名为 booth（中文：摊位）。** 读者此前看到的一切 "actor"——
本地或远程、Rust 注册或浏览器驱动——都是摊位：申报接口（摊货自报）、接收路由
事件、应答调用、持有自己的状态。层级词汇与 probe 配对：**probe = 远程执行**
（跑控制平面代码的执行容量，拨入；名字不变，ADR 不变），**booth = 远程/本地
参与**（带自己的代码和存储）。

1. **代码：** `aura-actor` crate → `aura-booth`（`crates/booth`）；
   `ActorType` → `BoothType`、`ActorDef` → `BoothDef`、`PersistedActor` →
   `PersistedBooth`、`ActorName` → `BoothName`；字段与帧段
   `actor_type`/`actor_id` → `booth_type`/`booth_id`；`ACTOR_NS_BASE` →
   `BOOTH_NS_BASE`。
2. **不留兼容别名（策略 A）。** meta 平面持久行与帧字段名直接换拼写，无
   serde alias——现有本地数据清空重注册。静默的双拼写正是改名要消除的漂移
   （ADR-0028 先例，同一裁决）。
3. **文档：** 历史 ADR 一并清扫，不做原地加注——读者按术语过滤检索，可能
   永远读不到那篇修正文档；清扫本身就是修正。外部概念保留 actor 一词：
   Akka/Erlang/Orleans/Actix/tellus 的讨论、"the actor model"、ActorRef、
   Virtual Actor——那些命名的是别的系统的思想，realm.md §5.8/§5.9 对比章
   依赖它们原地不动。probe 侧的工件名（~/world/probe 的 `actor-guest`、
   `counter_actor`）不归本次改名管辖。
4. **"actor" 一词在讨论调度的行文里仍然合法**（"actor 模型从未要求确定性"
   读作对外部范式的指称；realm.md §5.1 的术语校准段维持对照的诚实）。被
   禁用的是把 actor 用作【我们的参与者】的名字。

备选考量：`worker`（被线程池义项过度占用，且会模糊 probe 的执行容量概念）、
`consumer`（对投递是 MQ 真话，但丢掉串行状态语义，且暗含只拉取）、`nexus`
（占了 realm 已持有的枢纽位）、`beacon`/`transponder`（意象鲜明，但方向性
意象与拨入/拨出的对称性打架）。选 `booth` 的决定性理由是方向中立，且它把
ADR-0031 的治理词汇自然承载（申报=摊货自报、审批=市场所有者自己的决定、
词表=市场的叫卖规矩）——隐喻在文档里干活，不只是装饰。

## 后果

- **本次提交：** aura 代码 + aura 文档清扫；realm.md §5.1 的配对句变成一个
  指针（术语校准段作为定义保留）。
- **跨仓对齐（同批落地）：** probe（注释/文档；线上本无
  `actor_type` 字段，无需帧改动）、prism（`actors.rs` → `booths.rs`、
  `echo_actors` → `echo_booths`、path 依赖）、gravity（PLAN/README 措辞）、
  okm 文档、wiki（中文术语用"摊位"；外部范式页不动）。
- **ADR-0031** 作为本次清扫的一部分移动改名为 `0031-remote-booths.*`；
  正文已按 booth 读通。
- **兄弟仓库的 Cargo.lock / path 依赖**随各自仓库的提交移动，不在本提交。
