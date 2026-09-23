# Aura 建模指南（Actors Guide）

如何把一个业务域建模为 Aura Actor：先选分区键（恒等归属判据），再定事件与 handler，状态只属于实例，跨实例协作走事件与投影。本文是应用侧的使用规范；机制细节见 [partitioning.md](partitioning.md)、[realm.md](realm.md)、[actor-api.md](actor-api.md)。

> **Languages:** 中文（本文）· [English](modeling-en.md)

## 第一步：为每种 Actor 选分区键

一个 Actor 类型 = 一份代码 + 按 partition key 展开的实例群。选键的判据是**实例的恒等归属**，不是请求碰巧携带的字段：

- **身份恒等于归属 → 用身份做键。** 用户级数据（购物车、session、user profile）按 user_id 分区。实例身份本身就编码了用户：handler 读 `ctx.self_id.key` 即得身份——这是构造级保证（实例按构造只属于自己的 key），比调用方自报身份更强，无需防伪造。
- **归属大于身份 → 用归属做键，身份走参数。** 群聊、房间、订单协作按 channel_id/room_id/order_id 分区：一个实例服务多个用户，user_id 不是实例的恒等属性。消息**自带归属键**（客户端知道发往哪个 channel，引擎无需查表），发送者身份作为请求参数携带（handler 内做成员校验、发言归因）。此时把 user_id 挂上 ctx 逻辑冲突——ctx 是 per-instance 的，挂上即意味着「本实例的 user」，而群聊实例没有「本实例的 user」。
- **单例（无键）→ 只用于全局观察者。** 通配订阅（`@on("order.*")`）与无键 handler 落单例实例——天然无状态分片意义，不要把可分区的数据放进来。

一句话：**分区键回答「这条消息该由谁串行处理」，请求参数回答「这次请求是谁发起的」**——两个问题各自独立作答，不互相挂载。

```
按 user_id 分区（购物车）           按 channel_id 分区（群聊）

emit("add_to_cart",               emit("channel_msg",
  { user_id: "u1", ... })           { channel_id: "c1",
       │                              caller: { user_id: "u1" }, ... })
       ▼                                   │
("cart", "u1") 实例                        ▼
handler 内 ctx.self_id.key = "u1"   ("channel", "c1") 实例
—— 身份来自实例，无需传             handler 参数里读 caller.user_id
                                       —— 归属来自分区键，
                                          身份来自请求参数
```

## 第二步：定事件与订阅面

事件是 Actor 之间的唯一协作通道（场域 pub/sub，见 actor-api.md）：

- **事件名是寻址名**：`@on(event, key=...)` 声明每个 handler 监听的事件；事件投递的分区键取自事件数据中的 key 字段（如 `key="channel_id"` 取 `data.channel_id`），取不到落 `__default__` 兜底实例——所以发出的事件**必须携带声明为 key 的字段**，否则全部堆进兜底实例。
- **emits 不声明**（ADR-0012）：接收者集合是运行时事实，无订阅者的事件落 dead-event ring（可观测审计面）。建模时不需要也不应该维护「谁在听」的清单。
- **一个队列多个订阅者**是结构性的：多个 Actor 类型可监听同一事件（如 `order.created` 同时被 `inventory` 和 `audit` 消费），各自有独立 cursor，互不干扰。
- **直接调用（`ctx_invoke`）是例外路径**：用于请求-响应形态，载荷必须声明目标 `{type, key, handler, args}`——能用事件表达的协作不用直接调用，事件留下投递记录且天然多订阅。

## 第三步：状态只放实例级

`ctx.state` 是本实例的 KV 状态（`ctx_state_get/set/delete`），按字段独立落盘、随激活/休眠整体载入写回。建模约束：

- **只放本实例恒等归属的数据**：购物车实例放条目列表；群聊实例放成员表、最近消息。把其它实例的数据复制进来会产生第二真相源。
- **跨实例读取不可表达**：这是设计而非限制——跨实例的数据协作走事件（对方 Actor 处理后 emit 结果），或直接调用取回。
- **实例状态不跨节点复制**（联邦裁决 ADR-0013）：数据跟随所属节点；节点级部署选择见 partitioning.md §5。

## 第四步：跨实例聚合走投影 Actor

跨分区查询（统计部门所有用户的购物车、某用户在哪些群）不能 JOIN 也不该全扫——用**投影 Actor**：

- 一个普通的事件接收 Actor，按自己的维度分区（如按 dept_id 或 user_id），`@on` 监听上游实例 emit 的事件，持续把事件聚合进自己的 ctx.state。
- 查询时直接读投影实例的状态（直接调用）——原理与流计算预聚合一致：不查询时计算。
- 投影是**可重建的衍生数据**：源头事件流是真相，投影状态丢失可从事件回放重建（mq 队列保留活跃订阅者水位线之上的数据）。

```
("cart","u1") ──emit cart_updated──► 事件队列
("cart","u2") ──emit cart_updated──►    │
                                        ▼
                        ("dept_stats", "d7")  ← 按 dept_id 分区的投影
                        ctx.state: { dept_total, ... }
                                        ▲
                            查询：ctx_invoke(dept_stats, "d7")
```

## 驻留（retention）：留不留，算三笔账

实例休眠不丢数据（状态落盘、事件留在队列），驻留省的是**下次激活的成本**：重新拉起 VM/probe、重载脚本、重建内存态。`idle_ttl` 按 Actor 类型声明（`with_idle_ttl` builder，声明于 `interface_schema` 的 lifecycle 段），不驻留是默认——留不留按激活成本与使用模式的对比判断：

- **状态修改类（加购物车、改密码）→ 不驻留**：下次操作何时出现无法预计，驻留是纯浪费。状态已落盘，激活重建即可。
- **无状态高扇出查询（商品列表）→ 看共享性**：每次查询内容不重复、无可缓存时，留的只是 probe/VM 启动开销。列表 per-user 不同，单用户刷新频率远低于启动成本 → 不驻留；列表全用户共享（实例是单例或按列表分区的少数实例），请求不断则实例常在 → 短驻留（如 10s idle_ttl）——效果即传统架构的缓存，但缓存的就是活实例本身，无需第二套缓存设施。
- **长驻会话（gravity 对话、chat channel）→ 长驻留**：实时流式应用，活跃 channel 的消息几乎不间断，驻留窗口内零激活开销。LLM 调用成本远高于实例驻留成本时，驻留是显然的选择——这正是 turn-executor（Phase 6.5）的形态：per-type 长 TTL，同 session 连续调用走内存 oneshot，turn 结束或窗口到期才释放。

判据一句话：**驻留省的钱（激活成本 × 窗口内预期到达量）大于驻留花的钱（内存 × 窗口时长）就留**——per-type TTL 是把这个判断变成一处声明的机制。

## 外发投递：场域的边界在哪里

场域的事件投递覆盖 Actor ↔ Actor；把消息送到场域外的连接（用户的 WS 长连接）是**外发投递**，由外层的连接平面（prism，Phase 8）承担——aura 不知道 WS 的存在：

```
群聊 channel actor (分区键 c1)
  │  handler 处理完一条群消息后，要发给 channel 内每个在线用户
  ▼
emit("outbound_message", { user_id: "u1", payload: ... })   ← 每个目标用户一条
emit("outbound_message", { user_id: "u2", payload: ... })
  │
  ▼  订阅：连接平面的 outbound 桥（单例消费者，prism 侧装配）
prism 按 payload.user_id 找到该用户的 WS 连接，逐一下发
  │  连接不存在（离线）→ 桥侧落 per-user 离线队列 / 丢弃（应用语义决定）
  ▼
用户浏览器收到推送
```

要点：

- **actor 只 emit，不寻址连接**：`emit` 的载荷带目标 user_id，WS 连接的注册表在连接平面——actor 层永远不需要知道「谁在线、连接在哪」，这同样是「分区键答路由、参数答身份」的边界应用。
- **outbound 桥是普通订阅者**：单例 wildcard 消费 `outbound.*`，无特殊通道——与投影 Actor 同构，只是它把「投」作为副作用而不是写状态。
- **反向（入站）已解**：客户端消息经 prism 按事件名投进场域（`emit("channel_msg", ...)`，分区键 channel_id 自带在消息里），见第一步的群聊例子。
- **离线用户**：emit 落在持久队列，per-user 的会话/离线逻辑由一个按 user_id 分区的普通 Actor（或连接平面的离线队列）承接——是应用建模决策，不是引擎机制。

## 第五步：业务数据导入导出走专用 Actor

引擎不提供业务数据面通道；与外部存储（S3、文件、外部数据库）的批量数据交互，用一个普通 Actor 承担：`@on("import_users")` 收一批数据 → handler 内经外部通道写入 → emit 完成事件。与投影 Actor 同构——同一套事件模型覆盖，不引入第二类基础设施。

## 选型速查

| 需求 | 形态 |
|---|---|
| 用户自己的数据 | 按 user_id 分区的 Actor，身份读 `ctx.self_id.key` |
| 多人共享的协作空间 | 按 channel_id/room_id 分区；归属键自带在消息里，发起者身份走参数 |
| 全局观察/审计 | 通配订阅单例 Actor |
| 跨分区统计/反查 | 投影 Actor（按聚合维度分区） |
| 请求-响应 | `ctx_invoke`（声明 handler），事件优先 |
| 外部数据导入导出 | 专用 Actor 包装 |
| 长时等待外部结果 | timer wheel / 事件回投，不占驻留 |
| 驻留决策 | per-type `idle_ttl`：改状态类不留；共享高频查询短留（缓存形态）；长会话/LLM 调用长留 |
| 推送给在线用户 | emit `outbound_message`（载荷带 user_id）→ 连接平面的 outbound 桥下发 WS；离线走 per-user 队列 |
