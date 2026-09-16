# 场域模型（设计细节）

> 自 `~/.hermes/wiki/aura-architecture.md` §5 迁入的实现细节；wiki 保留综述。
> 综述：wiki [Aura 架构 §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md)。
> 相关：[数据分区（内部机制）](partitioning.md)、[Actor API（脚本语言参考）](actor-api.md)。

## 5. 场域模型：Actor 间交互与外部世界

### ctx 边界：什么在 ctx 上，什么不在

注入 Actor 入口函数的 `ctx` 只收**实例身份相关 + 需要 Host 管控/记录**的运行时能力：

| 在 ctx 上 | 职责 |
|:--|:--|
| `ctx.state` | 本实例状态（KV，Fjall/SlateDB，不走网络路径） |
| `ctx.metadata` | 受控元数据（meta okm 实例，控制平面单写 + 节点缓存；不做跨节点全局同步） |
| `ctx.invoke()` | 唯一受控调用面——超时、审计、限流、可观测收口于此（§5.13） |

不在 ctx 上的能力与其归属：

- **emit / on**：场域 pub/sub，脚本层裸函数（emit）与激活期装配（on，对应 interface_schema 的静态契约）。事件投递的 partition 来自事件数据而非发射者身份，不依赖实例；emit 是进程内 fire-and-forget，没有需要管控的生命周期；on 放 ctx 会暗示运行时动态订阅，与静态契约矛盾。
- **interface_schema() / set()**：定义期契约与部署面，执行中的 Actor 看不到。
- **on_sleep / on_wake**：生命周期钩子是 Host → Actor 方向，ctx 是 Actor → Host 方向的使用接口，两者方向相反。
- **入口函数 return**：语言原生行为，Host 拦截填入 reply_to，不需要 `ctx.return()`。
- **@cron / on_debounce**：定时是声明式触发模式（投递事件唤醒 Actor），不是可调用的定时 API（无 `ctx.sleep()`/`ctx.every()`）。
- **日志、纯计算、语言标准库**：凡需 Host 管控的外部交互都经 `ctx.invoke()` 注册目标编址，其余用宿主语言原生设施。

新能力按此判据归位：实例绑定 + Host 管控 → 进 ctx；静态契约 / Host 驱动 / 场域层 → 排除。决策记录见 Aura 仓库 ADR-0011。

### 5.1 核心设计

传统 Actor 框架（Akka、Erlang、Actix）的通信原语是 ActorRef 直发（tell/ask）——调用者必须知道目标 Actor 的地址。Aura 采用不同的原语：Actor 之间不直接寻址，而是通过共享的"场域"（Event Realm）用 `emit` / `on` 交互。

场域是引擎内部的事件空间。Actor 通过 `on(name, fn)` 订阅事件、`emit(name, data)` 发射事件。发射者不关心谁处理，处理者不关心谁发射。外部世界（HTTP/WS）的协议层由调用方（Fluxora、网关）处理——Fluxora 将外部请求转成 `emit()`，将 `on()` 事件转成 HTTP 响应或 WS 推送。Aura 引擎本身不碰 HTTP/WS。

**开发者体验方向**：Fluxora 不做 MQ（Kafka/NATS 已去掉），只做 HTTP/WS 协议桥接。进一步的 DX 目标：Web 控制台 + 嵌入 VSCode，开发者直接在浏览器里写 Python Actor。Actor 之间只管发消息，存数据由框架处理。这是 FaaS + Web 框架的融合形态——不是"给你一个数据库让你写 CRUD"，而是"给你一个事件空间让你编排 Actor"。

### 5.2 场域拓扑

```
┌─────────────── Event Realm (namespace: default) ───────────────┐
│                                                                │
│  Ingress（入口）                                                │
│  ┌──────────┐                                                  │
│  │ Fluxora  │  emit("order_created", data)                     │
│  │ Webhook  │───────┐                                          │
│  └──────────┘       │                                          │
│                      ▼                                         │
│               ┌────────────┐     emit("order_completed")      │
│               │  Actor A   │──────────────────┐                │
│               │ on("order_ │                   │                │
│               │  created") │                   ▼                │
│               └────────────┘          ┌────────────┐          │
│                                        │  Actor B   │          │
│  ┌──────────┐                          │on("order_  │          │
│  │  Actor C │◄──emit("inventory_upd.") │completed") │          │
│  │ on("inv_ │         ┌────────────┐   └────────────┘          │
│  │  updated")│        │  Actor D   │        │                  │
│  └──────────┘        │on("order_  │        │ emit("notif.")   │
│       │ emit("shipped")│completed")│        │                  │
│       ▼               └────────────┘        ▼                  │
│  ┌──────────┐                                   Egress（出口）  │
│  │  Actor E │                          ┌──────────┐            │
│  │on("ship- │                          │ Fluxora  │            │
│  │ ped")    │                          │on("notif"│ → WS push │
│  └──────────┘                          └──────────┘            │
└────────────────────────────────────────────────────────────────┘
```

- 事件名标记入口/出口语义（Fluxora 的模式）
- Actor 不感知协议（HTTP/WS），只收发事件
- 元数据不跨节点同步：每节点独立 meta 实例（控制平面单写）；Actor 状态事件走 SlateDB+S3 或本地 Fjall；联邦节点间走 well-known 协议认证身份

### 5.3 Actor 定义接口

```
set(<lang>, <script/wasm>)
```

提交或更新一个 Actor **定义**（类型）。`lang` ∈ {Steel, Python, Wasm}。脚本内以 `@on` 装饰器（或 steel `on` 函数 / wasm 导出约定）声明多入口 handler（事件名映射为函数参数），`interface_schema` 由装饰器推导（手写可覆盖 lifecycle）。名字为 `execute` 的 handler 是直接调用通道（`ctx.invoke`）的目标，无特权。运行时调用 `set()` 可热替换 Actor 实现——不仅换行为，还换语言。

`set()` 定义的是类型，不是实例。Actor 实例由 Realm 根据 partition key 按需激活（详见 [§5.11](#511-actor-实例化与分片)）。

**脚本持久化**：`set()` 提交的脚本内容（或 Wasm 字节码）存储在 **meta okm 实例**（与 Actor 状态的 data 实例分离，Phase 4 两实例模型；当前实现为 `actor/src/persist.rs` + `meta_engine`/`meta_dir` 配置），不从文件系统读取。脚本是静态资产，跨节点同步走文件系统（git/S3）。存储引擎天然支持版本化，每次 `set()` 保留新版本，旧版本可回滚。脚本条目附带元数据（提交时间、语言类型、版本号、提交者、内容哈希），存储结构：

```
meta instance, partition: "actor_defs"
  key:   <actor_type_name>
  value: CBOR { lang, script_bytes, version, content_hash, committed_at, committed_by }
```

**去重**：`set()` 提交前先计算 `script_bytes` 的哈希（content_hash），与 meta 实例中最新版本的 `content_hash` 比较——相同则忽略，不写入新版本。避免 CI 重复部署或无意义的热重载。

**三条生命周期线分离**（Phase 4.5b 裁决）：上传（`set`）是独立生命周期——上传时 Host 自省 `interface_schema()` 一次，元数据（receives/emits/lifecycle）与定义一并持久化；执行永不调用 `interface_schema`——消息处理只加载脚本（最新版本）调 handler，元数据从 meta store 读取；版本变更（新 `set`）重新自省一次、更新持久化元数据，此前旧元数据治理。已实现：`PersistedActor` 记录 + `engine.register` 持久化 + boot 重载（见 PLAN Phase 4.5b）。

`on()` handler 在 Actor 实例激活时从 meta 实例读取最新版本的脚本，加载到对应 VM 执行。实例驱逐后，下次激活重新从 meta 实例读取。

### 5.4 interface_schema()

脚本元数据的统一声明面。注册时 Host 调用一次（纯函数，无 ctx，方向是 Host ← 脚本——脚本从不反向访问 engine）。返回结构的 `lifecycle` 段声明驻留策略：`"idle_ttl"` 接受秒数或带单位字符串（`"300s"` / `"5m"` / `"2h"`，单位必填）；Host 侧 builder（Rust 类型）显式声明的值优先于脚本自省值。注意 carrier 契约：入口函数统一带一个参数调用，`interface_schema(args)` 需声明形参（语言允许默认值时可用 `def interface_schema(args=None)`）。

`interface_schema()` 是 Actor 的**统一契约**——声明接收事件，以及可选的发射事件清单。

**核心洞察：事件名就是引用。** 当 `interface_schema()` 声明 Actor B 接收 `"charge"` 事件时，任何人 emit `"charge"` 就是在引用 B。事件名 = 引用，partition key = 实例定位。不需要单独的 `invoke` / `direct_send` 原语——emit 本身就是引用调用。

```python
def interface_schema():
    return {
        "receives": {
            "add_to_cart": {
                "mode": "on",
                "key": "user_id",
                "params": {"type": "object", "properties": {
                    "user_id": {"type": "string"},
                    "item": {"type": "object"}
                }}
            },
            "remove_from_cart": {
                "mode": "on",
                "key": "user_id",
                "params": {"type": "object", "properties": {
                    "user_id": {"type": "string"},
                    "item_id": {"type": "string"}
                }}
            }
        },
        "returns": {
            "type": "object",
            "properties": {
                "cart_count": {"type": "integer"},
                "total": {"type": "number"}
            },
            "required": ["cart_count", "total"]
        },
        "emits": ["cart_updated"]
    }
```

**`receives` 中每个事件声明：**
- `mode`：触发模式——`on`（单事件）、`on_join`（多事件收齐）、`on_batch`（同类打包）、`on_debounce`（去抖）。详见 [§5.6 事件组合原语](#56-事件组合原语)
- `key`：partition key 字段——Realm 按此字段值路由到 Actor 实例
- `params`：入参 JSON Schema

**`returns` 是 Actor 级别的单一返回值声明。** 每个 Actor 有一个入口函数，多个事件映射到多个参数，返回一个值：

```python
# Actor 入口函数 — 多事件映射到多参数
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    if add_to_cart:
        ctx.state["items"].append(add_to_cart["item"])
    if remove_from_cart:
        ctx.state["items"] = [i for i in ctx.state["items"]
                              if i["id"] != remove_from_cart["item_id"]]
    emit("cart_updated", {"user_id": ..., "items": ctx.state["items"]})
    return {"cart_count": len(ctx.state["items"]),
            "total": sum(i["price"] for i in ctx.state["items"])}
```

- 声明了 `returns` 的 Actor 支持 `ctx.invoke()` 同步调用——调用者通过 Actor 名获取返回值
- 不声明 `returns` 的 Actor 只能通过 `emit()` 异步触发（fire-and-forget）
- `ctx.invoke("cart_actor", data)` → 调用入口函数 → 返回单一值

| 用途 | 说明 |
|------|------|
| **内部事件** | 声明事件名即可，消费方的 `on` 已包含 schema |
| **安全管控** | Realm 强制白名单：只允许 `emits` 中声明的事件名被发射，未声明的拒绝 |
| **图表生成** | 声明的事件名可用于自动绘制 Actor 间事件流拓扑图 |

`emits` 声明的是**约束**——Actor 只能发射列表中的事件名。emit 时 Realm 校验事件名是否在 `emits` 声明中，未声明的拒绝发射。不声明 `emits` = 不能 emit 任何事件（纯接收型 Actor）。

**外部订阅者**：Fluxora、Webhook、分析系统等外部消费者是 Realm 的订阅者——和 Actor 共享同一个路由表，使用相同的 `RouteMode`（on / join / batch / debounce），只是投递目标不同（Actor → 入口函数，外部 → HTTP/WS/Webhook）。Realm 的 `emits` 约束了哪些事件可出界，外部系统从声明列表中选择订阅。

```
# Fluxora：去抖推 WS
Route { target: "fluxora", event: "cart_updated", mode: Debounce(300ms) }

# Webhook：单事件触发
Route { target: "webhook:order-service", event: "order_completed", mode: On }

# 分析系统：join 后批量发送
Route { target: "analytics", events: ["payment", "inventory"], mode: Join(5s) }
```

外部订阅者不需要声明 `interface_schema()`——它们是 Realm 的消费者，不是 Actor。路由表由 Realm 管理，外部系统通过配置（或 API）声明订阅关系。

**事件名 = 引用（路由机制）：**

Host 启动时调用 `interface_schema()`，构建事件路由表：

```
事件名 → partition key 字段 → params schema → returns? → Actor 定义 → on handler
```

当 A emit `"add_to_cart"` 时：
1. Realm 查路由表 → `"add_to_cart"` 只有 Cart Actor 注册了
2. 从事件数据提取 `key` 字段值（`user_id`）→ 路由到 Cart Actor 的对应实例
3. 校验载荷是否符合 `params` schema
4. 投递到 `on("add_to_cart")` handler

A 通过事件名精确指向了 Cart Actor——事件名就是引用，schema 就是类型签名。Host 在投递前可校验事件载荷是否符合 schema；Fluxora 可从 schema 自动生成 TypeScript 类型定义。

**ctx.invoke() 同步调用：**

Actor 声明了 `returns`（JSON Schema）时，调用者可以通过 `ctx.invoke()` 同步获取 Actor 入口函数的 return 值：

```python
# 调用方
result = await ctx.invoke("cart_actor", data={"user_id": "42", "item": {...}})
# result = Actor 入口函数的 return 值

# 被调用方（Cart Actor 入口函数）
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    if add_to_cart:
        cart = add_item(add_to_cart["user_id"], add_to_cart["item"])
    return {"cart_count": len(cart), "total": sum(i["price"] for i in cart)}
```

Realm 内部通过 reply_to 机制实现同步：emit 事件 + `__reply_to` → 等待入口函数 return → 返回给调用者。入口函数只需正常 `return`，不需要关心 reply_to 细节。

入口函数同时可以 `emit` 通知其他 Actor——`return` 给调用者，`emit` 给系统，各走各的路。

**fire-and-forget：**

没有 `returns` 的 Actor 只能通过 `emit()` 触发。调用者不等待返回值，入口函数的 return 值被丢弃。

**emit 到有 `returns` 的 Actor（允许）：**

`returns` 声明的是**能力**（"这个 Actor 可以返回值"），不是**约束**（"只能同步调用"）。`emit()` 到有 `returns` 的 Actor 是合法的——入口函数正常执行，return 值丢弃。这解锁了触发但不等待的模式：

```python
# 你关心的是"这件事发生"，不关心结果
emit("charge", {"user_id": "42", "amount": 100})
# 入口函数执行了（扣款、发通知），return 值丢弃

# 同一 Actor 也可以同步调用
result = await ctx.invoke("charge_processor", {"user_id": "42", "amount": 100})
```

`interface_schema()` 也是热重载的入口：脚本修改 → 重新加载 → 重新 `interface_schema()` → 路由表更新。

### 5.5 事件 API

**emit(name, data)**：向 Realm 提交事件。data 是 `ciborium::Value`（内存值树）。进程内 Actor ↔ Actor 传递时保持 Value 形态，通过 Tokio MPSC channel clone，零序列化。序列化只在跨边界时发生：

| 边界 | 格式 | 说明 |
|------|------|------|
| 进程内 Actor ↔ Actor | `ciborium::Value` | 内存 clone，零编解码 |
| Actor → Fjall 持久化 | CBOR bytes | `ciborium::serialize()` 写入 LSM-Tree |
| 元数据（无跨节点复制） | — | 各节点 meta 实例独立，控制平面单写 |
| Actor → Fluxora（HTTP/WS） | JSON | 外部系统消费 JSON |
| Actor → Webhook | CBOR 或 JSON | 按配置选择 |

**on(name, fn)**：注册事件监听。每个 Actor 有一个入口函数，事件名映射为函数的 keyword 参数——多个 `on` 声明编译为单一 dispatch 函数。Realm 内部按事件名路由到对应的参数。状态共享通过 `ctx`（Actor 的统一状态树），不依赖闭包捕获。

**通配符监听**：`on()` 支持前缀通配符 `"prefix.*"`，匹配所有以 `prefix.` 开头的事件。与 etcd 的 key 前缀匹配类似——适用于日志、审计、投影 Actor 等需要监听一类事件的场景：

```python
# 监听所有 order 相关事件
@on("order.*")
def handle_order_events(ctx, data):
    log(ctx, data)
    # 投影 Actor：把 order.* 事件聚合到部门统计
    emit("dept_stats_updated", aggregate(data))
```

通配符声明和精确声明可以共存——事件同时匹配通配符参数和精确参数，各自独立路由到入口函数的对应参数。通配符声明不指定 partition key（它监听一类事件，不绑定具体实体），路由到场域中该 Actor 的单例实例。

**通配符声明**：通配符参数不在 `interface_schema()` 的 `receives` 中声明具体事件名，而是用 `wildcard_receives` 列表：

```python
def interface_schema():
    return {
        "receives": {
            "add_to_cart": {"key": "user_id", "schema": {...}},
            "remove_from_cart": {"key": "user_id", "schema": {...}}
        },
        "wildcard_receives": ["order.*"],  # 前缀通配符
        "emits": ["dept_stats_updated"]
    }
```

**前缀匹配规则**：`on("order.*")` → 前缀 `"order."`，匹配 `order_created`、`order_completed`、`order_cancelled`。不匹配 `order`（无 `.` 分隔）。不支持后缀通配（`*.created`）或中间通配（`order.*.created`）——只有前缀匹配，与 etcd 一致。

**路由表结构**：

```rust
pub struct EventRouter {
    exact: HashMap<String, Vec<Route>>,      // 精确匹配：事件名 → 路由规则
    wildcard: Vec<WildcardRoute>,             // 通配符匹配：前缀 → 路由规则
}

struct Route {
    actor_type: String,
    partition_key_field: String,              // 从事件数据中取哪个字段作为 key
    handler: HandlerRef,
}

struct WildcardRoute {
    prefix: String,                           // "order."（去掉 .* 后的前缀）
    actor_type: String,
    handler: HandlerRef,
    // 无 partition_key_field —— 通配符 handler 是单例实例
}
```

通配符用 `Vec` 而非 HashMap，因为匹配是反向的（给定事件名，找哪些前缀能匹配），HashMap 帮不上忙。通配符数量通常很少（几个投影 Actor），线性扫描足够。如果通配符多到成为瓶颈，再换 Trie。

**分发路径（Phase 4.5c 事件队列模型，已实现）**：

```rust
impl Realm {
    // emit 的投递目标不是实例 mailbox，而是 (声明事件, partition) 队列。
    // 两段式：先激活全部匹配路由的目标实例（订阅先于发送），再按队列去重发送。
    async fn emit(self_arc: &SharedRealm, emitter: Option<&str>,
                  event: &str, data: Value) -> anyhow::Result<()> {
        let routes = self.router.matches(event);
        if routes.is_empty() { self.dead_events.push(event, data); return Ok(()); }

        let mut queued: HashSet<(String, String)> = Default::default();
        let mut targets = Vec::new();

        // 第一段：激活 + 去重
        for route in routes {
            // 队列身份：@on 声明了 key → (route 事件, partition)；
            // 未声明 key → (route 事件, "__singleton__")。
            // partition 值来自事件数据（key 字段取值），不来自发射者。
            let partition = if route.partition_key_field.is_empty() {
                "__singleton__".to_string()
            } else {
                data.get(&route.partition_key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("__default__").to_string()
            };
            // 虚拟 Actor 语义：emit 到未激活的实例先激活它——
            // 它的 @on 订阅在消息入队前绑定（每个订阅者私有 Receiver，
            // 即 per-subscription cursor；实例不拥有队列）。
            let target = InstanceId { actor_type: route.actor_type.clone(), key: partition.clone() };
            if !self.instances.contains_key(&(route.actor_type.clone(), partition.clone())) {
                self.instance(self_arc.clone(), &target).await?;
            }
            // 按队列去重：两条路由绑定同一队列（多个订阅类型监听同一事件）
            // 时只发送一次——队列扇出到全部订阅者，重复发送会双重投递。
            if queued.insert((route.event.clone(), partition.clone())) {
                targets.push((route, partition));
            }
        }

        // 第二段：向每个队列广播一次
        for (route, partition) in targets {
            let queue = self.event_queues
                .entry((route.event.clone(), partition))
                .or_insert_with(|| broadcast::channel(self.mailbox_capacity).0);
            if queue.receiver_count() == 0 {
                // 活过又离开的订阅者是自己的信号；刚激活的已有 Receiver。
                self.dead_events.push(event, data.clone());
                continue;
            }
            let _ = queue.send(QueuedJob { handler: event.into(), args: data.clone() });
        }
        Ok(())
    }
}
```

**队列语义**：

- 事件不属于任何 Actor。队列在场域层，实例激活时按其类型的 `@on` 声明绑定订阅（私有 Receiver = per-subscription cursor）。
- 一个队列可有多个订阅者（多个 Actor 类型监听同一事件）——一对多投递是结构性的，不是 fan-out 模拟。
- 串行语义：每实例的订阅消费任务一次只处理一条（逐队列顺序 drain）——同一实例串行由 cursor 保持，实例不拥有队列。
- 直接调用不走队列：`ctx.invoke` / `engine.invoke` 是点对点（实例 mailbox 保留用于统一调用模型），事件投递才走共享队列。

**通配符参数的实例化**：通配符参数不绑定 partition key，路由到固定 key `"__singleton__"` 的实例——整个 Actor 类型只有一个实例。这与投影 Actor 的场景一致：一个 DeptStatsActor 实例监听所有 `order.*` 事件，持续聚合。

**精确 + 通配符同时匹配**：一个事件可以同时命中精确参数和通配符参数，各自独立投递：

```
emit("order_created", {"user_id": "A", ...})

→ 精确匹配：CartActor 的 on("order_created")，partition key = "A"
→ 通配符匹配：DeptStatsActor 的 on("order.*")，单例实例

两个 Actor 实例各自独立处理，互不阻塞。
```

```python
from aura import emit

# Actor 入口函数 — 多事件映射到多参数
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    if add_to_cart:
        ctx.state["items"].append(add_to_cart["item"])
        # 已落盘（WAL + memtable）
    if remove_from_cart:
        ctx.state["items"] = [i for i in ctx.state["items"]
                              if i["id"] != remove_from_cart["item_id"]]
        # 已落盘（WAL + memtable）
    emit("cart_updated", {"user_id": ..., "items": ctx.state["items"]})
```

```scheme
;; Steel Lisp — schema 内嵌在 on 调用中，Realm 从 AST 自动提取

(on "add_to_cart"
  (schema
    (key "user_id")
    (params (hash 'type "object"
                  'properties (hash 'user_id (hash 'type "string")
                                      'item (hash 'type "object"))
                  'required '("user_id" "item"))))
  (lambda (ctx data)
    (ctx-update! ctx "items"
      (lambda (items) (append items (list (hash-ref data "item")))))
    ;; 已落盘（WAL + memtable）
    (emit "cart_updated"
      (list (cons "user_id" (hash-ref data "user_id"))
            (cons "items" (ctx-ref ctx "items"))))))

(on "remove_from_cart"
  (schema
    (key "user_id")
    (params (hash 'type "object"
                  'properties (hash 'user_id (hash 'type "string")
                                      'item_id (hash 'type "string"))
                  'required '("user_id" "item_id"))))
  (lambda (ctx data)
    (ctx-update! ctx "items"
      (lambda (items)
        (filter (lambda (i) (not (equal? (hash-ref i "id") (hash-ref data "item_id"))))
                items)))
    ;; 已落盘（WAL + memtable）
    (emit "cart_updated"
      (list (cons "user_id" (hash-ref data "user_id"))
            (cons "items" (ctx-ref ctx "items"))))))
```

```rust
// Wasm — interface_schema 是 well-known 导出函数
// Host 调用 module.call("interface_schema", "") 拿 JSON
// on() handler 通过 Wasm export 函数注册
```

### 5.6 事件组合原语

事件到达即处理。但很多业务场景需要跨事件的时间或空间聚合。Realm 内置四种触发模式，覆盖不可分解的事件组合需求。

#### 不可分解性分析

能否拆成多个事件映射 + actor 组合，是判断"是否需要 Realm 原语"的标准：

| 组合模式 | 能否分解 | 理由 |
|---------|---------|------|
| **合并同类事件** | ✅ 能 | 多个事件映射到同一入口函数的多个参数，共享逻辑 |
| **多事件收齐（join）** | ❌ 不能 | 需等待多个事件全部到达后合并投递，单个参数无法触发 |
| **同类事件打包** | ❌ 不能 | 计数/窗口状态横跨多次事件到达，actor 无跨事件状态能力 |
| **去抖** | ❌ 不能 | 计时器必须在 Realm 层，actor 无时间感知 |

#### 四种触发模式

**`on` — 单事件触发（现有）**

```python
@on("order_created")
def handle(ctx, data):
    # data = 单个事件的载荷
    ...
```

**`on_join` — 多事件收齐**

等待多个**不同类型**的事件全部到达后触发。入口函数的 join 参数接收 map，按事件名索引。

```python
@on_join("payment_received", "inventory_reserved",
         timeout_ms=5000,
         schema={
             "payment_received": {"type": "object", "properties": {
                 "amount": {"type": "number"}
             }},
             "inventory_reserved": {"type": "object", "properties": {
                 "sku": {"type": "string"}, "qty": {"type": "integer"}
             }}
         })
def handle(ctx, data):
    # data = {
    #     "payment_received": {"amount": 100},
    #     "inventory_reserved": {"sku": "abc", "qty": 2}
    # }
    process_order(data["payment_received"], data["inventory_reserved"])
```

Realm 内部维护 `Collector`：追踪每个事件是否到达，收齐后合并投递到入口函数。超时未收齐时仍触发，数据中带 `__partial` 标记。

**Collector 是瞬态状态，不持久化到 Fjall。** 崩溃恢复后，源 actor 从 Fjall 恢复状态并重新 emit 事件，Collector 从零开始重新收集。持久化的锚点是 Actor 状态（写入 Fjall），不是事件聚合的中间缓冲区。

```rust
struct Collector {
    expected: HashSet<String>,              // 期望的事件集
    collected: HashMap<String, Value>,      // 已收集的事件
    window: Duration,                       // 超时窗口
    created_at: Instant,
}

impl Collector {
    fn is_complete(&self) -> bool {
        self.expected.iter().all(|e| self.collected.contains_key(e))
    }

    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.window
    }

    fn drain(&mut self) -> Value {
        // 返回合并后的 map，未到达的事件标记 null
        let mut result = Map::new();
        for event in &self.expected {
            let value = self.collected.remove(event)
                .unwrap_or(Value::Null);
            result.insert(event.clone(), value);
        }
        Value::Map(result)
    }
}
```

**`on_batch` — 同类事件打包**

收集**同一类型**事件，按数量或时间窗口打包后触发。入口函数的 batch 参数接收数组。

```python
@on_batch("order_created", count=5, window_ms=10000,
          schema={"type": "array", "items": {
              "type": "object",
              "properties": {
                  "user_id": {"type": "string"},
                  "item": {"type": "object"}
              }
          }})
def handle_batch(ctx, events):
    # events = [
    #     {"user_id": "A", "item": {...}},
    #     {"user_id": "B", "item": {...}},
    #     ...
    # ]
    # 攒够 5 个或 10 秒窗口到期，取先到者触发
    bulk_insert(events)
```

**`on_debounce` — 去抖**

最后一次事件到达后，静默指定时间才触发。入口函数的 debounce 参数接收单个对象（最后一次事件的载荷）。

```python
@on_debounce("search_keystroke", delay_ms=300,
             schema={"type": "string"})
def handle_search(ctx, query):
    # 用户停止输入 300ms 后触发，query 是最后一次输入
    results = search(query)
    emit("search_results", results)
```

#### 触发模式与数据结构

| 模式 | 事件类型 | 入口函数参数 | Realm 状态 |
|------|---------|-------------|-----------|
| `on` | 单一 | 单个对象 | 无 |
| `on_join` | 多种不同 | map（key = 事件名） | Collector + timer |
| `on_batch` | 同一种 | 数组 | Counter + window timer |
| `on_debounce` | 单一 | 单个对象 | Timer + pending value |

#### 自动推导的 schema 格式

无论 Python 装饰器还是 Steel AST 提取，Realm 最终构建的路由表 schema 格式统一如下：

```python
def interface_schema():
    return {
        "receives": {
            "add_to_cart": {
                "mode": "on",
                "key": "user_id",
                "params": {"type": "object", "properties": {
                    "user_id": {"type": "string"},
                    "item": {"type": "object"}
                }}
            },
            "order_ready": {
                "mode": "join",
                "events": ["payment_received", "inventory_reserved"],
                "timeout_ms": 5000,
                "params": {
                    "type": "object",
                    "properties": {
                        "payment_received": {"type": "object"},
                        "inventory_reserved": {"type": "object"}
                    }
                }
            },
            "bulk_orders": {
                "mode": "batch",
                "source": "order_created",
                "count": 5,
                "window_ms": 10000,
                "params": {
                    "type": "array",
                    "items": {"type": "object", "properties": {
                        "user_id": {"type": "string"},
                        "item": {"type": "object"}
                    }}
                }
            },
            "search_input": {
                "mode": "debounce",
                "source": "search_keystroke",
                "delay_ms": 300,
                "params": {"type": "string"}
            }
        },
        "emits": ["cart_updated"]
    }
```

`interface_schema()` 由 Realm 自动推导，开发者不需要手写独立的 schema 声明。两种语言的机制不同，但目标一致：**schema 跟着事件声明走，单点维护，不可能不同步**。

**Python**：装饰器收集。`set("python", "actor.py")` 时 Realm import 模块 → 扫描所有 `@on` / `@on_join` / `@on_batch` / `@on_debounce` 装饰器 → 自动构建 `interface_schema()` → 注册路由表。

**Steel**：AST 提取。S-表达式天然可遍历，Realm 解析 `(on ...)` 调用的 AST，从内嵌的 `(schema ...)` 表达式中提取事件名和 schema。`set("steel", "actor.scm")` 时 Realm 读取源码 → sexp parser 解析 → 扫描所有 `(on ...)` 顶层调用 → 提取事件名 + schema → 构建路由表。不需要开发者手写独立的 `(define (interface-schema) ...)`。

```scheme
;; Steel — schema 内嵌在 on 调用中
(on "add_to_cart"
  (schema
    (key "user_id")
    (params (hash 'type "object"
                  'properties (hash 'user_id (hash 'type "string")
                                      'item (hash 'type "object"))
                  'required '("user_id" "item"))))
  (lambda (ctx data)
    (ctx-update! ctx "items"
      (lambda (items) (append items (list (hash-ref data "item")))))
    (emit "cart_updated"
      (list (cons "user_id" (hash-ref data "user_id"))
            (cons "items" (ctx-ref ctx "items"))))))

(on_join ("payment_received" "inventory_reserved")
  (schema
    (timeout_ms 5000)
    (params (hash 'type "object"
                  'properties (hash 'payment_received (hash 'type "object")
                                      'inventory_reserved (hash 'type "object")))))
  (lambda (ctx data)
    (let ((payment (hash-ref data "payment_received"))
          (inventory (hash-ref data "inventory_reserved")))
      (process-order payment inventory))))

(on_batch "order_created"
  (schema
    (count 5)
    (window_ms 10000)
    (params (hash 'type "array"
                  'items (hash 'type "object"
                               'properties (hash 'user_id (hash 'type "string")
                                                   'item (hash 'type "object"))))))
  (lambda (ctx events)
    (bulk-insert events)))
```

#### 路由表结构扩展

```rust
enum RouteMode {
    On,                                              // 单事件，现有逻辑
    Join { events: Vec<String>, timeout_ms: u64 },   // 多事件收齐
    Batch { source: String, count: usize, window_ms: u64 },  // 同类打包
    Debounce { source: String, delay_ms: u64 },       // 去抖
}

struct Route {
    actor_type: String,
    mode: RouteMode,
    partition_key_field: Option<String>,  // on 有，join/batch/debounce 可选
    handler: HandlerRef,
}
```

路由表从"事件名 → Route"变为"事件名 → Vec<Route>"——同一事件可以被多个不同模式的 Route 匹配：

```
"order_created" → [
    Route { mode: On, handler: CartActor },           // 精确单事件
    Route { mode: Batch, handler: BulkWriter },        // 5 个打包
]
```

Realm 分发时，对每个匹配的 Route 按 mode 分别处理：On 直接投递，Join/Batch/Debounce 交给 Collector。

### 5.7 投递语义

| 维度 | 选择 | 说明 |
|------|------|------|
| **可靠性** | at-least-once | 事件不丢，可能重复。消费端需幂等 |
| **命名空间** | namespace（默认 "default"） | 场域按 namespace 隔离，跨 namespace 的事件不投递 |
| **顺序保证** | 因果一致性 | 如果 e1 因果先于 e2（e1 的处理导致 e2 的发射），则任何订阅者收到 e1 必在 e2 之前。无因果关系的并发事件可乱序 |

因果一致是甜区：保证逻辑正确性（因先于果），不需要全序的共识开销。进程内事件天然因果有序（同一线程内的 emit 序列）；同节点并发 Actor 的事件用向量时钟标记 happened-before 关系。元数据不跨节点复制（每节点独立），跨节点次序问题在单写入点模型下不存在。

### 5.8 传统 Actor 模型可借鉴的设计

| 机制 | 来源 | Aura 对应 |
|------|------|-----------|
| **Supervision** | Erlang/OTP | Actor 崩溃时从 Fjall 恢复状态、重新加载脚本和入口函数。策略：one-for-one（独立重启）/ one-for-all（关联 Actor 一起重启，防止状态不一致） |
| **Location Transparency** | Akka | 刻意不采纳（联邦裁决）：目标地址含节点域是特性——跨域交互显式寻址，域内 emit/on 才是透明的 |
| **Become/Unbecome** | Akka | `set(lang, script)` 是更激进的版本——运行时切换行为函数和语言。可实现状态机：收到事件后 `set("python", "active_handler.py")` 切换入口函数 |
| **Stash** | Akka | Actor 刚唤醒、Fjall 状态还在恢复时，先 stash 事件，恢复完成后回放 |
| **Dead Letters** | Akka/Erlang | emit 的事件如果没有匹配的接收 Actor，或目标 Actor 崩溃且无 supervisor 重启，进入 dead event log。用于调试和事件审计 |
| **Backpressure** | Reactive Streams | 事件队列（bounded broadcast channel，容量 = mailbox capacity）满了时，emit 方收到压力信号（阻塞/降级/丢弃）；单个订阅者跟不上时自己收到 Lagged 信号（丢弃计数可见） |
| **Passivation / Virtual Actor** | Akka / Orleans | 空闲 Actor 从内存驱逐（Scale-to-Zero），按需激活。Aura 的实例化机制基于此（详见 [§5.11](#511-actor-实例化与分片)） |

### 5.9 与现有 Actor 框架的对比

| 框架 | 类似点 | 关键差异 |
|------|--------|---------|
| **Akka EventStream** | 进程内事件总线，pub/sub | JVM/GC；EventStream 是 Actor 通信的补充，主通信仍 ActorRef 直发；无嵌入式存储，无 Scale-to-Zero |
| **Erlang gen_event** | 事件管理器，多订阅者 | BEAM 限定；无嵌入式多语言；跨节点靠分布式 Erlang，无强一致共识 |
| **Orleans Virtual Actor** | Actor 按需激活，"一直存在" | .NET 运行时；Grain 间通信是直接方法调用，不是事件总线；无嵌入式 KV |
| **Proto.Actor** | Go 实现，有 EventStream | Go runtime；事件总线是辅助；无嵌入式存储 |
| **Actix（Rust）** | Rust Actor 框架 | 无事件总线（Actor 间直发）；无分布式；无嵌入式脚本/沙箱；无共识 |
| **tellus（Rust）** | 经典近距离 Actor：状态机建模，`Message/State/Error` 三关联类型 | 无事件总线、无分布式、无嵌入式存储；但状态机建模的多处设计值得借鉴（见下） |

没有现有框架同时做到：事件总线作为主通信原语 + 嵌入式多语言 + 嵌入式 KV（Scale-to-Zero）+ 无外部消息队列 + 无共识依赖的多机组网。

### 5.9.1 tellus 状态机建模的可借鉴之处

tellus 的核心立场是**「actor 是状态机，而不是带可变字段的对象」**——`State` 作为关联类型按值传入 `receive(state, msg) -> Control::Continue(next)/Stop`，可变数据全塞进 State 跨消息传递，actor 值本身只是 unit struct。这与 Aura 的 `ctx.state`（统一状态树、handler 内原地可变）是两个极端。Aura 不照搬其按值传递（多语言脚本 + KV 持久化下状态是跨语言 blob，无法也不应在 handler 间整体 move）。逐条权衡：

**错误建模（tellus 的 `Error` 关联类型）——不引入第二条错误通道。**

tellus 的 `Error` 是单一语言（Rust）类型系统的产物：同一种语言有统一错误表示，`Result<T, E>` 能全程静态检查。这条在 Aura 不成立，是**语言级事实，不是架构可选**：

- **失败已经是返回值的一部分**：handler 的 `returns` 就是业务值，失败作为带 `error` 字段的对象返回，或 emit 一个专用 error 对象，均属"返回值即唯一结果通道"，已是干净设计，无须第二条错误通道。
- **跨语言不存在统一错误类型**：Python 异常、Steel 结构、Rust Result，语法与检查机制各异，Aura 没有营造"语言统一错误模型"的空间。
- **强行统一要在 JSON 边界做错误协议**：序列化 + 判别 + 反序列化，正常路径也被污染，成本远大于收益。

因此 Error 建模差异是语言机制差异，不照搬是正确取舍，而非能力缺失。

**不值得借鉴的部分**：`State` 按值整体传递与显式 `Control::Stop` 指令——前者与 KV 增量落盘冲突（会放大 I/O、破坏 WAL 写模型），后者已被 supervisor 与 passivation 接管，照搬属过度借用。tellus 的 `Nothing`（无消息 supervisor）在 Aura 没有对应落点——Aura 的无状态不在 actor 层而在运维层（API 有状态、运维无状态：actor 开发者按有状态变量编程，状态持久化/恢复/scale-to-zero 全程由底层接管），引入"无状态 actor"类别与系统本质目标相反。

### 5.10 Aura 的根本区别

**先定性：既非传统 Actor 模型，也非 CSP——是「场域化事件总线的 actor 封装」的混合体。**

传统 Actor 模型的三根支柱——actor 树（监管层级）、ActorRef 一等收件箱、tell/ask 直发——Aura 都没有（或只有退化形态）：无监管树（仅保留崩溃重启式 supervision，§5.8）、无 ActorRef 一等收件箱（内部的事件队列 + per-subscription cursor 只是路由之下的串行化 + 背压实现细节，不可寻址）、通信靠匿名事件总线而非直发。它不是 CSP：没有显式类型化 channel，也没有同步会合（rendezvous）——`emit` 是 fire-and-forget 的 pub/sub。

Aura 从 actor 保留下来的是**封装性**：state 按 partition key 隔离、单线程串行消费、实例状态自持。真正的新东西是把**事件总线升格为主通信原语**（见第 1 条），用一个共享场域（Event Realm）替代 per-entity 的寻址队列——一片匿名 pub/sub 黑板，不关心谁发射、谁处理。

**1. 事件总线是主通信原语，不是辅助**

传统 Actor 框架的主通信是 ActorRef 直发（tell/ask）。EventStream 是补充机制。Aura 把事件总线提到中心位置——Actor 之间只通过 emit/on 在场域中交互，完全解耦：发射者不关心谁处理，处理者不关心谁发射。

**2. 替代消息队列——MQ 的三层拆解**

传统架构中服务间通信靠 Kafka/NATS。传统 Actor 框架跨节点靠框架自带 RPC（Akka Remote、Erlang dist）。Aura 的元数据不跨节点同步（每节点独立 meta 实例，控制平面单写）；Actor 状态不跨节点复制（走 SlateDB+S3 或本地 Fjall）。不需要外部消息队列。Fluxora 去掉 Kafka/NATS，由 Aura 场域替代。

消息队列在 Aura 中**不是被替代，而是被拆解**——它的三个职能分别归入 Aura 已有的原生能力：

| 传统 MQ 职能 | Aura 对应机制 |
|:--|:--|
| 容量与保留 | S3 对象存储（不可变对象 + 生命周期策略） |
| 吞吐与削峰 | 水平扩展（一致哈希分片 + scale-to-zero），而非缓冲队列 |
| 消费进度 | KV（Fjall 点查 offset、重试进度） |

MQ 的第三个组件身份消失。任何残留需求（外部投递、审计日志、消费组重试）都用「S3 为真理源 + KV 管元数据」的二分实现，不引入独立队列组件。详见 [§5.15 MQ 分解架构](#515-mq-分解架构)。

**3. 嵌入式多语言 + 嵌入式存储 = 零外部依赖**

其他框架要么绑定一种运行时（BEAM、JVM、.NET），要么绑定一种语言（Actix/Rust）。Aura 的 Actor 实现可 Steel/Python/Wasm 任意切换，且 `set()` 运行时热替换。存储是进程内 Fjall。整个场域是一个单体二进制。

**4. 事件驱动 + 共识 = 反应式架构的进程内实现**

反应式架构主张"数据变更主动推送"，传统实现需要 CDC + Flink + Kafka + WS 网关的完整链路。Aura 的场域把这条链路压缩到进程内：Actor 写入 Fjall → emit 事件 → 订阅者进程内收到 → 零网络跳数。

### 5.11 Actor 实例化与分片

`set()` 定义的是 Actor **类型**（脚本 + interface_schema）。运行时，Realm 根据事件的 partition key 激活对应的 Actor **实例**。

**问题**：不同用户同时 emit("add_to_cart")，如果一个 Actor 实例串行处理所有请求，用户 B 要等用户 A 处理完——瓶颈。正确做法是按 user_id 分区，每个用户一个实例，互不阻塞。

**机制**：

```
emit("add_to_cart", {"user_id": "A", "item": "X"})
emit("add_to_cart", {"user_id": "B", "item": "Y"})

Realm 从 interface_schema 查到 add_to_cart 的 key = "user_id"
  → 事件 1: partition key = "A" → CartActor 实例 #A
  → 事件 2: partition key = "B" → CartActor 实例 #B
  → 两个实例并行处理，互不阻塞

同一用户后续事件：
emit("remove_from_cart", {"user_id": "A", "item_id": "X"})
  → partition key = "A" → 同一个 CartActor 实例 #A（串行，保证状态一致）
```

- 同一 partition key 的事件始终路由到同一 Actor 实例，保证该实体的状态一致性
- 不同 partition key 的事件路由到不同实例，并行处理
- 跨节点：联邦语义（ADR-0013）——partition key 的作用域是节点内部，用户数据跟随所属节点，无全局放置问题

**实例生命周期**（Virtual Actor 模式，与 Orleans 一致）：

| 阶段 | 行为 |
|------|------|
| **激活** | 事件到达，Realm 按 partition key 查找实例 → 不存在则从 Fjall 恢复状态（或新建空状态）→ 加载脚本和入口函数 |
| **运行** | 处理事件，可 emit，状态立即持久化。事件经 (事件, partition) 队列投递到该实例的私有订阅 Receiver，串行消费 |
| **空闲** | 超时无事件 → 状态落盘 Fjall → 内存驱逐（Scale-to-Zero） |
| **再激活** | 新事件到达 → 从 Fjall 恢复 → 继续 |

Actor "一直存在"（逻辑上），按需激活/驱逐（物理上）。开发者不显式创建实例——`set()` 定义类型，`emit()` 触发激活。

**领域映射**：DDD 的聚合根天然对应 Actor 类型，聚合根 ID 作为 partition key：

| Actor 类型 | 实例 key | receives | emits |
|-----------|---------|----------|-------|
| CartActor | user_id | add_to_cart, remove_from_cart | cart_updated |
| OrderActor | order_id | place_order, cancel_order | order_completed, order_cancelled |
| InventoryActor | product_id | reserve_stock, release_stock | stock_reserved, out_of_stock |
| PaymentActor | payment_id | charge, refund | payment_confirmed, payment_failed |

跨聚合的交互通过场域事件完成，聚合内部直接操作 ctx.state。

### 5.12 跨 Partition 查询

Actor 实例的状态是隔离的——CartActor #A 看不到 CartActor #B 的数据。企业场景需要跨 partition 聚合查询（如统计部门所有用户的购物车）时，不能直接 JOIN。

| 方案 | 原理 | 延迟 | 适用场景 |
|------|------|------|---------|
| **投影 Actor**（推荐） | 独立 Actor 订阅事件流，持续维护聚合视图 | 低（预计算） | 常用聚合，可预定义 |
| **Arrow HTAP** | Fjall KV blob 导出为列式格式，Polars 执行 ad-hoc 查询 | 中（列式扫描） | 任意维度临时查询 |
| **Scatter-gather**（不推荐） | emit 查询事件，各实例响应后汇总 | 高（等最慢的实例） | 实例数已知且少的场景 |

**投影 Actor**：一个独立的 Actor（如 DeptStatsActor），按 dept_id 分片，`on("cart_updated")` 持续把用户级数据聚合到部门级 ctx.state。查询时直接读该 Actor 的状态。这是场域模型的自然延伸——投影 Actor 就是一个普通的事件接收 Actor，不需要额外基础设施。原理与反应式架构的流计算预聚合一致：不查询时计算，而是持续监听事件流维护聚合状态。

**Arrow HTAP**：[Arrow 大一统 HTAP 引擎](https://github.com/orbsh/wiki/blob/main/arrow-unified-htap-engine.md) 解决了 ad-hoc 查询问题——Fjall 的 KV blob 可以通过 Arrow 列式化 + Polars 执行多维度扫描、过滤、聚合。适合报表、BI、后台管理等无法预先定义的查询场景。

**Scatter-gather**：emit 一个查询事件，所有相关 Actor 实例各自响应 partial 结果，由汇总 Actor 收集。问题：不知道有多少实例、不知道何时收齐、延迟取决于最慢的实例。仅在实例数已知且少的场景使用。

### 5.13 ctx.invoke() 统一调用原语

Actor 不直接通过 PyO3 调用外部系统（`httpx.get()` 等）——这绕过了 Host 的管控，不可审计、不可限流、不可观测。所有同步调用（HTTP、Actor 间）通过 `ctx.invoke()` 统一入口。

**两种通信，分离**：

- **`emit`/`on`**：场域事件，Actor 间的异步通信（fire-and-forget、pub/sub、partition-keyed）。不承担请求-响应语义。
- **`ctx.invoke()`**：同步调用，阻塞等待返回值。目标可以是外部 HTTP 服务，也可以是场域内的 Actor。

**统一调用注册表（invoke.toml）**：

所有可调用目标通过声明式配置注册，统一目录：

```toml
# invoke.toml

# 外部 HTTP 服务
[[call]]
name = "user_info"
target = "https://api.example.com/users/{user_id}"
method = "GET"
timeout = 5000

[[call]]
name = "send_email"
target = "https://api.mailgun.net/v3/send"
method = "POST"
timeout = 10000

# 场域内 Actor（interface_schema 中声明了 returns 的事件）
[[call]]
name = "charge_processor"
target = "actor:charge_processor"
partition_key = "user_id"  # data 中哪个字段是 partition key

[[call]]
name = "db_query"
target = "actor:db_bridge"
partition_key = "query_id"
```

注册表是声明式配置，Fluxora 读取后负责实际路由——HTTP 目标走 Fluxora HTTP 请求，Actor 目标走 Realm 路由。

**Actor 侧**：

```python
# Actor 入口函数
async def handle(ctx, add_to_cart=None):
    if add_to_cart:
        # 调用外部 HTTP 服务
        user = await ctx.invoke("user_info", {"user_id": add_to_cart["user_id"]})

        # 调用场域内 Actor（同步等待返回值）
        charge = await ctx.invoke("charge_processor", {"user_id": add_to_cart["user_id"], "amount": 100})

        ctx.state["items"].append(add_to_cart["item"])
        # 已落盘（WAL + memtable）
        emit("cart_updated", {"user_id": add_to_cart["user_id"], "items": ctx.state["items"]})
```

`ctx.invoke()` 是同步语义——`await` 期间当前 Actor 实例阻塞（串行语义符合预期），其他实例不受影响。HTTP 和 Actor 调用在调用者视角完全一致：`ctx.invoke(name, data)` 拿到结果。

**Realm Actor 调用的内部机制**：

`ctx.invoke("charge_processor", data)` 对 Actor 目标的执行路径：

1. 查 `invoke.toml` → `target = "actor:charge_processor"`，`partition_key = "user_id"`
2. 从 `data` 中提取 `partition_key` 字段值 → 路由到 `charge_processor` 实例
3. Realm emit 事件 + `__reply_to` 到目标实例
4. 等待入口函数的 `return` 值（reply_to 机制）
5. 返回给调用者

入口函数只需正常 `return`，不需要关心 `__reply_to` 细节。入口函数同时可以 `emit` 通知其他 Actor——`return` 给调用者，`emit` 给系统，各走各的路。

**Webhook Ingress**：外部 HTTP 请求进来时，Fluxora 查注册表或路由配置，转成 `emit()` 投递到场域。Ingress 方向走 emit/on，因为这是业务事件入口，不是 RPC。

### 5.14 ctx.invoke 的 Rust 宿主实现

`ctx.invoke()` 的核心是 oneshot channel + pending call 表 + Actor 实例状态机。call 根据 `invoke.toml` 中的 target 前缀分派：HTTP 目标交给 Fluxora，Actor 目标走 Realm 路由。

**实例状态机**：

```
┌─────────┐     event 到达      ┌─────────┐
│  Idle   │──────────────────►│  Busy   │
└─────────┘                    └────┬─────┘
     ▲                              │
     │ 入口函数完成                │ ctx.invoke()
     │                              ▼
     │                        ┌──────────────────┐
     │                        │ WaitingForResponse │
     │                        │ (不消费 mailbox)    │
     │                        └────┬─────┬───────┘
     │              响应到达        │     │ 超时
     │◄─────────────────────────────┘     │
     │◄───────────────────────────────────┘
```

`WaitingForResponse` 状态下不消费 mailbox 中的新事件——保证同一实例的串行语义。其他实例（不同 partition key）不受影响。

**Host 核心结构**：

```rust
pub struct RealmHost {
    // call_id → 响应投递通道
    pending_calls: Arc<Mutex<HashMap<Uuid, PendingCall>>>,
    // 统一调用注册表（invoke.toml）
    call_registry: HashMap<String, CallTarget>,
    router: EventRouter,
    instances: Arc<Mutex<HashMap<String, ActorInstance>>>,
}

struct PendingCall {
    actor_id: String,
    responder: Responder,
    deadline: Instant,
}

enum Responder {
    Async { tx: oneshot::Sender<Value> },   // Python async / Rust actor
    Callback { cb: CallbackHandle },         // Steel Lisp
}

enum CallTarget {
    Http { endpoint: String, method: String, timeout: u64, schema: Option<Schema> },
    Actor { actor_type: String, partition_key: String, timeout: u64 },
}

struct ActorInstance {
    partition_key: String,
    state: Value,                            // CBOR 状态树
    mailbox: mpsc::Receiver<Event>,
    status: InstanceStatus,
}

enum InstanceStatus {
    Idle,
    Busy,
    WaitingForResponse,
}
```

**ctx.invoke 发起侧**：

```rust
impl Context {
    pub async fn call(
        &mut self,
        service: &str,
        data: Value,
    ) -> Result<Value, CallError> {
        let call_id = Uuid::new_v4();
        let (tx, rx) = oneshot::channel();

        // 查统一调用注册表
        let target = self.host.call_registry.get(service)
            .ok_or(CallError::UnknownService)?;

        let timeout_ms = match target {
            CallTarget::Http { timeout, .. } => *timeout,
            CallTarget::Actor { timeout, .. } => *timeout,
        };
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);

        // 注册 pending call
        self.host.pending_calls.lock().insert(
            call_id,
            PendingCall {
                actor_id: self.actor_id.clone(),
                responder: Responder::Async { tx },
                deadline,
            },
        );

        // 标记实例状态
        self.instance.status = InstanceStatus::WaitingForResponse;

        match target {
            CallTarget::Http { endpoint, method, .. } => {
                // HTTP 目标 → 交给 Fluxora 执行
                self.host.call_dispatcher.send(CallRequest::Http {
                    call_id,
                    endpoint: endpoint.clone(),
                    method: method.clone(),
                    data,
                    deadline,
                });
            }
            CallTarget::Actor { actor_type, partition_key, .. } => {
                // Actor 目标 → Realm 路由 + reply_to
                let pk_value = data.get(partition_key)
                    .ok_or(CallError::MissingPartitionKey)?;
                self.host.router.emit_to_actor(
                    actor_type,
                    pk_value.as_str().unwrap_or_default(),
                    data,
                    Some(call_id),  // reply_to channel
                );
            }
        }

        // 挂起，等 tx.send() 或超时
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms), rx,
        ).await {
            Ok(Ok(response)) => {
                self.instance.status = InstanceStatus::Busy;
                Ok(response)
            }
            Ok(Err(_)) => {  // tx 被 drop
                self.instance.status = InstanceStatus::Busy;
                Err(CallError::Cancelled)
            }
            Err(_) => {  // 超时
                self.host.pending_calls.lock().remove(&call_id);
                self.instance.status = InstanceStatus::Busy;
                Err(CallError::Timeout)
            }
        }
    }
}
```

**响应投递**：

```rust
impl RealmHost {
    // HTTP 响应到达 或 Actor 入口函数 return 值到达
    async fn resolve_call(&self, call_id: Uuid, response: Value) {
        let mut pending = self.pending_calls.lock();
        if let Some(req) = pending.remove(&call_id) {
            match req.responder {
                Responder::Async { tx } => {
                    let _ = tx.send(response);  // 唤醒 await 的 Actor
                }
                Responder::Callback { cb } => {
                    // Steel Lisp：向 Actor mailbox 投递 ResumeEvent
                    if let Some(inst) = self.instances.get(&cb.actor_id) {
                        inst.mailbox.send(Event::Resume {
                            callback: cb,
                            data: response,
                        });
                    }
                }
            }
        }
    }
}
```

HTTP 响应和 Actor return 值走同一个 `resolve_call` 通道——call 的响应不污染事件命名空间。

**Python 桥接**：Python 的 `await ctx.invoke()` 通过 PyO3 桥接为 Rust future。`await` 时 Python coroutine 挂起并释放 GIL，Tokio runtime 调度其他 task。oneshot 解锁 → Rust future 完成 → Python coroutine 恢复。

**Steel Lisp 桥接**：Steel 没有 async/await，用 callback。`ctx-invoke` 调用后立即返回，入口函数暂停，Actor 进入 `WaitingForResponse`。响应到达时 Host 不直接调用 Steel VM（跨线程不安全），而是向 Actor 的 mailbox 投递 `ResumeEvent`，事件循环收到后恢复执行 callback：

```scheme
(ctx-invoke ctx "user_info" (hash 'user_id "123")
  (lambda (response)        ; 响应到达时调用
    (ctx-update! ctx "items" ...)
    ;; 已落盘（WAL + memtable）
    (emit "cart_updated" ...))
  (lambda ()                ; 超时时调用
    (emit "error" ...)))
```

**Wasm**：Wasm 通过 host 函数调用 `ctx.invoke()`——Host 在 Wasm 挂起时执行 async 操作（emit + 等待 reply_to），结果返回后恢复 Wasm 执行。对 Wasm 来说 `ctx.invoke()` 是一个普通的同步 host 函数调用，内部异步由 Host 封装。Wasmtime 的 host 函数调用天然支持阻塞，不需要"中间 Actor 桥接"。

**超时扫描**：Host 后台 task 每 100ms 扫描 `pending_calls`，清理过期条目，向对应 Actor 投递超时 `ResumeEvent`（Callback）或 send 超时值（Async）。

### 5.15 MQ 分解架构

消息队列（MQ）在 Aura 中**不是被替代，而是被拆解**——它的三个职能分别归入 Aura 已有的原生能力。

#### 场域内：事件总线（无 MQ）

Aura 场域内部（Actor ↔ Actor）的事件通信已经完整内化了传统 MQ 的职责：

| 传统 MQ 职能 | Aura 对应机制 |
|:--|:--|
| 服务解耦 | 场域事件总线 emit/on（§5.5） |
| 跨节点消息 | 无全局消息面——元数据每节点独立，Actor 状态走 SlateDB+S3；联邦节点间经 well-known 协议认证交互 |
| 事件持久化 | 每次 emit 落盘 Fjall WAL |
| 事件重放 | Fjall 状态恢复 + stash 回放 |
| 投递语义 | at-least-once + 幂等消费端（§5.7） |
| 背压 | bounded mailbox（§5.8） |

**场域内部不需要 MQ**——这是设计初衷，也是 §5.10「无外部消息队列」的定位。

#### 边界：S3 为事件真理源

MQ 的「无限容量 + 不可变日志 + 保留期删除」本质，在对象存储里是**原生语义**：

- **不可变**：追加 = 写一个新对象，从不原地更新
- **保留**：S3 Lifecycle 按对象过期删除，**无 compaction、无墓碑**
- **容量**：S3 桶无限

边界事件存储两种形态：

| 用途 | 方案 | 理由 |
|:--|:--|:--|
| 纯审计/保留，按 retention 整批清 | 裸 S3 对象（`partition/offset` 键 + Lifecycle） | 零计算节点，log 语义最纯粹 |
| 需按分区/时间回放、扫描 | SlateDB（KV over S3） | 前缀扫描 + 字典序，复用场域查询模式 |

#### 吞吐：水平扩展而非缓冲

bounded mailbox 收到背压信号时，正确的反应是**触发水平扩展**，而不是引入缓冲队列：

```
突发流量 → mailbox 满 → 背压信号
  → 节点内扩容由存储引擎承接（数据跟随所属节点，无全局重分片——联邦裁决 ADR-0013）
  → 吸收突发，而非暂存
```

「反应式架构的进程内实现」（§5.10.4）的完整逻辑：**不是用队列把流量摊平，而是让引擎快得能直接吃下流量，或抓住流量把负载分摊出去。** 加 MQ 只是把问题外包给另一个组件，扩展是内生的。

#### 消费组：KV 管元数据

外部消费者的投递语义（消费组 offset、重试进度）不需要 MQ——用「S3 为真理源 + Fjall 管 offset」的二分实现：

```
消费者从 S3 事件日志拉取
  → 读 Fjall: c:{group}:{partition} → offset   （点查）
  → 消费成功后原子推进 offset（WriteBatch）
  → 失败 → 记录重试进度到 Fjall
```

#### 双轨互斥：Fjall 或 SlateDB，二选一

**存储引擎本身也是二选一，不共存。** 与 [KV 存储引擎 §11](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#11-两条架构路径fjall-vs-slatedb) 的严格互斥一致：

| 模式 | 存储引擎 | 分发层 | 真理源 | 归档职责 |
|:--|:--|:--|:--|:--|
| **Fjall（本地）** | Fjall → 本地 NVMe | 落湖备份（元数据单写，无共识） | 本地 Fjall（落湖兜底） | **自管**：自行截断/上传 S3 归档 |
| **SlateDB + S3** | SlateDB → S3 | 无（S3 自身 HA） | S3 桶 | 天然（S3 即归档） |

选择 Fjall（本地模式）时，冷数据归档**不在 SlateDB 通道里**——Fjall 自管，分两步走：

**首版：直接截断**。数据超过阈值或达到保留期，直接删除本地 SSTable/段，不做外部归档。语法简单、无外部依赖，牺牲的是历史数据不可恢复。

**后续：上传 S3 归档**。在截断路径上加一步：先把过期段上传到 S3 对象桶，再删本地。S3 对象 = 归档真相源，本地淘汰。这一步复用 S3 的不可变对象 + 生命周期语义，与 SlateDB 模式共享同一归档终点，只是**归档动作由 Fjall 主进程主动发起**。

#### 收敛结论

存储平面分两半，边界队列不需要第三个组件：

| 层 | 组件 | 负责 |
|:--|:--|:--|
| 场域内状态/事件 | Fjall 本地+落湖（或 SlateDB + S3）＋ 独立 meta 实例 | 低延迟、随机读写、元数据单写可控 |
| 边界事件/审计/归档 | S3（本模式由 Fjall 自管上传） | 无限容量、不可变日志、保留删除 |
| 消费组元数据 | KV（Fjall 或 SlateDB） | offset 点查、重试进度 |

**MQ 在 Aura 中整个消失**——被拆解为「容量→S3、吞吐→扩展、进度→KV」三个原生能力。存储引擎二选一，Fjall 方案自留归档职责（首版截断，后续上传 S3）。

→ [KV 存储引擎架构 §11](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#11-两条架构路径fjall-vs-slatedb) — 双轨互斥的完整论证


