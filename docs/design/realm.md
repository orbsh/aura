# 场域模型（设计细节）

> 自 `~/.hermes/wiki/aura-architecture.md` §5 迁入的实现细节；wiki 保留综述。
> 综述：wiki [Aura 架构 §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md)。
> 相关：[数据分区（内部机制）](partitioning.md)、[摊位 API（脚本语言参考）](booth-api.md)。

## 5. 场域模型：摊位间交互与外部世界

### ctx 边界：什么在 ctx 上，什么不在

注入摊位入口函数的 `ctx` 只收**实例身份相关 + 需要 Host 管控/记录**的运行时能力：

| 在 ctx 上 | 职责 |
|:--|:--|
| `ctx.store.emit(op)` | 本类型声明的 collections（ADR-0026 §3；寻址绑定类型 ns，跨类型不可表达） |
| `ctx.interface_schema` | 本类型持久化的 interface_schema 副本（对自身声明形状的反射） |
| `ctx.invoke()` | 唯一受控调用面——超时、审计、限流、可观测收口于此（§5.13） |

不在 ctx 上的能力与其归属：

- **emit / on**：场域 pub/sub，脚本层裸函数（emit）与激活期装配（on，对应 interface_schema 的静态契约）。事件投递的 partition 来自事件数据而非发射者身份，不依赖实例；emit 是进程内 fire-and-forget，没有需要管控的生命周期；on 放 ctx 会暗示运行时动态订阅，与静态契约矛盾。
- **interface_schema() / set()**：定义期契约与部署面，执行中的摊位看不到。
- **on_sleep / on_wake**：生命周期钩子是 Host → 摊位方向，ctx 是摊位 → Host 方向的使用接口，两者方向相反。
- **入口函数 return**：语言原生行为，Host 拦截填入 reply_to，不需要 `ctx.return()`。
- **@cron / on_debounce**：定时是声明式触发模式（投递事件唤醒摊位），不是可调用的定时 API（无 `ctx.sleep()`/`ctx.every()`）。
- **日志、纯计算、语言标准库**：凡需 Host 管控的外部交互都经 `ctx.invoke()` 注册目标编址，其余用宿主语言原生设施。

新能力按此判据归位：实例绑定 + Host 管控 → 进 ctx；静态契约 / Host 驱动 / 场域层 → 排除。决策记录见 Aura 仓库 ADR-0011。

### 5.1 核心设计

传统 Actor 框架（Akka、Erlang、Actix）的通信原语是 ActorRef 直发（tell/ask）——调用者必须知道目标 Actor 的地址。Aura 采用不同的原语：摊位之间不直接寻址，而是通过共享的"场域"（Event Realm）用 `emit` / `on` 交互。

场域是引擎内部的事件空间。摊位通过 `on(name, fn)` 订阅事件、`emit(name, data)` 发射事件。发射者不关心谁处理，处理者不关心谁发射。外部世界（HTTP/WS）的协议层由调用方（Fluxora、网关）处理——Fluxora 将外部请求转成 `emit()`，将 `on()` 事件转成 HTTP 响应或 WS 推送。Aura 引擎本身不碰 HTTP/WS。

**术语校准**：在 Aura，"摊位" 只承诺**调度语义**——按实例键串行、状态住在本类型声明的 collections、失败是值（ADR-0012/0026）。它不描述**通讯语义**：per-摊位 mailbox 模型已在 PLAN 4.5c 退役（ADR-0014），事件投递是 MQ 形态——按事件名的 (event, partition) 持久队列 + 每订阅者游标，多订阅者零复制；直接调用（`ctx_invoke`）只是请求-响应的例外路径（能用事件表达的协作不用直接调用）。驻留也不同于传统 actor 的"永生邮箱"：实例可睡、可驱逐，唤醒靠积压与游标（scale-to-zero）。一个 realm 参与者的精确一句话：**MQ 投递、按实例键串行的状态消费者**。

**开发者体验方向**：Fluxora 不做 MQ（Kafka/NATS 已去掉），只做 HTTP/WS 协议桥接。进一步的 DX 目标：Web 控制台 + 嵌入 VSCode，开发者直接在浏览器里写 Python 摊位。摊位之间只管发消息，存数据由框架处理。这是 FaaS + Web 框架的融合形态——不是"给你一个数据库让你写 CRUD"，而是"给你一个事件空间让你编排摊位"。

### 5.2 场域拓扑

```
┌──────────────── Event Realm (realm: default) ────────────────┐
│                                                                │
│  Ingress（入口）                                                │
│  ┌──────────┐                                                  │
│  │ Fluxora  │  emit("order_created", data)                     │
│  │ Webhook  │───────┐                                          │
│  └──────────┘       │                                          │
│                      ▼                                         │
│               ┌────────────┐     emit("order_completed")      │
│               │  Booth A   │──────────────────┐                │
│               │ on("order_ │                   │                │
│               │  created") │                   ▼                │
│               └────────────┘          ┌────────────┐          │
│                                        │  Booth B   │          │
│  ┌──────────┐                          │on("order_  │          │
│  │  Booth C │◄──emit("inventory_upd.") │completed") │          │
│  │ on("inv_ │         ┌────────────┐   └────────────┘          │
│  │  updated")│        │  Booth D   │        │                  │
│  └──────────┘        │on("order_  │        │ emit("notif.")   │
│       │ emit("shipped")│completed")│        │                  │
│       ▼               └────────────┘        ▼                  │
│  ┌──────────┐                                   Egress（出口）  │
│  │  Booth E │                          ┌──────────┐            │
│  │on("ship- │                          │ Fluxora  │            │
│  │ ped")    │                          │on("notif"│ → WS push │
│  └──────────┘                          └──────────┘            │
└────────────────────────────────────────────────────────────────┘
```

- 事件名标记入口/出口语义（Fluxora 的模式）
- Booth 不感知协议（HTTP/WS），只收发事件
- 引擎内部元数据不跨节点同步：每节点独立（控制平面单写，ADR-0025 后住数据面 okm 实例）；Booth 状态事件走 SlateDB+S3 或本地 Fjall；联邦节点间走 well-known 协议认证身份

### 5.3 Booth 定义接口

```
set(<lang>, <script/wasm>)
```

提交或更新一个 Booth **定义**（类型）。`lang` ∈ {Steel, Python, Wasm}。脚本内以 `@on` 装饰器（或 steel `on` 函数 / wasm 导出约定）声明多入口 handler（事件名映射为函数参数），`interface_schema` 由装饰器推导（手写可覆盖 lifecycle）。名字为 `execute` 的 handler 是直接调用通道（`ctx.invoke`）的目标，无特权。运行时调用 `set()` 可热替换摊位实现——不仅换行为，还换语言。

`set()` 定义的是类型，不是实例。摊位实例由 Realm 根据 instance key 按需激活（详见 [§5.11](#511-摊位-实例化与分片)）。

**脚本持久化**：`set()` 提交的脚本内容（或 Wasm 字节码）存储在数据面 okm 实例的 **BoothDef 表**（ns 41，与 mq/state 并列——ADR-0025 方案 A；实现为 `booth/src/persist.rs`），不从文件系统读取。脚本是静态资产，跨节点同步走文件系统（git/S3）。存储引擎天然支持版本化，每次 `set()` 保留新版本，旧版本可回滚。脚本条目附带元数据（提交时间、语言类型、版本号、提交者、内容哈希），存储结构：

```
meta instance, partition: "booth_defs"
  key:   <booth_type_name>
  value: CBOR { lang, script_bytes, version, content_hash, committed_at, committed_by }
```

**去重**：`set()` 提交前先计算 `script_bytes` 的哈希（content_hash），与 BoothDef 表中最新版本的 `content_hash` 比较——相同则忽略，不写入新版本。避免 CI 重复部署或无意义的热重载。

**三条生命周期线分离**（Phase 4.5b 裁决）：上传（`set`）是独立生命周期——上传时 Host 自省 `interface_schema()` 一次，元数据（receives/emits/lifecycle）与定义一并持久化；执行永不调用 `interface_schema`——消息处理只加载脚本（最新版本）调 handler，元数据从 BoothDef 表读取；版本变更（新 `set`）重新自省一次、更新持久化元数据，此前旧元数据治理。已实现：`PersistedBooth` 记录 + `engine.register` 持久化 + boot 重载（见 PLAN Phase 4.5b）。

`on()` handler 在摊位实例激活时从 BoothDef 表读取最新版本的脚本，加载到对应 VM 执行。实例驱逐后，下次激活重新读取。

### 5.4 interface_schema()

脚本元数据的统一声明面。注册时 Host 调用一次（纯函数，无 ctx，方向是 Host ← 脚本——脚本从不反向访问 engine）。返回结构的 `lifecycle` 段声明驻留策略：`"idle_ttl"` 接受秒数或带单位字符串（`"300s"` / `"5m"` / `"2h"`，单位必填）；Host 侧 builder（Rust 类型）显式声明的值优先于脚本自省值。注意 carrier 契约：入口函数统一带一个参数调用，`interface_schema(args)` 需声明形参（语言允许默认值时可用 `def interface_schema(args=None)`）。

`interface_schema()` 是摊位的**统一契约**——声明接收事件，以及可选的发射事件清单。

**核心洞察：事件名就是引用。** 当 `interface_schema()` 声明摊位 B 接收 `"charge"` 事件时，任何人 emit `"charge"` 就是在引用 B。事件名 = 引用，instance key = 实例定位。不需要单独的 `invoke` / `direct_send` 原语——emit 本身就是引用调用。

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
- `key`：instance key 字段——Realm 按此字段值路由到摊位实例
- `params`：入参 JSON Schema

**`returns` 是摊位级别的单一返回值声明。** 每个摊位有一个入口函数，多个事件映射到多个参数，返回一个值：

```python
# Booth 入口函数 — 多事件映射到多参数；状态住在本类型声明的 carts collection
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    cart = ctx.store.emit(get_document("carts", ctx.self_id.key)) or {"items": []}
    if add_to_cart:
        cart["items"].append(add_to_cart["item"])
    if remove_from_cart:
        cart["items"] = [i for i in cart["items"] if i["id"] != remove_from_cart["item_id"]]
    ctx.store.emit(put_document("carts", ctx.self_id.key, cart))
    emit("cart_updated", {"user_id": ..., "items": cart["items"]})
    return {"cart_count": len(cart["items"]),
            "total": sum(i["price"] for i in cart["items"])}
```

- 声明了 `returns` 的摊位支持 `ctx.invoke()` 同步调用——调用者通过摊位名获取返回值
- 不声明 `returns` 的摊位只能通过 `emit()` 异步触发（fire-and-forget）
- `ctx.invoke("cart_booth", data)` → 调用入口函数 → 返回单一值

| 用途 | 说明 |
|------|------|
| **内部事件** | 声明事件名即可，消费方的 `on` 已包含 schema |
| **安全管控** | Realm 强制白名单：只允许 `emits` 中声明的事件名被发射，未声明的拒绝 |
| **图表生成** | 声明的事件名可用于自动绘制摊位间事件流拓扑图 |

`emits` 声明的是**约束**——摊位只能发射列表中的事件名。emit 时 Realm 校验事件名是否在 `emits` 声明中，未声明的拒绝发射。不声明 `emits` = 不能 emit 任何事件（纯接收型摊位）。

**外部订阅者**：Fluxora、Webhook、分析系统等外部消费者是 Realm 的订阅者——和摊位共享同一个路由表，使用相同的 `RouteMode`（on / join / batch / debounce），只是投递目标不同（摊位 → 入口函数，外部 → HTTP/WS/Webhook）。Realm 的 `emits` 约束了哪些事件可出界，外部系统从声明列表中选择订阅。

```
# Fluxora：去抖推 WS
Route { target: "fluxora", event: "cart_updated", mode: Debounce(300ms) }

# Webhook：单事件触发
Route { target: "webhook:order-service", event: "order_completed", mode: On }

# 分析系统：join 后批量发送
Route { target: "analytics", events: ["payment", "inventory"], mode: Join(5s) }
```

外部订阅者不需要声明 `interface_schema()`——它们是 Realm 的消费者，不是摊位。路由表由 Realm 管理，外部系统通过配置（或 API）声明订阅关系。

**事件名 = 引用（路由机制）：**

Host 启动时调用 `interface_schema()`，构建事件路由表：

```
事件名 → instance key 字段 → params schema → returns? → Booth 定义 → on handler
```

当 A emit `"add_to_cart"` 时：
1. Realm 查路由表 → `"add_to_cart"` 只有 Cart 摊位注册了
2. 从事件数据提取 `key` 字段值（`user_id`）→ 路由到 Cart 摊位的对应实例
3. 校验载荷是否符合 `params` schema
4. 投递到 `on("add_to_cart")` handler

A 通过事件名精确指向了 Cart 摊位——事件名就是引用，schema 就是类型签名。Host 在投递前可校验事件载荷是否符合 schema；Fluxora 可从 schema 自动生成 TypeScript 类型定义。

**ctx.invoke() 同步调用：**

摊位声明了 `returns`（JSON Schema）时，调用者可以通过 `ctx.invoke()` 同步获取摊位入口函数的 return 值：

```python
# 调用方
result = await ctx.invoke("cart_booth", data={"user_id": "42", "item": {...}})
# result = Booth 入口函数的 return 值

# 被调用方（Cart Booth 入口函数）
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    if add_to_cart:
        cart = add_item(add_to_cart["user_id"], add_to_cart["item"])
    return {"cart_count": len(cart), "total": sum(i["price"] for i in cart)}
```

Realm 内部通过 reply_to 机制实现同步：emit 事件 + `__reply_to` → 等待入口函数 return → 返回给调用者。入口函数只需正常 `return`，不需要关心 reply_to 细节。

入口函数同时可以 `emit` 通知其他摊位——`return` 给调用者，`emit` 给系统，各走各的路。

**fire-and-forget：**

没有 `returns` 的摊位只能通过 `emit()` 触发。调用者不等待返回值，入口函数的 return 值被丢弃。

**emit 到有 `returns` 的摊位（允许）：**

`returns` 声明的是**能力**（"这个摊位可以返回值"），不是**约束**（"只能同步调用"）。`emit()` 到有 `returns` 的摊位是合法的——入口函数正常执行，return 值丢弃。这解锁了触发但不等待的模式：

```python
# 你关心的是"这件事发生"，不关心结果
emit("charge", {"user_id": "42", "amount": 100})
# 入口函数执行了（扣款、发通知），return 值丢弃

# 同一 Booth 也可以同步调用
result = await ctx.invoke("charge_processor", {"user_id": "42", "amount": 100})
```

`interface_schema()` 也是热重载的入口：脚本修改 → 重新加载 → 重新 `interface_schema()` → 路由表更新。

### 5.5 事件 API

**emit(name, data)**：向 Realm 提交事件。data 是 `ciborium::Value`（内存值树）。进程内摊位 ↔ 摊位传递时保持 Value 形态，通过 Tokio MPSC channel clone，零序列化。序列化只在跨边界时发生：

| 边界 | 格式 | 说明 |
|------|------|------|
| 进程内摊位 ↔ 摊位 | `ciborium::Value` | 内存 clone，零编解码 |
| 摊位 → Fjall 持久化 | CBOR bytes | `ciborium::serialize()` 写入 LSM-Tree |
| 引擎内部元数据（无跨节点复制） | — | 各节点独立，控制平面单写 |
| 摊位 → Fluxora（HTTP/WS） | JSON | 外部系统消费 JSON |
| 摊位 → Webhook | CBOR 或 JSON | 按配置选择 |

**on(name, fn)**：注册事件监听。每个摊位有一个入口函数，事件名映射为函数的 keyword 参数——多个 `on` 声明编译为单一 dispatch 函数。Realm 内部按事件名路由到对应的参数。状态共享通过 `ctx`（摊位的统一状态树），不依赖闭包捕获。

**通配符监听**：`on()` 支持前缀通配符 `"prefix.*"`，匹配所有以 `prefix.` 开头的事件。与 etcd 的 key 前缀匹配类似——适用于日志、审计、投影摊位等需要监听一类事件的场景：

```python
# 监听所有 order 相关事件
@on("order.*")
def handle_order_events(ctx, data):
    log(ctx, data)
    # 投影 Booth：把 order.* 事件聚合到部门统计
    emit("dept_stats_updated", aggregate(data))
```

通配符声明和精确声明可以共存——事件同时匹配通配符参数和精确参数，各自独立路由到入口函数的对应参数。通配符声明不指定 instance key（它监听一类事件，不绑定具体实体），路由到场域中该 Booth 的单例实例。

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
    booth_type: String,
    instance_key_field: String,              // 从事件数据中取哪个字段作为 key
    handler: HandlerRef,
}

struct WildcardRoute {
    prefix: String,                           // "order."（去掉 .* 后的前缀）
    booth_type: String,
    handler: HandlerRef,
    // 无 instance_key_field —— 通配符 handler 是单例实例
}
```

通配符用 `Vec` 而非 HashMap，因为匹配是反向的（给定事件名，找哪些前缀能匹配），HashMap 帮不上忙。通配符数量通常很少（几个投影摊位），线性扫描足够——且 emit 时线性扫描的成本是**订阅者声明的前缀数量**（每 emit 一次 O(wildcard 订阅数)），与事件词汇量无关；词汇量大不影响。若通配订阅本身多到成为瓶颈（数百个模式），再换基数树（radix trie，web 框架路由形态：按段分叉、`*` 段为通配捕获）——当下不做。

**分发路径（Phase 4.5c 事件队列模型，已实现）**：

```rust
impl Realm {
    // emit 的投递目标不是实例的 queue，而是 (声明事件, partition) 事件队列。
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
            let partition = if route.instance_key_field.is_empty() {
                "__singleton__".to_string()
            } else {
                data.get(&route.instance_key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("__default__").to_string()
            };
            // 虚拟 Booth 语义：emit 到未激活的实例先激活它——
            // 它的 @on 订阅在消息入队前绑定（每个订阅者私有 Receiver，
            // 即 per-subscription cursor；实例不拥有队列）。
            let target = InstanceId { booth_type: route.booth_type.clone(), key: partition.clone() };
            if !self.instances.contains_key(&(route.booth_type.clone(), partition.clone())) {
                self.instance(self_arc.clone(), &target).await?;
            }
            // 按队列去重：队列身份 = (具体事件名, partition)。多个订阅类型监听
            // 同一事件时各持私有 cursor，事件只落一份；通配路由匹配到的
            // 具体事件名就是队列名（模式串只在 router 匹配层存在）。
            if queued.insert((event.to_string(), partition.clone())) {
                targets.push((route, partition));
            }
        }

        // 第二段：向每个队列持久化写入（4.5c step 2b）。队列身份 = 具体事件名
        // （通配路由匹配到的名字，模式串不落队列），经 EventName registry 换成
        // event_id 后按 [event_id][part_id][time] 落 okm 分区——MqHead 保
        // 逻辑时间单调，append O(1)。每个订阅者一条 cursor 指向同一分区。
        for (route, partition) in targets {
            let event_name = event.to_string();
            let mut store = realm.mq.clone();
            if let Err(e) = mq::append(&mut store, &event_name, &partition, &data) {
                eprintln!("mq append failed for {event_name}/{partition}: {e}");
                realm.dead_events.push(event, data.clone());
                continue;
            }
            // 落盘即触发 min-watermark 压缩（写路径压缩，分母 = EventRoute
            // 持久注册表）。
            Self::compact_queue_locked(&mut realm, &event_name, &partition, &mut store).await?;
        }
        Ok(())
    }
}
```

**队列语义**：

- 事件不属于任何 Booth。队列在场域层，实例激活时按其类型的 `@on` 声明绑定订阅（私有 cursor = per-subscription 消费位）。
- 一个队列可有多个订阅者（多个摊位类型监听同一事件）——一对多投递是结构性的，不是 fan-out 模拟。
- 串行语义：每实例的订阅消费任务一次只处理一条（逐队列顺序 drain）——同一实例串行由 cursor 保持，实例不拥有队列。
- 直接调用不走队列：`ctx.invoke` / `engine.invoke` 是点对点（实例的 queue 保留用于统一调用模型），事件投递才走共享队列。

**通配订阅的具体名展开**：router 层保存模式串（`"order.*"`），但队列身份和 handler 名永远是具体事件名。emit 时以发出的具体名落队列；通配订阅者的消费任务每轮把模式前缀经 `mq::events_matching`（EventName registry 前缀扫描）展开为已注册的具体名集合，逐名读 cursor/backlog，投递 handler = 具体名，每个具体名一条 cursor（有界：只累积实际见过的词汇）。新事件名出现时自动加入下一轮展开——无需订阅者做任何事。

**通配符参数的实例化**：通配符参数不绑定 instance key，路由到固定 key `"__singleton__"` 的实例——整个摊位类型只有一个实例。这与投影摊位的场景一致：一个 DeptStatsBooth 实例监听所有 `order.*` 事件，持续聚合。

**精确 + 通配符同时匹配**：一个事件可以同时命中精确参数和通配符参数，各自独立投递：

```
emit("order_created", {"user_id": "A", ...})

→ 精确匹配：CartBooth 的 on("order_created")，instance key = "A"
→ 通配符匹配：DeptStatsBooth 的 on("order.*")，单例实例

两个 Booth 实例各自独立处理，互不阻塞。
```

```python
from aura import emit

# Booth 入口函数 — 多事件映射到多参数；状态住在本类型声明的 carts collection
def handle(ctx, add_to_cart=None, remove_from_cart=None):
    cart = ctx.store.emit(get_document("carts", ctx.self_id.key)) or {"items": []}
    if add_to_cart:
        cart["items"].append(add_to_cart["item"])
    if remove_from_cart:
        cart["items"] = [i for i in cart["items"] if i["id"] != remove_from_cart["item_id"]]
    ctx.store.emit(put_document("carts", ctx.self_id.key, cart))
    # 已落盘（WAL + memtable）
    emit("cart_updated", {"user_id": ..., "items": cart["items"]})
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
    ;; 状态经 ctx.store.emit 写本类型声明的 collections（ADR-0026 §3）
    (ctx_store_emit (hash "collection" "carts" "op" "put_document"
                          "key" (hash "user" (hash-ref data "user_id"))
                          "doc" (hash "items" (append items (list (hash-ref data "item"))))))
    (emit "cart_updated"
      (list (cons "user_id" (hash-ref data "user_id"))))))

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

能否拆成多个事件映射 + booth 组合，是判断"是否需要 Realm 原语"的标准：

| 组合模式 | 能否分解 | 理由 |
|---------|---------|------|
| **合并同类事件** | ✅ 能 | 多个事件映射到同一入口函数的多个参数，共享逻辑 |
| **多事件收齐（join）** | ❌ 不能 | 需等待多个事件全部到达后合并投递，单个参数无法触发 |
| **同类事件打包** | ❌ 不能 | 计数/窗口状态横跨多次事件到达，booth 无跨事件状态能力 |
| **去抖** | ❌ 不能 | 计时器必须在 Realm 层，booth 无时间感知 |

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

**Collector 是瞬态状态，不持久化到 Fjall。** 崩溃恢复后，源摊位从 Fjall 恢复状态并重新 emit 事件，Collector 从零开始重新收集。持久化的锚点是摊位状态（写入 Fjall），不是事件聚合的中间缓冲区。

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

**Python**：装饰器收集。`set("python", "booth.py")` 时 Realm import 模块 → 扫描所有 `@on` / `@on_join` / `@on_batch` / `@on_debounce` 装饰器 → 自动构建 `interface_schema()` → 注册路由表。

**Steel**：AST 提取。S-表达式天然可遍历，Realm 解析 `(on ...)` 调用的 AST，从内嵌的 `(schema ...)` 表达式中提取事件名和 schema。`set("steel", "booth.scm")` 时 Realm 读取源码 → sexp parser 解析 → 扫描所有 `(on ...)` 顶层调用 → 提取事件名 + schema → 构建路由表。不需要开发者手写独立的 `(define (interface-schema) ...)`。

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
    booth_type: String,
    mode: RouteMode,
    instance_key_field: Option<String>,  // on 有，join/batch/debounce 可选
    handler: HandlerRef,
}
```

路由表从"事件名 → Route"变为"事件名 → Vec<Route>"——同一事件可以被多个不同模式的 Route 匹配：

```
"order_created" → [
    Route { mode: On, handler: CartBooth },           // 精确单事件
    Route { mode: Batch, handler: BulkWriter },        // 5 个打包
]
```

Realm 分发时，对每个匹配的 Route 按 mode 分别处理：On 直接投递，Join/Batch/Debounce 交给 Collector。

### 5.7 投递语义

| 维度 | 选择 | 说明 |
|------|------|------|
| **可靠性** | at-least-once | 事件持久化在 okm 队列分区，消费端游标推进——不丢，可能重复。消费端需幂等 |
| **realm** | realm 名（默认 "default"） | 场域按 realm 隔离，跨 realm 的事件不投递（ADR-0028 改名） |
| **顺序保证** | 因果一致性 | 如果 e1 因果先于 e2（e1 的处理导致 e2 的发射），则任何订阅者收到 e1 必在 e2 之前。无因果关系的并发事件可乱序 |

**持久队列（4.5c step 2b 裁决）**：事件投递的存储形态是 okm 内嵌持久分区，不是内存 broadcast——

```
[mq-data][event][part_id][time]      ← 事件被动落盘（emit 即持久，与 Booth 状态的主动保存同引擎）
[mq-cursor][event][part_id][booth]   ← 每订阅实例一个游标
```

- **Scale-to-zero 不丢触发**：实例被驱逐期间产生的事件留在队列里，重新激活后游标未动，积压照常送达（broadcast 模型下这些事件静默丢失——与「事件数据被动持久化」矛盾，已废弃）
- **慢消费者积压可见**：积压是可计量的队列长度，不是 broadcast 的 Lagged 整段静默丢失；失效显式化优于静默
- **跳到最新（skip-to-now）向下兼容**：积压过多时，消费端可按消息时间把游标直接推到最新——丢弃陈旧积压、立即处理新事件。兜底阀门，让「消费不及时」可以选择性放弃而不必逐条消化
- **多订阅者零复制**：N 个 Booth 监听同一事件 = 一个队列分区 + N 个游标；旧的 per-摊位 mailbox 模型下同一事件存 N 份的冗余从结构上消失
- **保留 = 活跃订阅者的最小水位线**：一个 [ev][part] 队列只保留所有活跃订阅者游标仍需要的范围——最小游标之前的数据在写入路径 compaction 时删除。水位线的分母来自**路由注册表**（4.5b 持久化的 @on 元数据），不是原始 cursor 键：已永久退出的 booth 的陈旧游标不得把水位线钉死——注销 booth 时同步删其 cursor 行，它的积压随后跌破水位线、随普通 compaction 消失，无需独立回收器。**积压深度是 mq-data 前缀上的实时 okm reduce 计数**（写入 +1，水位线 compaction -1 unfold）——零扫描的运维面，skip-to-now 的决策直接读它
- **诚实语义代价**：队列是缓冲不是存储——at-least-once 仅在「所有订阅者保持注册且在消费」期间成立。被永久注销且尚有未消费积压的订阅者，积压随之消失。与分层一致：Booth 状态（类型 collections）是 durable truth，队列只保证「活着就能追上」

因果一致是甜区：保证逻辑正确性（因先于果），不需要全序的共识开销。进程内事件天然因果有序（同一线程内的 emit 序列）；同节点并发 Booth 的事件用向量时钟标记 happened-before 关系。元数据不跨节点复制（每节点独立），跨节点次序问题在单写入点模型下不存在。

### 5.8 传统 Actor 模型可借鉴的设计

| 机制 | 来源 | Aura 对应 |
|------|------|-----------|
| **Supervision** | Erlang/OTP | Booth 崩溃时从 Fjall 恢复状态、重新加载脚本和入口函数。策略：one-for-one（独立重启）/ one-for-all（关联 Booth 一起重启，防止状态不一致） |
| **Location Transparency** | Akka | 刻意不采纳（联邦裁决）：目标地址含节点域是特性——跨域交互显式寻址，域内 emit/on 才是透明的 |
| **Become/Unbecome** | Akka | `set(lang, script)` 是更激进的版本——运行时切换行为函数和语言。可实现状态机：收到事件后 `set("python", "active_handler.py")` 切换入口函数 |
| **Stash** | Akka | 摊位刚唤醒、Fjall 状态还在恢复时，先 stash 事件，恢复完成后回放 |
| **Dead Letters** | Akka/Erlang | emit 的事件如果没有匹配的接收摊位，或目标摊位崩溃且无 supervisor 重启，进入 dead event log。用于调试和事件审计 |
| **Backpressure** | Reactive Streams | 持久队列下背压退化为积压——消费慢的订阅者在队列分区里积累可见的 backlog（可计量、可跳到最新），emit 方永不阻塞（写入即落盘）。broadcast 模型的「容量满即丢」语义已废弃 |
| **Passivation / Virtual Actor** | Akka / Orleans | 空闲摊位从内存驱逐（Scale-to-Zero），按需激活。Aura 的实例化机制基于此（详见 [§5.11](#511-摊位-实例化与分片)） |

### 5.9 与现有 Actor 框架的对比

| 框架 | 类似点 | 关键差异 |
|------|--------|---------|
| **Akka EventStream** | 进程内事件总线，pub/sub | JVM/GC；EventStream 是 Actor 通信的补充，主通信仍 ActorRef 直发；无嵌入式存储，无 Scale-to-Zero |
| **Erlang gen_event** | 事件管理器，多订阅者 | BEAM 限定；无嵌入式多语言；跨节点靠分布式 Erlang，无强一致共识 |
| **Orleans Virtual Actor** | Actor 按需激活，"一直存在" | .NET 运行时；Grain 间通信是直接方法调用，不是事件总线；无嵌入式 KV |
| **Proto.摊位** | Go 实现，有 EventStream | Go runtime；事件总线是辅助；无嵌入式存储 |
| **Actix（Rust）** | Rust Actor 框架 | 无事件总线（Actor 间直发）；无分布式；无嵌入式脚本/沙箱；无共识 |
| **tellus（Rust）** | 经典近距离 Actor：状态机建模，`Message/State/Error` 三关联类型 | 无事件总线、无分布式、无嵌入式存储；但状态机建模的多处设计值得借鉴（见下） |

没有现有框架同时做到：事件总线作为主通信原语 + 嵌入式多语言 + 嵌入式 KV（Scale-to-Zero）+ 无外部消息队列 + 无共识依赖的多机组网。

### 5.9.1 tellus 状态机建模的可借鉴之处

tellus 的核心立场是**「摊位是状态机，而不是带可变字段的对象」**——`State` 作为关联类型按值传入 `receive(state, msg) -> Control::Continue(next)/Stop`，可变数据全塞进 State 跨消息传递，摊位值本身只是 unit struct。这与 Aura 的 `ctx.store.emit`（状态以 document 形式住在本类型声明的 collections 里，handler 经存储指令读写）是两个极端。Aura 不照搬其按值传递（多语言脚本 + KV 持久化下状态是跨语言 blob，无法也不应在 handler 间整体 move）。逐条权衡：

**错误建模（tellus 的 `Error` 关联类型）——不引入第二条错误通道。**

tellus 的 `Error` 是单一语言（Rust）类型系统的产物：同一种语言有统一错误表示，`Result<T, E>` 能全程静态检查。这条在 Aura 不成立，是**语言级事实，不是架构可选**：

- **失败已经是返回值的一部分**：handler 的 `returns` 就是业务值，失败作为带 `error` 字段的对象返回，或 emit 一个专用 error 对象，均属"返回值即唯一结果通道"，已是干净设计，无须第二条错误通道。
- **跨语言不存在统一错误类型**：Python 异常、Steel 结构、Rust Result，语法与检查机制各异，Aura 没有营造"语言统一错误模型"的空间。
- **强行统一要在 JSON 边界做错误协议**：序列化 + 判别 + 反序列化，正常路径也被污染，成本远大于收益。

因此 Error 建模差异是语言机制差异，不照搬是正确取舍，而非能力缺失。

**不值得借鉴的部分**：`State` 按值整体传递与显式 `Control::Stop` 指令——前者与 KV 增量落盘冲突（会放大 I/O、破坏 WAL 写模型），后者已被 supervisor 与 passivation 接管，照搬属过度借用。tellus 的 `Nothing`（无消息 supervisor）在 Aura 没有对应落点——Aura 的无状态不在 actor 层而在运维层（API 有状态、运维无状态：actor 开发者按有状态变量编程，状态持久化/恢复/scale-to-zero 全程由底层接管），引入"无状态摊位"类别与系统本质目标相反。

### 5.10 Aura 的根本区别

**先定性：既非传统 Actor 模型，也非 CSP——是「场域化事件总线的 actor 封装」的混合体。**

传统 Actor 模型的三根支柱——actor 树（监管层级）、ActorRef 一等收件箱、tell/ask 直发——Aura 都没有（或只有退化形态）：无监管树（仅保留崩溃重启式 supervision，§5.8）、无 ActorRef 一等收件箱（内部的事件队列 + per-subscription cursor 只是路由之下的串行化 + 背压实现细节，不可寻址）、通信靠匿名事件总线而非直发。它不是 CSP：没有显式类型化 channel，也没有同步会合（rendezvous）——`emit` 是 fire-and-forget 的 pub/sub。

Aura 从摊位保留下来的是**封装性**：state 按 instance key 隔离、单线程串行消费、实例状态自持。真正的新东西是把**事件总线升格为主通信原语**（见第 1 条），用一个共享场域（Event Realm）替代 per-entity 的寻址队列——一片匿名 pub/sub 黑板，不关心谁发射、谁处理。

**1. 事件总线是主通信原语，不是辅助**

传统 Actor 框架的主通信是 ActorRef 直发（tell/ask）。EventStream 是补充机制。Aura 把事件总线提到中心位置——摊位之间只通过 emit/on 在场域中交互，完全解耦：发射者不关心谁处理，处理者不关心谁发射。

**2. 替代消息队列——MQ 的三层拆解**

传统架构中服务间通信靠 Kafka/NATS。传统 Actor 框架跨节点靠框架自带 RPC（Akka Remote、Erlang dist）。Aura 的引擎内部元数据不跨节点同步（每节点独立，控制平面单写）；摊位状态不跨节点复制（走 SlateDB+S3 或本地 Fjall）。不需要外部消息队列。Fluxora 去掉 Kafka/NATS，由 Aura 场域替代。

消息队列在 Aura 中**不是被替代，而是被拆解**——它的三个职能分别归入 Aura 已有的原生能力：

| 传统 MQ 职能 | Aura 对应机制 |
|:--|:--|
| 容量与保留 | S3 对象存储（不可变对象 + 生命周期策略） |
| 吞吐与削峰 | 水平扩展（一致哈希分片 + scale-to-zero），而非缓冲队列 |
| 消费进度 | KV（Fjall 点查 offset、重试进度） |

MQ 的第三个组件身份消失。任何残留需求（外部投递、审计日志、消费组重试）都用「S3 为真理源 + KV 管元数据」的二分实现，不引入独立队列组件。详见 [§5.15 MQ 分解架构](#515-mq-分解架构)。

**3. 嵌入式多语言 + 嵌入式存储 = 零外部依赖**

其他框架要么绑定一种运行时（BEAM、JVM、.NET），要么绑定一种语言（Actix/Rust）。Aura 的摊位实现可 Steel/Python/Wasm 任意切换，且 `set()` 运行时热替换。存储是进程内 Fjall。整个场域是一个单体二进制。

**4. 事件驱动 + 共识 = 反应式架构的进程内实现**

反应式架构主张"数据变更主动推送"，传统实现需要 CDC + Flink + Kafka + WS 网关的完整链路。Aura 的场域把这条链路压缩到进程内：摊位写入 Fjall → emit 事件 → 订阅者进程内收到 → 零网络跳数。

### 5.11 摊位实例化与分片

`set()` 定义的是摊位 **类型**（脚本 + interface_schema）。运行时，Realm 根据事件的 instance key 激活对应的摊位 **实例**。

**问题**：不同用户同时 emit("add_to_cart")，如果一个摊位实例串行处理所有请求，用户 B 要等用户 A 处理完——瓶颈。正确做法是按 user_id 分区，每个用户一个实例，互不阻塞。

**机制**：

```
emit("add_to_cart", {"user_id": "A", "item": "X"})
emit("add_to_cart", {"user_id": "B", "item": "Y"})

Realm 从 interface_schema 查到 add_to_cart 的 key = "user_id"
  → 事件 1: instance key = "A" → CartBooth 实例 #A
  → 事件 2: instance key = "B" → CartBooth 实例 #B
  → 两个实例并行处理，互不阻塞

同一用户后续事件：
emit("remove_from_cart", {"user_id": "A", "item_id": "X"})
  → instance key = "A" → 同一个 CartBooth 实例 #A（串行，保证状态一致）
```

- 同一 instance key 的事件始终路由到同一 Booth 实例，保证该实体的状态一致性
- 不同 instance key 的事件路由到不同实例，并行处理
- 跨节点：联邦语义（ADR-0013）——instance key 的作用域是节点内部，用户数据跟随所属节点，无全局放置问题

**实例生命周期**（Virtual Actor 模式，与 Orleans 一致）：

| 阶段 | 行为 |
|------|------|
| **激活** | 事件到达，Realm 按 instance key 查找实例 → 不存在则从 Fjall 恢复状态（或新建空状态）→ 加载脚本和入口函数 |
| **运行** | 处理事件，可 emit，状态立即持久化。事件经 (事件, partition) 队列投递到该实例的私有订阅 Receiver，串行消费 |
| **空闲** | 超时无事件 → 状态落盘 Fjall → 内存驱逐（Scale-to-Zero） |
| **再激活** | 新事件到达 → 从 Fjall 恢复 → 继续 |

Booth "一直存在"（逻辑上），按需激活/驱逐（物理上）。开发者不显式创建实例——`set()` 定义类型，`emit()` 触发激活。

**领域映射**：DDD 的聚合根天然对应摊位类型，聚合根 ID 作为 instance key：

| 摊位类型 | 实例 key | receives | emits |
|-----------|---------|----------|-------|
| CartBooth | user_id | add_to_cart, remove_from_cart | cart_updated |
| OrderBooth | order_id | place_order, cancel_order | order_completed, order_cancelled |
| InventoryBooth | product_id | reserve_stock, release_stock | stock_reserved, out_of_stock |
| PaymentBooth | payment_id | charge, refund | payment_confirmed, payment_failed |

跨聚合的交互通过场域事件完成，聚合内部直接操作本类型 collections（ADR-0026 §3，经 ctx.store.emit）。

### 5.12 跨 Partition 查询摊位实例的状态是隔离的——CartBooth #A 看不到 CartBooth #B 的数据。企业场景需要跨 partition 聚合查询（如统计部门所有用户的购物车）时，不能直接 JOIN。

| 方案 | 原理 | 延迟 | 适用场景 |
|------|------|------|---------|
| **投影摊位**（推荐） | 独立摊位订阅事件流，持续维护聚合视图 | 低（预计算） | 常用聚合，可预定义 |
| **Arrow HTAP** | Fjall KV blob 导出为列式格式，Polars 执行 ad-hoc 查询 | 中（列式扫描） | 任意维度临时查询 |
| **Scatter-gather**（不推荐） | emit 查询事件，各实例响应后汇总 | 高（等最慢的实例） | 实例数已知且少的场景 |

**投影摊位**：一个独立的摊位（如 DeptStatsBooth），按 dept_id 分片，`on("cart_updated")` 持续把用户级数据聚合到部门级状态。查询时直接读该摊位的状态。这是场域模型的自然延伸——投影摊位就是一个普通的事件接收摊位，不需要额外基础设施。原理与反应式架构的流计算预聚合一致：不查询时计算，而是持续监听事件流维护聚合状态。**ADR-0026 §3 后投影摊位只剩跨类型聚合一个用途**：同类型实例间的聚合 = 类型 ns 内的普通 scan/reduce，不再需要专门的投影摊位。

**Arrow HTAP**：[Arrow 大一统 HTAP 引擎](https://github.com/orbsh/wiki/blob/main/arrow-unified-htap-engine.md) 解决了 ad-hoc 查询问题——Fjall 的 KV blob 可以通过 Arrow 列式化 + Polars 执行多维度扫描、过滤、聚合。适合报表、BI、后台管理等无法预先定义的查询场景。

**Scatter-gather**：emit 一个查询事件，所有相关摊位实例各自响应 partial 结果，由汇总摊位收集。问题：不知道有多少实例、不知道何时收齐、延迟取决于最慢的实例。仅在实例数已知且少的场景使用。

### 5.13 ctx.invoke() 统一调用原语摊位不直接通过 PyO3 调用外部系统（`httpx.get()` 等）——这绕过了 Host 的管控，不可审计、不可限流、不可观测。所有同步调用（HTTP、摊位间）通过 `ctx.invoke()` 统一入口。

**两种通信，分离**：

- **`emit`/`on`**：场域事件，摊位间的异步通信（fire-and-forget、pub/sub、instance-keyed）。不承担请求-响应语义。
- **`ctx.invoke()`**：同步调用，阻塞等待返回值。目标可以是外部 HTTP 服务，也可以是场域内的摊位。

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

# 场域内 Booth（interface_schema 中声明了 returns 的事件）
[[call]]
name = "charge_processor"
target = "booth:charge_processor"
instance_key = "user_id"  # data 中哪个字段是 instance key

[[call]]
name = "db_query"
target = "booth:db_bridge"
instance_key = "query_id"
```

注册表是声明式配置，Fluxora 读取后负责实际路由——HTTP 目标走 Fluxora HTTP 请求，Booth 目标走 Realm 路由。

**Booth 侧**：

```python
# Booth 入口函数
async def handle(ctx, add_to_cart=None):
    if add_to_cart:
        # 调用外部 HTTP 服务
        user = await ctx.invoke("user_info", {"user_id": add_to_cart["user_id"]})

        # 调用场域内 Booth（同步等待返回值）
        charge = await ctx.invoke("charge_processor", {"user_id": add_to_cart["user_id"], "amount": 100})

        cart = ctx.store.emit(get_document("carts", ctx.self_id.key)) or {"items": []}
        cart["items"].append(add_to_cart["item"])
        ctx.store.emit(put_document("carts", ctx.self_id.key, cart))
        # 已落盘（WAL + memtable）
        emit("cart_updated", {"user_id": add_to_cart["user_id"], "items": cart["items"]})
```

`ctx.invoke()` 是同步语义——`await` 期间当前摊位实例阻塞（串行语义符合预期），其他实例不受影响。HTTP 和摊位调用在调用者视角完全一致：`ctx.invoke(name, data)` 拿到结果。

**Realm 摊位调用的内部机制**：

`ctx.invoke("charge_processor", data)` 对摊位目标的执行路径：

1. 查 `invoke.toml` → `target = "booth:charge_processor"`，`instance_key = "user_id"`
2. 从 `data` 中提取 `instance_key` 字段值 → 路由到 `charge_processor` 实例
3. Realm emit 事件 + `__reply_to` 到目标实例
4. 等待入口函数的 `return` 值（reply_to 机制）
5. 返回给调用者

入口函数只需正常 `return`，不需要关心 `__reply_to` 细节。入口函数同时可以 `emit` 通知其他摊位——`return` 给调用者，`emit` 给系统，各走各的路。

**Webhook Ingress**：外部 HTTP 请求进来时，Fluxora 查注册表或路由配置，转成 `emit()` 投递到场域。Ingress 方向走 emit/on，因为这是业务事件入口，不是 RPC。

### 5.14 ctx.invoke 的 Rust 宿主实现

`ctx.invoke()` 的核心是 oneshot channel + pending call 表 + 摊位实例状态机。call 根据 `invoke.toml` 中的 target 前缀分派：HTTP 目标交给 Fluxora，摊位目标走 Realm 路由。

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
     │                        │ (不消费 queue)      │
     │                        └────┬─────┬───────┘
     │              响应到达        │     │ 超时
     │◄─────────────────────────────┘     │
     │◄───────────────────────────────────┘
```

`WaitingForResponse` 状态下不消费 queue 中的新事件——保证同一实例的串行语义。其他实例（不同 instance key）不受影响。

**Host 核心结构**：

```rust
pub struct RealmHost {
    // call_id → 响应投递通道
    pending_calls: Arc<Mutex<HashMap<Uuid, PendingCall>>>,
    // 统一调用注册表（invoke.toml）
    call_registry: HashMap<String, CallTarget>,
    router: EventRouter,
    instances: Arc<Mutex<HashMap<String, BoothInstance>>>,
}

struct PendingCall {
    booth_id: String,
    responder: Responder,
    deadline: Instant,
}

enum Responder {
    Async { tx: oneshot::Sender<Value> },   // Python async / Rust booth
    Callback { cb: CallbackHandle },         // Steel Lisp
}

enum CallTarget {
    Http { endpoint: String, method: String, timeout: u64, schema: Option<Schema> },
    Booth { booth_type: String, instance_key: String, timeout: u64 },
}

struct BoothInstance {
    instance_key: String,
    state: Value,                            // CBOR 状态树
    queue: mpsc::Receiver<Event>,
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
            CallTarget::Booth { timeout, .. } => *timeout,
        };
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);

        // 注册 pending call
        self.host.pending_calls.lock().insert(
            call_id,
            PendingCall {
                booth_id: self.booth_id.clone(),
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
            CallTarget::Booth { booth_type, instance_key, .. } => {
                // Booth 目标 → Realm 路由 + reply_to
                let pk_value = data.get(instance_key)
                    .ok_or(CallError::MissingPartitionKey)?;
                self.host.router.emit_to_booth(
                    booth_type,
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
    // HTTP 响应到达 或 Booth 入口函数 return 值到达
    async fn resolve_call(&self, call_id: Uuid, response: Value) {
        let mut pending = self.pending_calls.lock();
        if let Some(req) = pending.remove(&call_id) {
            match req.responder {
                Responder::Async { tx } => {
                    let _ = tx.send(response);  // 唤醒 await 的 Booth
                }
                Responder::Callback { cb } => {
                    // Steel Lisp：向 Booth 的 queue 投递 ResumeEvent
                    if let Some(inst) = self.instances.get(&cb.booth_id) {
                        inst.queue.send(Event::Resume {
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

HTTP 响应和 Booth return 值走同一个 `resolve_call` 通道——call 的响应不混入事件流。

**Python 桥接**：Python 的 `await ctx.invoke()` 通过 PyO3 桥接为 Rust future。`await` 时 Python coroutine 挂起并释放 GIL，Tokio runtime 调度其他 task。oneshot 解锁 → Rust future 完成 → Python coroutine 恢复。

**Steel Lisp 桥接**：Steel 没有 async/await，用 callback。`ctx-invoke` 调用后立即返回，入口函数暂停，摊位进入 `WaitingForResponse`。响应到达时 Host 不直接调用 Steel VM（跨线程不安全），而是向摊位的 queue 投递 `ResumeEvent`，事件循环收到后恢复执行 callback：

```scheme
(ctx-invoke ctx "user_info" (hash 'user_id "123")
  (lambda (response)        ; 响应到达时调用
    (ctx-update! ctx "items" ...)
    ;; 已落盘（WAL + memtable）
    (emit "cart_updated" ...))
  (lambda ()                ; 超时时调用
    (emit "error" ...)))
```

**Wasm**：Wasm 通过 host 函数调用 `ctx.invoke()`——Host 在 Wasm 挂起时执行 async 操作（emit + 等待 reply_to），结果返回后恢复 Wasm 执行。对 Wasm 来说 `ctx.invoke()` 是一个普通的同步 host 函数调用，内部异步由 Host 封装。Wasmtime 的 host 函数调用天然支持阻塞，不需要"中间摊位桥接"。

**超时扫描**：Host 后台 task 每 100ms 扫描 `pending_calls`，清理过期条目，向对应摊位投递超时 `ResumeEvent`（Callback）或 send 超时值（Async）。

### 5.15 MQ 分解架构

消息队列（MQ）在 Aura 中**不是被替代，而是被拆解**——它的三个职能分别归入 Aura 已有的原生能力。

#### 场域内：事件总线（无 MQ）

Aura 场域内部（摊位 ↔ 摊位）的事件通信已经完整内化了传统 MQ 的职责：

| 传统 MQ 职能 | Aura 对应机制 |
|:--|:--|
| 服务解耦 | 场域事件总线 emit/on（§5.5） |
| 跨节点消息 | 无全局消息面——元数据每节点独立，摊位状态走 SlateDB+S3；联邦节点间经 well-known 协议认证交互 |
| 事件持久化 | 每次 emit 落盘 Fjall WAL |
| 事件重放 | Fjall 状态恢复 + stash 回放 |
| 投递语义 | at-least-once + 幂等消费端（§5.7） |
| 背压 | bounded queue（§5.8） |

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

bounded queue 收到背压信号时，正确的反应是**触发水平扩展**，而不是引入缓冲队列：

```
突发流量 → queue 满 → 背压信号
  → 节点内扩容由存储引擎承接（数据跟随所属节点，无全局重分片——联邦裁决 ADR-0013）
  → 吸收突发，而非暂存
```

「反应式架构的进程内实现」（§5.10.4）的完整逻辑：**不是用队列把流量摊位平，而是让引擎快得能直接吃下流量，或抓住流量把负载分摊位出去。** 加 MQ 只是把问题外包给另一个组件，扩展是内生的。

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
| 场域内状态/事件 | Fjall 本地+落湖（或 SlateDB + S3），单一 okm 实例 | 低延迟、随机读写、内部元数据单写可控 |
| 边界事件/审计/归档 | S3（本模式由 Fjall 自管上传） | 无限容量、不可变日志、保留删除 |
| 消费组元数据 | KV（Fjall 或 SlateDB） | offset 点查、重试进度 |

**MQ 在 Aura 中整个消失**——被拆解为「容量→S3、吞吐→扩展、进度→KV」三个原生能力。存储引擎二选一，Fjall 方案自留归档职责（首版截断，后续上传 S3）。

→ [KV 存储引擎架构 §11](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#11-两条架构路径fjall-vs-slatedb) — 双轨互斥的完整论证


