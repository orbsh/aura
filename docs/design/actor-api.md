# Actor API（脚本语言参考）

> 每种语言的脚本契约与 host 函数。中文为主，英文版成对：[English](actor-api-en.md)。
> 执行模型与设计背景见 [realm.md](realm.md)；自省机制见 [§5.4](realm.md#54-interface_schema)。

## 生命周期（三条线分离）

```
上传（set）     独立生命周期，可以永远不执行
  └─ Host 自省一次（调 interface_schema()，或从 @on 装饰器推导）
  └─ 元数据（receives/wildcard_receives/lifecycle）提取后持久化（ActorDef 表，数据面 okm 实例——ADR-0025）
  └─ receives 派生投递路由（事件 → 类型 + key 字段）
执行            每条消息：加载脚本（最新版本）→ 按事件名寻址 handler → 执行
  └─ 永不调用 interface_schema —— schema 已是 ActorDef 表里的静态记录
版本变更        新 set 重新自省一次、更新持久化元数据与路由；此前旧元数据治理
```

## 事件驱动模型（多入口）

一个 Actor 类型是**多入口**的：`@on` 装饰器（steel 为 `on` 函数，wasm 为导出约定）声明每个 handler 监听的事件，事件名就是 handler 的寻址名。没有单一入口——单入口模型下 emits 是多出口而入口只有一个，不对称；多个 handler 的事件共享逻辑被迫拆成多个 Actor 复制底层代码。

```python
@on("add_to_cart", key="user_id")   # instance key 在装饰器上声明
def add(args): ...

@on("remove_from_cart")             # 无 key → 单例消费者
def remove(args): ...

@on("order.*")                      # 前缀通配符 → wildcard_receives，单例
def audit(args): ...
```

**投递语义：事件队列，不是实例内的 queue**。事件不属于任何 Actor——`emit("add_to_cart", data)` 把事件写入 `add_to_cart` 事件的队列；`@on` 声明了 `key` 的队列按 `(event, partition)` 分区（key 字段值取自事件数据），没声明 key 的队列按 event 单队列。一个队列可以有**多个订阅者**（多个 Actor 类型监听同一事件——一对多是结构性的，不是 fan-out 模拟）。Actor 实例按自己的 `@on` 声明订阅队列，per-subscription cursor 保证同一实例串行消费，实例不拥有队列。

**emits 不声明、不收集、不校验（ADR-0012）**：事件的接收者集合是运行时事实——无订阅者的 emit 落入 dead-event ring，那是可观测的审计面。源码级 emit 收集推迟到有真实消费端再做（Windmill 判据：解析要驱动一个只有解析才能做对的动作时才解析）。

**interface_schema 隐式生成 + 显式合并**：carrier 在模块组装时生成隐式的 `interface_schema`（`receives` 从 `@on` 参数收集、通配符入 `wildcard_receives`），与脚本显式声明的部分 schema 按字段合并——装饰器提供 receives，手写部分提供 lifecycle 等装饰器表达不了的元数据，两者共存不互斥。合并后的单一函数就是 aura 唯一调用的入口。rust/wasm 走 `#[on(x)]` 注解生成同样的隐式函数；steel/nushell 直接手写这一个函数。

## 通用契约（所有语言）

一个脚本 Actor = **一个源文件** + **handler 函数集**：

| 函数 | 必需 | 作用 |
|------|------|------|
| handler（多个 `@on` 函数） | 是 | 消息处理入口；事件名映射为函数参数。事件投递按事件名寻址 handler，直接调用（`ctx.invoke` / engine `invoke`）在调用载荷里声明要调的 handler 名 |
| `interface_schema(args)` | 可选 | 手写元数据（lifecycle）；缺省时由装饰器推导 |

**执行契约**：JSON 参数进（单参数，已解码的结构化值），JSON 可序列化值出；失败以错误值上抛（各语言原生异常/error），Host 转为 error value，绝不 panic。

**Host 函数**（ctx bridge，Phase 2.5）：脚本内可调用以下名字的函数——每个接受一个 JSON 参数，返回 JSON 值：

- `ctx_store_emit(op)` → 操作结果（一条存储指令：collection 名 + 操作 + 参数，作用于**本类型声明的 collection**——ADR-0026 §3；存储寻址绑定类型的 ns，跨类型访问不可表达；类型未声明 storage schema 时报错——没有 ctx.store 面）
- `ctx_interface_schema(arg)` → 本类型持久化的 interface_schema 副本（handler 对自身声明形状的反射）
- `ctx_invoke({"type": ..., "key": ..., "handler": ..., "args": ...})` → 目标 Actor 的返回值（阻塞等待，走统一调用模型，超时=失败值）

**语言能力差异**：

| | python | steel | nushell | wasm |
|---|---|---|---|---|
| 进程内 | ✅ | ✅ | ❌ 子进程 | ✅ VM |
| ctx host 函数 | ✅ | ✅ | ❌（显式报错） | 经帧上抛（Phase 6.6） |
| VM 驻留（Phase 2.6） | ✅ | ✅ | ❌ one-shot | ✅ |
| `@on` 多入口 | ✅ 装饰器 | `on` 函数 | 暂单 handler（直接调用声明 handler 名） | 导出约定 |
| 适用 | 业务逻辑 | AI 生成操作 | 管道/CLI 形态 | 重隔离三方代码 |

**`interface_schema` 对执行路径透明**：probe 的执行 carrier 只负责"加载源码 → 调 entry → 序列化结果"，从不触碰 `interface_schema`。声明收集（python `@on` 注入、steel `on` 内建）是语言形态，组装与合并语义在 `carrier::introspect` 一层——aura 上传时调用它一次并持久化。同一个脚本交给 probe 执行时，`interface_schema` 只是一个没人调用的函数；交给 aura 上传时，它成为类型定义的元数据来源。

---

## Python

[English](#python-1)

```python
@on("add_to_cart", key="user_id")
def add(args):
    # args: 解码后的 JSON 值（dict/list/...），非字符串
    ctx_store_emit(json.dumps({"collection": "counters", "op": "put_document",
                               "key": {"id": 1}, "doc": {"visits": 1}}))   # host 函数传 JSON 字符串
    got = ctx_store_emit(json.dumps({"collection": "counters", "op": "get_document", "key": {"id": 1}}))
    echo = ctx_invoke('{"type": "echo", "key": "k1", "args": {"x": 1}}')
    return {"stored": got["visits"], "echo": echo["x"]}

@on("remove_from_cart")
def remove(args):
    return {"removed": True}

@on("order.*")          # 通配符：监听一类事件，单例实例
def audit(args):
    return None

# 可选的显式部分声明：与 @on 装饰器推导的 receives 按字段合并
# （装饰器管 receives，这里管 lifecycle 等其它元数据）
def interface_schema(args=None):
    return {"lifecycle": {"idle_ttl": "5m"}}   # 数字=秒；字符串必须带单位 s/m/h
```

注意：

- host 函数的参数是**一个 JSON 字符串**（carrier 边界解码），脚本内用 `json.dumps(...)` 构造；返回值已是原生 dict（无需再 `json.loads`）
- `@on` 装饰器由 carrier 注入，脚本不需要（也不应该）自己定义 `on`；装饰器是恒等变换，函数照常可直接调用
- 通配符只有前缀形态 `prefix.*`（与 etcd 一致），匹配 `order.created` 不匹配 `order`；声明了通配符的 handler 路由到单例实例
- **直接调用要声明调哪个函数**：`ctx_invoke` 载荷必须带 `handler` 字段（函数名），engine `invoke(target, handler, args)` 同理——没有保留函数名，没有隐式入口；无 entry 注册时源码顶层 `result` 变量亦可

## Steel

[English](#steel-1)

```scheme
;; 多入口：on 内建声明事件监听（carrier 注入，body 运行时收集）
;; 参数：事件名、key 字段（空字符串 = 单例）、handler
(on "add_to_cart" "user_id"
  (lambda (args)
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                          "key" (hash "id" 1) "doc" (hash "visits" 1)))
    (let* ((got (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" 1))))
           (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"k1\", \"handler\": \"execute\", \"args\": {\"x\": 1}}")))
      (hash "visits" (hash-ref got "visits")
            "echo" (hash-ref echoed "x")))))

(on "order.*" "" (lambda (args) #t))   ;; 通配符 → wildcard_receives

;; 可选的显式部分声明：与 on 收集的 receives 按字段合并
;; （收集器管 receives，这里管 lifecycle 等其它元数据）
;; ——用 hash，不用 alist（alist 的 pair 无 JSON 映射）
(define (interface_schema args)
  (hash "lifecycle" (hash "idle_ttl" "5m")))
```

host 函数参数为 JSON 字符串（可传原生 steel 值，自动 marshal）；**返回值是原生 steel 值**——hash/number/bool 直接可用，不需要再解析 JSON 字符串。alist（`'((k . v))`）不支持 JSON marshal——声明结构一律用 `hash`。

无 entry 语义：源码定义 `*result*` 变量。

## Nushell

[English](#nushell-1)

```nu
# 子进程执行：无 ctx host 函数（无法回调 host——需要 ctx 的脚本 Actor
# 必须用进程内 carrier）；无 VM 驻留（one-shot，无内存态）
export def execute [args] {
    { sum: ($args.items | math sum) }
}
```

- 入口必须 `export def <name>`；裸 `main` 不可通过模块导入寻址，会被显式拒绝
- 参数是一个解析后的值（record/list），不是字符串；返回值必须可 `to json --raw`
- nushell 是**直接调用通道单 handler**（`execute`，子进程形态与多入口寻址不匹配）；`interface_schema` 声明经由通用 wrapper 生效（`export def interface_schema [args]` 可声明 lifecycle TTL）；需要 `@on` 多入口 + ctx 的场景用 python/steel

## Wasm（Rust 编写）

[English](#wasm-rust-1)

Rust 服务的唯一发布形态：编译为 `.wasm` 运行时上传（`set(lang="wasm", bytes)`），不编译进 host——编译进 host 会让每个应用 fork 一份 aura，平台退化成框架。存储不进沙箱：OKM schema 原样编译进 wasm，`VirtualStorage` 实现替换为帧上抛，host 侧 NestStorage 执行器在 registry 分配的 app ns 前缀下承载物理存储（ADR-0007 存储承载分流）。静态 OKM derive，不需要 okm-dynamic。

约定（已落地——CBOR 过线性内存，无 JSON 债）：

```rust
// 编译目标 wasm32-wasi。模块导出：
//   - memory：线性内存
//   - aura_alloc(len: i32) -> i32：guest 分配器（bump allocator 即可；
//     模块生命周期 = session 生命周期）
//   - 每个 handler 一个函数，以事件命名，签名 (ptr: i32, len: i32) -> i64
#[no_mangle]
pub extern "C" fn add_to_cart(args_ptr: i32, args_len: i32) -> i64 {
    // args 是 host 写入 guest 内存 (args_ptr, args_len) 处的 CBOR 字节。
    // 返回 (ptr: u32) << 32 | len: u32，指向 guest 写好的 CBOR 结果
    // （经 aura_alloc 分配）。
    let result: Vec<u8> = cbor_encode(handle(add_to_cart_inner(args_ptr, args_len)));
    let ptr = aura_alloc(result.len() as i32);
    (ptr as u64) << 32 | result.len() as u64
}
```

- 值以 **CBOR 字节**过线性内存——host 序列化 args、经 `aura_alloc` 写入、调用 handler、解包 `(ptr, len)` 打包返回。JSON 只出现在 host 侧 `ResidentSession` 边界，与所有 carrier 一致
- 多入口导出约定：每个 handler 导出为以事件名命名的函数（`add_to_cart`）——事件名即导出名；通配 handler 以模式串导出（`order.*`）
- `interface_schema` 同一约定：导出同名函数优先（按 handler 方式调用，schema JSON 以 CBOR 编码过线）；否则 receives 半边由导出清单推导——`aura_alloc`/`memory`/`interface_schema` 之外的每个函数导出都是事件 handler
- Host imports（ctx bridge）注册在 `aura_host` 模块命名空间下，每个 host 函数一个 import，统一签名 `(ptr: i32, len: i32) -> i64`、同样打包返回：guest 把参数 CBOR 编码进线性内存后调用 import；host 跑 HostFn 并经 guest 的 `aura_alloc` 写回结果。import 了未声明 host 函数的模块实例化即失败（能力拒绝，不是运行时错误）
- Host imports 刻意最小化：无 fs、无 network——能力面（Phase 5）决定授予什么
- aura 引擎本身**不提供进程内 Rust Actor**——框架机制（evictor 类）就是 realm 内的普通逻辑；Rust 代码要成为 Actor 只有一条路：编译为 wasm 上传

---

## 与 probe 的关系（再述）

probe = **操作执行面**：`ToolCall` 进 → `execute()` → `ToolResult` 出。它不知道 Actor、不知道事件、不知道 `interface_schema` 的语义——所有这些是 **aura 的场域层概念**。同一个 python 文件：作为 probe 操作时只有 `execute` 被调用；作为 aura Actor 上传时自省先行、每个 `@on` handler 成为实例的一个消息入口。一个文件，两种宿主，契约透明。
