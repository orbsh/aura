# Actor API（脚本语言参考）

> 每种语言的脚本契约与 host 函数。中文为主，英文版成对：每节末尾链接对应英文版。
> 执行模型与设计背景见 [realm.md](realm.md)；自省机制见 [§5.4](realm.md#54-interface_schema)。

## 通用契约（所有语言）

一个脚本 Actor = **一个源文件** + **两个约定入口**：

| 入口 | 必需 | 作用 |
|------|------|------|
| `interface_schema(args)` | 可选 | 注册时被 Host 调用一次，声明元数据（事件契约、驻留策略）。纯函数，无副作用，参数被忽略 |
| `execute(args)`（或自定义 entry） | 是 | 消息处理入口；事件名映射为函数参数 |

**执行契约**：JSON 参数进（单参数，已解码的结构化值），JSON 可序列化值出；失败以错误值上抛（各语言原生异常/error），Host 转为 error value，绝不 panic。

**Host 函数**（ctx bridge，Phase 2.5）：脚本内可调用以下名字的函数——每个接受一个 JSON 参数，返回 JSON 值：

- `ctx_state_get(field)` → `{"present": bool, "value": ...}`（读本实例状态字段；**只能读本实例**——跨实例访问不可表达）
- `ctx_state_set({"field": ..., "value": ...})` → `{"ok": true}`
- `ctx_state_delete(field)` → `{"ok": true}`
- `ctx_invoke({"type": ..., "key": ..., "args": ...})` → 目标 Actor 的返回值（阻塞等待，走统一调用模型，超时=失败值）

**语言能力差异**（源码见 [partitioning.md](partitioning.md) 附注的取舍立场）：

| | python | steel | nushell | wasm |
|---|---|---|---|---|
| 进程内 | ✅ | ✅ | ❌ 子进程 | ✅ VM |
| ctx host 函数 | ✅ | ✅ | ❌（显式报错） | 经帧上抛（Phase 6.6） |
| 适用 | 业务逻辑 | AI 生成操作 | 管道/CLI 形态 | 重隔离三方代码 |

**`interface_schema` 对 probe 透明**：probe 的 carrier 只负责"加载源码 → 调 entry → 序列化结果"，`interface_schema` 对它就是普通函数调用，没有任何特殊意义。 Aura 是赋予其意义的唯一一方——注册时调用它做元数据自省（事件契约、`lifecycle.idle_ttl`）。同一个脚本交给 probe 执行时，`interface_schema` 只是一个没人调用的死函数；交给 aura 注册时，它成为类型定义的元数据来源。

---

## Python

[English](#python-1)

```python
# 元数据声明（可选；注册时被调用一次，args 被忽略）
def interface_schema(args=None):
    return {
        "receives": {
            "add_to_cart": {"key": "user_id"}
        },
        "emits": ["cart_updated"],
        "lifecycle": {"idle_ttl": "5m"}   # 数字=秒；字符串必须带单位 s/m/h
    }

# 消息入口
def execute(args):
    # args: 解码后的 JSON 值（dict/list/...），非字符串
    ctx_state_set({"field": "visits", "value": 1})
    got = ctx_state_get('{"field": "visits"}')   # host 函数传 JSON 字符串
    echo = ctx_invoke('{"type": "echo", "key": "k1", "args": {"x": 1}}')
    return {"stored": got["value"], "echo": echo["x"]}
```

注意：host 函数的参数是**一个 JSON 字符串**（carrier 边界解码），脚本内用 `json.dumps(...)` 构造；返回值已是原生 dict（无需再 `json.loads`）。

无 entry 语义：源码顶层设置 `result` 变量亦可（无 entry 注册时）。

## Steel

[English](#steel-1)

```scheme
;; 元数据声明（可选）
(define (interface_schema args)
  '((lifecycle . ((idle_ttl . "5m")))))

;; 消息入口
(define (execute args)
  (ctx_state_set "{\"field\": \"visits\", \"value\": 1}")
  (let* ((got (ctx_state_get "\"visits\""))
         (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"k1\", \"args\": {\"x\": 1}}")))
    (hash "visits" (hash-ref got "value")
          "echo" (hash-ref echoed "x"))))
```

host 函数参数为 JSON 字符串（可传原生 steel 值，自动 marshal）；**返回值是原生 steel 值**——hash/number/bool 直接可用，不需要再解析 JSON 字符串。

无 entry 语义：源码定义 `*result*` 变量。

## Nushell

[English](#nushell-1)

```nu
# 子进程执行：无 ctx host 函数（无法回调 host——需要 ctx 的脚本 Actor
# 必须用进程内 carrier）
export def execute [args] {
    { sum: ($args.items | math sum) }
}
```

- 入口必须 `export def <name>`；裸 `main` 不可通过模块导入寻址，会被显式拒绝
- 参数是一个解析后的值（record/list），不是字符串；返回值必须可 `to json --raw`
- `interface_schema` 声明在 nushell 上不生效（子进程注册期自省技术上可行但当前未接线；nushell Actor 的 TTL 用 Host 侧声明）

## Wasm（Rust 编写）

[English](#wasm-rust-1)

三方不可信代码的承载形态：硬件级隔离（Wasmtime），与进程内 VM 的软隔离区分。

约定（骨架，Phase 4 `link` payload 落地指针编解码）：

```rust
// 编译目标 wasm32-wasi；模块导出二选一：
// 1. WASI command：导出 _start（args 经 WASI 传入）
// 2. 类型化导出：execute(i64) -> i64（args JSON 指针进，结果 JSON 指针出）
#[no_mangle]
pub extern "C" fn execute(args_ptr: i64) -> i64 {
    // 线性内存编解码随 Phase 4 link payloads 落地（MB 级字节，执行前哈希校验）
    todo!()
}
```

- Host imports 刻意最小化：无 fs、无 network——能力面（Phase 5）决定授予什么
- **Rust 服务的唯一发布形态**——k10r/gravity 一类存储型 Rust 服务编译为 `.wasm` 上传运行（`set(lang="wasm", bytes)`），不是编译进 host：编译进 host 会让每个应用 fork 一份 aura（加服务就要重打包），平台退化成框架。OKM schema 代码原样编译进 wasm，存储走 `VirtualStorage` 帧上抛——host 侧 NestStorage 执行器（Phase 6.6）在 registry 分配的 app ns 前缀下承载物理存储（ADR-0007 存储承载分流）。静态 OKM derive，不需要 okm-dynamic
- aura 引擎本身**不提供进程内 Rust Actor**——框架机制（evictor 类）就是 realm 内的普通逻辑，包装成 Actor 绕一圈没有意义；Rust 代码要成为 Actor 只有一条路：编译为 wasm 上传。`ActorType` 的 Rust 闭包形态仅存在于测试脚手架
- `interface_schema` 声明路径与 python/steel 相同（导出同名函数返回 JSON），注册期生效

---

## 与 probe 的关系（再述）

probe = **操作执行面**：`ToolCall` 进 → `execute()` → `ToolResult` 出。它不知道 Actor、不知道事件、不知道 `interface_schema` 的语义——所有这些是 **aura 的场域层概念**。同一个 python 文件：作为 probe 操作时只有 `execute` 被调用；作为 aura Actor 注册时 `interface_schema` 先行、`execute` 成为实例的消息入口。一个文件，两种宿主，契约透明。
