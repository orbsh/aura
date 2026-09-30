# 0037 — 摊位的存储接入：typed host channel + 进程内绑定（ADR-0026 §3 合同修订）

> **语言：** [English](0037-typed-storage-plane.md)（主文档） · [中文](0037-typed-storage-plane.zh-CN.md)

**状态：** Accepted（2026-09-28）。§1 的 python 绑定面已落地（Phase
4.16a，2026-09-30）；steel 的 Collection 方法面（4.16b）与 typed 帧
host 通道 + CBOR（4.16c，§2）未动，见后果。由用户的合同质疑引发：
python/steel 有直接绑定的 Collection 面（okm 的嵌入器用法，
ADR-0022 已定），ADR-0026 §3 却把进程内桥也写成了翻译到指令文档——
"bridging cost paid once in the adapter" 的正确终态是根本不翻译。

## 背景

ADR-0026 §3 的合同："`ctx.store` 恰好暴露一个接口——
`ctx.store.emit(op)`"。这作为**能力合同**成立（单一入口、无第二词汇表），
但 §3 随后把 python 的实现写成"脚本实现 VirtualStorage adapter，把每次
引擎调用翻译成一次 `ctx.store.emit`"——进程内载体也绕道文档层。

用户指出这不是合同的唯一实现，且不是最好的：

1. **okm 的既定嵌入器用法是直接绑定。** `bindings/okm-python` 把
   `DynamicCollection`（put/get/delete/scan/plan_put/plan_delete）注册成
   Python 类；`bindings/okm-steel` 已注册 schema/encode/decode 函数面。
   绑定的类型签名在语言边界拦截误操作，文档层做不到——`{"op":
   "get_document"}` 的拼写错误、未知 op、缺参，在绑定里是 TypeError，
   在 JSON 里要等 `from_value` 失败（op 名）或**静默成功**（字段拼写
   错误落进动态段）。
2. **字节正确性与编码无关。** ADR-0026 的 no-bypass 裁决把指令面停在
   Collection 语义层（raw 原语不在 op set 里）；绑定层同样不暴露 raw。
   所以"绑定优于 JSON"的差别不是字节正确性（两边都由
   `DynamicCollection` 补偿索引/reduce），而是**校验发生在哪一层**。
3. **载体分层。** 进程内载体与摊位同线程，直接绑定是零翻译；进程外
   载体（bgi、wasm）必须运载荷，但载荷不该是 JSON——wasm 的 OpFrame
   字节缝已经正确，bgi 的 `{"host":{"op":"ctx_store_emit",…}}` 帧才是要
   修的那处。

## 决策

**裁决——进程内直绑；进程外走 typed host channel；CBOR 是整条 host
通道的载荷编码（存储操作是其中一个类型），不是为存储新开一套。**

### 1. 进程内载体（python / steel）：绑定面，无翻译

- **python**：`okm-python` 的 Collection 绑定直接注册进会话模块（与
  `ctx_store_emit` 并列的入口；`add_class::<Collection>()` 是现成的）。
  翻译次数 = 0。
- **steel**：`okm-steel` 已有 schema/encode/decode 注册；缺 Collection
  方法面（put/get/scan）——实施项，不是裁决问题。补法对齐 python 绑定：
  同一 `DynamicCollection` 的两个 host 面，一份执行体。

### 2. 进程外通道 = 一条流，类型化帧；CBOR 是通道的编码（一个计划类型）

用户的模型：host 通道不是"每操作一条 JSON 文本缝"，是**一条类型化帧的
消息流**，存储操作是其中一个类型。帧词汇保持 ADR-0035 §3 的形状，载荷
类型化：

```
child → parent   {"host": {"type": "invoke",  "args": <typed>}}
child → parent   {"host": {"type": "iterate", "op": "start|next|dispose", …}}
child → parent   {"host": {"type": "store",   "op": <typed okm 指令>}}
parent → child   {"host_reply": {"ok": <typed>}}
```

- **编码升级 = 整条通道的**：JSON-lines → 长度前缀 CBOR。这是 ADR-0035
  §3 已记录的 CBOR 计划的落法（Windmill 判据：帧解析只在驱动一个只有
  解析才能做对的动作时才建——CBOR 解析器在载荷成为字节流的那一刻才
  值得建）。不借存储之名新造编码。
- **类型化消灭文档层错误**：`{"op":"put_docment"}` 的拼写错误静默
  通过 `from_value` 前的文本层、在 serde 才炸；类型化帧的 op 判别在
  帧结构层，CBOR tag/字段号拼错 = 解码失败 = 错误值，无静默路径。
- **wasm 不动**：wasm 的 OpFrame 字节缝（`aura_host.emit`）本就是这个
  立场的既有实现——引擎调用层字节，不是文档；ADR-0026 §3 的"wasm 不走
  动态指令路径"继续成立。
- **nushell**：绑定不可行（nu 没有 okm 绑定；BGI 垫片也不绑存储面——
  通道是类型化帧，nu 读帧即可），走 §2 的 typed 缝。bgi 垫片因此【不
  需要】顺带存储实现：垫片只做帧循环 + 派发，store 帧按 §2 原样转发。

### 3. `ctx_store_emit` JSON 的处置

闸门 1 刚落地的 JSON 指令文档（`HostOp::StoreEmit`、
`host_bridge_for("ctx_store_emit")`）是**过渡形态**：typed channel
（§2）落地时随 JSON 载荷一并退役；在那之前它是 bgi/nu 摊位唯一的存储
缝，**不再往它上面加新 op**（op set 冻结在 ADR-0026 落地的 Collection
语义集）。python/steel 的 §1 落地后，JSON 缝只剩 bgi 消费者。

## 诚实语义代价

- **同日返工**：闸门 1（`HostOp::StoreEmit` JSON 臂 + 桥 fn）落地不到
  一天即被声明为过渡形态。接受——合同（§3 一句话）的歧义被用户的
  okm 绑定事实戳破，修合同的成本低于让过渡形态长成永久形态。
- **probe 的依赖面扩大**：python 绑定注册 = probe-runtime 的 python
  feature 需要 `okm-python`（或其逻辑内联到 probe 的 python 载体）。
  不违"probe 不依赖 aura crate"铁律（okm 独立于 aura），但 okm 成为
  probe 的传递依赖——注册表 + 类型化帧协议 + 直接绑定三者都在把 probe
  从"协议搬运工"推向"运行时"，这条线要一直盯着。
- **steel 绑定扩面有真实缺口**：`okm-steel` 无 Collection 方法面，补它
  是 okm 仓的活（跨仓两刀），且 steel 的 `'static`/thread-local 约束
  （RegisterFn 先例）会决定绑定对象句柄的传递形态。
- **过渡期 = 两种正确形态并存**：§1 落地后 python/steel 用绑定、bgi/nu
  仍用 JSON——缝不统一，直到 §2 落地。接受：绑定先行有独立价值（误
  操作拦截 + 翻译次数归零），不必等 CBOR。

## 后果

- **okm**（实施项）：`okm-python` 已加宿主注入面——`Collection::with_store`
  骑字节级 `Engine` trait（4.16a，提交 58cf72b）。未动：`okm-steel` 补
  Collection 方法面（put/get/delete/scan，对齐 `okm-python` 的
  `#[pymethods]` 形态）。
- **probe**（4.16a 已落地）：`HostBridge` 带 `storage` 槽——宿主引擎藏在
  四个字节闭包后面（`StorageEngineFns`：缝上无 okm 类型，probe 与 aura
  各用不同 okm rev 编译互不干扰）+ plan 的原始条目；python 载体 load 步
  逐条目建 `Collection` 并 `module.add` 到集合名下（引擎句柄不跨缝回传
  ——pyclass 带 `*mut PyObject`，非 Send）。未动：bgi 的 `exchange()` 按
  §2 的 typed 帧形状实施（先 JSON、载荷即帧类型字段，后整通道 CBOR）；
  wasm 不动。
- **aura**（4.16a 部分落地）：`run_job` 的 script 臂从 `StorePlan.entries`
  填槽，引擎闭包捕获**裸 realm-mq 句柄**——注入的 `Collection` 自绑
  ns，绑定面与 `ctx_store_emit` 字节同一（`ns_raw` 形态只属于 wasm
  平面）。`host_bridge_for` 的 `ctx_store_emit` JSON 入口随 §2 退役
  （op set 冻结，见 §3）；realm 侧执行体（`store_exec`、plan 解析）
  全部幸存——变的只是载荷的到达形状。
- **文档**：ADR-0026 §3 的 python 措辞按本 ADR 修订（绑定，无 adapter）；
  ADR-0035 §3 的 host 帧形状随 §2 更新；本文件取代两者的存储桥段落。
- **排期**：不插队 4.14 剩余项与 4.15。建议顺序：4.14 闸门 2/3（nu
  垫片 + PTY 退役，走过渡 JSON 缝——垫片与缝的形态解耦，§3.1 已记）→
  4.15 信封合并（host 通道与 call 通道共用信封，typed 载荷顺势）→
  本 ADR 实施（§1 先行，§2 随 CBOR）。
