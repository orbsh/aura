# 0037 — 摊位的存储接入：typed host channel + 进程内绑定（ADR-0026 §3 合同修订）

> **语言：** [English](0037-typed-storage-plane.md)（主文档） · [中文](0037-typed-storage-plane.zh-CN.md)

**状态：** Accepted（2026-09-28）；§2 于 2026-09-30 由用户的
双协议裁决修订（见下）。全部已落地：§1（Phase 4.16a python 绑定面
2026-09-30；Phase 4.16b steel Collection 面 2026-09-30）与 §2
（Phase 4.16c typed 宿主帧 + 声明式双编码 2026-09-30）。由用户的合同质疑引发：
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

**裁决——进程内直绑；进程外走 typed host channel；通道编码按摊位声明、
双协议（json/cbor，用户裁决 2026-09-30 替换整通道 CBOR 计划，见 §2）
——存储操作是通道帧里的一个类型，不是为存储新开一套编码。**

### 1. 进程内载体（python / steel）：绑定面，无翻译

- **python**：`okm-python` 的 Collection 绑定直接注册进会话模块（与
  `ctx_store_emit` 并列的入口；`add_class::<Collection>()` 是现成的）。
  翻译次数 = 0。
- **steel**：`okm-steel` 已有 schema/encode/decode 注册；Collection 方法面
  已在 4.16b 补齐，对齐 python 绑定：同一 `DynamicCollection` 的两个 host
  面，一份执行体。句柄形态由 `'static`/thread-local 约束裁决：PER-VM
  注册表（非 codec 句柄那种 thread_local——会话 VM 会跨 worker 线程迁移）
  + 六个固定名全局 fns，脚本按名字字符串寻址集合
  (`(collection-put! "notes" pkey doc)`)。不用点号 per-collection shim
  （`Counters.put`——实测 define 与调用两侧都能解析），因为 steel 在
  DEFINE 编译期解析自由标识符：introspect 的临时引擎必须携带同名符号
  （ctx-stub 先例），而 shim 名是脚本内容、在临时引擎里无法打桩——会重开
  声明静默丢失的陷阱。

### 2. 进程外通道 = 一条流，类型化帧；通道编码按声明（双编码）

用户的模型：host 通道不是"每操作一条 JSON 文本缝"，是**一条类型化帧的
消息流**，存储操作是其中一个类型。帧词汇保持 ADR-0035 §3 的形状，载荷
类型化：

```
child → parent   {"host": {"type": "invoke",  "args": <typed>}}
child → parent   {"host": {"type": "iterate", "op": "start|next|dispose", …}}
child → parent   {"host": {"type": "store",   "op": <typed okm 指令>}}
parent → child   {"host_reply": {"ok": <typed>}}
```

- **编码按摊位声明，双协议（用户裁决 2026-09-30，替换整通道单编码
  计划）**：通道带两种编码，由声明选择——`BoothType::encoded
  (ChannelEncoding)`（aura-booth，serde 值 `json`/`cbor`，持久化为
  `BoothDef` 热段尾部追加字段）与远程线上的 `ToolCall.encoding`
  （进程内：realm 下传；远程：随调用到节点）。`Json` = 换行分隔行，
  stdlib 可达的默认，所有旧摊位与旧持久化行都骑它；`Cbor` = 每帧一个
  自定界文档（无行终止符——ciborium 恰好读声明的字节数，阻塞管道上
  连续解码逐帧落位）。子进程经 spawn 注入的 `BGI_ENCODING` env 得知
  编码——不追加 argv（fifo 形恰好传 `[req rep]`，生成的参数会破作者
  `def main` 的 arity）。逼出双形的实测事实：nu 0.115 没有 CBOR 编解码
  ——入口判据（ADR-0035 §3：任何语言用 stdlib 解析器就能到达）对整通道
  CBOR 是硬墙。CBOR 声明撞上 nu fifo spec = 启动期错误，绝不静默降级。
  下面的类型化在两种编码下同样成立——编码改编解码，类型化改词汇表；
  正交两轴。第三种编码仍由 Windmill 判据把关：解析驱动不了只有解析
  才能做对的动作，就不建。
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

闸门 1 落地的 JSON 指令文档（`HostOp::StoreEmit`、
`host_bridge_for("ctx_store_emit")`）是声明过的过渡形态；§2 落地退役了
它的**入口**——bgi 缝上 `op: "ctx_store_emit"` 自由字符串查表已消失，
由类型化 `store` 帧取代（指令本身仍按**数据**搬运——effector 保持
schema-blind；退役的是入口，不是文档）。远程 WS 的 `HostOp::StoreEmit`
变体本就是类型化帧（serde 判别枚举），那里没有退役对象。§1 落地后
python/steel 不再经过这条缝；bgi/nu 摊位读的是类型化 store 帧。

## 诚实语义代价

- **同日返工**：闸门 1（`HostOp::StoreEmit` JSON 臂 + 桥 fn）落地不到
  一天即被声明为过渡形态。接受——合同（§3 一句话）的歧义被用户的
  okm 绑定事实戳破，修合同的成本低于让过渡形态长成永久形态。
- **effector 的依赖面扩大**：python 绑定注册 = effector-runtime 的 python
  feature 需要 `okm-python`（或其逻辑内联到 effector 的 python 载体）。
  不违"effector 不依赖 aura crate"铁律（okm 独立于 aura），但 okm 成为
  effector 的传递依赖——注册表 + 类型化帧协议 + 直接绑定三者都在把 effector
  从"协议搬运工"推向"运行时"，这条线要一直盯着。
- **steel 绑定的缺口已闭合，且闭合里含一个真实裁决**：补 `okm-steel` 的
  Collection 方法面（4.16b，okm 仓的一刀）兑现了上面预告的形状裁决——
  per-VM 注册表 + 固定名按字符串寻址（见 §1）；点号 shim 方案被
  define-compile 陷阱否决，不是被口味否决。
- **过渡期 = 两种正确形态并存**：§1 落地后 python/steel 用绑定、bgi/nu
  仍走 JSON 缝——§2 落地后**入口**形状统一（两种编码下的类型化帧）；
  绑定与帧的分层正是 §2 的载体分层意图，不是残留。
- **双编码是实测让路，不是对冲**：原整通道 CBOR 计划死于 nu 的 stdlib
  （没有 CBOR 编解码，把编解码塞进一门语言的能力面正是入口判据禁止的
  library tax）。声明带两种编码而非一次整体升级——每个摊位终身恰好
  用一种、通道不按帧协商；第三种编码仍关在 Windmill 判据后面。

## 后果

- **okm**（实施项）：`okm-python` 已加宿主注入面——`Collection::with_store`
  骑字节级 `Engine` trait（4.16a，提交 58cf72b）。4.16b 把引擎面 + 条目
  解析抽为共享 crate `okm-entry`（python 改骑它——绑定复刻条目语义，一如
  复刻字节布局，都是被否决的那类债），`okm-steel` 的 Collection 方法面
  （put/get/delete/scan + reduce 读，per-VM 注册表，按名字字符串寻址）建
  其上。4.16b 同时修复 `okm-steel` 的编译欠账：`Value::Obj`/`Value::Array`
  （okm 0c2a354）在 `value_to_steel` 里一直没有臂——该绑定自此编译不过。
  接线宿主时又挖出一个潜在缺陷：slatedb 同步门面持有 runtime 并
  `block_on`，在已进入 tokio context 的线程上必 panic（spawn_blocking
  保留 context——realm 恰好在其中驱动注入）；门面改为专用驱动线程（okm
  92b2551，锁：`okm-core/tests/driver_thread_test.rs`）。
- **effector**（4.16a 已落地）：`HostBridge` 带 `storage` 槽——宿主引擎藏在
  四个字节闭包后面（`StorageEngineFns`：缝上无 okm 类型，effector 与 aura
  各用不同 okm rev 编译互不干扰）+ plan 的原始条目；python 载体 load 步
  逐条目建 `Collection` 并 `module.add` 到集合名下（引擎句柄不跨缝回传
  ——pyclass 带 `*mut PyObject`，非 Send）；steel 载体消费同一槽（4.16b）：
  `ClosureEngine` 把四个字节闭包适配到 okm-steel 的 Engine trait，
  `SteelSession::new` 在会话启动建 per-VM 注册表，stub 臂只注册进
  introspect 的临时引擎（ctx-stub 的定域规则——同名 `register_fn` 叠加会
  遮蔽常驻会话里的真函数）。bgi 的 `exchange()` 按 §2 的 typed 帧形状
  **已落地**（4.16c）：`{"host": {"type": …}}` 反序列化进类型化枚举再
  映射回桥表（判别符错 = 解码失败 = 错误值，静默查表落空已消失）；会话
  编码=声明（JSON 行 / 自定界 CBOR 文档），经 `BGI_ENCODING` spawn env
  传给子进程；exec 一次性载体同字段；wasm 不动。
- **aura**（4.16a/b）：`run_job` 的 script 臂从 `StorePlan.entries`
  填槽，引擎闭包捕获**裸 realm-mq 句柄**——注入的 `Collection` 自绑
  ns，绑定面与 `ctx_store_emit` 字节同一（`ns_raw` 形态只属于 wasm
  平面）。锁按载体：`py_injection.rs`（python，4.16a）、
  `steel_injection.rs`（steel，4.16b——同一互读形态：绑定写 ↔ emit 读、
  反向亦然，evict 后 per-VM 注册表在幸存行上重建）。
  `host_bridge_for` 的 `ctx_store_emit` JSON 入口随 §2 退役（4.16c；
  期间 op set 冻结，见 §3）；realm 侧执行体（`store_exec`、plan 解析）
  全部幸存——载荷的到达形状现在是类型化 `store` 帧。声明面（4.16c）：
  aura-booth 带自有的 `ChannelEncoding`（crate 保持 effector-free）、
  `BoothType::encoded` 选择编码、`PersistedBooth`/`BoothDef` 以热段尾部
  追加字段持久化（旧行重读为 json——其实际行为），`introspect_schema`
  按声明编码起抛却子进程。
- **文档**：ADR-0026 §3 的 python 措辞按本 ADR 修订（绑定，无 adapter）；
  ADR-0035 §3 的 host 帧形状随 §2 更新；本文件取代两者的存储桥段落。
- **排期**：按预定顺序落地——4.14 闸门 2/3 与 4.15 先行（nu 垫片 +
  PTY 退役走过渡 JSON 缝；垫片与缝的形态解耦，§3.1 已记；信封合并给了
  host 通道与 call 通道同一形状），随后本 ADR §1（绑定，4.16a/b）与
  §2（类型化帧 + 声明式双编码，4.16c）。CBOR 一半到达的形态**不是**
  本文最初勾勒的整通道升级，而是声明式双编码（用户裁决 2026-09-30）：
  nu 的 stdlib 墙把单升级计划实测判死。
