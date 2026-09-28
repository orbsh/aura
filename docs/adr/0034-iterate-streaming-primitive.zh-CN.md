# 0034 — iterate：摊位之间的生成器语义流式调用

> **语言：** [English](0034-iterate-streaming-primitive.md)（主文档） · [中文](0034-iterate-streaming-primitive.zh-CN.md)

**状态：** Accepted（2026-09-28）。与 emit/on/invoke 并列的新 ctx 原语。
动机来自 LLM provider 需求（gravity → OpenAI token 流）——按 ADR-0031，
它由兄弟摊位的自有代码服务；但这个需求暴露了缺失的形态：**一个摊位如何
消费另一个摊位的流**。

## 背景

`ctx.invoke` 返回单值。有些 handler 的工作天然是顺序输出：SSE token 流、
分页拉取、长扫描。问题：什么投递形态服务它？

诱人的形状——逐条 emit（WS 式消息流）——被否决，三个结构性缺陷：

1. **没有内建终止。** 流必须靠约定宣告结束（sentinel 事件）——与合法数据
   冲突、靠协调不靠结构。生成器的耗尽【就是】它的结束。
2. **没有消费方绑定。** emit 按事件名路由；消费方消失时生产者不会停——
   dead ring 噪声、白烧算力。
3. **没有背压。** 队列按生产速率吸收。兜底阀门（skip-to-now，Phase 4.5c）
   在这里语义错误：跳过 token = 丢内容。

生成器/迭代器模型把这三条性质内建在结构里：拉取式（终止、背压、消费方
活性都从「谁来要下一条」里掉下来），且各语言的惯用形态能原生包装它。

## 决策

**裁决——`ctx.iterate(target, handler, args)` 是一等 ctx 原语，与
emit/on/invoke 并列。** 返回游标；游标在每类 carrier 里包装为原生可迭代
对象，在无迭代协议的语言里为「拉到 done 为止」的显式循环。

### 1. 线路形状：类型化信封，不是魔法值

每次拉取往返交换一个信封：

```
{ "item": <值>, "done": false }   ← 下一条
{ "done": true }                  ← 耗尽（结构性结束）
{ "error": "..." }                ← 流中途失败（ADR-0012）
```

终止是类型化字段，绝不是哨兵字符串。有可被宿主驱动的生成器的语言做翻译：
wrapper 在 `done: true` 时抛 `StopIteration`——生产方 handler 用原生生
成器写（python `yield`），完全看不见线路协议；框架驱动生成器，生成器耗尽
对称地编码为 `done: true`。

无宿主可驱动生成器的语言（steel、nushell、wasm）：handler 是【可重复调用
的函数，显式返回信封】——`done: true` 是写出来的，不是推导出来的。除信封
外没有新发明；守卫值就是 schema 字段。wasm 没有特例：它就是既有的主机桥
函数调用（4.5b ABI 不需要新东西），Rust 写的模块在 guest 内部映射
`Iterator`——流状态与 `.next()` 调用住在模块状态里，耗尽时在 ABI 边缘投影
为显式信封。生成器语义在迭代协议所在之处成立：python 的生成器由宿主跨
FFI 边界驱动；wasm 内部由 guest 自己驱动。（steel 的 `(yield)` 生成器基于
call/cc，跨多次宿主驱动的引擎调用不可靠地续跑——记为被否决的 carrier
变体；信封模式才是那里的诚实形态。）

### 2. 流身份与 session 绑定

流住在生产方的常驻 session 里（Phase 2.6——正是驻留让有状态的拉取协议
成为可能，不是巧合）。`stream_id` 关联各次拉取的方式与 `pending_remote`
关联 invoke 回复相同：一个帧身份，零新机制。分区键选择（如 provider 摊位
用 `key = 消费方 session_id`）是摊位自己的事，循常规分区键规则。

### 3. dispose：强制的对偶

消费方可能中途弃流（`break`）。有原生析构钩子的 carrier（python/steel
wrapper 挂 `GeneratorExit`）自动发 dispose；没有的（nushell，以及任何
消费方的显式 break）【必须】调 `cursor.dispose()`。无后续拉取也无 dispose
的流靠驱逐释放，不靠魔法。只有 iterate 没有 dispose，等于 dead ring 问题
换了个请求形态的徽章。

### 4. 驻留计时：idle_ttl 从流停止起算

拉取就是 session 活动：每次 `next` 重置 idle 计时器，长流无需特别声明
即可让生产方保持驻留。流结束（耗尽或 dispose）时，经既有 timer API
（ADR-0016）重新武装计时器，常规 `idle_ttl` 驱逐照常生效。provider 摊位
像其它摊位一样声明 TTL——「TTL ≥ 流时长上界」的伪代价不存在。驻留中途
被驱逐打断的流 = 拉取失败（error 信封），归 ADR-0012 管。

### 5. 层级：只做 hot

每次拉取是 hot 调用（oneshot + 超时）。冷流式（人以生产方身份、拉取无限
挂起）不在范围：无已知消费者，且冷层纪律就是不为不存在的流量铺管道。

### 6. LLM 服务面的生产方在脚本 carrier

provider 摊位模式：python handler——`httpx.stream`、解析
SSE、每个 token 事件一次 `yield`——carrier 的原生生成器驱动信封。wasm
两侧都服务：作为消费方跑拉到 done 的循环（没有可停放的宿主驱动生成器），
作为生产方显式写信封——与 nushell 同一形态，Rust guest 在模块状态内映射
`Iterator`（§1）。它的
对外 HTTP 需求（本原语存在的原因）由消费兄弟摊位解决，不由 wasi-http
主机接线解决（aura PLAN 2026-09-28 的 wasi-http 条目被本 ADR 取代并撤销；
ADR-0031 的裁决原样成立：摊位在自己的代码里决定访问方式——兄弟摊位就是
那段代码，经场到达它，realm 的观测面因此保持诚实）。

## 职责切割（从 gravity 移出去的是什么）

provider 摊位是**传输适配器**：HTTP/SSE 机制、传输层重试退避、密钥保管
（env 注入，绝不进模块）。gravity 保留**编排决策**：模型选择、fallback、
circuit breaking、依赖 transcript 的重试——一切需要本轮上下文的留在发起
推理的一侧。「LLM identity stays Gravity-side」（gravity PLAN）不动；
搬家的是逐轮裸 socket 的活，而 gravity-as-wasm 原生做不了（§6）。

## 诚实的语义代价

- **逐条拉取是一次往返。** 当前默认且唯一的拉取粒度是一条一个 item；
  `pull(n)` 批量在计划中——Windmill 判据适用（快生产者把跳数成本变成
  真实成本时才建旋钮，不提前）。LLM 的 token 延迟由生成主导、不由跳数
  主导——第一个消费者尚无实测需求。但快生产者上跳数是真实成本，
  定价权在消费方。
- **iterate 是「有状态生产方的多次 invoke」。** MQ 投递、游标、队列一切
  不变——也一切帮不上你：流不是持久的、不可重放。生产方中途驱逐 = 流
  失败，消费方驱逐 = wrapper 死亡。需要 at-least-once 流式的用例想要的
  是事件，本 ADR 是错的工具。
- **三个动词，一套机制。** iterate/dispose 是骑在既有调用机件上的帧级
  调用；信封是 schema 不是协议。「代价」是文档面：各 carrier 必须包装同
  一个信封，而 nushell/守卫值路径邀请手写的 bug——类型化 `done` 字段的
  信封形状收窄但不消除这个坑。

## 后果

- **aura：** ctx 表面 + iterate/dispose 的帧管道；carrier 包装
  （python 宿主驱动原生生成器；steel/nushell/wasm 显式信封；python 消费
  wrapper 在 GeneratorExit 上自动 dispose）；ADR-0011 的 ctx
  边界清单加 iterate/dispose——实例绑定、Host 管控的能力。
- **gravity：** provider 摊位 = python 摊位类型（传输适配器）；Phase 1
  LLM 层经 iterate 消费。
- **okm/probe：** 帧词汇表加这两种调用（probe-protocol——远程生产/消费方
  骑同一信封）。
- **取代：** wasi-http carrier 任务（aura PLAN 遗留节，2026-09-28 立项）
  ——撤销；wasm 对外访问 = 消费兄弟摊位。

## 相关记录

ADR-0031（对外访问归摊位代码——provider 摊位是它的第一个应用）、
ADR-0011（ctx 边界）、ADR-0012（失败是值——流中途错误）、ADR-0016
（timer API——流停止时重武装 TTL）、Phase 2.6（驻留——有状态的生产方）、
Phase 3.5（hot 层——每次拉取）、modeling.md（流式消费模式节）。

## Errata（2026-09-28，ADR-0036）

上述决策不变。两处【形式】被取代：

- **§1 信封：** `{item, done}` 对泛化为与 invoke 共用的一套信封——终止轮
  带 `value` 而非 `item`（`{done: true, value}` / `{done: true}`），且
  `done` 恒为存在的布尔。invoke 是首应答即终止的流（ADR-0036 §1）。
- **"三个动词、一套机制"：** 改为"两个动词、一套信封"——invoke 与 iterate
  保留各自 ctx 名字（消费意图不同，ADR-0036 §3），但骑同一条线路形状。
  流的关联从 `done` 导出，不再靠 `stream_id` 字段的位置性存在来推
  （ADR-0036 §4 收口落地协议里的这处松散）。

上文正文按 2026-09-28 的裁决原样保留。
