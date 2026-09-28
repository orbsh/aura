# 0036 — 一套信封：invoke 是 iterate 的 1-流

> **语言：** [English](0036-one-envelope.md)（主文档） · [中文](0036-one-envelope.zh-CN.md)

**状态：** Accepted（2026-09-28）。设计裁决；实施未动，见后果。取代
ADR-0034 中【协议层】的动词分离（ctx 表面保留两个动词；线路只走一套
信封）。由用户的对称性提出：done 恒为布尔字段，终止的 done 可以带值——
invoke 就是第一轮就 done 且带值的那个。

## 背景

ADR-0034 落地了 iterate 信封 `{item, done}`，与 `ctx.invoke` 的单值并存，
而它自己的诚实代价一节写下了催生本 ADR 的那句自供：*"iterate 是有状态
生产方的多次 invoke。"* 用户从另一侧顶出了对称性：**invoke 是 `done`
在第一轮就到、且带着值的 1-流。** 两种调用形态不是兄弟——它们是同一
信封的特例，区别只在 `done` 于第几轮到达、带不带值。

python 语言层早已证明这个形状：生成器的 `StopIteration.value` 就是
"以带值的方式结束的流"（`def` 里既有 `yield` 又有 `return x`，返回的
就是这个 value）。线路没有理由去分裂宿主语言已经统一的东西。

而且合并修掉落地协议里一处真实的松散：流的关联目前是**位置性**的——
Start 应答靠"`stream_id` 字段在不在"来识别，那是形状启发式，不是规则。
统一信封把关联从 `done` 本身导出。

## 决策

**裁决——调用线路（进程内接缝、远程帧、exec stdio 皆然）上，每个 handler
应答共用一套信封。ctx 表面保留 `invoke` 与 `iterate` 两个动词。**

### 1. 信封

```
{ "done": false, "item": <值>, "stream_id": "<id>" }   已启动流的首应答
{ "done": false, "item": <值> }                         后续拉取
{ "done": true, "value": <值> }                         终止，带值（invoke / 生成器 return）
{ "done": true }                                        终止，空
```

- `done` 恒在，恒为布尔——用户的规则，原样采纳。
- `item` 只允许与 `done: false` 同现；`value` 只允许与 `done: true` 同现。
- `stream_id` 伴随**非终止**的首应答（realm 在 Start 铸造，首应答即终止时
  丢弃注册——见 §4；终止的首应答不带 id）。
- 失败走外层 Result 通道（ADR-0012）：信封内不设 error 字段。

`invoke` 就是首应答即 `done: true` 的流；realm 把 `value` 解包给 parked
caller 作为单值。合并的全部就是这些——不加动词，不加机制。

### 2. carrier 形态

plain return（非生成器 python、Rust closure、远程一次性脚本）包装为
`{done: true, value: <返回值>}`。原生生成器（python）把 `StopIteration.value`
投影进终止信封——框架读 `.value`，不再只看异常本身。信封模式 handler
（steel/nushell/wasm，ADR-0034 §1）写 `{done: true}`，也可以写 `value`；
校验拒绝终止轮携带 `item`、要求每轮携带 `done`。dispose 对偶不动：
`GeneratorExit` → close，无钩子处显式 `dispose()`。

### 3. ctx 表面保持两个动词

名字分开，因为**消费意图**不同：拿单值 vs 迭代序列。合并名字会逼每个
消费方在 `done` 上分支——每个调用点多一次判断，换框架内少一次协议分支，
不划算。ADR-0011 的表面最小化数的是语义完整，不是字符数：两个正交意图、
两个名字、一条线路。ADR-0034 的"三个动词、一套机制"就此改为
"两个动词、一套信封"。

终止 `value` 的到达方式是**整拉形态**：一轮、终止、解包。这正是
`ctx.invoke`——它本来就是统一机制上的取值糖；原生迭代刻意丢弃尾值
（python 对生成器 `for` 丢弃 `StopIteration.value`；Rust 游标循环丢掉末轮
`Envelope.value`）。需要它的消费方显式拉取，或直接 invoke。

### 4. 关联从 `done` 导出，不从字段位置推导

位置启发式（靠 `stream_id` 字段识别 Start 应答）被替换：`done: false`
而缺 `stream_id` 的应答是协议错误；realm 在 Start 注册流，若首应答即终止
则立刻注销（invoke 快路径：铸 id 再丢弃无可测成本，且代码路径只有一条
——不留"invoke 作业跳过注册"的特例）。事件投递（fire-and-forget）照旧：
信封产出即被 drop 丢弃。

## 诚实的语义代价

- **当日落地代码的返工。** 信封规则触及 probe 的 `ResidentSession` 接缝
  （`call` 并入流接缝）、python 的生成器投影（开始读 `.value`）、共享
  `envelope_pull` 校验、aura 的 Job/`JobKind`（invoke kind 消失）、
  `Realm::call`（变成 Start+解包糖）、以及六个 `iterate.rs` 验收测试
  （形状变，断言大体幸存）。真实的 diff；用户的对称性比一天的稳定更值钱，
  且协议足够年轻——没有外部消费方钉住了旧形状。
- **每个 carrier 返回值都要过一层包装。** plain 值变终止信封——接缝处
  每次调用构造一个 JSON 对象。交给 handler 的值不变；成本在框架侧构造
  与校验面。
- **校验长出两条跨字段规则**（`item` 当且仅当非终止、`value` 当且仅当
  终止）。类型化 `done` 字段正是让它们可校验的东西——本 ADR 用"被强制
  执行的形状规则"换掉哨兵坑（ADR-0034 已删的那个），不是换回约定。
- **`StreamCursor::value()`（done 之后取值）是新表面**，原生糖刻意不消费
  它——文档必须写明这是显式消费方的取值器，否则它邀请回 §3 刚拒绝的
  那种分支。

## 后果

- **probe：** `ResidentSession::call` 折进流接缝；python 捕获
  `StopIteration.value`；`envelope_pull` 加终止-值校验；nushell/wasm/steel
  的 handler 文档更新（终止轮 `{done:true, value}` 合法）。
- **aura：** `JobKind::Invoke` 删除（所有作业都是流操作）；`Realm::call`
  变为 Start+终止解包的糖；`Envelope` 加 `value`；`StreamCursor::value()`；
  ctx invoke/iterate 签名不变。ADR-0034 追加 erratum（其 §1 表与"三个
  动词"句在【形式】上被取代，决策不变）。
- **okm/probe-protocol：** 无线路格式变化（信封是骑既有帧的 schema，
  恰如 ADR-0034 定价的那样）。
- **gravity/Phase 4.14：** exec 载体（ADR-0035）从第一帧起就对着统一信封
  实现——不留过渡形状；`store_emit` 臂与之正交，不受影响。
- **PLAN：** Phase 4.15——信封统一；排在 4.14【落地之后】（exec 不能追一个
  变动中的协议；0036 的合并也不改帧词汇的任何一侧）。

## 考虑过的替代方案

- **ctx 动词一并合并**（`ctx.call → cursor` 包打一切）：因 §3 的理由否决
  ——它把 done 分支从框架搬进每个消费点。用户提议命名的是协议合并，不是
  名字合并；在此记录，因为一个字的指令仍可推翻本条。
- **保留两套信封**（invoke 裸值 vs iterate 成对）：本 ADR 之前的现状；否决，
  因为它保留了位置性 stream_id 启发式、把校验面翻倍（每 carrier 包装要认
  两种形状），而省不下线路字节——两种写法信封对象一样大。
- **`value` 恒在（每条信封都带，可为 null）**：否决——`done:false` 轮带着
  value 字段会诱发"值是中途载荷"的误读；那里载荷已经是 `item`。跨字段
  规则的存在正为了让误读不可表示。

## 相关记录

ADR-0034（被本 ADR 统一信封吸收的 iterate 裁决；其决策不变，形式更新）、
ADR-0012（失败是值——外层 Result 通道）、ADR-0011（ctx 边界——两动词
一条线路的判据）、ADR-0035/Phase 4.14（exec 载体对着统一信封实现）、
python `StopIteration.value`（语言层的先例）。
