# 0052 — job 的结果就是帧，而帧就是那条流

状态：已采纳（2026-11-20）

## 背景

ADR-0051 §4 只允许一样东西穿过临时 realm 的边界：job 产出的帧，发给派单它的那个摊位。这一条已落地（提交 `531a8c6`），但只带了 op 自己的 `ctx.emit`，没有带 job 的**结果**。

于是派单者真正需要的答案 —— 第 2 个 op 跑没跑、返回了什么、job 为什么停下 —— 只存在于 `JobOutcome` 里，交给同一进程内调用 `Engine::dispatch` 的那一方。经网络提交的 job 读不到：填了 `Job::booth`、带了声明的那一方，从此再也收不到这件工作的下落。那扇门于是只能在场地内部使用，恰好排除了 k10r 和 gravity —— 机制本来就是为它们存在的。

这份记录回答的是：job 的结果流，是一件需要**造**的东西，还是一件已经存在、只缺一张面孔的东西。

## 决定

1. **帧通道承载 job 全部可观测的生命。** 每个事件一份载荷，用 `kind` 字段区分（`frame_path::kind`）：`emit`（op 交给场地的东西）、`op.done`（`value`）、`op.skipped`（`error`、`attempts`）、`op.failed`（`error`、`attempts`）、`op.not_run`（`reason`）、`job.end`（`destroyed`、`ops`）。读者从 kind 得知形状，而不是从"恰好出现了哪些字段"去猜。

2. **没有另一条结果流。队列里的行就是那条流。** 它们已经有顺序、已经有序号、已经按分区寻址、已经有保留规则（ADR-0039）。再缓冲一份，是同样的数据配第二套生命周期去弄错 —— 首先就是"job 的 realm 死掉时它怎么办"，而队列天然回答了这件事：行在**父**realm 里，所以它们活得比产出它们的 realm 久。

3. **`job.end` 在 destroy 之后投出，并且是最后一帧。** 它带 `destroyed`，于是"job 结束了"和"什么都还没留下"是一句话，而不是两件要读者拿超时去对上的事。一个在 op 列表开跑之前就失败的 job 也会投它（`ops: 0`）—— 否则等结尾的读者会恰好卡在它最需要知道的那次失败上。

4. **帧的路径说清是谁在说话：op 的是 `[{job, op}]`，job 自己的是 `[{job}]`。** `op` 因此是可选的。没有这个区分，一个数 op 帧的读者会把 job 自己的首尾当成工作。

5. **读它需要地址，不只是 job id。** 行在派单那个摊位的队列里，所以 `job_frames(store, dispatcher, job, after_seq)` 与 `Engine::job_frames(dispatcher, job, after_seq)` 两个都要。派单者在 `Job::booth` 里填的就是地址，所以它两样都有；只知道 job id 的读者得把每个摊位的队列翻一遍。

6. **保留是队列的事，照旧。** 序号不前进的读者，按任何订阅同样的条件失去那些行（ADR-0039 §2）。这条流不另立政策。

## 否决的方案

- **在父 realm 里建一个 `JobStream` 注册表** —— 一个流 id、一个缓冲、一个 dispose。那是同一批行再存一份，外加一套要对着"job 的 realm 何时死"去定义的生命周期，什么都换不来：队列本身就是那个东西，而且带着整个平台都在用的游标纪律。
- **用 `iterate`（ADR-0034）把 op 流出去。** iterate 的产出者是**实例**，每次 `Next` 重新寻址它的 handler；job 在父 realm 里不是实例。为读一个队列去造一个合成实例，只是在已经存着这些数据的行存储前面加一层壳。
- **把帧当事件路由给派单者。** 派单者不是某个事件的订阅者；而它从未声明过的路由只会把这些结果送进死信环 —— 正是这一整串记录要防的那种静默丢失。

## 后果

- **派单者可以是一个轮询者。** `job_frames(after)` 就是全部协议：没有回调、没有连接亲和、没有会话。这才让跨进程的提交方真正用得上派单。
- **一个 job 的排查故事就是"读它的帧"。** 这条轨迹和进程内调用方在 `JobOutcome` 里看到的是同一份，所以对"发生了什么"只有一套说法，而不是每种传输一套。
- **对进程内调用方，`JobOutcome` 与帧是同一件事的两种拼法。** 两个都留：一个是同步的答案，一个是可观测的轨迹。将来合并是可能的，而线上读者不需要它。
- **忙碌派单者的队列会随它的 job 的帧一起长**，直到它的序号前进。这是与任何订阅共有的保留问题，不是新的。
- **`kind` 目前是封闭集合。** 新阶段意味着新常量加一份记录；遇到不认识的 kind 的消费者必须把它当信息，而不是当错误。

## 测试

- `crates/engine/tests/dispatch_job.rs` —— `a_job_reports_what_happened_as_frames_the_dispatcher_can_read`（一个会 emit 的 job：帧序列是 `emit, op.done, job.end`，emit 原样，结尾不指 op 且 `destroyed`，而 realm 消失后帧仍可读）、`every_op_is_reported_and_one_job_does_not_read_as_another`（三个 op 含一个被跳过的：`op.done / op.skipped / op.done / job.end`，每条都带 op 序号，且第二个 job 的帧不会混进来）、`a_job_with_no_channel_out_cannot_emit_at_all`。
- `crates/realm/tests/dispatch_realm.rs` —— `a_job_leaves_the_parent_keyspace_byte_identical`（父的状态命名空间不变；队列平面恰好多了这个 job 的三帧，全部发给派单者）。

## 相关记录

aura ADR-0051（dispatch 造域；§4 就是这份记录补全的那条边界）、ADR-0034（一次调用就是一次流的 Start）、ADR-0039（保留与水位）、effector ADR-0005（job 是一个信封）。