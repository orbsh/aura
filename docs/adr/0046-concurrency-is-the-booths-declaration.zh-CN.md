# ADR-0046 —— 并发是 booth 自己的声明，队列只是它的答案之一

**Status:** Accepted。已落：realm 的派发（`realm/src/instance.rs`）、session carrier 的注册表
（`effector-runtime` 的 `carrier/session.rs`）、induction carrier 的读线程（`carrier/exec.rs`）、
类型定义（`aura-booth`）、schema 的内省路径（`aura-engine`），以及 guest 侧的 ctx 标注
（`induction-protocol` 的 `HostFrame::call`、`induction-guest` 的 `Ctx`）。

## Context

actor 模型是**拥有**并发的办法，不是回避并发的办法：邮箱让调用方把活交出去而不必等，实例让这份活
的状态有个唯一的家。因此实例上的锁不是模型本身，而是为一个特例准备的补丁：**body 不可重入、而它的
常驻状态又被留着换性能**。

我们把这个补丁付到了所有地方。每个脚本 booth 的调用都要过一把互斥锁，锁在整个 handler 的执行期间
一直握着：`Sessions::with_session_encoded` 锁住实例的槽位，并在锁内跑完这次调用。对一个只有一台 VM、
又受 GIL 限制的 Python booth 来说，这是对的。对一个每次调用都在自己栈上处理的 Rust guest 来说，这是纯
损失——而且是可测量的损失：同一个 gravity turn 实例上的两轮，线上延迟 1.5 s 和 0.8 s，实测 **2.35 s**，
而不是重叠后的 1.5 s。这把锁什么也没买到，却正好赔掉了 actor 模型存在的意义。

同一个错误还有另一半：场地根本无法告诉 realm 某个 booth **可以**重入，于是 realm 只能假设全都不能。

## Decision

**booth 自己声明它的实例如何服务同时到达的调用，realm 按这个声明建它的常驻结构。**

1. **声明是类型的一部分。** `BoothType::concurrency`——一个 `Concurrency` 值，三种形状；类型来自
   脚本的，同一个事实写在它被内省的 schema 的 `lifecycle.concurrency` 下（数字是池子大小，字符串
   `unbounded` 是第三种形状）。宿主 builder 优先于脚本自己的话，与这里其它每一条生命周期事实的规则
   一致。

2. **`Queued`（默认）是一时一调、按到达顺序。** 一个常驻 session。什么都不声明的 booth 保持既有 booth
   已有的行为。这是给不可重入 body 的答案：一台 VM 压在 GIL 后面的解释器、跨调用携带可变状态的 session、
   任何正确性建立在「没人在我里面」之上的东西。

3. **`Pool { slots }` 是至多 `slots` 个常驻 session 并行执行**——php-fpm 的形状。一次调用取一个空
   槽位；发现全都忙时，池子还没到 `slots` 就开一个新 session；超过 `slots` 就等待。这个数既是并行度，
   也是 booth 愿意为常驻付的代价（进程载体上，一个进程一份）。

4. **`Unbounded` 是一个常驻 session，不限在飞调用数——不排队、不设上限。** 这是 booth 自己的进程本来
   就能同时服务多个请求时想要的形状：并行是进程的，再开一个进程买不到任何东西。让它成立的是线协议而不
   是运行时：每个请求帧都带 id，child 按 id 回答，child 发来的 ctx 帧标明它服务的是哪个请求
   （`HostFrame::call`）——于是轮次 A 里发出的工具调用只会由 A 的 bridge 回答，绝不会算到 B 头上。
   因此 `Unbounded` 只对具备**并发面**（`SharedSession`）的载体开放；没有并发面的载体是声明错误，
   而不是悄悄退化成队列——承诺了重叠的 booth 不能看起来像在重叠。

5. **载体从「对话」变成了「路由器」。** induction session 从前在调用方自己的栈里做 request→reply，
   于是无论 child 能做什么，进程都是串行的。现在它拥有：一个从 child stdout 取帧的**读线程**、一张按 id
   唤醒各个请求等待者的 `pending` 表、按请求 id 索引的 `bridges`（ctx 归属）、一把写通道互斥锁，以及一个
   死亡原因——child 退出或答非所问时，一次性唤醒所有等待者。一次调用 = 发一帧 + 等自己那条 channel；
   调用之间除了通道本身不共享任何东西。

6. **这个值只属于 booth，因为只有 booth 知道。** gravity 的 turn 声明 `unbounded`：它的状态活在调用的
   栈上和自己打开的流里，guest 让每个请求跑在自己的线程上（`Guest::concurrent(true)`），于是一个进程
   服务所有设备的所有轮次。Python booth 声明 `Queued`，并行改由载体给到进程级——它的线程本来也无法并发
   跑 Python。IO 密集的异步 body 声明一个数字，把交错交给异步运行时。realm 不猜，也不长出一张按语言分支
   的规则表：它只读一个值。

## Consequences

- **锁从此只存在于有 booth 主动要它的地方。** realm 不再串行化它没有理由串行化的工作，上一条消息还在流，
  下一条就能被服务——在同一个进程里。
- **`Unbounded` 把上限放回了它该在的地方。** realm 对这类 booth 没有准入上限；约束它的是 guest 自己的
  运行时——线程、fd、模型延迟。想要上限的 booth 声明 `Pool { slots }`，并按槽位付费。
- **并行度与常驻量是同一个声明。**「同时能跑多少」与「愿意留多少常驻」在这里是同一个值，因为一个槽位
  **就是**一个常驻 session；`Unbounded` 是「一个 session 足够承载任意多调用」的那种情况。
- **放宽声明是换池，不是改一个正在跑的池。** 老池里的 session 把在飞的调用跑完再被丢掉；下一次调用落在
  按新声明建的池里。
- **驱逐与队列不打架。** 超过容量的调用等的是槽位、不是实例，所以空闲回收仍然看得到「有活来了」，看门狗
  仍然按 job 计时。
- **驱逐现在会真的结束进程。** induction session 在 drop 时 kill 并 reap 它的 child；没有这一步，一个会
  驱逐 booth 的 realm 会在宿主整个生命周期里泄漏子进程。

## Tests

- gravity `a_turn_takes_the_next_round_while_it_is_still_running`：一个 turn 实例（一个进程）上的两轮
  （线上延迟 1.5 s 与 0.8 s）在 1.9 s 内完成。声明落地之前它是 2.35 s，失败；guest 不再声明并发、或宿主
  不再回显 ctx id 时，它同样会失败。
- aura 的 engine 全量：什么都不声明的 booth 行为不变（整套测试都骑着 `Concurrency::Queued`）。
- 其余各仓（effector、gravity、prism）在读线程落地后全绿：串行路径跑的就是其它 booth 仍在走的同一段代码。