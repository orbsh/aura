# 0051 — 派单即建域：从未注册过的工作

> **语言：** [English](0051-dispatch-creates-the-realm.md)（主） · [中文](0051-dispatch-creates-the-realm.zh-CN.md)

**状态：** 已接受（2026-11-19）。**步骤 2a 已落地（2026-11-20）** —— 派单的 realm 侧：`crates/realm/src/dispatch.rs`，从引擎的 `Engine::dispatch` 进入；它到底覆盖了什么见下文《已落地》，没覆盖什么见《推迟》。它裁决"没有注册过的工作在哪里跑"；ADR-0050 裁决 realm 是什么，本记录使用它。
**日期：** 2026-11-19

## 背景

有些工作没法预先注册。gravity 的 skill 步骤是从 k10r 里取出来的数据：一张节点图，每个节点是一个摊位接口，在平台部署之后才被定义，而且可能只定义过一次。现在的办法是在分发层预置一组固定节点、把图从它们身上过一遍 —— gravity 的复杂性就来自这里，而这个办法无法表达某人今天早上才写出来的图。

看起来更直接的替代 —— 注册定义、执行、再删除定义 —— 把一个瞬时事实变成三次往返和一份必须清理的持久状态。更糟的是，这份状态可能被留下：在"执行"与"删除"之间崩一次，注册表里就留下一个没人拥有的摊位类型。

ADR-0050 给了 realm 身份、创建 API 和真正有实义的销毁。本记录用的就是这个口子。

## 决定

**裁决 —— 一次派单可以自带定义。realm 为这个 job 建一个临时 realm，把那些声明注册进去，在那里执行工作，然后销毁它。关于这个 job 的任何东西都不比 job 活得更久。**

1. **定义以声明随工作旅行，绝不作为注册。** 一条声明是数据：

```rust
struct BoothDecl {
    name: String,            // 在这个 realm 里的类型名
    body: Body,              // Script { language, source, encoding } —— 与已注册摊位同样的 body
    storage: Vec<CollectionDecl>,  // 该类型可用的集合；在临时 realm 里它们住在内存
    calls: Vec<CallDecl>,          // 图里的边所寻址的接口
}
```

声明随提交一起到达，因为本来就不指望它们已经存在。在这个仓里它们走的是 job **旁边** —— `Engine::dispatch(&job, &decls)` —— 而不是信封里面：`effector_protocol::JobSource` 是面向 effector 的信封，保持对 aura 无知，而一条声明是 aura 自己的类型（body、storage schema、它的 `receives`）。两者一起旅行，信封不必知道 aura 存在。

2. **建域的是 realm，不是摊位。** 摊位只要求做事；由 realm 判定这件事需要自己的一块空间，并把它建出来。这让建域成为"能授予能力的那一层"的能力，也是摊位完全不需要 realm 词汇的原因。

3. **临时 realm 继承父的节点视图与选点策略。** 它能在同样的标签下寻址同一批节点；它不能扩大那个集合。没有这一条，"临时"就成了绕开选点规则的通道 —— 一个 job 想要笔记本，只需要要一个"能看见笔记本的 realm"。

4. **临时 realm 里没有任何东西被持久化。** 它的类型注册不写 `interface_schema`，它的存储命名空间来自它自己的命名空间段而不是活过进程的行，它的 mq 是一个背后没有持久性的独立引擎（okm ADR-0072）。跨越边界的只有 job 产出的帧，而且是发给派单那个摊位的。

5. **销毁是 job 的最后一步，不是另一件杂务。** 实例、常驻执行器、流、在飞调用随 realm 一起丢弃；存储里没有东西可删，因为什么都没写。job 中途崩溃不会留下任何定义 —— 这正是"注册/执行/删除"三步给不了的保证。

6. **临时 realm 不能再建 realm。** job 可以嵌套（effector ADR-0005），realm 不行（ADR-0050 §7）。嵌套的 job 落在同一个临时 realm 里，这也正是图里的节点能互相看见的原因。

## 后果

- **gravity 不再需要分发层里的固定节点。** 它的 skill 图变成：从 k10r 读出节点（一步）、声明它们、派单。那些预置节点与喂给它们的路由被删除，而不是为了兼容保留。
- **prism 永远不知道临时 realm 存在。** 它持有 `main`、画帧；临时 realm 是 realm 自己的事。
- **帧的寻址要带路径。** `(job, op)` 是下限；嵌套 job 的帧要带整条链，否则摊位分不清自己那一步的输出与孙辈说的。
- **声明了同名类型的 job 会与父的类型相遇。** 在临时 realm 内，声明胜出，父的类型不可见 —— realm 边界就是命名空间边界。这是隔离在工作，也意味着图不能假设自己能按名字够到平台类型；要够到就再派一次单。
- **临时 realm 的代价必须小。** 建它就是一次 map 插入、一个内存 ns 计数器、一个替身存储；一旦建域变贵，这个设计就把每个 skill 步骤变成分配风暴，那时才需要池化 —— 刻意不在这里设计，因为在便宜路径被量出来之前池化是过早的。

## 已落地 —— 步骤 2a（2026-11-20）

派单的 realm 侧实现在 `crates/realm/src/dispatch.rs`，从引擎的 `Engine::dispatch(&job, &decls)` 进入。一个 job 现在按这个顺序做五件事，而顺序本身就是设计：

1. **预检，在任何 realm 存在之前。** 整个 job 先被检查：只接受 `JobSource::Ops`（`Plan` 按名拒绝 —— 推进一份 schematic run 是那份 run 自己的权限）、只接受 `Scope::Turn`（`Shared` 的结果属于整个安装，而一个随 job 死掉的 realm 装不下它们）、声明不重名也不无名、每个 op 的 `executor` 都能解析成 `type/key`（ADR-0042）、每个 op 指名的类型都是本 job 的某条声明、每个 op 的 `needs` 都授得出来。于是"job 指了自己没声明的类型"会在**任何 op 跑之前按名失败**，且什么都没留下。
2. **建域。** `RealmSet::create_ephemeral` —— 一个 uuidv7 加一个属于它自己的内存引擎（ADR-0050 §5）。没有行、没有前缀、水位不前进，所以一个被拒或失败的 job 动不了父的 id 序列。
3. **注册。** 每条声明都经调用方给的 `Registrar`。引擎的实现是 `Engine::register_into_with` → `Realm::register_definition`，也就是 `Engine::register_into` 最后落到的那段身体：`BoothName` 行、带着声明里 `interface_schema` 的 `BoothDef` 行、`EventRoute` 行、代码 blob，全部写进 realm **自己**的存储。一条规则，两个引擎（ADR-0050 §6）—— 没有第二个注册表，没有第二条 ns 规则；并且声明的 schema 压过自省，因为声明是为这个 job 写下的数据。
4. **执行。** op 列表按序，每个 op 按自己的 `on_error`：`Fail` 记下失败并停下 job，其后的 op 报成 `NotRun` 并带上原因（不是从更短的列表里消失）；`Skip` 记下失败继续；`Retry { times }` 至多再做 `times` 次**追加**尝试（effector ADR-0005：`times: 2` 是三次运行，不是两次）；`Never` 恰好一次尝试，且永不被重新进入。outcome 逐个 op 列出它的策略、尝试次数与结果。
5. **销毁。** `RealmSet::destroy` 在每一条返回路径上都跑 —— 成功、某个 op 停下了 job、注册失败 —— 而销毁失败本身是一个 error，不是一个调用方可能不读的字段。

`needs` 按 effector ADR-0005 说的意思执行：它是这个 op 被授予的 ctx **服务**集合，realm 给不出来就在 op 跑之前按名拒绝。`store` 恰在声明给了该类型存储时才授予（计划由注册所写的同一份 schema 解析）；`timer` 授予（realm 有自己的定时平面，条目随它灭亡）；`emit` 授予，因为往外的通道已经存在 —— dispatch 在注册任何声明之前就装好它，于是"已授予"是事实而不是承诺；`resource` 按名拒绝，因为场地的授权不属于这一步。

### 穿过去的帧，以及它们落在哪里

§4 只允许一样东西穿过边界，就是它。dispatch 在注册任何声明之前，先给它刚铸出的 realm 装一条出站通道（`Realm::frames_out`）；一条没有通道的临时 realm 会拒绝自己的 emit 而不是把帧丢掉 —— 于是"这个 job 什么都没发"不可能成为一个静默状态。

规则是**两者都做，永远**：job 的 op 发出的事件，既由 job 自己的路由投递，也投给派单者。做过滤 —— "这些是内部的、那些是消息" —— 需要一份逐类型的声明，而它不存在；弄错了恰好会藏起运维正要看的那几帧。所以每一帧都出去，本地平面不受影响。

投递是**定向的，不是路由的**：帧被追加到父 realm 上那个派单摊位的队列里（`job.frame`，分区 = 摊位的 key），因为派单者不是一个事件的订阅者 —— 它收到的是一封关于自己提交的 job 的信。由此两件事成立：父不需要任何路由行来收帧，而一个不在线的派单者拿到的是积压，不是丢失的帧。

每一帧带着自己的**路径** —— `[{job, op}]`，嵌套 job 则是那条链，而 op 步在每次调用前被改写，因为 op 一次只跑一个；job 自己投的帧（它的结尾）带的是 `[{job}]`，没有 op。地址是 `Job::booth`，由派单者自己填写，所以 job 改不了它的听众。

这条通道承载 job 全部可观测的生命，不只是 op emit 的那些：`emit`、`op.done`、`op.skipped`、`op.failed`、`op.not_run`、`job.end`，以 `kind` 字段区分，并按游标读回（`Engine::job_frames`）。ADR-0052 是那个决定及其理由 —— 一条队列，没有第二个缓冲。

单向是结构性的，不是一条规则：job 的 realm 只从自己的 `@on` 路由收帧，而那些队列只有 realm 自己写。没有向内的门，因为没造过。

### 四条边界，以及各自由什么执行

- **(a) 里面的摊位写不了父的存储 —— 结构性的。** `Realm::with_mq` 拒绝把一个非内存存储绑到 `Ephemeral` id 上，`MqStore::for_realm` 拒绝把引擎键空间的一片绑给它；两个拒绝都按名说出原因。生产环境的父是 fjall，所以父的引擎根本递不进一个 job 的 realm。（内存场地的根句柄与一个全新的内存引擎不可区分，这正是临时 realm 的唯一构造器设在 set 上的原因：`create_ephemeral` 自己铸一个存储。）
- **(b) 它不能向父发布 MQ —— 结构性的。** 每条队列行、游标行与路由行都经 `Realm::mq`，也就是 realm 自己的引擎写下；不存在可以表达的跨 realm emit。这是被证明的而不是被声明的：一个既经计划写入、又 emit 的 job，让父的**状态**命名空间逐字节不变，让父里那个事件的队列深度为 0，而父的队列平面只多出一行 —— 那一帧，在 `job.frame` 下，发给派单者。
- **(c) 它不能开子 realm —— 结构性的，外加按名拒绝。** 没有任何摊位持有 `RealmSet`，所以嵌套不可表达（§2、ADR-0050 §7）。能表达它的那扇门 `Realm::create_child_realm` 回答 `BoundaryRefusal::ChildRealm` 而不是铸一个 —— 一个按名拒绝，加一个 `Infallible` 的成功类型，于是没有调用方能握着"成功创建的子 realm"。
- **(d) 它继承父的视图 —— 配置性的。** realm set 把引擎的 code base URL 与游标 TTL 带进它建的每一个 realm，也把场地的节点池带进去 —— 连同 job 的 `Placement` 收窄成的 `Reach`（ADR-0053）。类型视图是刻意**不**继承的（§后果：realm 边界就是命名空间边界，声明会遮蔽同名的父类型）。

### 信封说不出来的那部分

声明随信封一起到达：`Job::attachments` 里带一个不透明的载荷，名字是 `boothset`（effector ADR-0005 §attachments），`Engine::dispatch(&job)` 负责解码。这就是那道门 —— 只有一道，所以经网络到达的 job 与本进程里组合出的 job 走同一条路，谁也没法给 realm 塞另一个给不了的定义。线上形态是 `aura_booth::persist::PersistedBooth`（注册本来就要写的那条记录）：名字、语言、源码、编解码、那份 `interface_schema`、ttl、并发度。没有第二套声明格式，而唯一无法旅行的 body —— `Body::Rust`（链接进来的函数）—— 由构造排除，而不是靠一条要人记住的规则。`BoothDecl` 仍是本 crate 对 §1 的内存读法（`body` + `storage` + `receives`）；`Dispatch::run(&job, &decls)` 是门下面的接缝，给已经握着声明的调用方。

## 推迟

写明，免得这份记录被读成比它更大的承诺：

- **在一次调用里替你在节点之间挑选** —— `Placement` 是池子上的一道过滤器（ADR-0053），不是挑选器：声明自己指名节点，所以为某次调用挑最闲或最近的节点，需要晚绑定声明，那是另一项改动。
- **嵌套 job** —— §6 说一个 job 的嵌套 job 落在同一个 realm 里。目前还没有从某个 op 内部派单的路径；单层隔离由边界 (c) 执行。
- **gravity / k10r 集成** —— 从 k10r 读出 skill 图并派单。§后果描述的是那个终局；步骤 2a 只是 realm 这一半。
- **Shared 作用域的 job** —— 按名拒绝，而不是勉强携带：它们需要一扇通向"比 job 活得久"的 realm 的门，那是另一个决定。

## 测试

由工作区套件跑（`cargo test --workspace`）；逐个点名，因为每一条都是本记录的主张：

- `crates/engine/tests/dispatch_job.rs` —— `a_job_runs_its_ops_in_order_and_needs_store_is_granted_when_declared`（顺序即依赖，且 `needs.store` 真的被授予）、`skip_records_the_failure_and_carries_on`、`fail_stops_the_job_and_its_successors_are_reported_not_run`、`retry_two_runs_the_step_three_times_and_no_more`、`never_runs_exactly_once_even_when_a_later_step_fails`、`a_failing_never_step_is_one_attempt_and_is_not_retried`、`a_job_naming_a_type_it_did_not_declare_fails_by_name_before_any_op_runs`、`needs_store_is_refused_before_any_op_runs_when_the_type_declared_no_storage`、`a_plan_job_and_a_shared_scope_job_are_refused_by_name`。这些全部走引擎的注册门，所以被检验的是生产路径。
- `crates/engine/tests/dispatch_job.rs` —— `a_frame_a_job_emits_reaches_the_booth_that_dispatched_it`（op 里 `ctx.emit` 的一帧落在派单者队列里，带路径、事件名与载荷）、`a_job_with_no_channel_out_cannot_emit_at_all`（没有通道的临时 realm 按名拒绝 emit）。
- `crates/realm/tests/dispatch_realm.rs` —— `a_job_leaves_the_parent_keyspace_byte_identical`（真实 fjall 键空间在一个"注册带存储的声明、经计划写入、再 emit"的 job 前后各扫一遍：键**与**值都相同、realm 从 `live()` 消失、父的注册表与队列不受影响）、`a_booths_realm_cannot_be_bound_to_the_parent_store`（边界 (a) 的两个拒绝，按名）、`a_realm_cannot_open_a_child_realm`（边界 (c)，按名）。

## 仍待定

- ~~一套声明是 `JobSource` 的变体，还是与 `resources` 并列的字段。~~ 两者都不是，结案为附件：信封上一个通用的承载位（`Job::attachments`，名字 + media + 字节），effector 从不解释它。`JobSource` 变体会让信封学会 aura 的词汇（body、storage schema）；附件只让它学会"有东西随这个 job 旅行"，含义留在填它的那一侧。被它取代的那条规则 —— 声明走并排参数 —— 只活了一天，现已删除。
- ~~临时 realm 的帧是否对同一次派单里的其它 job 可见，还是只对派单那个摊位可见。~~ 结案为后者：帧是定向投递的，不是广播，地址就是派单者自己的名字。一个 job 的帧要抵达兄弟 job，需要两个 realm 之间的一条通道 —— 那不过是换了名字的边界 (a)。

## 相关记录

aura ADR-0050（realm 是一行记录）、ADR-0026（摊位存储按类型划分）、effector ADR-0005（job 是信封）、gravity ADR-0011（组合是图 —— 这里声明的正是那张图）。