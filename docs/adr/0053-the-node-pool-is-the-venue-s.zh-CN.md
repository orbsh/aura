# 0053 — 节点池属于场地，placement 是一道过滤器

状态：已采纳（2026-11-20）

## 背景

`Placement` 从 effector ADR-0005 起就搭在 job 信封上，aura 一直只把它抄进 `JobOutcome` 而不据此行动，理由是"选节点需要一个节点池，而它还不存在"（ADR-0051 §Status）。与此同时有两件事都为真：

- **连接确实存在，但是按域存的。** `Realm.effectors` 是 alias → 连接的 `HashMap`，由准入该节点的网关填写。一个域一份。
- **job 的域什么都不继承。** ADR-0051 在它第一个 op 之前一瞬才把它造出来。于是它看见的那份表是**空的**，派单出去的 job 里的 `Body::Remote` op 找不到拥有 executor 的那个节点 —— 它报"effector not connected"，而那个节点其实是连着的。按域存的表不是一个小一点的池子，而是一个根本做不了远程工作的域。

于是问题变成两问：节点到底是域的属性还是场地的属性？而在声明自己指名节点（`Body::Remote { node_alias }`，因为 mount 是"节点 × 语言"注册的）的前提下，job 级的 `Placement` 究竟能意味着什么？

## 决定

1. **池子属于场地：每个 engine 一个，被每个域借用。** `NodePool` 持有 `NodeRecord { alias, conn, carriers, class, labels }`。它归 `RealmSet` 所有，它造出的每个域拿到同一个 `Arc` —— 而且只在**一处**做（`RealmSet::adopt`），因为两条建域路径都必须做；漏掉的那条会在“一微秒前刚派单的 job”里报"effector not connected"，离真正的原因很远。

2. **节点说明它是哪台机器，准入方说明它用来干什么。** `NodeRecord::machine` 是节点对自己硬件的说法（序列号，或者它只生成一次并保住的那个 id —— effector ADR-0006）；`labels` 是准入方的说法。这个划分既不是信任梯度，也不是"含不含特权" —— 它问的是**一个错的声明能否被看出是错的**。机器标识是唯一的：一台机器声称别人的序列号就是一个**矛盾**，报出 `same_machine` 就是那个矛盾浮出水面。标签则可猜、且本来就被共享 —— 很多机器都是 `site:home` —— 所以错的声明与对的声明无法区分，除了准入之外没有任何东西能做那道校验。

3. **标签是准入方的说法，而且继续如此。** 关于位置的一切都不走注册帧：effector ADR-0005 §2b 已经定下，一个能说出自己标签的节点也就能说出别人的标签，这份记录不重开这一条。所以池子从准入那一方取标签 —— 可能来自 grant（prism，拥有注册表的准入方），也可能来自部署文件。由不知道 grant 的东西准入的节点是**未标注位置**的：对任何不要求位置的 job 可达，对任何要求位置的 job 都不可达。aura 自己的 `serve_effectors` 网关因此传空集：它没有注册表可读，也就没有资格给任何人定位置，说出来比猜一个更好。

4. **`Placement::Labels` 是 job 所见池子的一道过滤器。** dispatch 在域存在之前就把它解析成 `Reach`：
   - `Placement::Any`，或 `Labels { labels: [] }`（信封把空列表定义为 `Any`）→ `Reach::All`。
   - `Labels { labels }` → `Reach::Only(pool.matching(labels))`，子集匹配：要求的每个标签都要在节点上。
   job 的域携带这个 `Reach`，远程调用路径在发送之前检查它。指名到范围之外的节点的调用被明确拒绝 —— `node 'x' is outside this job's placement — it may reach [a, b]` —— 而不是悄悄由一台它没指名的机器来服务。

5. **匹配不到任何节点的 placement 在域存在之前就被拒**（`DispatchError::Nowhere`）。在一个没有节点的"地方"里运行的 job 稍后会在每个 op 上失败，而那个理由读起来像是节点的问题；它还必须跑在某个地方 —— 唯一剩下的就是 job 明确排除掉的那些。

6. **结果记录 job 看得见什么**（`JobOutcome::nodes`），不只是它要什么。placement 因此可以事后核对：一个带着 `site:home` 跑完、报出两个节点的 job，是一个 placement 可满足、且被按本意读了的 job。

7. **标签不会在一次调用里替你在节点之间挑一个。** 声明自己指名节点，所以 job 级 placement 今天唯一能意味着的就是"这个 job 能碰到哪些节点"。晚绑定 —— 声明写的是要求而不是 alias、由 dispatch 选节点 —— 是另一项改动，有它自己的记录：它改的是 mount **是什么**，不是 job 指向哪。

## 否决的方案

- **保留按域的表，建域时拷贝一份。** 节点一重连，副本就分叉了，"这次调用看见的是哪一份"就成了一个没有好答案的问题。池子一把锁、一个真相。
- **让 placement 替调用选节点、忽略声明里的 alias。** alias 正是 mount 注册时用的名字；悄悄覆盖它等于在声明从未提到的机器上跑代码，而且还得指望那台机器上有这个 executor。
- **把空匹配当成 `Reach::Only({})`、逐 op 拒绝。** 同一个失败说 N 遍，而且是在为它白造了域和注册表之后。
- **让节点在 `Register` 里带自己的标签。** 那是最省事的样子，而错在 effector ADR-0005 §2b 说的理由上：一个说得出 `site:prod` 的节点，就能用它去接本不该给它的活。标签随准入走，不随节点走。

## 后果

- **`Body::Remote` 在派单出去的 job 里能用了**，而这才是 job 能跑到场地进程之外的前提。
- **`Needs.resource` 仍是按名拒绝**（ADR-0048/0049）：资源是场地状态，"能否碰到某节点"是另一个问题。
- **同一 alias 重连的节点会顶掉前一个**，而且顶替要连两个 peer 一起报出来（ADR-0015 §7）—— 池子把被顶掉的记录交回来，好让网关能说这件事。
- **未标注位置的节点，对任何不要求位置的 job 都可达，对任何要求位置的 job 都不可达。** 今天这句话描述的是 aura 自己网关准入的每一个节点，因为它不读 grant。production 里位置功能可用的前提是准入方换成 prism，由它在准入时签发标签；在那之前，测试充当那个准入方，直接插入已定位置的记录。
- **按负载与就近的挑选仍未实现。** `Placement::Any` 并不挑最闲的那个节点，它把整个池子敞开。为单次调用在节点间挑选需要晚绑定声明，§7 推迟了它。

## 测试

- `crates/realm/src/pool.rs` —— 标签按子集匹配、空请求匹配全部；顶替会把被顶掉的记录交回来；删除带身份检查，所以重连保得住名字；`Reach` 会过滤，也能列出它允许的集合。
- `crates/engine/tests/dispatch_job.rs` —— `a_placement_no_node_is_in_is_refused_before_a_realm_exists`（带着它要的标签被拒，域没被造出来）、`a_job_reaches_only_the_nodes_its_placement_names`（节点被**找到**了 —— 不是"not connected" —— 然后被过滤器按名拒绝；结果只列出 job 能碰到的那个节点）、`a_job_that_asks_for_no_place_reaches_every_node`（默认 placement 看得见两个节点）。
- `crates/engine/tests/remote_effector.rs`、`alias_takeover.rs`、`deadline_stop.rs` —— 既有的远程路径现在读池子：注册、alias 顶替、在场过期都通过 `realm_set.pool()` 断言。

## 相关记录

aura ADR-0051（dispatch 造域；边界 (d) 就是这份记录补全的继承）、ADR-0015 §7（alias 顶替要报出来，不许静默）、ADR-0027（代码按引用）、ADR-0048/0049（资源是场地状态）、effector ADR-0005（信封的 `Placement`，以及 job 为什么要携带它）、effector ADR-0006（节点说明它是哪台机器；标签是准入方的说法）、ADR-0054（机器 id 是地址，名字一旦解析成硬件，寻址用的就是它）。