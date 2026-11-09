# ADR-0016: 定时器——timer wheel 延迟投递与 cron 语义

**状态**：部分落地（2026-09-23：DelayQueue 驱动、两类条目、idle 从作业完成起算、`max_exec` watchdog；2026-10-09：命令式 `ctx_timer_register/cancel` 宿主函数——仅内存层；2026-11-09：durable 层已落地——ns 27 `TimerEntry`，ADR-0040；`ctx.timer.register {durable: true}` 写行，驱动开机从表重建轮子；声明式 `lifecycle.cron` 仍待）
**日期**：2026-09-22
**英文版**：[0016-timers-timer-wheel-cron.md](0016-timers-timer-wheel-cron.md)

## 背景

Aura 的摊位是事件驱动的：工作以 call、emit、事件队列作业的形式
到达，没有作业到达的实例在留存期过后被 idle-TTL evictor 回收
（按类型 `idle_ttl`，自作业完成起算）。运行时**没有延迟唤醒
原语**：信号只存在于「有事发生」，从不存在「无事持续了 N 秒」。
krystallizer 侧的两个需求（k10r 的 ADR-0008/0009 定义策略；投递
机制归 aura）暴露了这个缺口：

1. **被动插话**（ADR-0009）：channel 里的 agent 要在静默 N 秒后
   判断「该不该说话」——但静默不 emit 任何东西，没有事件到达摊位。这个决策需要一个「正因为什么都没来才到来」的信号。
2. **压缩触发**（ADR-0008）：缓存时钟检查（如「1 小时缓存的第
   50 分钟空闲」）需要一个在计算出的未来时刻的唤醒。

对留存故事的修正：gravity 实例是**按 channel 一个，不是按
user 一个**——积累上下文、做发言/压缩决策的单位绑定在一条
channel 的日志上（ADR-0008 的投影按 (channel, member) 键控；
gravity 按 channel 键控，让同一用户的不同 channel 携带独立的
checkpoint 与游标，一个 agent 服务多个 channel 就是多个独立
实例）。分区键路由（`InstanceId = (booth_type, key)`、
`@on(event, key_field)`）、per-instance TTL 驱逐、per-instance
状态名字空间都已支持这个形态——gravity 以 `key = channel_id`
注册，`idle_ttl` 严格大于其最长的定时窗口。

## 决策

### 1. 每 realm 的调度落在 tokio-util `DelayQueue`

每 realm 一个 **timer queue**，建在 `tokio_util::time::DelayQueue`
上：一个公开的 hashed wheel，暴露 `insert_at(value, when) -> Key`、
`remove(&Key)`（O(1) 取消）、`poll_expired()`（流式取到期条目）。
每 realm **一个驱动任务**按值持有队列
（`while let Some(expired) = queue.poll_expired().await`），对每个
到期条目执行动作；任务数保持有界。背后的 wheel 是分层的——6 级 ×
64 slot，level 0 的 slot 宽 1 ms、覆盖 64 ms，每上一级 slot 宽度
×64（约 4 秒、4 分、4 小时、12 天），顶层覆盖约 2 年——条目插入到
其截止时刻落在范围内的那一级，每次到期扫描向下一级 tier down，
直到 level 0 才被交出。aura 完全不碰这些级：只调 `insert_at` /
`remove` / `poll_expired`，因此分级语义（以及逐级调参）都不在我们
这侧。到期由驱动 `Sleep` 唤醒即刻观测，分辨率为 level 0
（1 ms 量化，受调度延迟影响）——不是 5 秒 evictor tick，也没有
tick 形状的界。没有亚毫秒或精确墙钟保证；没有消费者需要更好的。

队列同时承载两种条目：

- **投递型条目** `(deliver_at, target, tag, durable)`——唤醒一个
  摊位，投递 `__on_timer` 作业（插话检查、压缩唤醒、cron）。持久型
  写 StateStore 保留名字空间，`on_wake` 恢复路径重新 `insert_at`。
- **回收型条目** `(deliver_at, target, kind)`——没有人会来了，
  收回资源（idle-TTL 驱逐、执行看门狗）。到期动作不是投递作业，
  而是 `evict_instance`（on_sleep + executor 丢弃；看门狗场景对
  在等的 reply 发超时错误）。回收型条目恒为内存态；它们需要
  **取消**（新工作到达 → 作废未决的 idle 定时器），这就是
  `DelayQueue::remove`——与摊位用的 cancel/re-arm 是同一 API。

投递型条目走普通作业路径：

- **投递 = 普通邮箱作业。** 到期定时器在摊位侧与事件作业
  无异（`QueuedJob`，保留 handler 名如 `__on_timer`，payload 带
  注册方自选的 tag）。不引入新的回调概念。
- **投递算活动。** 定时作业与任何作业一样刷新 `last_activity`
  ——但有一条守则：定时投递**不隐式自我重排**。想周期唤醒的摊位显式重挂（见 cron 一节）；忘了重挂的照常被 TTL 回收。
  定时器只在摊位持续请求的范围内维持存活——不存在「自我
  喂食的不朽实例」。
- **持久性问题——定时器默认不跨驱逐存活。** 定时器是内存调度状态，与邮箱同类；摊位的持久真相在它自己声明的 collection 里。但 krystallizer 的两个场景需要定时器活得比驱逐久：50 分钟压缩唤醒与 cron。因此定时器可注册为**持久**：一行落在 realm 的定时器表 `TimerEntry`，ns 27，属事件面那一段（ADR-0040），按到期时刻编码，开机一次区间读就重建轮子，重启后做一次到期扫描就能找到"没人跑的时候到期的那些"。原本挂在这个记录上的 `StateStore` 已经不存在了（ADR-0026 §3）；定时器是平面数据（什么时候叫醒哪个实例），不是摊位数据，所以也不该落进任何类型的 ns。内存定时器随驱逐消亡——短窗口（插话检查）的廉价默认，冷启动后在下一个真实事件上重挂即可。
- **合并**：同一目标的多个到期定时器合并为一个作业（带到期 tag
  列表）——一批调度检查只花一次唤醒，不是 N 次。

### 2. 取消与重挂

注册时返回定时器句柄（`Key`）；`cancel(id)` 经
`DelayQueue::remove` 移除，O(1)。重挂 = 注册新定时器（先取消后
注册）。插话模式：channel 有活动 → 取消未决的静默定时器 → 评估 →
按空闲阈值注册新的静默定时器。模式完全由摊位驱动；运行时不提供
隐式重挂。

### 3. cron 语义：schema 声明、摊位计算、投递时重挂

cron 有两个入口，引擎在两者中的角色都被刻意收窄：

- **声明式**（`interface_schema` 中的 `@cron`）：脚本在
  `lifecycle.cron` 下声明 cron 规格；Host 注册时内省（与
  `lifecycle.idle_ttl` 已走的路径相同，realm/src/lib.rs 的
  `extract_idle_ttl`），把声明翻译成持久单次定时器。「循环工程」
  一类的周期任务就是这个入口的直接消费者。
- **命令式**（`ctx.timer.register`）：由对话状态在运行时算出的
  唤醒时刻（gravity 的正交唤醒）无法用静态规格表达——摊位自己
  算出下一时刻并注册。

两种形态下，调度**语义**都归摊位；引擎只拥有投递**机制**：

- 下次触发计算是对调度规格与 `now` 的纯函数，摊位侧执行
  （声明式的**首次**触发由 Host 在注册时计算——后续触发由摊位侧重挂）。
- 每次投递时，从 **now**（不是上次触发时刻）重算下一时刻——
  错过/驱逐造成的空档直接跳过，不爆发补偿性连跑——然后重挂。
- 引擎为什么从不在运行时解释 cron 表达式：表达式集合是摊位的策略，不是运行时的；运行时对 cron 的全部支持就是——持久
  单次定时器跨驱逐存活、唤醒时恢复，加上注册时对声明规格的
  一次翻译。一切 cron 形态的需求都归约到这一条。
- 错过策略是显式的：skip-and-jump-to-next（默认；上面的
  from-now 重算规则免费得到它）。补偿性连跑（每个错过的 tick
  补一次）要求运行时追踪触发历史——拒绝：审计需求由摊位自己记录触发满足，停机后的补偿性爆发恰是 ADR-0008 拒绝过的
  「一口气全压」的失败形态。

### 3b. ctx 定时器接口与 host-fn 命名空间化

定时器 API 挂在 ctx 上——ADR-0011 的判据裁决了它（实例身份绑定 +
Host 可控）；其 Update 注记把旧的拒绝收窄到阻塞/自调度形态。
接口：

- `ctx.timer.register(at, tag, durable) -> TimerId`
- `ctx.timer.cancel(id)`

脚本侧 ctx bridge（HostBridge host 函数）不再使用扁平的
`ctx_state_*` 名字，改为点号命名空间组——`ctx.store.get/set/delete`、
`ctx.timer.register/cancel`——注入的组集合可自省发现（carrier
枚举它暴露的 `ctx.*` 组；schema 可声明实例实际携带哪些组）。新的
注入能力（metadata、effector target）以组的形式加入，不再是扁平名
堆积。

### 3c. idle 回收与执行预算

`last_activity` 保持原义——观测面：最后作业到达时刻——但不再驱动
驱逐。`run_job` 在入口取消该实例未决的 idle 回收条目（工作到达
了），完成时重新注册一条（`deliver_at = now + idle_ttl`）。长执行
（LLM 调用吃满整个 TTL）不再可能在执行中途驱逐实例：作业运行期间
idle 回收条目根本不存在。

`lifecycle.max_exec`（per-type，在 `interface_schema` 中与
`idle_ttl` 并列声明）是执行看门狗：作业入口注册回收条目
（`deliver_at = now + max_exec`），完成时取消。到期 = 实例超出执行
预算：驱逐（on_sleep、executor 丢弃）并对在等的 reply 发超时错误。
这是最大时长控制，不是空闲计量——两个关注点沿两种条目干净分离。

evictor 任务仅保留 `pending_calls` 的 deadline 扫描（其自身也是
将来回收型条目的迁移对象）；`evict_idle` 的线性扫描删除——驱逐
改为定时器驱动，O(到期) 而非 O(实例数)。

### 4. 什么不变

- 驱逐语义（per-instance、on_sleep hook、session 随驱逐销毁）不动；
  变的只是触发点——作业完成时重挂的 idle 回收条目取代了 5 秒 tick
  上的线性扫描。
- `last_activity` 仍是最后到达的观测面；它不再决定驱逐（见 §3c）。
  定时器在摊位侧就是普通作业。
- 每个条目不再派生异步任务（不 `tokio::spawn` + `sleep`）：每 realm
  一个驱动任务服务所有条目，持久型条目经 `on_wake` 恢复。

## Why Not

- **按 user 的 gravity 实例**：把一个用户的 agent 拆散到各
  channel；投影、checkpoint、游标都按 (channel, member) 键控——
  按 user 键控会把 ADR-0008 拆开的东西重新合并，一个用户在两个
  活跃 channel 中会争夺同一个上下文。拒绝。
- **运行时原生 cron（引擎解析 cron 表达式）**：把调度 DSL 塞进
  引擎，还为补偿语义强制触发历史追踪。摊位侧模式只需要持久
  单次定时器——其余都是摊位的策略。拒绝。
- **每定时器一个 `tokio::sleep` 任务**：任务数无界，且定时器随
  进程消失（无持久注册路径）。一个驱动任务服务全部定时器；被拒绝
  的形态是每定时器一任务。拒绝。
- **隐式周期定时器**：注册一次被永久唤醒，隐藏留存成本、击穿
  idle-TTL。重挂保持显式。

## 后果

- 新运行时接口：`register_timer(target, deliver_at, tag,
  durable: bool) -> TimerId`、`cancel_timer(id)`、保留的
  `__on_timer` 投递路径、激活/`on_wake` 序列中的持久定时器恢复。
  同一队列承载 idle-TTL 驱逐与 `lifecycle.max_exec` watchdog 背后的
  回收型条目。摊位侧：`ctx.timer.register / cancel`（ADR-0011）；
  `interface_schema` 中的声明式 `lifecycle.cron` 在注册时翻译为
  持久定时器。ctx bridge 的 host-fn 表从扁平 `ctx_state_*` 名字
  改为点号命名空间组（`ctx.store.*`、`ctx.timer.*`），可自省。
- gravity（按 channel 实例）经此接口注册插话与压缩唤醒；
  krystallizer 的策略参数（ADR-0008/0009）决定时长，aura 的
  定时器负责送达。
- 到期条目在过期那一刻触发：队列自驱动，每 realm 一个驱动任务，
  5 秒 evictor tick 不再扫描 wheel。
- 定时分辨率为 level 0（1 ms 量化，其上叠加调度延迟）；没有 tick
  界，也没有亚毫秒或精确墙钟保证。
