# ADR-0016: 定时器——timer wheel 延迟投递与 cron 语义

**状态**：Accepted（设计定案，实现未开始）
**日期**：2026-09-22
**英文版**：[0016-timers-timer-wheel-cron.md](0016-timers-timer-wheel-cron.md)

## 背景

Aura 的 actor 是事件驱动的：工作以 call、emit、事件队列作业的形式
到达，没有作业到达的实例在留存期过后被 idle-TTL evictor 回收
（按类型 `idle_ttl`，evictor 5 秒 tick）。运行时**没有延迟唤醒
原语**：信号只存在于「有事发生」，从不存在「无事持续了 N 秒」。
krystallizer 侧的两个需求（k10r 的 ADR-0008/0009 定义策略；投递
机制归 aura）暴露了这个缺口：

1. **被动插话**（ADR-0009）：channel 里的 agent 要在静默 N 秒后
   判断「该不该说话」——但静默不 emit 任何东西，没有事件到达
   actor。这个决策需要一个「正因为什么都没来才到来」的信号。
2. **压缩触发**（ADR-0008）：缓存时钟检查（如「1 小时缓存的第
   50 分钟空闲」）需要一个在计算出的未来时刻的唤醒。

对留存故事的修正：gravity 实例是**按 channel 一个，不是按
user 一个**——积累上下文、做发言/压缩决策的单位绑定在一条
channel 的日志上（ADR-0008 的投影按 (channel, member) 键控；
gravity 按 channel 键控，让同一用户的不同 channel 携带独立的
checkpoint 与游标，一个 agent 服务多个 channel 就是多个独立
实例）。分区键路由（`InstanceId = (actor_type, key)`、
`@on(event, key_field)`）、per-instance TTL 驱逐、per-instance
状态名字空间都已支持这个形态——gravity 以 `key = channel_id`
注册，`idle_ttl` 严格大于其最长的定时窗口。

## 决策

### 1. Timer wheel + 驱逐时计算到期

每个 realm 一个 **timer wheel**：条目为
`(deliver_at, target: InstanceId, payload)`。既有 evictor tick
（5 秒）增加扫描 wheel，到期的条目作为普通队列作业投递给目标
实例。

- **投递 = 普通邮箱作业。** 到期定时器在 actor 侧与事件作业
  无异（`QueuedJob`，保留 handler 名如 `__on_timer`，payload 带
  注册方自选的 tag）。不引入新的回调概念。
- **投递算活动。** 定时作业与任何作业一样刷新 `last_activity`
  ——但有一条守则：定时投递**不隐式自我重排**。想周期唤醒的
  actor 显式重挂（见 cron 一节）；忘了重挂的照常被 TTL 回收。
  定时器只在 actor 持续请求的范围内维持存活——不存在「自我
  喂食的不朽实例」。
- **持久性问题——定时器默认不跨驱逐存活。** 定时器是内存调度
  状态，与邮箱同类；actor 的持久真相在 StateStore。但 krystallizer
  的两个场景需要定时器活得比驱逐久：50 分钟压缩唤醒与 cron。
  因此定时器可注册为**持久**（写入 StateStore 的保留字段名字
  空间，激活时经 `on_wake` 恢复）。内存定时器随驱逐消亡——短窗口
  （插话检查）的廉价默认，冷启动后在下一个真实事件上重挂即可。
- **合并**：同一目标的多个到期定时器合并为一个作业（带到期 tag
  列表）——一批调度检查只花一次唤醒，不是 N 次。

### 2. 取消与重挂

注册时返回定时器句柄（id）；`cancel(id)` 移除。重挂 = 注册新
定时器（先取消后注册）。插话模式：channel 有活动 → 取消未决的
静默定时器 → 评估 → 按空闲阈值注册新的静默定时器。模式完全
由 actor 驱动；运行时不提供隐式重挂。

### 3. cron 语义：schema 声明、actor 计算、投递时重挂

cron 有两个入口，引擎在两者中的角色都被刻意收窄：

- **声明式**（`interface_schema` 中的 `@cron`）：脚本在
  `lifecycle.cron` 下声明 cron 规格；Host 注册时内省（与
  `lifecycle.idle_ttl` 已走的路径相同，realm/src/lib.rs 的
  `extract_idle_ttl`），把声明翻译成持久单次定时器。「循环工程」
  一类的周期任务就是这个入口的直接消费者。
- **命令式**（`ctx.timer.register`）：由对话状态在运行时算出的
  唤醒时刻（gravity 的正交唤醒）无法用静态规格表达——actor 自己
  算出下一时刻并注册。

两种形态下，调度**语义**都归 actor；引擎只拥有投递**机制**：

- 下次触发计算是对调度规格与 `now` 的纯函数，actor 侧执行
  （声明式的**首次**触发由 Host 在注册时计算——后续触发由
  actor 侧重挂）。
- 每次投递时，从 **now**（不是上次触发时刻）重算下一时刻——
  错过/驱逐造成的空档直接跳过，不爆发补偿性连跑——然后重挂。
- 引擎为什么从不在运行时解释 cron 表达式：表达式集合是 actor
  的策略，不是运行时的；运行时对 cron 的全部支持就是——持久
  单次定时器跨驱逐存活、唤醒时恢复，加上注册时对声明规格的
  一次翻译。一切 cron 形态的需求都归约到这一条。
- 错过策略是显式的：skip-and-jump-to-next（默认；上面的
  from-now 重算规则免费得到它）。补偿性连跑（每个错过的 tick
  补一次）要求运行时追踪触发历史——拒绝：审计需求由 actor
  自己记录触发满足，停机后的补偿性爆发恰是 ADR-0008 拒绝过的
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
注入能力（metadata、probe target）以组的形式加入，不再是扁平名
堆积。

### 4. 什么不变

- idle-TTL evictor 及其 5 秒 tick 增加 wheel 扫描；驱逐语义
  （per-instance、on_sleep hook、session 随驱逐销毁）不动。
- `last_activity` 语义保持「最后作业到达」；定时器就是作业。
- 每个定时器条目不派生异步任务（不 `tokio::spawn` + `sleep`）：
  wheel 由既有 tick 扫描——内存有界、单任务，5 秒粒度对两类
  消费者（秒级插话窗口、分钟级压缩/cron 唤醒）都够。

## Why Not

- **按 user 的 gravity 实例**：把一个用户的 agent 拆散到各
  channel；投影、checkpoint、游标都按 (channel, member) 键控——
  按 user 键控会把 ADR-0008 拆开的东西重新合并，一个用户在两个
  活跃 channel 中会争夺同一个上下文。修正为按 channel。
- **运行时原生 cron（引擎解析 cron 表达式）**：把调度 DSL 塞进
  引擎，还为补偿语义强制触发历史追踪。actor 侧模式只需要持久
  单次定时器——其余都是 actor 的策略。拒绝。
- **每定时器一个 `tokio::sleep` 任务**：任务数无界，且定时器随
  进程消失（无持久注册路径）。wheel 扫描共享既有 tick。拒绝。
- **隐式周期定时器**：注册一次被永久唤醒，隐藏留存成本、击穿
  idle-TTL。重挂保持显式。

## 后果

- 新运行时接口：`register_timer(target, deliver_at, tag,
  durable: bool) -> TimerId`、`cancel_timer(id)`、保留的
  `__on_timer` 投递路径、激活/`on_wake` 序列中的持久定时器恢复。
  Actor 侧：`ctx.timer.register / cancel`（ADR-0011 已修订）；
  `interface_schema` 中的声明式 `lifecycle.cron` 在注册时翻译为
  持久定时器。ctx bridge 的 host-fn 表从扁平 `ctx_state_*` 名字
  改为点号命名空间组（`ctx.store.*`、`ctx.timer.*`），可自省。
- evictor tick 增加 wheel 扫描（同一个 5 秒循环；wheel 在驱逐
  之前检查，同一 tick 内到期投递先于驱逐）。
- gravity（按 channel 实例）经此接口注册插话与压缩唤醒；
  krystallizer 的策略参数（ADR-0008/0009）决定时长，aura 的
  定时器负责送达。
- 定时粒度受 evictor tick 约束（±5 秒）；亚 tick 精度不是目标
  （无消费者需要）。
