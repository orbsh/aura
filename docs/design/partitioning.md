# 数据分区（内部机制）

> 综述在 wiki：[Aura 架构 §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md)。
> 本文档描述分区方案从键字节布局到集群拓扑的完整机制。双语版：[English](partitioning-en.md)。

## 1. 分区单位：Actor 实例，instance key 定归属

分区的最小单位不是表、不是 realm，而是 **Actor 实例**。`InstanceId = (actor_type, key)`，其中 `key` 就是 instance key（如 session_id、channel_id、order_id）。归属规则：

- **同 key 串行**：同一 instance key 的所有消息进同一个实例的 queue，单消费者逐条处理——状态一致性不靠锁，靠队列串行
- **异 key 并行**：不同 key 的实例完全独立，互不阻塞
- **通配订阅例外**：`on_wildcard` 的 Actor 绑定单例 `__singleton__`，不参与分区（监听全局事件的观察者天然无状态分片意义）

instance key 的提取方式现在和终态不同：当前是路由表声明 `instance_key_field`（从事件 payload 按字段名取值，取不到落 `__default__` 兜底实例）；终态（动态 schema 落地后）是事件名映射到 okm ns，通过该 ns 的访问方法扫描出 id——扫描天然一对多，一次 emit 可投递多个实例。

## 2. actor_type：类型与实例

```rust
pub struct InstanceId {
    pub actor_type: String,  // 类型：哪一种 Actor
    pub key: String,         // instance key：这一种里的哪一个实例
}
```

`actor_type` 是 Actor 的类型名——同一逻辑角色的标识；instance key 是这个类型下的具体实例。

```
ActorType "cart"                ← 蓝图：状态 schema + handler + 订阅声明
  ├─ Instance ("cart", "alice")   ← 具体实例：自己的 queue、自己的状态
  ├─ Instance ("cart", "bob")
  └─ Instance ("cart", "carol")   ← 同类型不同 key，互相独立、可并行
```

- **注册**：`engine.register(ActorType::simple("echo", handler))` 或 `ActorType::script("py-ctx", "python", source, entry)`——类型名在这里定，body（Rust handler 或脚本）挂在类型上，所有实例共享同一份代码
- **路由**：`router.on("order.created", "cart", "user_id")` 的事件投递目标是 `(类型, 从事件提取的 key)`；`ctx_invoke` 也用 `{type, key}` 定位目标
- **状态布局**：存储隔离在类型层（ADR-0026 §3）——每个 Actor 类型占一个真实 okm ns，类型声明自己的 collections，实例是其中的 document；实例键只回答「谁串行处理这条消息」，不再决定存储布局
- **分片归属**：`(actor_type, key)` 合起来构成完整的分区标识；单看 key 不够（"alice" 在 `cart` 和 `session` 里是两个无关实例）

本质是**类与实例的关系**：actor_type 是部署和代码分发的单位（热更新按类型换定义），实例是串行化和状态归属的单位（按 `(type, key)` 寻址、分片、恢复）。

### 2.1 分区设计原则：什么身份选什么键

选 instance key 的判据是**实例的恒等归属**，不是请求的携带字段：

- **身份恒等于归属 → 用身份做键。** 用户级数据（购物车、session）按 user_id 分区：实例身份本身就编码了用户，handler 读 `ctx.self_id.key` 即得身份——这是构造级保证（实例只属于自己的 key），比调用方挂载更强（无需防伪造）。
- **归属大于身份 → 用归属做键，身份走参数。** 群聊按 channel_id 分区：一个实例服务多个用户，user_id 不是实例的恒等属性。消息**自带 channel_id**（客户端知道发往哪个 channel，无需引擎侧查表），发送者身份作为请求参数携带（handler 内做成员校验、发言归因）。此时把 user_id 挂上 ctx 逻辑冲突——ctx 是 per-instance 的，挂上即意味着「本实例的 user」，而群聊实例没有「本实例的 user」。也不需要中间路由 actor 先按 user_id 查 channel 再转发：那会多一跳、多一份状态，且路由表沦为成员关系的第二真相源。
- **跨分区的反向索引（user ↔ channels、user ↔ orders）→ 投影 Actor**：per-user 的 actor 订阅事件流维护自己的索引，与投影聚合同构，不进投递热路径。

一句话：**分区键回答「这条消息该由谁串行处理」，请求参数回答「这次请求是谁发起的」**——两个问题各自独立作答，不互相挂载。


## 3. 单节点内的键布局：三层二进制段

一个实例的状态落盘为定宽二进制段拼接（okm 键纪律，无文本分隔符）：

```
[ns 2B BE][slot 1B][字段编码…][pkey]
```

- **ns（2 字节）**：okm 层的表/边表编号，单一 okm 实例内统一编址（ADR-0025 后 actor 定义与数据同实例：ActorDef ns 41 与 mq/state 并列）
- **slot（1 字节）**：实例内访问方法判别（0 = 主条目），同一张表的全部索引条目共享 ns 段
- **Actor 状态**：实例状态不是每实例一份平铺 document——类型在自己的 ns 内声明 collections（schema 随 interface_schema 上传持久化），handler 经 `ctx.store.emit(op)` 以 okm Collection 指令读写（put/get_document、fields、scan、reduce）；同类型跨实例聚合 = 类型 ns 内的普通 scan/reduce。

realm 隔离（Phase 3.6 机制，ADR-0028 由 namespace 改名而来）与类型 ns 正交：realm 前缀加在最外层（`MqStore::for_realm`），类型 ns 在其内——同一物理引擎内不同 realm 的键空间结构性分离，跨 realm 的访问在类型上就不可表达。realm 绑定什么维度（用户、项目、或不绑）是应用的决定（PLAN 4.10 降级裁决）——框架的隔离单元只有两个：类型 ns（存储）与实例串行（路由），用户不在其中。

## 4. 序列化边界：激活时载入，休眠时写回

实例在内存中是活对象（Rust handler 或脚本），**分区状态的生命周期与实例驻留解耦**：

- **激活（on_wake）**：驻留释放后数据仍在类型的 collections 里，下次触发重新激活、按需读写
- **休眠（on_sleep/evict）**：驻留释放——scale-to-zero 丢的是驻留，不丢数据（验收测试锁定：声明 collection 的脚本 Actor 状态跨 eviction 存活）
- Phase 6.5 的驻留窗口是这条边界上的优化：retention 窗口内同一 partition 的连续调用全走内存 oneshot，零持久化；窗口结束才落盘释放

## 5. 集群层：分片映射与路由不变性（Phase 5，未实施）

- **shard map 住本节点存储**（ADR-0025 后即数据面 okm 实例），单写入点模型：只有一个逻辑写入者（控制平面）写 shard map/Actor 注册表，节点缓存读取——不引入多写共识——联邦内部没有通向共识的路径：多控制面部署是方向性倒退，内部元数据保持控制平面只写面使写入者永远单一；整体转向逻辑单集群是推翻 ADR-0013 的新裁决，不是本架构内的扩展点
- **路由不变性**：instance key → shard 的映射稳定，**请求跟着数据走**——session 的每个 turn 都路由到持有该分区的机器；历史数据不会"丢失"，只是不被错误路由的请求看到
- **结构性代价，明确接受**：节点故障时该节点的分区冻结直到恢复/迁移，零副本写放大。需要高可用的分片由 FDB/TiKV 承载（wiki 裁决：不自建强一致复制）——分区方案与复制方案解耦，默认路径零复制
- Actor 定义热更新走本节点存储：写新定义 → 各节点激活时重读

## 附：evictor 的复杂度取舍

当前驱逐是 5s 固定 tick + 全表线性扫（O(实例数)）。最小堆方案（`BinaryHeap<(Instant, InstanceId)>` 按**到期时刻**排序——放堆里的是到期时刻而非 TTL 值，策略参数变更不作废旧条目没有意义；配惰性删除：pop 时与 `instances` 表对照，条目时刻与当前 `last_activity + ttl` 不符即丢弃）被明确推迟：它的收益要等单节点驻留实例到十万级、且 profiling 证明 evict 扫描真实占比才成立，在此之前 HashMap 本身的锁竞争先成为瓶颈。触发条件两条同时满足时再做。

## 设计要点

这套方案的骨架是**「串行单位 = 分区单位 = 恢复单位」**：instance key 同时决定消息串行化、键空间归属和故障爆炸半径。一致性来自单写者+信箱串行而非共识协议；可用性缺口（节点故障分区冻结）被显式接受并用外部强一致 KV 兜底，而不是内建副本。
