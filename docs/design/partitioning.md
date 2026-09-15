# 数据分区（内部机制）

> 综述在 wiki：[Aura 架构 §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md)。
> 本文档描述分区方案从键字节布局到集群拓扑的完整机制。双语版：[English](partitioning-en.md)。

## 1. 分区单位：Actor 实例，partition key 定归属

分区的最小单位不是表、不是 namespace，而是 **Actor 实例**。`InstanceId = (actor_type, key)`，其中 `key` 就是 partition key（如 session_id、user_id、order_id）。归属规则：

- **同 key 串行**：同一 partition key 的所有消息进同一个实例的 mailbox，单消费者逐条处理——状态一致性不靠锁，靠信箱串行
- **异 key 并行**：不同 key 的实例完全独立，互不阻塞
- **通配订阅例外**：`on_wildcard` 的 Actor 绑定单例 `__singleton__`，不参与分区（监听全局事件的观察者天然无状态分片意义）

partition key 的提取方式现在和终态不同：当前是路由表声明 `partition_key_field`（从事件 payload 按字段名取值，取不到落 `__default__` 兜底实例）；终态（动态 schema 落地后）是事件名映射到 okm ns，通过该 ns 的访问方法扫描出 id——扫描天然一对多，一次 emit 可投递多个实例。

## 2. actor_type：类型与实例

```rust
pub struct InstanceId {
    pub actor_type: String,  // 类型：哪一种 Actor
    pub key: String,         // partition key：这一种里的哪一个实例
}
```

`actor_type` 是 Actor 的类型名——同一逻辑角色的标识；partition key 是这个类型下的具体实例。

```
ActorType "cart"                ← 蓝图：状态 schema + handler + 订阅声明
  ├─ Instance ("cart", "alice")   ← 具体实例：自己的 mailbox、自己的状态
  ├─ Instance ("cart", "bob")
  └─ Instance ("cart", "carol")   ← 同类型不同 key，互相独立、可并行
```

- **注册**：`engine.register(ActorType::simple("echo", handler))` 或 `ActorType::script("py-ctx", "python", source, entry)`——类型名在这里定，body（Rust handler 或脚本）挂在类型上，所有实例共享同一份代码
- **路由**：`router.on("order.created", "cart", "user_id")` 的事件投递目标是 `(类型, 从事件提取的 key)`；`ctx_invoke` 也用 `{type, key}` 定位目标
- **状态布局**：实例状态的键编码第一段就是 type（`[4B len(type)][type][4B len(key)][key][field]`）——同类型的实例键空间聚在一起，前缀扫描能按类型枚举实例
- **分片归属**：`(actor_type, key)` 合起来构成完整的分区标识；单看 key 不够（"alice" 在 `cart` 和 `session` 里是两个无关实例）

本质是**类与实例的关系**：actor_type 是部署和代码分发的单位（热更新按类型换定义），实例是串行化和状态归属的单位（按 `(type, key)` 寻址、分片、恢复）。

## 3. 单节点内的键布局：三层二进制段

一个实例的状态落盘为定宽二进制段拼接（okm 键纪律，无文本分隔符）：

```
[ns 2B BE][slot 1B][字段编码…][pkey]
```

- **ns（2 字节）**：okm 层的表/边表 namespace，data 和 meta 两个 okm 实例各自独立编址，互不冲突（两实例模型：普通数据与元数据是两套 okm，引擎各自可选 fjall|slate，单机模式下都跑 fjall 但在不同目录）
- **slot（1 字节）**：实例内访问方法判别（0 = 主条目），同一张表的全部索引条目共享 ns 段
- **实例状态字段**：Actor 的 ctx_state 每个字段是一个独立 KV 条目，字段名直接编进键尾（`ctx_state_get/set/delete` 即对这段键空间的点读写）

跨实例（Actor 实例，非 okm 实例）隔离：用户 namespace 用 `PrefixStore` 嵌套前缀 `[2B len][ns]` 加在最外层——同一物理引擎内不同用户的键空间结构性分离，跨 namespace 的访问在类型上就不可表达。

## 4. 序列化边界：激活时载入，休眠时写回

实例在内存中是活对象（Rust handler 或脚本），**分区状态的生命周期与实例驻留解耦**：

- **激活（on_wake）**：从 StateStore 批量读回该 `(actor_type, key)` 的全部字段，重建内存态
- **休眠（on_sleep/evict）**：内存态写回 StateStore，驻留释放——scale-to-zero 丢的是驻留，不丢数据（验收测试锁定：脚本 Actor 状态跨 eviction 存活）
- Phase 6.5 的驻留窗口是这条边界上的优化：retention 窗口内同一 partition 的连续调用全走内存 oneshot，零持久化；窗口结束才落盘释放

## 5. 集群层：分片映射与路由不变性（Phase 5，未实施）

- **shard map 放 meta 实例**（slatedb），单写入点模型：只有一个逻辑写入者（控制平面）写 shard map/Actor 注册表，节点缓存读取——不引入多写共识，openraft 只在出现真正的第二个元数据写入者时回归
- **路由不变性**：partition key → shard 的映射稳定，**请求跟着数据走**——session 的每个 turn 都路由到持有该分区的机器；历史数据不会"丢失"，只是不被错误路由的请求看到
- **结构性代价，明确接受**：节点故障时该节点的分区冻结直到恢复/迁移，零副本写放大。需要高可用的分片由 FDB/TiKV 承载（wiki 裁决：不自建强一致复制）——分区方案与复制方案解耦，默认路径零复制
- Actor 定义热更新走 meta 实例：写新定义 → 各节点激活时重读

## 附：evictor 的复杂度取舍

当前驱逐是 5s 固定 tick + 全表线性扫（O(实例数)）。最小堆方案（`BinaryHeap<(Instant, InstanceId)>` 按**到期时刻**排序——放堆里的是到期时刻而非 TTL 值，策略参数变更不作废旧条目没有意义；配惰性删除：pop 时与 `instances` 表对照，条目时刻与当前 `last_activity + ttl` 不符即丢弃）被明确推迟：它的收益要等单节点驻留实例到十万级、且 profiling 证明 evict 扫描真实占比才成立，在此之前 HashMap 本身的锁竞争先成为瓶颈。触发条件两条同时满足时再做。

## 设计要点

这套方案的骨架是**「串行单位 = 分区单位 = 恢复单位」**：partition key 同时决定消息串行化、键空间归属和故障爆炸半径。一致性来自单写者+信箱串行而非共识协议；可用性缺口（节点故障分区冻结）被显式接受并用外部强一致 KV 兜底，而不是内建副本。
