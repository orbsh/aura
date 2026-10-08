# 0041 — 事件面的内部布局：实例键词汇、发号器长在数据表上、只留一处物理分区（修订 ADR-0039 §1 与 ADR-0040 的表）

> **语言：** [English](0041-event-plane-internal-layout.md)（主文档） · [中文](0041-event-plane-internal-layout.zh-CN.md)

**状态：** Accepted（2026-10-08）——已落地（commit `74221de`，2026-10-08：代码改名/撤表批次与文档同笔提交）。
用户裁决（2026-10-08）：切片段按它的值命名（instance key），不承载信息的名字（`part`）出局，
发号器不需要自己一张表，布局叙述不再夹带引擎注解。

## 上下文

事件面的存储**只有 aura 自己在读写**。`mq.rs` 是唯一入口，调用方是 emit 路径（`events.rs`）、
消费循环（`instance.rs`）和两个泄压阀 host fn（`ctx.rs`）。摊位只声明 receives 与 emit；
它能触到的 MQ 面就是那两个 fn，而两者都从**调用者自己的** instance key 解析 `bound_*`
——摊位根本寻址不到别的实例的队列。所以访问面是静态的、闭合的、单写者的（realm 锁）。

这个事实从没被用来做判断，三个缺陷由此而来：

1. **两个不同的东西都叫「分区」。** 队列切片是 MQ 键的第 2 段（字典发的代理 id）；okm 的
   物理 KV 分区是 `#[ok_partition(N)]`（`[0xFF][N]` 键前缀 = 一个 compaction 分组）。
   推导链把引擎开关摆在事件面词汇旁边，还给它挂了一个正交的理由（「按 event_id 分片」
   说的是键的前缀顺序，不是物理分区）。同一个结构体里一个词干两件事是缺陷，不是简写。
2. **发号器是拿错误的替代方案论证的。** `MqHead` 只被论证成「O(1)，不扫全分区取 max」
   ——而同一页刚在数据表上挂了 live `Count` reduce，而 okm 的 `HighWater` preset 就是
   「读累加值、+1」。
3. **没有任何写下的轴能证明 `MqCursor` 的物理分区。** `#[ok_partition]` 的落档理由
   （PLAN，2026-09-16）是「追加/水位删除 vs 点写」两条 compaction profile——而
   `MqData` 一张表就已经满足了。

## 裁决

**裁决——事件面按「框架契约 + 内部布局」两层来写，它的存储词汇是 instance key。**

### 1 切片段就是 instance key

- 这个段的值就是路由解析出的 instance key：订阅侧传自己的 instance key
  （`bound_instance_key`），emit 侧取 route 声明的 payload 字段，两侧同值由构造保证。
  id 命名的是这个值。
- **改名（一套词汇，不留同音词）：** `part_id` → `instance_key_id`；`PartitionName`
  （ns 21）→ `InstanceKeyRegistry`；`mq::Partition { Singleton | Named }` →
  `mq::InstanceKey`；`SINGLETON_PART` → `SINGLETON_KEY_ID`（仍是 `0`、仍保留、
  仍发号不可达——字典从 1 起发）；`part_id` / `partition_id_of` / `partition_name_of` /
  `resolve_partition_id` / `bound_partition` → `instance_key_id` /
  `instance_key_id_of` / `instance_key_of` / `resolve_instance_key_id` /
  `bound_instance_key`。
- **这里为什么用 `Registry` 而不是 `…Name`。** 这张表是「名字 ↔ 键空间 id」的注册表
  （一个文本索引、一个水位）；`InstanceKeyName` 读起来像「某个 instance key 的名字」，
  而这不是一行里装的东西。兄弟字典保留原名：`EventName`/`BoothName` 今天就有信息量
  （行的载荷就是那个名字），且被大量落地当时的记录引用；而「registry」在这个仓里已经
  指持久订阅表（`EventRoute`——订阅注册表）——把三张字典都塞进这个词，会让一个词指
  两件事，正是本 ADR 要清掉的那类毛病。
- **这个 id 不是什么。** 它不是复合实例身份——aura 的 `InstanceId { booth_type, key }`
  才是；这一段是其中的 key 半边。一个 id 对应**每个订阅类型各一个**实例：数据键不带
  booth 段（两个类型共享一行），而游标键带，emit 先按切片去重再 append——这正是设计的
  扇出去重，所以这一段命名的是共享的切片，绝不是某个实例。保留值 `0` 覆盖无键投递
  （此时根本没有键值；单例**实例**另有自己的名字 `__singleton__`，那是实例 ns 里的名字，
  不是这个 id）。
- 落地形态：ns 21 `InstanceKeyRegistry`——键 `id u32`（4 B，从 1 起发）；（`Registry` 是
  这个形态的名词：这张表就是一份「名字 ↔ 键空间 id」的注册表。兄弟字典 EventName/BoothName
  不改名，理由见 §1 的注记。）值 = `name String`
  （instance key）+ `id u32` 镜像 + `global u32`（单组常量 0）+ `by_name` 文本索引
  （名→id）+ `HighWater(id)` reduce。镜像在 okm 的 key 字段 fold 退休前保留
  （ADR-0024 的落地是**分宿主**的——见 §3）。

### 2 契约与布局是文档的两层

- 文档叙述的是**契约**：emit/`@on`、队列先于消费者存在、消费者集合是闭集、保留承诺、
  无静默丢弃。
- 键空间是**内部布局**：只描述一次，落成一张布局表，`#[ok_ns]` / `#[ok_partition]` /
  `#[ok_index]` / `#[ok_reduce]` 作为实现注记住在那里。事件面的读者不该需要知道
  `#[ok_partition(1)]` 存在；实现者不该需要读契约才知道键宽。注解在代码里仍是权威，
  表是它的索引。

### 3 发号器是数据表上的水位

- 约束不变：追加不能扫全分区取 max，序列只能有一个发号者。
- **`MqHead`（ns 24）撤销。** 写头是 `seq` 上的单调水位：
  MqData 上声明 `#[ok_reduce(HighWater(seq) { group(event_id, instance_key_id) })]`。
  追加读累加值（reduce entry 的一次点读）、算 `+1`，由数据行自己的 put 折上去。
- **为什么它是正当的。** okm 的 reduce 纪律是一条账本恒等式——`acc(group)` 等于主表**当前**
  行集合的 fold——而 `HighWater`/`LowWater` 是它**声明出来的例外**（unfold 是 no-op）。
  发号器的值必须比它的行活得久，而视图做不到；在这里例外就是机制，不是绕路。所以保留
  压缩删行永远不会让写头下降。
- **为什么撤掉那张表。** `MqHead` 把发号器写成一个显式陈述，泛泛地说这站得住——但这里
  没有东西需要发号器独立于数据表存在：访问面静态、单写者、框架内部，不存在要保的第三方
  兼容面。两者都留 = 一个值两套机制。
- **不撤的：** 计数器 vs 时间戳的论证（`max(now_ms, last+1)` 的退役）。它原样成立，留在
  设计文档发号器那一步：这个值是排序键、因此是行的身份，必须全序且唯一，只有「读计数器
  +1」给得了。唯一性仍由 realm 锁（`self_arc.lock()`）保证——锁才是保证，计数器只是记账。
- **代价/收益：** 少一张表、每 append 少一次写；写头的 fold 搭在数据行的 put 上，发号与
  行落进同一个物理分区（append 的原子域）。宿主注记（记录在此，免得这个选择悄悄依赖它）：
  aura 走 derive 路径读这个 fold，okm 在那里用双源规则（KEY WINS）解析 key 字段聚合；
  okm-dynamic 的 preset 路径忽略解出来的 key，解析不到。这处不对称是 okm 自己的挂账
  （ADR-0024 的宿主对称），不是本裁决的约束。

### 4 物理分区：只有一张表带

- `#[ok_partition]` 是引擎的 workload 开关（一个 compaction 分组），不是事件面的概念，
  而事件面恰好需要一处：**MqData**——唯一「批量追加 + 范围删除」的 workload
  （`Count` 与 `HighWater` 两个 fold 也挂在它上面）。
- **`MqCursor` 去掉 `#[ok_partition(2)]`。** 它是点表——与摊位状态同类，而摊位状态就住
  默认键空间——而能证明它自成一组的轴从来没写下来。游标与 MqData 的关系两种做法下都一样
  （那由 MqData 自己的注解定死）；这一改只决定游标是否进默认树。
- **ns 24 腾出**，回到事件面的空段：表在 20、21、22、23、25；保留 24 与 26–29。

## 落地连带

- **代码**（`crates/realm/src/mq.rs` 及其调用方 `events.rs` / `instance.rs` / `ctx.rs`，
  加测试）：§1 的改名、MqData 上的 `HighWater` reduce 声明、`MqHead` 结构/键/表与其 ns 的
  退役、`#[ok_partition(2)]` 的删除，以及 append / skip-to-head 从「读写头行」改成
  「读 reduce entry」。
- **文档**：`event-flow.md` / `-en.md` 的 §1（词汇）、§2（第 2–5 步）、§6.1 与 §7
  （布局表与注解注记）同批改写，两语同步。ADR-0039 §1 与 ADR-0040 的表保留各自的落地时
  措辞，由本 ADR 的注记修订，不重写（ADR-0040 自己定的规矩：记录当时号位的文档保留当时的
  措辞）。
- **迁移**：既有部署本来就要清低位块（ADR-0040），而游标行的字节（因去注解）与所有改名
  的表一并随之作废——全部被那次清除吸收。新库没有可迁移的东西。号位不复用规则不受影响：
  没有任何号被改配给另一张表。
- **仍开放（非设计）**：写头读取助手的准确形态（经生成的 entry-key 函数走 `reduce_get`），
  以及 `instance_key_of` 是否保留现今这个面向运维的名字。