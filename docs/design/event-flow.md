# 事件流转（emit / on 全机制）

> **语言：** [English](event-flow-en.md)（主文档） · [中文](event-flow.md)

本文是事件平面的单一整合文档，按推导组织：约束怎样逼出每一个持久面（§2）→ 作者声明什么
（§3）→ 注册一次性装配（§4）→ 每次 emit 的匹配（§5）→ 投递、消费与保留（§6）→ 推导的
落点即键空间布局（§7）→ 终态裁决与实施清单（§8，4.13+ 挂账）。原
`ns-layout.md`/`ns-layout-en.md` 退为指针。裁决依据：ADR-0007/0012（接收者集合是运行时
事实）、ADR-0026（类型级存储面）、ADR-0002（事件不占真实 ns）、ADR-0038（消费者集合与
身份：通配收窄、单字典、声明语义、无静默丢弃）、ADR-0039（分区编码与游标保留承诺）、
`docs/design/partitioning.md` §1（路由终态目标）。

## 1 词汇与不变式

事件平面有三个正交词汇，混用是历史上最容易出错的地方：

- **事件（Event）**：发生的事实，开放词汇。不占真实 ns，占 EventName 代理 id。
- **分区（Partition）**：队列的切片标识，由路由解析出的 instance key 派生。
  今天：值 = 字符串 → FNV-1a 定宽 u64 哈希，`0` 保留给单例分区（碰撞映射到 1）。
  已落（ADR-0039 §1）：分区是**代理词汇**——PartitionName 字典（ns 21）发定宽 `u32`
  id，与事件名/类型名同构。理由：okm 主键在构造上定宽（`KeyEncode` 遇 `String` 编译期
  panic），内联字符串表达不出来；而开放词汇走代理 id 本就是本节的既有规则。哈希已删，
  `0` 是发号不可达的哨兵 id，路由层用 `mq::Partition`（Singleton | Named）做结构标记。
- **参与者（Booth）**：谁在消费。类型名走**唯一字典**（ns 30）拿 id；实例名不进任何
  字典——游标的主语是类型（ADR-0038 §2，已落；见 §8.3）。

四条不变式（§2 的推导即它们的展开）：

1. **emit 无接收者地址**。发起者只喊事实 `emit(event, data)`；接收者集合是运行时
   事实，发起时不可知也不应可知（ADR-0012：无 emits 白名单，dead ring 是观测面）。
2. **订阅是自我登记**。一行 `on` 的主语 = 声明所在的 BoothType，作者面不写类型名
   （`ReceiveDecl{event, key_field, wildcard}` 没有类型字段；框架
   `router.on(event, booth.name, decl)` 填主语）。冒充别的类型的订阅在声明面上
   不可表达——想以同名注册就是热替换语义（后版本赢）。
3. **队列先于消费者存在**。MqData 键里没有摊位身份（"事件不属于任何摊位"）；
   实例可以死（scale-to-zero），队列不跟着死——backlog 重放靠游标，不靠活进程。
   但这条承诺是有界的：死得够久，积压作废（ADR-0039 §2 的 `cursor_ttl`）。
4. **类型级订阅，实例级消费位置**。EventRoute 行主语是类型；MqCursor 行主语是
   参与者（当前形状——ADR-0038 §1 已裁收窄为单例投递，见 §8.3）。
5. **无静默丢弃**。每条 emit 的每个匹配 route 要么产生至少一个真目标，要么留下
   一条可观测记录（ADR-0038 §4；今天的入口见 §5、§6.1）。配套前提：一个队列的
   消费者集合必须是**闭集**——它同时是保留承诺（§6.3）与通配收窄（ADR-0038 §1）
   的前提。

## 2 从约束推导持久面

事件平面没有可以挑的设计空间：每一步都是上一步逼出来的。链条走完，§7 的键空间表就是
它的落点（每一步末尾括注落点表与 ns 号）。

1. **发起者不知道谁在听**（不变式 1）。→ 投递不能按地址寻址，也不存在可校验的接收者
   白名单；「没人订阅」只能事后观测（dead ring）。
2. **一个事实可能有 N 个消费者，且它们互不相识**。→ 投递不能是函数调用或返回值，只能
   是「事实先落盘，各方自取」。**落点：MqData（ns 22）**——一行一个 emit、payload 随行；
   键里没有摊位身份，因为事件不属于任何摊位（不变式 3）。
   键 `[event_id u32][part_id u32][seq u64]`（16 B，`#[ok_partition(1)]`，按 event_id 分片）；
   值 = payload 的**动态段**（native nTLV map，无声明字段——键已定身份，事件自身的形状是
   作者的数据）+ 挂 live `Count` reduce（group = 键的 event_id + part_id，见第 11 步）。
3. **每个消费者要独立地知道自己读到哪，而消费者可以死**（scale-to-zero、驱逐、热替换）。
   → 消费位置必须独立持久，且单调：**落点：MqCursor（ns 23）**，行主语是订阅者，
   绝不回卷（skip-to-head 的持久性靠这条——跳过的 backlog 不得在下一轮 drain 重现）。
   键 `[event_id u32][part_id u32][booth_id u32]`（12 B，`#[ok_partition(2)]`）；
   值 = `cursor u64`（最后消费的 seq，0 = 未消费）+ `last_active_ms u64`（v2 热尾：
   `cursor_ttl` 谓词的输入，0 = 未标记、永不判过期）。
4. **同一事件的不同批次要能各自排队**（异实例并行，同实例串行）。
   → 队列要切分：**partition = 路由解析出的 instance key**。分区值是开放词汇，按 §7 的
   既有规则走**代理 id**（ADR-0039 §1 已落：PartitionName 字典发号、哈希删除——哈希只能
   解决定宽，而 okm 主键本就要求定宽、内联字符串根本表达不出来）。
   字典落点 **PartitionName（ns 21）**：键 `id u32`（4 B，发号从 1 起，0 留给单例、构造性
   不可达）；值 = `name String`（原始分区值）+ `id u32` 镜像（reduce 折载荷字段）+
   `global u32`（单组常量 0）+ `by_name` 文本索引（名→id）+ `HighWater(id)` reduce
   （注册表级水位，见第 6/8 步同一形态）。
5. **追加不能扫全分区取 max；而序列只能有一个发号者。** → **落点：MqHead（ns 24）**
   分区写头：键 `[event_id u32][part_id u32]`（8 B），值 = `last_seq u64`（该分区已发出的
   最大序列号）；追加读它、算 `last + 1`、写回——O(1)，且没有第二次扫描。
   ——**「为什么是计数器而不是时间戳？」**：这个值是**排序键**，在同一分区内它就是行的身份，
   所以必须全序且唯一——这两点只有「读头 + 1」给得了：① 另起一个墙钟来源（同毫秒两次追加
   算出相等的键）会让同一个主键的后一次写入**覆盖**前一次（不是重复投递，是丢数据）；
   ② 墙钟会倒退（NTP 回拨、多节点偏斜），落进已消费游标之下的值**永远不会被读到**——
   静默丢弃。把墙钟读数与「上一个值 +1」混在一个数里（曾经是 `max(now_ms, last+1)`）只会
   让人以为它是时间：它既不是「何时」（会领先墙钟若干下），也不是纯粹的「第几个」（量级
   跟着墙钟跳）。现在它是纯序列，名字与语义一致。**唯一性由谁保证**：emit 路径握着 realm
   锁跨过 append（`self_arc.lock()`），所以发号是单写的——锁才是保证，计数器只是记账。
   **事实发生的墙钟时间要留证就走 payload 字段**（排序用的 seq vs 事实发生的时刻，两个不同的
   东西，不要塞进同一个值）。
6. **键是定宽二进制段、无文本分隔符，行里不能存字符串；而事件名是开放词汇，不配真实
   ns。** → **落点：EventName（ns 20）** 代理 id 字典：键 `id u32`（4 B）；值 = `name String`
   + `by_name` 文本索引（变长字段，ADR-0005：至多一个、必须最后、不带长度前缀——所以匹配
   靠行校验：无分隔符，"add" 会前缀命中 "add_to_cart"，行比较才是精确性）。
7. **光有队列没人知道该投给谁；订阅事实还必须扛重启**（不重新内省脚本）。
   → **落点：EventRoute（ns 25）**，注册时把类型的 `receives` 装配成行：
   键 `[event_id u32][booth_id u32]`（8 B）；值 = `booth_id u32` 镜像（索引字段必须是载荷字段）
   + `key_field String`（空 = 无 key 订阅；通配订阅时存的是**模式串**）+ `wildcard u8`
   （0 精确 / 1 通配——okm 字段类型没有 Bool）；`by_booth` 索引供激活绑定与压缩水位
   分母（§6.3）。开放词汇的**通配模式走同一行形**——模式串存行内，匹配由 emit 路径做。
8. **路由行同样不能存类型的名字。** → 订阅者身份要发号 → **落点：BoothName 字典
   （ns 30）**——一张，meta 面的类型唯一字典：键 `id u32`（4 B，发号从 1 起）；值 =
   `name String` + `id u32` 镜像 + `ns u32`（该类型的数据 ns = `100+id` 的分配结果）+
   `global u32`（单组常量 0）+ `by_name` 索引（名→id）+ `HighWater(id)` reduce（兼定数据
   ns 的发号）。ADR-0038 §2 已落：事件面自留的那张（旧 ns 33）已删除，号位不复用（§8.3）。
9. **摊位不只有订阅关系。** → **落点：BoothDef（ns 31）**（一行一类型）+
   **CodeBlob（ns 32）**（ADR-0027 内容寻址）。
   BoothDef：键 `booth_id u32`（4 B）；值 = `name String` + `language String` +
   `code_sha256 [u8;32]`（代码的指针，不再是代码本身）+ `idle_ttl_secs u64`（0 = 用 realm
   默认——零 TTL 无意义）+ `encoding u64`（v2 热尾：0 = json、1 = cbor，ADR-0037 §2）+
   `schema` 动态段（introspected schema，结构化 nTLV，非不透明文本）。
   CodeBlob：键 `sha256 [u8;32]`（32 B）；值 = `sha256 [u8;32]` 镜像 + `data Bytes`。
   sha256 即版本身份，同码重注册去重，构造性不可变。
10. **类型自己的状态要住在自己的 ns 里。** → 注册的副作用链：唯一类型字典发 id → 数据
    ns = `100 + id`（ADR-0026，单调不复用）→ 取上传的持久 schema 副本 →
    `StorePlan::from_schema` 编译存储路由表（collections/索引/reduce 的槽位编码）。
11. **运维要零扫描地看队列深度**（skip-to-head 的决策输入）。→ MqData 上挂 live `Count`
    reduce（group(event_id, part_id)）：append 折 +1、水位压缩删行折 −1，`depth()` 是
    一次点读，永不扫描。

链条收束成一句：**事件是事实（要队列），订阅是关系（要注册表），两者都不许在键里带
名字（要两张 id 字典），摊位自己的东西——定义、代码、状态——各有其表。**

## 3 声明面（作者写什么）

| 面 | 形状 | 主语绑定方式 |
|---|---|---|
| Rust 摊位 | `BoothType::rust(...).on("order.created", key_field)` builder | 声明挂在哪个类型对象上 |
| python | `@on("order.created", key="user_id")` 装饰器收集 | 声明长在哪个模块里 |
| steel | `(on "order.created" "user_id" handler)` 收集器 | 同上 |
| bgi/exec/wasm | 手写的 `interface_schema` 帧（含 `receives` 块） | 同上（无收集器，声明即数据） |

脚本面声明经 `carrier::introspect` 在**上传时**物化为 `interface_schema.receives`；
宿主不按语言分支。声明的内容只有两问：**订什么事件**（事件名/通配模式）、
**怎么定位实例**（今天：`key_field`——payload 哪个字段是 instance key；
4.13 终态：访问方法引用 resolve，见 §8.1）。

订阅语法里的名字是开放词汇；**声明"住在哪个类型里"才是身份**——事件名本身不携带
主语。

## 4 登记（一次性，register_type）

`crates/realm/src/registry.rs`：

1. 热替换语义先行：`router.drop_booth(name)`（内存）+ `mq::routes_drop_booth`
   （持久表，走 `by_booth` 索引扫描）——同名再注册 = 最新版本独占，不重复投递。
2. 逐条 `receives` 装配两表：内存 `router.on(event, booth.name, key_field)` +
   持久 `mq::route_put(event, booth.name, key_field, wildcard)`（行主语 booth_id
   由框架 resolve 填充，不经作者手）。
3. 该类型全部驻留实例与会话回收（旧 source 不再应答，下条消息按新代码冷启动——
   实例表是可丢弃热缓存：状态在类型 collections，backlog 在队列，重放由游标完成）。
4. 注册的副作用链（§2 第 10 步的落点）：`meta::ns_and_schema_of` 解析类型 ns
   （唯一类型字典发 id + 分配 `BOOTH_NS_BASE + id` 数据 ns）+ 取上传的持久 schema
   副本 → `StorePlan::from_schema` 编译存储路由表（collections/索引/reduce 的槽位编码）。

## 5 匹配（每次 emit，热路径）

`crates/realm/src/events.rs::emit`：

- 匹配走**内存** `EventRouter`：`exact: HashMap<事件名, Vec<Route>>` +
  `wildcard: Vec<(前缀, Route)>`（线性扫——通配数构造性地小，Trie 是过度设计）。
- 匹配形状是集合不是单值：一个事件名可同时命中精确路由和若干通配路由，逐条独立投递。
- **无命中 = dead ring**（`realm.dead_events.push`，有界、可观测——ADR-0012 的
  观测面）。注意方向：dead ring 只收「无任何路由匹配」的事件；有路由匹配但实例
  没活着的不是丢失，是 backlog 写入（§6.1）。另有一条入口：匹配到了路由但
  `mq::append` 失败（存储故障），事件同样落 dead ring——它收了「没写完的事实」，
  与「没人订阅」共用同一个观测面。第三条入口（匹配到路由但无真目标）已随 ADR-0038 §4
  落地（`MissingKeyField`），见 §1 不变式 5。
- 持久真相源是 EventRoute 注册表（ns 25）：重启后路由存活，不需要重新内省脚本；
  内存 router 是它的热面，boot reload 时经同一注册代码重建。注册表行是
  订阅事实 + 压缩水位分母。

## 6 投递、消费与保留

### 6.1 投递与分区解析（当前形状）

逐匹配 route：

```
partition =  key_field 空 → Partition::Singleton
            否则 payload[key_field] as str → Partition::Named(该值)   // 恰好一个目标
            缺字段/非字符串 → 畸形事件：带 MissingKeyField 落 dead ring（ADR-0038 §4）
target   = InstanceId{booth_type, 单例实例 key = "__singleton__"，否则 = 分区值}
激活       → 不在实例表就先 instance() 拉起（同 pass 内先激活后投递）
入队       → mq::append(event, &Partition, payload)   按 (event, partition) 去重
```

三个如实的注脚：

1. **`__default__` 兜底已退役**（ADR-0038 §4）：payload 缺声明的 key 字段 = 畸形事件，
   带原因标签落 dead ring，不再静默喂给一个没人寻址的兜底实例；与「扫描零命中」同级。
2. **一条 route 恰好产出一个目标**（单键路由）。"一次 emit 扇出到 N 个实例"
   在类型级早就有（多类型订一事 = EventRoute 多行）；缺的是**类型内**按业务
   事实扇出（region.escalation → 西部所有门店）——单字段表达不了，扫描天然
   一对多（§8.1）。
3. **投递/消费两侧的分区一致性靠构造**：消费端 `bound_partition` 用"本实例的
   key"回填 route 的 key_field 语义，与投递端从 payload 取值的约定在类型层
   对齐（实例名=分区名=键字段值三位一体是今天的隐含契约）。
4. 去重键是 `(具体事件名, partition)`：多个类型订同一事件时只入队一次，队列对
   所有订阅者扇出；一次 emit 对同一分区写两行就是双投递。

### 6.2 消费循环

`crates/realm/src/instance.rs`：实例激活时绑定订阅集（内存 `router.routes_of`），
然后每个实例起一条 consumer loop：

- 键位路由：partition = 本实例的 key；键空路由（含通配）：partition = 单例。
- 每轮扫**全部**绑定队列：通配订阅用 `mq::events_matching(prefix)` 每轮重新展开成
  具体事件名（新名字自动加入）；其余订阅用声明的事件名本身。
- 每个队列：读游标 → `mq::backlog(取游标之后的行)` → 逐行 `run_job` 串行执行 →
  `mq::advance`。backlog 是分区前缀的全扫（无批上限），收敛靠游标推进与压缩。
- 一轮无进展则 park 50ms。**同实例串行、异实例并行在这个循环里，不在锁里。**

游标的主语是**类型**（ADR-0038 §2）：两个类型订同一事件各持独立游标。

- **booth_id 段的混装已成历史**：同一个 resolve 曾既接类型名（EventRoute 登记、
  `routes_of_booth`）又接参与者名 `"type/key"`（游标语义），压缩路径里
  `split_once('/')` 手工抠类型名就是它的下游症状。**已落（ADR-0038 §2）**：唯一字典
  （ns 30，`mq::booth_id_of` → `meta::resolve_booth_id`），游标键第三段 = `booth_id`，
  参与者名不再发号。
- **通配语义**：无 key 订阅投递给该类型的**单例实例**（`partitioning.md` §1 原文即此
  语义）——消费者集合必须是闭集（§1 不变式 5、§6.3）。已落（ADR-0038 §1）：消费循环只为
  单例实例绑定无 key 路由，其余实例不绑；wildcard fan-out 测试已随语义改写。

### 6.3 保留与压缩

水位在 **emit 路径**（写路径压缩），分母不是游标键而是 **EventRoute 注册表**：

- 一个游标行只在其所属类型**当前**注册了**匹配该具体事件**的 route 时才进分母
  （`mq::booth_subscribes`：精确行按 event_id 匹配，通配行按前缀匹配——注册表存的是
  模式串）。实例被驱逐不进/不出分母（backlog 还等着重放）；类型热替换或注销使 route
  消失，其陈旧游标行随之掉出分母（§4 的 drop 语义在下游的解释）。
  （实施 §6.3 时补的一处：原先只按精确 event_id 查表，会让**通配**订阅者静默掉出分母、
  积压被压缩吃掉——违反 §1 不变式 5。）
- 水位 = 分母内游标的最小值；`delete_before(event, partition, min_seq)` 删掉 mq-data
  中 seq < 水位的行为止。分母为空（没有注册订阅者）时不动。
- seq 比较即顺序比较（§2 第 5 步：它是计数器，不是时间）。
- **depth**：MqData 的 live `Count` reduce 一次点读（§2 第 11 步）——skip-to-head
  的决策输入，永不扫描。
- **skip-to-head**（`mq::skip_to_head`；脚本面 host fn 同名 `ctx_skip_to_head`，probe carrier 白名单已同步）：把游标跳到分区写头，丢弃陈旧 backlog（泄压
  阀）。游标单调（`advance` 只进不退）是它持久的原因；`rewind_cursor` 仅测试支持，
  生产路径不用。
- **游标过期（ADR-0039 §2，已落）**：全局 `cursor_ttl`（`EngineConfig` 一个字段，
  KDL `mq { cursor_ttl "30d" }`，默认 30 天，与 `idle_ttl` 解耦——天级 vs 秒级，绑定
  等于用一个秒级事件决定天级的数据承诺；不给 per-type 覆盖，因为分母是跨类型的 min
  比较）。过期 = 该行退出分母（允许压缩越过它 = 积压作废），**不是删行**：删行会让游标
  读回 0 而 `backlog(after=0)` 重放现存行，若该游标原本领先于水位就是重复投递；物理
  删除（`drop_cursor`）只留给「游标已低于当前水位」的行。活跃时间 `last_active_ms`
  （v2 热尾）随 advance 刷新，`0` 哨兵不参与判定（旧行把缺失字段解码为 0，否则新代码
  首次运行全体瞬时过期）。判定发生在压缩时（谓词），不是后台守夜进程。
- 扫描代价被订阅者数量界定（逐 emit 运行）。

## 7 键空间布局（原 ns-layout 并入）

一个 realm 的持久面 = 一个 okm 实例；框架低位块分两段（ADR-0040）：**事件面 ns 20–29**、
**meta 面 ns 30–39**（段内表序是概念序：词汇 → 数据 → 位置 → 注册表）；**摊位类型 ns**
运行时从 100 起分配；空段 `26–29`（事件面空段）与 `33–99` 预留。

框架低位块（ADR-0040，已落）。**键/值完整布局——字段顺序、宽度、哨兵与索引/reduce 形态
——见 §2 各步的落点行；本表是索引，权威定义住代码（`#[ok_ns]`/`#[ok_layout]`）**：

| ns | 表 | 键 | 用途 | 代码 |
|---|---|---|---|---|
| 20 | EventName | `id u32` | 事件名字典（`by_name` 文本索引；开放词汇不占真实 ns，代理 id 形态） | mq.rs |
| 21 | **PartitionName**（新） | `id u32` | 分区字典（ADR-0039 §1）：`by_name` 文本索引 + `HighWater` 水位，反解析 id→名供运维；FNV-1a 哈希已删 | mq.rs |
| 22 | MqData | `[event_id u32][part_id u32][seq u64]` | 事件数据：一行一个 emit，N 订阅者 = N 游标；排序键 = 每分区序列（计数器）；挂 live `Count` reduce（group 同键）供 `depth()` 点读 | mq.rs |
| 23 | MqCursor | `[event_id u32][part_id u32][booth_id u32]` | 订阅游标：最后消费 seq，单调不回卷；第三段 = ns 30 的类型 id；另增 `last_active_ms`（ADR-0039 §2，已落） | mq.rs |
| 24 | MqHead | `[event_id u32][part_id u32]` | 分区写头（序列发号器）：值 `last_seq u64`；O(1) 追加（`last+1`），realm 锁下单写 | mq.rs |
| 25 | EventRoute | `[event_id u32][booth_id u32]` | 持久订阅注册表（`by_booth` 索引）；通配同行形态存模式串；`booth_id` = ns 30 的类型 id | mq.rs |
| 30 | **BoothName**（原 TypeName；改名 dbc5d60、改号 Phase 4.18） | `id u32` | 摊位类型唯一字典：`by_name` 索引 + `HighWater(id)` preset + 数据 ns 分配（`100+id`）；双向解析（名→id `resolve_booth_id`、id→名 `booth_name_of`） | meta.rs |
| 31 | BoothDef | `booth_id u32` | 摊位定义行：name/language/encoding/idle_ttl + `code_sha256` 指针；introspected schema 走动态段 nTLV | meta.rs |
| 32 | CodeBlob | `sha256 [u8;32]` | 代码字节内容寻址（ADR-0027）：纯内容行，构造性不可变 | meta.rs |

事件面自留的订阅者身份字典（旧 ns 33）**已删除**（ADR-0038 §2），不占新分段任何号位。

partition 注解：MqData=partition 1（按 event_id）、MqCursor=partition 2；`SINGLETON_PART = 0`。

**已落（ADR-0039 §1）**：分区是**代理词汇**——`PartitionName`（ns 21）发定宽 `u32` id
（与另两张字典同一布局），`0` 保留给单例、发号从 1 起（构造性不可达），
`part_hash`/FNV-1a/`part_id_of`/`part_hash_of` 已删，路由层用 `mq::Partition`
（Singleton | Named）结构标记而非魔法字符串。理由：开放词汇走代理 id 是本节既有的规则；
okm 主键构造上定宽（`KeyEncode` 遇 `String` 编译期 panic），内联字符串表达不出来；而哈希
带来碰撞即错误投递、保留值与值空间共用名字空间、哈希不可反查（运维列不出人话队列）。
键宽随之收紧：MqData 20→16 B、MqCursor 16→12 B、MqHead 12→8 B。
`MqCursor.last_active_ms`（§6.3 的过期谓词）同批落地。

摊位类型 ns（运行时）：注册类型 = 唯一字典发 id、数据 ns = `100 + id`，单调不复用；
ns 内是 interface_schema 声明的 collections + 访问方法（槽位 `ns + slot` 编码，
dict/junction 基址住 collection schema 常量）；实例是 ns 内 document。类型隔离
构造性：ctx 存储句柄注册期绑定拥有类型 ns，跨类型访问表达不出来（ADR-0026 §3）。

不变量：低位块编译期固定，新框架面从空段取号且必须进本表（双语同步）；摊位类型
永不落进低位块；`#[ok_ns]`/`#[ok_partition]` 注解改动必须同步本页——权威定义住代码。
**号位永不复用**：一个号一旦属于某张表，就永不给另一张表。本次重新编号把旧号
`30–35`/`40–42` 整段作废，而新 meta 段（30–32）正落在旧事件面的号上——所以在**既有
部署**上清除低位块是一次运维动作（否则新 `BoothName` 会把旧 `EventName` 行读成类型名，
即「把旧数据读成新数据」）。清除的代价比「mq 字节转瞬即逝」更大：已持久化的摊位定义与
代码 blob 一并作废，部署方要重新注册类型（ADR-0040 记录在案）。新库没有可清除的东西。

**词汇表不占真实 ns**：开放词汇（事件名、分区名、参与者名）走代理 id + 文本索引，封闭
词汇（booth 类型）才配真实 ns——EventName/PartitionName/BoothName(ns 20/21/30) 的形态
由此而来；ns 只增不减（类型注销不回收——复用键空间前缀等于把旧数据读成新数据，id 与 ns
永不复用是同一条裁决）。

双字典的收编已完成：唯一字典 = ns 30（ADR-0038 §2 已落，事件面自留表已删）。

## 8 终态裁决与实施清单（4.13+ 挂账）

本节记录事件面的**终态裁决**（裁决文本住 ADR-0038/0039/0040，这里只留机制、理由摘要与
实施连带）。§8.3（身份与投递）与 §8.4（分区身份、分段、保留承诺）**已落地**；
§8.1（路由终态：访问方法扫描）仍待决，前置 = 动态 schema；§8.5 仍开放。

### 8.1 路由终态：instance key 走访问方法扫描（目标形状已锁）

partitioning.md §1 记录的目标：`key_field` 取 payload 字段（恰好一值、一实例）
→ 事件经该类型 ns 的**访问方法扫描**出 id（天然一对多）。机制拆解（声明/登记/
求值三段，作者只写第一段）：

```
声明（作者在 receives 里）： resolve = {collection, index, probe_field}
登记（框架，一次性）：       EventRoute 行载荷携带该引用（名字字符串，按名寻址——
                             不存跨面 id，符合"行存事实、解析在拥有 schema 的一侧"）
求值（框架，每次 emit）：     探针 = payload[probe_field]
                            → StorePlan.entries →(okm-entry::collection_from_entry)
                              DynamicCollection over 类型 ns（4.16 已落好的解析面）
                            → 扫 index 前缀：命中行代理键 = 目标实例 key（扇出 b 缺省）
                            → 逐目标激活 + 按 (event, partition) 去重入队（现成）
```

已定案（本轮对话敲定）：

- **主语绑定不变**：resolve 只长在类型自己的 `receives` 里，行主语由框架填——
  作者面依旧无类型名字段。
- **目标=命中行键（b 缺省）**：声明面不出现 target 字段；"扇出到命中行本身"是
  唯一语义。载荷字段取法（a）与索引尾段取法（c）均**不进词汇表**——前者造第二
  可指错声明，后者要改 okm 索引返回形状、且本质是 a 的优化（premature，Windmill
  判据）。实例身份不声明，框架扫描得出；摊主代码只从 `ctx.self_id.key` 读自己。
- **旧单字段形态保留为行的另一种形状**（判别在行结构层，不搞哨兵混合）：
  keyed 场景两者等价且 key_field 更便宜（零扫描）；迁移逐类型进行。
- **多目标失败语义**：逐目标 dead-ring/失败记录，与 ADR-0012 一致（append 本就
  逐 partition 独立，失败天然逐条）。
- **MQ 格式不动**：`append(event, partition)` 本就逐分区独立行、游标逐
  (event, partition)——扇出能力缺的只是路由侧"产出一组 partition"这一步。

**已裁（ADR-0038 §3）**：①声明粒度 = 逐事件，没有类型默认——`resolve` 是 (type, event)
的函数，类型级默认只在「该类型每个事件恰好同构」时才有定义；且缺省值必须唯一确定语义
（既无 `resolve` 也无 `key_field` = 单例投递），「缺失 = 继承」会让同一字段有两个读法 =
声明-执行漂移温床。方案 B（类型默认 + 覆盖）由此否决。**多目标失败语义**亦已裁：逐目标
dead-ring，与 ADR-0038 §4 的无静默丢弃一致。实施 = Phase 4.17。

### 8.2 EventRoute 行形状

`key_field: String` 载荷换成引用载荷。**已裁（ADR-0038 §3）**：引用不带集合 ns 号——
行里存 collection/index/probe 的**名字**（解析住在拥有 schema 的一侧），存 slot/ns 号
等于把位置当身份，schema 一改就静默改指向。三种声明形状（单例 / payload 解析 / 索引
扫描）是三种机制，判别在行结构层，不搞哨兵混合。实施 = Phase 4.17。

### 8.3 双字典收编与游标键正交化（已落，ADR-0038 §1/§2）

通配订阅收窄为「投递给该类型的单例实例」——广播会让消费者集合变成开集（每次新 key 的
emit 都激活一个新实例，而新实例游标从 0 起 = 重放该队列现存全部历史，水位被永久钉住、
订阅漂移成状态同步）；消费者集合必须是闭集（§1 不变式 5）。随之：唯一类型字典 = ns 30，
事件面自留表已删（号位不复用），游标键第三段 = `booth_id`，`mq` 的类型解析委托给
`meta::resolve_booth_id`。**B 方向（拜实现保留参与者级游标）已否决**——不是不方便，是开集。

历史留档：正交方案当年的卡点正是通配的参与者级扇出（两实例共享 singleton 游标会让一条
消息只被一个消费，当时被读作语义回归）；裁决解在另一头——那个语义本就该是单例投递。

落地连带（已完成）：`bound_partition`/`routes_of_*`/compact 的解析链改造、测试调用点、
`by_type` → `by_booth` 索引、`split_once('/')` 消失。残余（未覆盖）：实例键空间仍用
`"__singleton__"` 哨兵字符串，payload 里字面等于它的 key 仍会别名到单例**实例**——
让实例身份结构化会牵动整个 call model 与 probe 缝上的 `InstanceId`（ADR-0038 已记录）。

### 8.4 分区身份、键空间分段与游标保留承诺（已落，ADR-0039 §1/§2、ADR-0040）

分区改为代理字典（`PartitionName`，哈希删除）；低位块分两段（事件面 20–29、meta 面
30–39，段内概念序）；`cursor_ttl` 全局配置（默认 30 天），过期退出分母、不删行。机制与
理由见 §6.3 与 §7。

落地连带（已完成）：`#[ok_ns]` 改号、`PartitionName` 表 + `mq::Partition` 结构标记、
键宽收紧、`crates/config` 的 `mq { cursor_ttl }` + realm 字段、`MqCursor.last_active_ms`
（v2 热尾 + 0 哨兵）、分母改造（`booth_subscribes`）+ 惰性行回收（`drop_cursor`）。
**同期改名（2026-10-02 第二批）**：第三段与游标键的标识符 `type_id` → `booth_id`、
派生索引 `by_type` → `by_booth`（ADR-0032 的 booth 命名清扫落到字段层）。MqData 的第三段
是**序列** `seq`（`MqHead.last_seq`，`last+1`）——不是时间戳：它是排序键兼行的身份，
墙钟读数混进来只会带来碰撞（同毫秒覆盖 = 丢数据）与回拨（落到游标之下 = 静默丢弃），
唯一性由「emit 路径持 realm 锁 = 单写者」保证；`mq::skip_to_now` → `mq::skip_to_head`
（脚本面 host fn `ctx_skip_to_now` → `ctx_skip_to_head` 同步改名，aura 侧与 probe carrier
白名单一并更新）。
**既有部署**需清除低位块（新 meta 段落在旧事件面的号上；代价含已持久化的定义与代码 blob，
ADR-0040 记录在案）。

### 8.5 杂项挂账（仍开放）

- `events_matching` 增量化（通配每轮 50ms 全量重扫，可缓存——单例收窄后代价已收敛到
  一个消费者，词汇很大时才值得，Windmill 判据）。
- cold call over the wire（依赖 Phase 6 消费者出现，勿单独实施）。