# 事件流转（emit / on 全机制）

> **语言：** [English](event-flow-en.md)（主文档） · [中文](event-flow.md)

本文是事件平面的单一整合文档：声明面 → 登记 → 匹配 → 投递 → 持久队列 → 游标消费 →
保留与压缩 → 观测面，附键空间布局（ns-layout 并入本文，原 `ns-layout.md`/`ns-layout-en.md`
退为指针）与待决问题清单（4.13 挂账）。裁决依据：ADR-0007/0012（接收者集合是运行时
事实）、ADR-0026（类型级存储面）、ADR-0002（事件不占真实 ns）、
`docs/design/partitioning.md` §1（路由终态目标）。

## 1. 词汇与不变式

事件平面有三个正交词汇，混用是历史上最容易出错的地方：

- **事件（Event）**：发生的事实，开放词汇。不占真实 ns，占 EventName 代理 id。
- **分区（Partition）**：队列的切片标识，由路由解析出的 instance key 派生。
  值 = 字符串 → FNV-1a 定宽 u64（键 FIELD 哈希，非 ns 字典——ADR-0002 的拒绝
  对象是 ns，不是投递扇出键）；`0` 保留给单例分区（哈希碰撞映射到 1，保留是
  构造性的不是巧合）。
- **参与者（Booth）**：谁在消费。类型名（"cart"）与实例名（"cart/alice"）今天
  混在同一张字典里——这是待决问题的根源（§6、§8.3）。

不变式：

1. **emit 无接收者地址**。发起者只喊事实 `emit(event, data)`；接收者集合是运行时
   事实，发起时不可知也不应可知（ADR-0012：无 emits 白名单，dead ring 是观测面）。
2. **订阅是自我登记**。一行 `on` 的主语 = 声明所在的 BoothType，作者面不写类型名
   （`ReceiveDecl{event, key_field, wildcard}` 没有类型字段；框架
   `router.on(event, booth.name, decl)` 填主语）。冒充别的类型的订阅在声明面上
   不可表达——想以同名注册就是热替换语义（后版本赢）。
3. **队列先于消费者存在**。MqData 键里没有摊位身份（"事件不属于任何摊位"）；
   实例可以死（scale-to-zero），队列不跟着死——backlog 重放靠游标，不靠活进程。
4. **类型级订阅，实例级消费位置**。EventRoute 行主语是类型；MqCursor 行主语是
   参与者（当前形状——见 §8.3 的待决收窄）。

## 2. 声明面（作者写什么）

| 面 | 形状 | 主语绑定方式 |
|---|---|---|
| Rust 摊位 | `BoothType::rust(...).on("order.created", key_field)` builder | 声明挂在哪个类型对象上 |
| python | `@on("order.created", key="user_id")` 装饰器收集 | 声明长在哪个模块里 |
| steel | `(on "order.created" "user_id" handler)` 收集器 | 同上 |
| bgi/exec/wasm | 手写的 `interface_schema` 帧（含 `receives` 块） | 同上（无收集器，声明即数据） |

脚本面声明经 `carrier::introspect` 在**上传时**物化为 `interface_schema.receives`；
宿主不按语言分支。声明的内容只有两问：**订什么事件**（事件名/通配模式）、
**怎么定位实例**（今天：`key_field`——payload 哪个字段是 instance key；
4.13 终态：访问方法引用 resolve，见 §8）。

订阅语法里的名字是开放词汇；**声明"住在哪个类型里"才是身份**——事件名本身不携带
主语。

## 3. 登记（一次性，register_type）

`crates/realm/src/registry.rs`：

1. 热替换语义先行：`router.drop_booth(name)`（内存）+ `mq::routes_drop_booth`
   （持久表，走 `by_booth` 索引扫描）——同名再注册 = 最新版本独占，不重复投递。
2. 逐条 `receives` 装配两表：内存 `router.on(event, booth.name, key_field)` +
   持久 `mq::route_put(event, booth.name, key_field, wildcard)`（行主语 booth_id
   由框架 resolve 填充，不经作者手）。
3. 该类型全部驻留实例与会话回收（旧 source 不再应答，下条消息按新代码冷启动——
   实例表是可丢弃热缓存：状态在类型 collections，backlog 在队列，重放由游标完成）。
4. 注册的副作用链：`meta::ns_and_schema_of` 解析类型 ns（唯一类型字典发 id +
   分配 `BOOTH_NS_BASE + id` 数据 ns）+ 取上传的持久 schema 副本 →
   `StorePlan::from_schema` 编译存储路由表（collections/索引/reduce 的槽位编码）。

## 4. 匹配（每次 emit，热路径）

`crates/realm/src/events.rs::emit`：

- 匹配走**内存** `EventRouter`：`exact: HashMap<事件名, Vec<Route>>` +
  `wildcard: Vec<(前缀, Route)>`（线性扫——通配数构造性地小，Trie 是过度设计）。
- 匹配形状是集合不是单值：一个事件名可同时命中精确路由和若干通配路由，逐条独立投递。
- **无命中 = dead ring**（`realm.dead_events.push`，有界、可观测——ADR-0012 的
  观测面）。注意方向：dead ring 只收"无任何路由匹配"的事件；有路由匹配但实例
  没活着的不是丢失，是 backlog 写入（§5）。
- 持久真相源是 EventRoute 注册表（ns 35）：重启后路由存活，不需要重新内省脚本；
  内存 router 是它的热面，boot reload 时经同一注册代码重建。注册表行是
  订阅事实 + 压缩水位分母。

## 5. 投递与分区解析（当前形状）

逐匹配 route：

```
partition =  if key_field 空 → "__singleton__"
             else payload[key_field].as_str() → unwrap_or("__default__")   // 恰好一个
target   = InstanceId{booth_type, partition 值即实例 key}
激活       → 不在实例表就先 instance() 拉起（同 pass 内先激活后投递）
入队       → mq::append(event_id, part_id, payload)   按 (event, partition) 去重
```

三个如实的注脚：

1. **`__default__` 是静默兜底**：payload 缺字段不报错，落兜底实例——拼错字段名
   的错在文档层不可见。这是 4.13 换访问方法扫描的动因之一（类型化在边界拦错）。
2. **一条 route 恰好产出一个目标**（单键路由）。"一次 emit 扇出到 N 个实例"
   在类型级早就有（多类型订一事 = EventRoute 多行）；缺的是**类型内**按业务
   事实扇出（region.escalation → 西部所有门店）——单字段表达不了，扫描天然
   一对多（§8）。
3. **投递/消费两侧的分区一致性靠构造**：消费端 `bound_partition` 用"本实例的
   key"回填 route 的 key_field 语义，与投递端从 payload 取值的约定在类型层
   对齐（实例名=分区名=键字段值三位一体是今天的隐含契约）。

## 6. 持久队列、游标与消费

MQ 面六张表在一个 okm 实例（`MqStore`：生产 fjall keyspace，测试 TestStore）；
realm 前缀压在 ns 之下（跨 realm 同 ns 互不相干）。键空间全表见 §7。

- **MqData**：`[event_id][part_id][time]` → payload。排序键是**逻辑时间**
  （ms，MqHead 分区单调 `max(now_ms, last+1)`——O(1) 追加、跨发射器单调；
  墙钟真实时间随 payload 字段走，不承担排序职责）。
- **MqCursor**：`[event_id][part_id][booth_id]` → 最后消费 seq。**单调推进，
  绝不回卷**（skip-to-now 的持久性靠这条：跳过的 backlog 不得在下一轮 drain
  重现；`rewind_cursor` 仅测试支持）。
- **booth_id 段的混装事实**：同一个 resolve 函数既接类型名（EventRoute 登记、
  `routes_of_booth`——主语是类型）又接参与者名 `"type/key"`（游标语义——主语是
  实例），且按开放词汇发号。compact 路径里 `split_once('/')` 手工从参与者名
  抠回类型名再核注册，就是这个混装的下游症状（待决：§8.3）。
- **消费循环**（`instance.rs`）：实例激活时绑定订阅集（通配每轮重新展开匹配
  已注册事件名，新名字自动加入）；每轮扫全部绑定队列，backlog 批读 →
  run_job 串行执行 → advance 游标——同实例串行、异实例并行在此循环，
  不在锁。
- **通配语义（当前实现）**：通配订阅把所有实例绑到 singleton 分区，
  每实例各持一条参与者游标 = **对该类型每个实例广播**（含 singleton）。
  注意 `partitioning.md` §1 的原文是"通配订阅绑定单例 `__singleton__`"——
  实现比文档宽（广播到每实例）；这个分叉正是游标键收编的 A/B 决策点（§8.3），
  **以文档为准收窄** 与 **拜实现保留** 是两个待裁方向。

## 7. 键空间布局（原 ns-layout 并入）

一个 realm 的持久面 = 一个 okm 实例；键空间三段：**框架固定表**（编译期，低位块）、
**摊位类型 ns**（运行时分配，100 起）、**中间空段**（36–39、43–99，预留）。

框架低位块：

| ns | 表 | 键 | 用途 | 代码 |
|---|---|---|---|---|
| 30 | EventName | `id u32` | 事件名字典（`by_name` 文本索引；开放词汇不占真实 ns，代理 id 形态） | mq.rs |
| 31 | MqData | `[event_id u32][part_id u64][time u64]` | 事件数据：一行一个 emit，N 订阅者 = N 游标；排序键=逻辑时间 | mq.rs |
| 32 | MqCursor | `[event_id u32][part_id u64][booth_id u32]` | 订阅游标：最后消费 seq，单调不回卷 | mq.rs |
| 33 | ~~BoothName~~ | `id u32` | **收编中**：订阅者身份字典（类型名与参与者名混表）——待 §8.3 裁决后删除，号位永不复用 | mq.rs |
| 34 | MqHead | `[event_id u32][part_id u64]` | 分区写头：O(1) 追加 + 跨发射器单调 | mq.rs |
| 35 | EventRoute | `[event_id u32][booth_id u32]` | 持久订阅注册表（`by_booth` 索引）；通配同行形态存模式串 | mq.rs |
| 40 | **BoothName**（原 TypeName，改名已落） | `id u32` | 摊位类型唯一字典：`by_name` 索引 + `HighWater(id)` preset + 数据 ns 分配（`100+id`）；双向解析（名→id `resolve_booth_id`、id→名 `booth_name_of`） | meta.rs |
| 41 | BoothDef | `type_id u32` | 摊位定义行：name/language/encoding/idle_ttl + `code_sha256` 指针；introspected schema 走动态段 nTLV | meta.rs |
| 42 | CodeBlob | `sha256 [u8;32]` | 代码字节内容寻址（ADR-0027）：纯内容行，构造性不可变 | meta.rs |

partition 注解：MqData=partition 1（按 event_id）、MqCursor=partition 2；
`SINGLETON_PART = 0`。

摊位类型 ns（运行时）：注册类型 = 唯一字典发 id、数据 ns = `100 + id`，单调不复用；
ns 内是 interface_schema 声明的 collections + 访问方法（槽位 `ns + slot` 编码，
dict/junction 基址住 collection schema 常量）；实例是 ns 内 document。类型隔离
构造性：ctx 存储句柄注册期绑定拥有类型 ns，跨类型访问表达不出来（ADR-0026 §3）。

不变量：低位块编译期固定，新框架面从空段取号且必须进本表（双语同步）；摊位类型
永不落进低位块；`#[ok_ns]`/`#[ok_partition]` 注解改动必须同步本页——权威定义住代码。

**词汇表不占真实 ns**：开放词汇（事件名、参与者名）走代理 id + 文本索引，封闭词汇
（booth 类型）才配真实 ns——EventName/BoothName(ns 33/40) 的形态由此而来；ns 只增
不减（类型注销不回收——复用键空间前缀等于把旧数据读成新数据，id 与 ns 永不复用是同
一条裁决）。

双字典的收编状态见 §8.3（40 改名与反解析已落码，33 删除待游标键裁决）。

## 8. 待决问题（4.13 挂账）

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

待定（下一批拍板）：①引用声明粒度——推荐逐事件（方案 A）；类型默认+覆盖（B）
被否的理由已记：默认对多集合类型无物理对应物，且"空=继承"与"空=单例"两个哨兵
读法同字段共存是声明-执行漂移温床。**B 的否决待用户终确认。**

### 8.2 EventRoute 行形状

`key_field: String` 载荷换成引用载荷。开放问题：引用是否带集合 ns 号（不带——
collection/index 名字在类型 ns 内解析；带 ns 则造跨面 id 引用，重蹈双字典覆辙）。

### 8.3 双字典收编与游标键正交化（代码半改，待裁）

已落地（未提交）：meta.rs `TypeName`→`BoothName` 全链改名 + 公开 `booth_name_of`
反解析——唯一类型字典双向可达。

未落地卡点：游标键第三段 `booth_id` 发号接的是参与者名 `"type/key"`（开放词汇），
不是纯类型名。正交方案（booth_id=type_id + 实例身份=part 哈希段，字符串 `"type/key"`
不再进字典、`split_once('/')` 症状消失）撞上一个语义事实：**通配队列当前按参与者
扇出**（每实例独立游标=对每实例广播），正交键下两实例共享 singleton 游标，一条
消息只会被一个消费——语义回归。

两方向待裁：

- **A（推荐，以文档为准）**：通配收窄为"投递给类型的 singleton 实例"
  （partitioning.md §1 原文读法）；33 删除，wildcard fan-out 测试随语义改写。
- **B（拜实现）**：保留参与者级游标——实例订阅身份确实需要发号，33 只改名/并入
  统一命名空间管理，`split_once` 的笨拙保留。

实施连带（无论 A/B）：ns 号位不回收的 ops 事实、`bound_partition`/`routes_of_*`/
compact 的解析链改造、测试调用点（`mq_okm.rs`/`events.rs`/`queue_relief.rs` 以
`"cart/alice"` 形状直调 mq 表面）、ns-layout 双语图与 ADR-0026 §1 表名更新、
probe 仓 USAGE 双语的帧形同步（4.16c 遗留）。

### 8.4 杂项挂账

- `events_matching` 增量化（通配每轮 50ms 全量重扫，可缓存——词汇大/订阅多才
  值得，Windmill 判据）。
- cold call over the wire（依赖 Phase 6 消费者出现，勿单独实施）。
