# okm 键空间布局（ns / partition）

> **语言：** [English](ns-layout-en.md) · [中文](ns-layout.md)

一页汇总：aura 在共享 okm 实例的键空间里占了哪些位置。权威定义住在代码的
`#[ok_ns]` / `#[ok_partition]` 注解里（`crates/realm/src/mq.rs`、
`crates/realm/src/meta.rs`），本页按代码核对（2026-09-29）；改动注解时必须
同步本页与英文版。裁决依据：ADR-0026 §1（类型级真实 ns 分配）、ADR-0027
（代码内容寻址进 meta 平面）、ADR-0023/0024（preset reduce 与 key-field
group）。

## 1. 形状

- 一个 realm 的持久面 = 一个 okm 实例（`MqStore` 句柄：生产 = fjall
  keyspace，测试 = okm TestStore；`MqStore::fjall/mem`）。
- 键空间分三段：**框架固定表**（编译期声明，低位块）、**摊位类型 ns**
  （运行时分配，100 起）、**中间空段**（36–39、43–99，预留未来框架面，
  当前无人认领）。
- realm 命名空间隔离（Phase 3.6 机制）前缀整个引擎、压在 ns 之下——
  同一个 ns 号在不同 realm 前缀下互不相干（ADR-0026 §1 的正交声明）。

## 2. 框架低位块（编译期固定）

| ns | 表 | 键 | 用途 | 来源 |
|---|---|---|---|---|
| 30 | EventName | `id u32` | 事件名字典（`by_name` 文本索引；名字是运行时数据，事件不占真实 ns——开放词汇表的代理 id 形态） | mq.rs |
| 31 | MqData | `[event_id u32][part_id u64][time u64]` | 事件数据：一行一个 emit，N 订阅者 = N 游标；排序键是逻辑时间（ms，经 MqHead 分区单调，非墙钟） | mq.rs |
| 32 | MqCursor | `[event_id u32][part_id u64][booth_id u32]` | 订阅游标：最后消费的 seq；单调推进，绝不回卷 | mq.rs |
| 33 | BoothName | `id u32` | 订阅者身份字典（`by_name` 索引，同 EventName 形态） | mq.rs |
| 34 | MqHead | `[event_id u32][part_id u64]` | 分区写头：append 读它、赋 `max(now_ms, last+1)` 写回——O(1) 追加 + 跨发射器单调 | mq.rs |
| 35 | EventRoute | `[event_id u32][booth_id u32]` | 持久订阅注册表（`by_booth` 索引）：重启后路由存活，无需重新内省；通配订阅同行形态 | mq.rs |
| 40 | TypeName | `id u32` | 类型名字典 + `HighWater(id)` preset（全局组）——type_id 分配的水位线 | meta.rs |
| 41 | BoothDef | `type_id u32` | 摊位定义：每类型一行（name、language、`code_sha256` 内容地址、idle_ttl；introspected schema 走动态段 nTLV，非不透明文本） | meta.rs |
| 42 | CodeBlob | `sha256 [u8;32]` | 代码字节内容寻址（ADR-0027）：纯内容行，构造性不可变——同键异内容=哈希碰撞，不是状态 | meta.rs |

partition 注解：`MqData` = partition 1（按 event_id）、`MqCursor` =
partition 2（按 event_id）；`SINGLETON_PART = 0` 是保留分区号——无 key
字段的路由（通配与单例订阅）落它。

## 3. 摊位类型 ns（运行时分配）

- 基址 `BOOTH_NS_BASE = 100`（meta.rs）。注册一个类型 = `type_id` 取
  HighWater+1，真实 ns = `100 + id`——ns 与 id 单调同行，节点生命周期内
  永不复用、永不回收。
- 分配是 `register_type` 的副作用（类型注册表本来就发 id；ADR-0026 §1）。
- 每类型 ns 内：类型经 interface_schema 声明 collections；
  `StorePlan::from_schema` 把声明编译成路由表——collection 的 primary/
  dynamic 槽 + 声明索引 + 声明 reduce 的 slot 全部 `ns + slot` 编码（okm
  的 dict_id/dict_name 槽与 junction 基址 4096/8192/12288 住在每个 collection
  自己的 schema 常量里，跨 ns 复用同一布局——真实 ns 只是前缀）。
- 实例是类型 ns 内的 document：同类型跨实例聚合 = 对类型自身 ns 的普通
  scan/reduce（投影摊位只为跨类型预计算保留）。
- 类型间隔离是构造性的：ctx 的存储句柄在注册期绑定拥有类型的 ns，
  跨类型访问表达不出来（ADR-0026 §3）。

## 4. 不变量

1. **低位块永远编译期固定**：框架表加行加列不动 ns 号；新框架面从
   36–39/43–99 的空段取号，且必须进本表（中英双语）。
2. **摊位类型永不落进低位块**：`BOOTH_NS_BASE` 之上才允许运行时分配。
3. **事件名/分区名永不占真实 ns**：开放词汇走代理 id + 文本索引
   （EventName/BoothName 形态）；封闭词汇（booth 类型）才配真实 ns。
4. **ns 只增不减**：类型注销不回收 ns——复用键空间前缀等于把旧数据
   读成新数据，id 与 ns 永不复用是同一裁决。
5. **realm 前缀与 ns 正交**：前缀在引擎层之下（`MqStore` 构造期绑定，
   句柄逃不出 realm），ns 在前缀之上——两段寻址互不掺假。

## 5. 代码锚点

- `crates/realm/src/mq.rs` — 表 30–35、`SINGLETON_PART`、MqStore/MqEngine
- `crates/realm/src/meta.rs` — 表 40–42、`BOOTH_NS_BASE`、`resolve_type_id`（id+ns 分配的唯一事实源）
- `crates/realm/src/store_exec.rs` — `StorePlan::from_schema`（声明→路由表，slot 编码）
- ADR-0026 §1/§2（类型级 ns 与文档可见性）、ADR-0027（CodeBlob 的由来）、ADR-0014（事件队列形状）
