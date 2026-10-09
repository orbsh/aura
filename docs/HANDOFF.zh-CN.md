# HANDOFF —— 事件面落地之后（aura + prism）

> **语言：** 中文（自 2026-10-08 起本文为唯一版本；英文孪生 `HANDOFF.md` 已退役删除）

写于 2026-10-02；2026-10-08 随事件面内部布局落地（ADR-0041）更新、同日随 ADR-0042
裁决再次更新；**2026-10-09 随 Phase 4.13 + ADR-0042 落地（A1/A2 完成）更新**。范围：
**aura** 与 **prism** 各自还剩什么。其余可能受影响的兄弟项目在文末点名。

**已落地并已提交（工作树干净）：**

| 仓 | commit | 内容 |
|---|---|---|
| aura | `2d1fafb` | `feat(realm,engine,config,docs): land the event-plane terminal form (ADR-0038/0039/0040; Phases 4.17/4.18/4.19)` —— 34 个文件 |
| effector | `b56faa3` | `refactor(runtime): rename the ctx_skip_to_now steel stub to ctx_skip_to_head` —— 1 个文件 |
| aura | `74221de` | `feat(realm,docs): event-plane internal layout — instance-key vocabulary, issuer on MqData, one physical partition (ADR-0041)` |
| aura | `e3e28b7` | `docs(design): event-flow clarity pass — dedupe §7, repair §1/§6.1, PLAN session record` |
| aura | `b892792` / `0c11a8e` | `docs(adr,plan): cite the landing commits (A6)` + `docs(handoff): A6/A7 resolved`（`~/world/aura-base` 陈旧克隆删除，A7） |
| aura | `2b7ba4c` / `80bd272` | `docs(adr/0042): structural instance identity`（实例身份结构化裁决）+ PLAN 配对记录 |
| aura | `33ef84e` | **Phase 4.13 + ADR-0042 主体**：`InstanceId.key` → `aura_booth::InstanceKey` 枚举（哨兵退役）+ 路由终态 `RouteResolution { Singleton, Field, Scan }`，24 文件 |
| aura | `e3690ac` | 内省 `receives[event].resolve` 接线 + 扫描路由 e2e 扇出锁 |
| aura | `72fc208` / `e346af5` | 文档：event-flow §8.1/8.2 LANDED、§8.3 残余关闭、partitioning 终态、ADR-0042 状态行、PLAN 勾 4.13 + 会话记录；残差改定性（okm-dynamic 对齐项） |
| okm | `5b355bf` + `7da4b26` | `feat(okm-dynamic): variable-width trailing index field`（`AccessMethod` 对齐 derive 规则）+ `encode_fields` 接受 cold 字段——aura §8.1 残差的前提 |
| aura | `c9425e5` | 扫描路由探针改用 Str 索引 + 字符串探针，event-flow §8.1 残差关闭（**依赖 okm 上述提交推送到 orbsh/okm 后生效**——本仓 aura 的 okm 依赖是 git 分支） |

裁决文本：`aura/docs/adr/0038-event-plane-identity.md`、
`0039-partition-encoding-and-retention.md`、`0040-keyspace-bands.md`、
`0041-event-plane-internal-layout.md`、`0042-structural-instance-identity.md`
（各带 `.zh-CN.md` 孪生）。键空间权威：
`aura/docs/design/event-flow.md` §7（键与值的完整布局见 §2 各步的落点行）；
路由终态与残差关闭记录在 §8.1/§8.2。会话记录：`aura/docs/PLAN.md` →
「会话记录（2026-10-02b）」「会话记录（2026-10-08）」「会话记录（2026-10-08b）」
与「会话记录（2026-10-09）」。

**2026-10-02 之后落地（ADR-0041）——下文列表里早于它的名字以新词汇为准**：切片段就是
instance key（`part_id` → `instance_key_id`、`PartitionName` → `InstanceKeyRegistry`、
`mq::Partition` → `mq::InstanceKey`、`SINGLETON_PART` → `SINGLETON_KEY_ID`、
`bound_partition` → `bound_instance_key`）；**MqHead（ns 24）已撤销**——写头改由 MqData
的 `HighWater(seq)` reduce 承载，ns 24 腾出（事件面在用表：20、21、22、23、25）；
`#[ok_partition(2)]` 从 MqCursor 删除（MqData 的 partition 1 是唯一物理分区）；事件面
文档分「契约 / 内部布局」两层。同一窗口 okm `MODELING` 新增两节（开放词汇代理 id 注册表；
读一个 reduce；commit `559d031`）、trigger 加入事件消费者拆分（`d3f4fde`）、trigger 降格为
「归属」而非第三种机制（`52cdcbe`）、两类消费者补工作示例（`69041f2`）。

**2026-10-08 晚些（ADR-0042）——A2 已裁决**：`InstanceId.key` 将随 Phase 4.13 变为枚举
`aura_booth::InstanceKey { Singleton, Named(String) }`，哨兵 `__singleton__` 从框架退役
（字面别名构造性不可达；状态面核实不受影响，无存储迁移；effector 唯一可观察缝 = 会话键格式）。
A1 与 A2 现在是**同一次落地**——都改 EventRoute 行的解析契约。

## 已落地的内容（免得下一个人重新推导）

- **身份与投递（4.17）**：无 key（通配）订阅投递给该类型的**单例实例**；事件面不再自留订阅者
  字典——路由与游标都键在 meta 面 BoothName 的 id 上（ns 30），所以游标键第三段是 `booth_id`，
  `split_once('/')` 消失。`__default__` 兜底退役：payload 缺声明的 key 字段 = 畸形事件 →
  带 `DeadReason::MissingKeyField` 落 dead ring。
- **分区身份与分段（4.18）**：`PartitionName`（ns 21）是代理字典（`by_name` + `HighWater` +
  反解析）；FNV-1a 哈希族已删；`mq::Partition`（`Singleton | Named`）以结构标记无 key 投递，
  `SINGLETON_PART = 0` 是发号不可达。框架低位块改号：事件面 **20–25**、meta 面 **30–32**
  （ADR-0040）；键宽收紧（MqData 20→16 B、MqCursor 16→12 B、MqHead 12→8 B）。
- **保留承诺（4.19）**：一个全局 `cursor_ttl`（KDL `mq { cursor_ttl "30d" }`，默认 30 天，
  与 `idle_ttl` 解耦）。过期 = 该行退出压缩分母，**绝不删行**；只有已低于水位的行才回收
  （`drop_cursor`）。`MqCursor.last_active_ms` 是墙钟输入。
- **落地时发现的两处修正**：分母判据是 `mq::booth_subscribes`（**匹配**精确或通配的路由）
  ——原先的精确 event_id 查表会把通配订阅者静默排除出分母，让压缩吃掉它们的积压；MqData 的
  第三段是**纯序列**（`seq` = `last + 1`，`MqHead.last_seq`）——**不是时间戳**。唯一性靠
  「emit 路径持 realm 锁（单写者）」，不靠时钟。
- **改名**：`type_id` → `booth_id`、`by_type` → `by_booth`、`type_subscribes` →
  `booth_subscribes`、`type_id_of` → `booth_id_of`、`mq::skip_to_now` → `mq::skip_to_head`；
  脚本面 host fn 一并改名（`ctx_skip_to_now` → `ctx_skip_to_head`），**aura 与 effector 的 steel
  stub 列表两边同改**。零调用者的转发 `mq::booth_name_of` 已删。

## AURA —— 待办

**A1. 已完成（2026-10-09，Phase 4.13；commits `33ef84e`/`e3690ac`/`c9425e5`）。** 路由终态
已落地：EventRoute 行携带 `RouteResolution { Singleton, Field, Scan }`（`resolution: u8`
判别 + collection/index/probe_field 名字列，存名不存号）；`on_resolve` 声明面 + 内省
`receives[event].resolve` 接线（无 resolve 无 key = 单例，无类型默认）；求值 =
`store_exec::resolve_scan_targets`（4.16 面）；扫描路由的集合主键裁为单 key 字段，
索引支持末位变宽（Str）字段（依赖 okm `5b355bf`/`7da4b26`）。e2e 锁：
`engine/tests/events.rs::scan_route_fans_out_to_the_hit_rows`。机制记录：
event-flow.md §8.1/§8.2。

**A2. 已完成（2026-10-09，随 A1 同批落地，ADR-0042）。** `InstanceId.key` =
`aura_booth::InstanceKey { Singleton, Named(String) }`；哨兵 `mq::SINGLETON` 退役；
`instance_of` 字符串往返删除；会话键/ctx 渲染 = `Display`（单例 = 空串）；effector wire
字符串经 `InstanceKey::parse` 进 variant（空串 = Singleton）。无存储迁移。

**A3. 既有部署的运维动作（ADR-0040，ADR-0041 扩充）。** 既有库存必须**清除低位块**：新
meta 段（30–32）正落在旧事件面用过的号上，不清就会让新的 `BoothName` 把旧的 `EventName`
行解码成摊位名（「把旧数据读成新数据」）。代价比「mq 字节转瞬即逝」大一圈：已持久化的摊位
定义与代码 blob 一并作废，部署方要重新注册类型。ADR-0041 的改名、MqHead 撤销与 MqCursor
去分区由**同一次清除**吸收——没有额外迁移步骤。新库没有可清除的东西——本仓测试每次构建
全新的库。

**A4. effector USAGE 双语帧形同步（4.16c 遗留）。** 本次未动：effector 的文档里没有引用被改名的
host fn，所以「USAGE 同步」的范围需要先定下来（它可能是 4.16c 落地的那批帧形的纯文档扫尾）。

**A5. python 载体在本机无法验证（仍未变）。** `cargo test --features python` 在 link
`okm-python` 时失败（pyo3 0.25.1 不支持本机 Python 3.14.7）。这是**环境问题，不是代码
缺陷**——但它意味着 **Phase 4.13/ADR-0042 这批改动之后 python 绑定面同样没有被复验**
（`InstanceId.key` 枚举化触及 `aura_booth`，python 绑定若直接构造 `InstanceId` 会编译
失败），且任何写着 `--workspace --features python` 的门（PLAN 的 4.16/4.15 闸门、prism
的 default features）都需要一台 Python ≤ 3.13 的机器。

**A6. 已解决（2026-10-08，commit `b892792`）。** ADR-0038/0039/0040 的状态行与 PLAN
条目已改为引用落地提交（`2d1fafb`；ADR-0041 引用 `74221de`）。

**A7. 已解决（2026-10-08）：`~/world/aura-base` 已删除。** 它是一个完整克隆（自带 `.git`，
origin = orbsh/aura），HEAD `3b30466` 是当前 HEAD 的祖先，没有独有内容。取失败基线用的
临时 worktree `aura-baseline` 也已删除。

## PRISM —— 待办

**P1. nushell feature 清理（它挡住其余一切）。** prism 是本机唯一 path 依赖 aura 的项目
（`aura-booth`、`aura-engine`、`aura-realm`）。它现在**编不过**，原因早于本次改动：

```
package `prism` depends on `aura-engine` with feature `nushell`
but `aura-engine` does not have that feature
```

PTY/nushell 已在 Phase 4.15 退役（ADR-0035 §6）——nu 现在骑 bgi 载体的 fifo 适配器，**不需要
任何 feature gate**（已在 effector 确认：`runtime/src/carrier/exec.rs`）。三处要改：

1. `crates/prism/Cargo.toml` —— 删掉 `nushell = ["aura-engine/nushell"]` 这一行，并从
   `default = [...]` 里去掉 `"nushell"`。
2. `crates/prism/src/booths.rs` —— 去掉 nu echo 摊位上的 `#[cfg(feature = "nushell")]`：
   摊位要**无条件保留**（语言仍然可用；删掉它是丢功能，不是清理）。
3. `crates/prism/tests/echo_e2e.rs` —— 同样去掉 nu 测试上的 cfg，以及约第 181 行那个列表。

然后构建。prism 自 2026-09-29 起没编过，所以预期还有别的漂移；真正的验收是**第一次成功的
`cargo check`**，不是上面这三处编辑。

**P2. `python` 在 prism 的 default features 里**（`default = ["steel", "python", "nushell",
"wasmtime", "fjall"]`）。在本机上这与 A5 是同一个 pyo3/Python-3.14 原因，让默认构建不可能。
要么把 `python` 从 `default` 里去掉（保留为 opt-in），要么钉一个 ≤3.13 的解释器；否则本地检查
永远得写 `--no-default-features --features steel,wasmtime,fjall`。

**P3. 编译通过之后（prism 自己的 PLAN，`prism/docs/PLAN.md`）。** Phase 1（WS 网关：鉴权 +
解析 + 把 turn 提交为 realm 事件、经事件订阅回流）是部分落地——身份那一半 LANDED
2026-09-25，Phase 1.9（静态代码导出）LANDED 2026-09-26；把 Phase 1 收尾，然后 Phase 2
（CLI 包同一套 WS 协议——不开第二个 RPC 面）。Phase 1.5 是无代码的范围注记。

**P4. 对着新 aura 测试前先处理 dev store。** 如果 prism 有既有的 aura 存储目录，先清低位块
（见 A3），或者把测试指到一个全新的目录——否则第一次运行就会把旧行读成新表。

**P5. 2026-10-02 核实的非问题 → 2026-10-09 复核后有一条升级为实活。** prism 只碰
`aura_realm::meta::{code_hash, code_hex, get_blob}`、`aura_realm::mq::MqStore`、
`aura_booth::{BoothType, InstanceId, call::Waited}` 与 `aura_engine::Engine`。ADR-0042
已把 `InstanceId.key` 改为 `aura_booth::InstanceKey` 枚举——prism 若直接构造
`InstanceId { key: "...".into() }` 字面量就会编译失败，要改成
`InstanceKey::Named(...)`/`Singleton`（MqStore 类型名那批 ADR-0041 改名无构造面，
预计不受影响）。第一次 `cargo check` 会给出答案。prism 里也没有任何脚本调用
`ctx_queue_depth`/`ctx_skip_to_*`（对 `~/world` 的全仓 grep 显示 aura/effector 之外没有
调用点），host-fn 集合未动。**另外：prism 编过前先确认 aura 的 okm git 依赖已拿到
okm `5b355bf`（变宽索引字段）——okm 两个提交尚未推送时，aura 本仓的扫描路由测试需要
临时 `[patch]` 指本地 okm（已验证可用，勿提交）。**

## 会反复咬人的跨仓规则

- **host fn 契约住在两个文件里。** aura 的 ctx bridge（`crates/realm/src/ctx.rs`）与 effector 的
  steel introspection stub（`crates/runtime/src/carrier/steel.rs`）必须给出**同一集合**——那个
  stub 列表决定 steel 脚本能否在**加载期**解析 `ctx_*` 标识符。当前集合：`ctx_invoke`、
  `ctx_store_emit`、`ctx_interface_schema`、`ctx_queue_depth`、`ctx_skip_to_head`、
  `ctx_timer_*`、`ctx_iter_*`。只改一边 = 脚本加载失败。
- **号位永不复用。** 事件面在用表：20、21、22、23、25（ns 24 由 ADR-0041 腾出）、meta 面
  30–32、摊位数据 ns 从 100 起；旧的 `30–35`/`40–42` 整段永久作废。
- **今天只有 prism 依赖 aura。** gravity 目前只有文档（它的 Phase 0 不需要 aura 依赖；
  Milestone B 的 Phase 4「Booth binding」才是开始使用契约的地方）；k10r/krystallizer、mudra、
  fluxora、klaw 都没有 aura/effector 依赖（只用 okm），不受影响。

## 验证配方

```sh
# aura —— 应当全绿，除了 exec_booth::bgi_booth_ctx_invoke_to_sibling（那是**预先存在**的：
# 已用 git archive 导出的 HEAD 只读副本复现同样的失败；2026-10-09 复跑依旧）。
# 注意：okm `5b355bf`/`7da4b26`（变宽索引字段）推送前，扫描路由测试需要临时
# [patch."https://github.com/orbsh/okm"] 指向 ~/world/okm（勿提交）。
cd ~/world/aura
cargo check -p aura-realm -p aura-engine -p aura-config --all-targets
cargo test --workspace --no-fail-fast          # 67 过 1 挂（上述既有失败）
cargo clippy --workspace --all-targets          # 只剩 proc-macro-error2 的未来兼容提示
cargo test -p aura-engine --test events scan_route   # 扫描路由 e2e（4.13 的锁）

# okm —— 变宽索引字段的门。
cd ~/world/okm
cargo test -p okm-core -p okm-dynamic --features test-engines,parquet   # 187 过 0 挂

# effector —— stub 列表自己的测试，加上 exec/bgi 载体。
cd ~/world/effector
cargo test -p effector-runtime --test steel_introspect

# prism —— P1 做完之后；因 A5 排除 python。
cd ~/world/prism
cargo check -p prism --no-default-features --features steel,wasmtime,fjall

# 改名的 host fn 的跨仓端到端证明（aura 脚本 -> effector steel stub -> aura 宿主桥）：今天通过。
cd ~/world/aura && cargo test -p aura-engine --test queue_relief
```