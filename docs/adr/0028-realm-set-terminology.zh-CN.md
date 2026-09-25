# 0028 — Realm-set 术语：外层隔离轴由 namespace 改名为 realm

> **Languages:** [English](0028-realm-set-terminology.md) (primary) · [中文](0028-realm-set-terminology.zh-CN.md)

**Status:** Accepted (2026-09-25) — 命名裁决；代码改名是第一工作项，文档清扫随后

## Context

两个隔离轴共用一个词。okm 的键段（`#[kv_ns(N)]`，单引擎内的 2 字节表/类型编号）与
Phase 3.6 的外层空间（字符串命名、构造期前缀绑定的数据世界：`register_in(namespace, …)`、
`MqStore::namespaced`、`node { namespace … }`）都读作 "ns"/"namespace"——读 partitioning 或
4.10 材料时，每个句子到底指哪个轴都要绊一下。这个撞名不是表面问题：两轴回答的是不同的
问题（哪个键段属于哪张表 vs 哪些世界互不可见），住在不同的层（编译期声明的数字 vs 运行时
字符串）。

外层轴的准确名字其实已经在代码库里：外层空间**就是一个 `Realm`**——`Namespaces` 内部就是
`HashMap<String, SharedRealm>`，`realm_of(namespace)` 返回的正是该 namespace 独占的那一个
`Realm` 对象。映射是 1:1，所以 "namespace" 是一个已有精确名称之物的第二名字。

## Decision

1. **外层轴命名为 realm；「哪个 realm」这个轴命名为 realm-set。**
   - `Realm::namespace` 字符串 → **realm 名**；`EngineConfig.namespace` 字段 → `realm`。
   - 集合类型 `Namespaces` → `RealmSet`（避开 `Realms`——它读起来像运行时对象的已占用
     复数）；`NamespacedRealm` → `NamedRealm`（携带自己名字的 realm）。
   - `MqStore::namespaced(inner, ns)` → `MqStore::for_realm(inner, name)`；所有 `*_in`
     面的参数同此改名。
   - KDL：`node { namespace "x" }` → `node { realm "x" }`。不留兼容别名——字段年轻、
     部署即配置，而静默的双拼写恰恰是本 ADR 要消灭的漂移。
2. **okm 的键段保留 `ns`**（`#[kv_ns(N)]`、类型 ns、ns 42……）。改名后 "ns" 只指一个
   东西：2 字节键段。缩写撞名是被切断的，不是被掩盖的。
3. **"租户"不进词汇表。** 改名不把该轴绑到租户语义上——一个 realm 可以装一个租户的完整
   应用、一个应用、一个项目，或无隔离部署里唯一的那个 default。绑定维度依旧是应用的决定
   （4.10 降级裁决原样成立，主语换成 realm）。

## Honest semantic cost

- 运行时结构体 `Realm` vs 名字意义上的 `realm`：一个词两个用法。接受——两者 1:1，指代
  永不歧义（「realm `alice`」= 名为 alice 的那个 Realm 对象），文档需要区分时以散文点明
  （"realm 名"）。
- 改名触及公共配置键（`node { namespace }` → `node { realm }`），零已部署消费者需要迁移
  ——正确性是唯一的输入（0026 §3 的裁决），不是迁移成本。

## Consequences

- 代码（第一工作项，同一提交）：`crates/realm/src/namespace.rs` → `realm_set.rs`，§1 的
  各标识符，配置字段 + KDL 子节点，engine 管路，测试。
- 文档清扫：partitioning §3、storage.md 联邦行、actor-api、modeling、PLAN 的活段落、
  ADR-0026 §1（追加 dated update——决策档案不改写正文），以及 wiki 里描述该机制的
  stateless-agent 段落。
- PLAN 3.6 的历史条目记录落地当时的形态——按日志规则保持原样；4.10 降级段落是活文档，
  换成 realm 措辞。
- 引用 aura 面的跨仓散文（prism ADR）在 prism 下次触碰挂载点时随改。
