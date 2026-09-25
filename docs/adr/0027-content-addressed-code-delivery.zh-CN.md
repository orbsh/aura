# 0027 — 内容寻址的代码投递：Inline 形态退役，投递形态只剩一种

> **Languages:** [English](0027-content-addressed-code-delivery.md) (primary) · [中文](0027-content-addressed-code-delivery.zh-CN.md)

**Status:** Accepted (2026-09-25) — 设计裁决；实施未动，见 Consequences

## Context

远程调用以 `CodePayload::Inline { bytes }` 投递代码——整个函数源码随控制帧穿行，每次
调用都如此，且经过网关（prism 挂载 `/probe/<alias>` 之后，再多一跳）。协议里已有
`CodePayload::Link { url, version, expected_sha256 }`，probe 也已实现拉取 + 校验（不匹配
= 错误，绝不静默接受），但 aura 从不构造 Link：Link 今天是死臂，Inline 是唯一活着的形态。

三个事实说明 Inline 不是「不修边幅」而是方向错了：

- **控制面在搬数据面。** 控制面只递指令与小结果、大产物留在执行侧——Inline 是这条裁决
  的唯一例外。AI 生成的函数体恰恰是会变大的那类载荷。
- **每次调用都在重投不变的东西。** 驻留 session 只加载一次源码；逐调用重投只在冷启动
  （及逐出后）才必要——而这恰是内容寻址缓存的键所定义的范围。
- **定义即字节。** `ActorDef` 目前把完整 `source` 字符串内联持久化；代码的「版本」隐含在
  那个字符串里。哈希才是天然的版本令牌，而它目前根本未被存储。

## Decision

### 1. 一种载荷：`CodeRef { url, sha256 }`

`CodePayload` enum 删除；`ToolCall.code` 变为 `CodeRef { url, sha256 }`。一个选项就是
没有选项——Inline 形态是退役，不是弃用过渡。

- `version` 删除：内容哈希 URL 自带失效策略；给同一内容配人读标签没有消费者
  （Windmill 判据）。
- `sha256` 由帧断言，不从 URL 解析：CDN 可能重写路径，把完整性校验建在传输细节上等于
  把验证安错了边。

### 2. blob 是内容寻址、不可变的，归 meta 平面所有

`CodeBlob` 住在 `realm/src/meta.rs`，紧挨它服务的定义（ns 42，TypeName 40 / ActorDef 41
之后）。键 = 32 字节 sha256；值 = 字节本身。无名字、无版本、无外键——这行就是纯粹内容。
`mq.rs` 是事件队列域，对它没有所有权；共用 `MqStore` 引擎句柄是管路事实，不是归属理由。

`ActorDef.source: String` 改为 `code_sha256: [u8; 32]`（定宽，对键纪律友好）。upload
生命周期（`register_inner`）对源码做哈希、写入 blob、把哈希持久化进定义——版本事实从
「字符串就在这儿」变成「这个哈希处的字符串是它」。

代码不变的重复注册免费去重（同哈希 → 同一行）；代码变了则产生新哈希，新定义版本指向
它。不存在第二套版本计数器，也不该有：内容哈希就是版本身份。

### 3. 投递只发引用；probe 按需拉取、按哈希缓存

远程 dispatch 臂用定义的哈希加部署声明的前缀构造 `CodeRef { url, sha256 }` 并发帧——仅此
而已。probe 解析代码的路径与今天解析 Inline 字节完全一致：sha256 缓存命中 → 源码；未命中 →
GET URL、对照帧内断言的哈希校验（不匹配 = 错误，绝不静默）、入缓存、继续；解析出的源码喂给
既有的 `with_session` 冷启动加载。按哈希的缓存是可丢弃的热层：与驻留 session 同一合法性
等级——执行节点依然不持有任何需要恢复的东西。（不发明新的 loader 接缝：probe 的
resolve-then-load 路径已经存在，变的只是 resolver 的输入形态。）

URL 搭乘部署声明的前缀：`node {}` 配置块里的 `code_base_url`（KDL）。无默认值。只有挂载
远程类型的部署才需要这个值；缺失时在投递处报错误值——错误面，对纯场内部署连启动依赖都
不是。值指向 prism 的静态导出或真 CDN 前缀；aura 不猜、不代理。

### 4. 服务端点是 prism 的面；无鉴权、无 code ACL

`GET /code/{sha256}`——纯静态下载（ADR-0017 §8 的资产规则：静态字节，不携带事件语义），
由 prism 服务（resident 组件，同进程读 aura 的 meta 平面），`Cache-Control: immutable`，
内容只在自己的 `/{hash}` 路径下服务——URL 在构造上自验证。

哈希就是 capability：不可猜，且 URL 只出现在控制面签名的调用帧里。能读 CDN 的人看到的，
正是同样的窃听者在同一条线路上读 Inline 早已得到的——代码本身。机密性是部署选择（信任
边界内的私有静态源，或 signed URL + CDN cache-key 归一化，免得每次签名把缓存打穿）——并且
刻意不是新的 per-node code ACL：per-node 授权若成为真需求，是 ADR-0015 节点身份的延伸
（已登记节点方可拉取），不是再造一个授权之家。

## Honest semantic cost

- **远程投递多了一个依赖：一个可达的源。** prism 的 `/code` 导出落地前，远程部署必须
  自行在 blob 存储前放一个静态源。Inline 没有这个依赖。如实记录：远程路径今天唯一的活
  消费者是测试（测试内立一个本地 HTTP 源），所以没有生产形态被回归——但第一个真实部署
  需要的是端点，不是枚举臂。
- **冷启动远程执行变成两次往返（fetch + call）。** 字节离开控制帧、进入数据路径；冷启动
  为它付账——缓存命中（同一份代码的后续调用）把开销完全吸收。
- **定义不再自含字节。** 光一行 ActorDef 不足以复活一个 actor——blob 必须还在它的哈希下
  存在。接受：两者同住本节点存储，而无引用 blob 的 GC（若真需要）是一次「扫定义表」的
  清理，不是引用计数。

## Consequences

- **probe-protocol**：`CodePayload` 删除；`ToolCall.code: CodeRef { url, sha256 }`。
- **probe**：`fetch_link` 成为唯一路径（缓存按 sha256 键控；校验不变）；其余消费面无改动。
- **aura**：`meta.rs` 增 `CodeBlob`（ns 42）与 `ActorDef.code_sha256`；`register_inner`
  写哈希 + blob；远程 dispatch 臂按存储的哈希构造 `CodeRef`；`EngineConfig`/KDL 增
  `code_base_url`（无默认，投递处报错）；`PersistedActor.source` → `code_sha256`（接缝
  结构体随之）。boot reload 按哈希从本地 blob 重新注水字节。
- **prism PLAN**：`GET /code/{sha256}` 静态导出条目（Phase 1.8 旁）；signed URL +
  cache-key 归一化记为机密性选项，不是默认。
- **测试**：remote_probe e2e 增设测试内静态源，走真 fetch + 哈希缓存路径（这是一次升级：
  它锁定的线路形态从此就是生产形态）。
