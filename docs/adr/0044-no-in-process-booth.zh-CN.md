# 0044 — 引擎不内置 booth

> **Languages:** [English](0044-no-in-process-booth.md)（主文档） · [中文](0044-no-in-process-booth.zh-CN.md)

**状态**：Accepted（2026-09-25 裁决，Phase 4.5/4.5a；2026-10-12 正式化为 ADR）

## 背景

`BoothType::simple` 曾允许 Rust 代码以进程内闭包（`Body::Rust`）的形态成为
booth，编译进宿主二进制。在该形态下，每个新服务都意味着重新打包引擎二进制，
Agent 应用每加一个功能都要重建 aura——平台塌缩成框架。Phase 4.5 / 4.5a
（2026-09-25 关闭）删除了公开构造器，把 cli demo 与全部 engine 测试迁到
script booth；本 ADR 把这项裁决记录为长期边界，使它不再只活在 PLAN 的
checklist 条目里。

## 决定

**引擎不内置任何 booth。**

- 框架机制（evictor 一类）是普通的 realm 逻辑，不是 booth——把它们包装成
  booth 是无意义的绕路。
- 用户的 Rust 代码只经进程外通道成为 booth：运行时上传的编译制品
  （wasm，ADR-0026 §4），或讲帧协议的原生子进程（bgi，ADR-0035）；远程
  effector 出站连接（ADR-0031）。存在哪些 carrier 是名册问题（PLAN 跟踪），
  不属于本边界。
- `Body::Rust` 保留为 enum variant、无公开构造器（framework-internal 备用；
  engine 测试以 struct literal 直构，属脚手架）。
- Booth 定义经上传生命周期住在数据面（Phase 4.5b：`set` 持久化定义 +
  内省元数据）——绝不进编译面。

## 后果

- 平台保持是引擎：加一个服务 = 运行时上传一个定义，不是重建引擎。
- 对编译型语言的作者，ctx 是**线上的契约**，不是进程内对象（ADR-0043
  §5/§6）：进程外 carrier 的"全局 ctx"是 shim 侧构造物，帧臂与可调用面
  1:1 镜像。任何地方都不存在 `static` 或模块级全局 ctx——进程内 ctx 是
  `Realm::ctx_for` 组装的 per-instance 投影（用户不可达，因为不存在进程内
  booth）；进程外它是帧词汇。
- 引擎自身的测试覆盖跑在 script booth 上（steel；慢 handler 用 nushell；
  python 走 feature 门）——与用户相同的通道，测试路径即真实 surface。
