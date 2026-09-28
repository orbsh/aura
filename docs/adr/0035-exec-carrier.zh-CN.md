# 0035 — exec 载体：进程外摊位的两种模式

> **语言：** [English](0035-exec-carrier.md)（主文档） · [中文](0035-exec-carrier.zh-CN.md)

**状态：** Accepted（2026-09-28）。设计裁决；实施未动，见后果。动机是
gravity 的全 Rust 诉求（wasm 限制感觉多余）与 nushell PTY 载体的维护成本
（四常驻载体里机件最脆的一个）。由用户提出："probe 增加类 CGI 模式——
信息走管道，不走环境变量"。

## 背景

常驻载体是三个嵌入器加一个包装器：steel（进程内 VM）、python（进程内
CPython）、wasmtime（进程内编译）、nushell（PTY 驱动的 REPL）。语言允许
嵌入的，就嵌入。nushell 的入口是证明规则的例外：REPL 被靠光标查询应答、
提示符静默排空、文件 req/resp 轮询哄成 RPC 服务——这套机件是在【模仿】
常驻 session，而不是在【运行】常驻 session。

与此同时，被需要的能力比任何载体都简单：跑一个可执行文件。Rust 摊位
binary 不该被迫穿上它没要的 wasm 马甲；AI 生成的 SKILL（动态脚本、跑一次、
返回）也不该被迫搭上它没要的 REPL。且按 ADR-0031，摊位的对外行为归摊位
自己的代码——进程外 binary 是这段代码的最强形态，没有需要保持诚实的
主机侧能力面。

本 ADR 要定的问题：什么形态让直接执行成为载体，而【不】溶解进程内载体
提供的摊位模型（状态、iterate、ctx 调用、驻留计时）。

## 决策

**裁决——exec 载体是一套机制、两种模式。模式 A（常驻桥）承载摊位模型；
模式 B（一次性）只承载 invoke。**

### 1. 模式 A：常驻桥（摊位模式）

probe 每摊位实例 spawn 一个子进程并保持存活；帧经子进程的
stdin/stdout 流动。它映射到既有 `ResidentSession` 接缝，无需新运行时形态：
load = spawn + 握手，call = 请求帧入 / 应答帧出，驱逐 = 关 stdin + SIGKILL
升级。监管复用既有 sandbox 策略（bwrap）：子进程仅有的 fd 是两根管道——
除声明的凭据变量外不继承环境，网络只在 jail 授予时有。

最初诉求里的"类 CGI"是个值得点名的误称：CGI 是每次调用 spawn；让摊位
成立的是 FastCGI 形态——进程常驻，协议活在管道上。

### 2. 模式 B：一次性（SKILL 模式）

每次调用 spawn：参数从 stdin 进，结果到 stdout 出，退出。这就是模式 A
去掉循环——帧流的生产者在 EOF 处离场，因此没有常驻 session、没有
iterate、没有 ctx 调用。它正是 AI 生成 SKILL 的形状（跑一次、返回一个值），
且严格简单于它替换的 PTY 路径：今天的 PTY 包装本来就在物化一次性调用
文件；模式 B 就是那个形状，只是不用再和 REPL 共享进程。

SKILL 维持 invoke-only 的既定降级：生成脚本不需要存储面、不需要驻留；
脚本本体不是摊位。

### 3. 线路：长度前缀 CBOR 帧；JSON 行作调试形态

两种模式共用一套帧编码。CBOR 是本币（二进制安全，wasm 载体已证明编组
纪律）；JSON-lines 是成文调试格式，任何语言不装 CBOR 库也够得着——同
ResidentSession 边界的"JSON 只在接缝"规则。成帧：4 字节大端长度前缀，
随后 CBOR 文档。

帧词汇（A 模式全双工；B 模式只用 Request/Response 一对）：

```
{ "t": "call", "id": "...", "event": "<handler>", "args": <值> }   ← 入
{ "t": "result", "id": "...", "ok": <值> }                          → 出
{ "t": "host", "id": "...", "op": <HostOp> }                       → 出（仅 A：摊位调主机）
{ "t": "host_reply", "id": "...", "ok": <值> }                     ← 入（仅 A）
```

模式 A 的 host 调用骑与网络桥相同的 `HostOp` 词汇表（invoke、iterate）。
`ctx_store_emit`【尚不是】网络臂——进程内 nushell 桥经 host-fn 表到达它，
模式 A 需要给 `HostOp` 加显式 `store_emit` 变体（记入后果：本载体闭合的
线路对齐缺口）。stdio 是既有操作的传输层，不是新表面。iterate 信封
（ADR-0034）是 CBOR 可编码 schema：原样过界，帧协议继承其类型规则
（`done` 是字段，绝不是哨兵）。

### 4. 语言 = spawn 声明，不是载体

载体只有一个；每语言的入口是 probe registry 里的 spawn 声明：`nu` =
`["nu", "--no-config-file", "-c", ...]` 配 probe 自带的帧循环垫片；编译的
Rust 摊位 = `["./booth"]`，binary 对着成文的帧契约自己实现循环（probe
不为此发布 guest crate；契约就是 ABI，如 `aura_alloc` 之于 wasm）。注册
时的能力宣告不变——probe 的能力表加 spawn 条目，不是加一类新东西。

### 5. nushell PTY 退役，带闸门——不留双轨

PTY 载体（NushellResident、bridge.nu、pump_quiet 及其回归锁）在以下条件
满足后删除：exec 模式 A 承载 `ctx_store_emit` 臂（`HostOp` 加 `store_emit`
变体——与进程内桥已提供的能力做线路对齐），且 nushell 往返测试
（echo.rs::nushell_store_emit_roundtrip 的形状）在其上通过。退役排在这个
闸门之后，因为 store-emit 往返是【今天活着的验收】——先删 PTY 会把测试
打红，并重开同一轮收尾本该关闭的双维护之门。每语言永远只有一种执行形态：
双轨仅存在于"已落地"与"闸门通过"之间。

### 6. 信任层级不变：exec 是受信姿态，wasm 保住不可信层级

exec 载体不削弱任何东西，因为它不替换任何沙箱：bwrap jail 是给宿主受信
代码的部署级隔离（与 PTY、嵌入载体同一姿态），wasm 仍是不可信交付代码
的唯一硬件隔离层级（ADR-0027 的取回-校验路径瞄准的就是它）。Rust 作者
写摊位 = exec binary；要跑陌生人的代码 = 编成 wasm。两个层级不是彼此的
替代品；wasm 的论据也不是"跨平台"——它的论据是 import 白名单的能力拒绝，
这是 fd 级成帧给不了的（子进程要么在 jail 里有能力要么没有；wasm 模块
在宿主接线之前【什么都没有】）。

## 诚实的语义代价

- **每次调用跨一次进程边界。** 模式 A 对进程内载体付一次管道往返；
  模式 B 付整次 spawn。定价权在消费方：热循环选嵌入载体，操作型与
  一次性工作选 exec。这是陈述，不是遮掩：exec 载体的存在不会让任何
  现在快的路径变快。
- **A 模式的 ctx 桥是帧协议，不是原生。** python 的 `ctx_store_emit` 是
  注册过的 builtin；exec 的是一条 `host` 帧和一次应答。更深的调试面，
  多一道序列化缝——进程隔离的标准价，每个 exec 语言等价支付。
- **两种执行形态要当一个载体来写文档。** A/B 除循环外共享一切；文档必须
  说"B 是无循环的 A"，不许长出两套载体叙事。
- **能力门控比 wasm 的 import 清单粗。** bwrap 授文件/网络范围，没有
  符号级概念。记为层级设计，不是待修的缺陷。

## 后果

- **probe：** exec 载体模块（spawn、帧循环、垫片 registry）；bwrap 策略
  复用；§5 闸门通过后退役 nushell PTY 机件。
- **aura：** 帧协议复用既有操作词汇（ToolCall/HostOp）；`HostOp` 加
  `store_emit` 臂——进程内 nushell 桥与远程 stdio 桥之间的线路对齐缺口。
  注册里的语言字符串选择 spawn 声明。ADR-0011 的 ctx 边界不动——host fn
  没有增加，是一种传输长出来了。
- **gravity：** 全 Rust 摊位面 = exec 模式 A binary；SKILLs = 模式 B。
  provider 摊位（python，ADR-0034 §6）不受影响——它的生成器模式留在宿主
  跨 FFI 缝驱动的地方，那道缝谁也躲不掉。
- **okm：** 无——帧契约住 probe-protocol。
- **PLAN：** exec 载体新 phase；nushell PTY 删除是该 phase 的最后一项
  （闸门 §5），绝不单独成事。

## 相关记录

ADR-0034（帧所承载的信封与 host-op 词汇）、ADR-0031（对外行为归摊位
自己的代码——exec binary 是这段代码的最强形态）、ADR-0027（内容寻址
交付——模式 B 取回临时路径后执行）、ADR-0015/0016（节点信任与驻留计时
对 A 模式实例不变）、probe 归属裁决（probe 执行交付的代码；spawn 就是
执行）。
