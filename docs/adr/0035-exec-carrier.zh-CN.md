# 0035 — exec 载体：进程外摊位的两种形态（bgi 与 exec）

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

**裁决——进程外摊位是两种形态，各按血统命名。bgi（Booth Gateway
Interface——带帧、常驻）承载摊位模型；exec（裸一次性、【无协议】）
只承载 invoke——按定义无状态，正如它所承袭的 cgi/fpm 血统。**

### 1. bgi：常驻桥（摊位形态）

probe 每摊位实例 spawn 一个子进程并保持存活；帧经子进程的
stdin/stdout 流动。它映射到既有 `ResidentSession` 接缝，无需新运行时形态：
load = spawn + 握手，call = 请求帧入 / 应答帧出，驱逐 = 关 stdin + SIGKILL
升级。监管复用既有 sandbox 策略（bwrap）：子进程仅有的 fd 是两根管道——
除声明的凭据变量外不继承环境，网络只在 jail 授予时有。

最初诉求里的"类 CGI"是个值得点名的误称：CGI 是每次调用 spawn；让摊位
成立的是 FastCGI 形态——进程常驻，协议活在管道上。

### 2. exec：裸一次性（cgi 形态——根本没有协议）

同日修正（用户的 fcgi-vs-cgi 分析）：exec【不是】"bgi 去掉循环"——它也
不继承成帧。spawn、把整个请求作为一个 JSON 文档写进 stdin（`{"handler":
"<事件>", "args": <值>}`）、关闭（EOF 就是脚本开跑的号角）、读到 EOF
拿 stdout 作为结果值。一次性脚本什么都不用实现：没有循环、没有帧解析、
没有退出协议。php-fpm 的血统精确且是刻意的——调用之间什么都不幸存——
这恰是 SKILL 想要的（跑一次、返回值），也恰是 nushell 运行时真正能做到
的（它的 `open` 在写方 EOF 时交付）。

词汇跟随血统：**bgi**（§1）是带帧常驻形态——循环住在子进程里（作者
自写，或 probe 发布的垫片；就是 fcgi 适配 cgi 那一步）。**exec** 是裸
cgi——父侧的逐调用 spawn 本身就是适配器，而一个每请求重新拉起进程的
适配器带的就是 php-fpm 语义：按定义无状态，不是疏漏。写下这条以免有人
把无状态当 bug 提：exec 摊位上的 iterate 是点名设计的错误值，没有 ctx
通道可挂调用，也没有驻留可驱逐。

SKILL 维持 invoke-only 的既定降级：生成脚本不需要存储面、不需要驻留；
脚本本体不是摊位。

### 3. 线路：换行分隔的 JSON 帧，仅 bgi（CBOR 为规划中的优化）

bgi 形态用一套帧编码，一行一帧。JSON-lines 是【已落地】形态：任何
语言用自带解析器即可到达——进门没有库税，这正是本载体的全部目的。
长度前缀 CBOR 是规划中的载荷优化（wasm 载体已证明编组纪律；exec 行成帧
在快路径需要时升级——Windmill 判据，与 ADR-0034 的 pull 批量同一条规则）。

帧词汇（bgi 全双工；exec 没有帧词汇——一进一出各一个 JSON 文档，见 §2）：

```
{ "id": N, "kind": "call", "event": "<handler>", "args": <值> }            ← 入
{ "id": N, "kind": "iterate_start|iterate_next|iterate_dispose",
  "event": "<handler>", "op": "start|next|dispose",
  "args": <值>, "stream_id": "<sid>" }                                      ← 入
{ "result": <值> }                                                          → 出
{ "host": {"op": "<ctx-fn 名>", "args": <值>} }                             → 出（仅 bgi：摊位调主机）
{ "host_reply": {"ok": <值>} }                                              ← 入（仅 bgi）
```

`result` 只承载成功值——调用中途失败经父侧错误通道浮现（外层 Result
纪律，ADR-0012；信封的终止形态是 Phase 4.15 的合并，ADR-0036）。

### 4. 语言 = spawn 声明，不是载体

载体只有一个；每语言的入口是 probe registry 里的 spawn 声明：`nu` =
`["nu", "--no-config-file", "-c", ...]` 配 probe 自带的帧循环垫片；编译的
Rust 摊位 = `["./booth"]`，binary 对着成文的帧契约自己实现循环（probe
不为此发布 guest crate；契约就是 ABI，如 `aura_alloc` 之于 wasm）。注册
时的能力宣告不变——probe 的能力表加 spawn 条目，不是加一类新东西。

### 5. BGI——Booth Gateway Interface：给包装层一个名字

常驻桥契约（§3）有一处不对称边：父侧是帧循环，子进程作者那侧【应该】
只是 handler。中间的翻译层——每语言一个外层循环，把"处理这一行"变成
"派发这个事件、调这个 handler、流式返回这个信封"——值得像 CGI 赢得名字
那样赢得一个名字：**BGI，Booth Gateway Interface**。命名要紧，因为它是
可移植面：每语言一个 BGI，"任何能读一行的语言"才意味着"任何语言都能成
为摊位"——与 CGI 当年从"每家服务器各一套"变成通用 HTTP 故事是同一个动作。

BGI 是接口；§3 的行协议是线路。包装层的职责：打开通道、循环、解码帧、
按名派发到作者的 handler、编码应答。各语言的作者可见形态（设计注记——
记录在案，本 phase 不要求落地）：

```nushell
run_bgi {|e|
    match $e.kind {
        "call" => dispatch $e.event $e.args,
        ...
    }
}
```

```python
import aura_bgi

@aura_bgi.event("order.created")
def on_order(e):
    ...

aura_bgi.run()          # 外层循环；handler 永远看不见帧
```

Rust 摊位实现 trait（或 `main` 里调垫片循环）；bash 用 `read -r line`
分支。probe 不为此发布 guest crate、不设 guest SDK——线路就是契约
（§4 的规则），每个 BGI 包装层要么是发布的垫片（probe 资产，如 nushell
适配器），要么就是作者自己三行循环。名字存在的意义：让这些工件有同一个
东西可称作"……的适配器"。

BGI 不是什么：不是第二套协议（它包装既有行帧），不是 aura 侧机件
（realm 永远看不见它），也不是 exec 的必需——裸形态根本没有循环可包装；
它的适配器就是逐调用 spawn 本身。

### 6. nushell PTY 退役，带闸门——不留双轨

PTY 载体（NushellResident、bridge.nu、pump_quiet 及其回归锁）在以下条件
满足后删除：bgi 承载 `ctx_store_emit` 臂（`HostOp` 加 `store_emit`
变体——与进程内桥已提供的能力做线路对齐），且 nushell 往返测试
（echo.rs::nushell_store_emit_roundtrip 的形状）在其上通过。退役排在这个
闸门之后，因为 store-emit 往返是【今天活着的验收】——先删 PTY 会把测试
打红，并重开同一轮收尾本该关闭的双维护之门。每语言永远只有一种执行形态：
双轨仅存在于"已落地"与"闸门通过"之间。

### 7. 信任层级不变：exec 是受信姿态，wasm 保住不可信层级

exec 载体不削弱任何东西，因为它不替换任何沙箱：bwrap jail 是给宿主受信
代码的部署级隔离（与 PTY、嵌入载体同一姿态），wasm 仍是不可信交付代码
的唯一硬件隔离层级（ADR-0027 的取回-校验路径瞄准的就是它）。Rust 作者
写摊位 = exec binary；要跑陌生人的代码 = 编成 wasm。两个层级不是彼此的
替代品；wasm 的论据也不是"跨平台"——它的论据是 import 白名单的能力拒绝，
这是 fd 级成帧给不了的（子进程要么在 jail 里有能力要么没有；wasm 模块
在宿主接线之前【什么都没有】）。

## 诚实的语义代价

- **每次调用跨一次进程边界。** bgi 对进程内载体付一次管道往返；
  exec 付整次 spawn。定价权在消费方：热循环选嵌入载体，操作型与
  一次性工作选 exec。这是陈述，不是遮掩：exec 载体的存在不会让任何
  现在快的路径变快。
- **bgi 的 ctx 桥是帧协议，不是原生。** python 的 `ctx_store_emit` 是
  注册过的 builtin；bgi 子进程的是一条 `host` 帧和一次应答。更深的调试面，
  多一道序列化缝——进程隔离的标准价，每个 bgi 语言等价支付。
- **两种形态，不是一个协议带个开关。** 本 ADR 初稿的"B 是无循环的 A"
  在制作中即错，此处更正：bgi 带帧且常驻，exec 不带帧且逐调用；两者
  共享的只有 spawn 监管，不是交换本身。
- **nushell 到 bgi 要经一个通道适配器，不是走 stdin。** 探针实证：nu
  无法阻塞读非 TTY stdin（`input line` 报错），且它的 `open` 在写方 EOF
  时交付。用户的 mkfifo + `loop { open pipe | lines | each }` 形态按写方
  会话逐批流式派发正确（含内联 ctx 应答读的完整往返已验证），所以 nu 的
  BGI 包装层是双 fifo 适配器，不是重写协议；在该包装层落地前，nushell
  走 exec（SKILL 语义本来就合身）——PTY 退役闸门（§6）等的是包装层，
  不是反过来。
- **能力门控比 wasm 的 import 清单粗。** bwrap 授文件/网络范围，没有
  符号级概念。记为层级设计，不是待修的缺陷。

## 后果

- **probe：** bgi + exec 载体模块（spawn、帧循环、垫片 registry）；bwrap
  策略复用；§6 闸门通过后退役 nushell PTY 机件。
- **aura：** 帧协议复用既有操作词汇（ToolCall/HostOp）；`HostOp` 加
  `store_emit` 臂——进程内 nushell 桥与远程 stdio 桥之间的线路对齐缺口。
  注册里的语言字符串选择 spawn 声明。ADR-0011 的 ctx 边界不动——host fn
  没有增加，是一种传输长出来了。
- **gravity：** 全 Rust 摊位面 = bgi binary；SKILLs = exec（裸一次性）。
  provider 摊位（python，ADR-0034 §6）不受影响——它的生成器模式留在宿主
  跨 FFI 缝驱动的地方，那道缝谁也躲不掉。
- **okm：** 无——帧契约住 probe-protocol。
- **PLAN：** exec 载体新 phase；nushell PTY 删除是该 phase 的最后一项
  （闸门 §6），绝不单独成事。

## 相关记录

ADR-0034（bgi 帧所承载的信封与 host-op 词汇）、ADR-0031（对外行为归摊位
自己的代码——exec binary 是这段代码的最强形态）、ADR-0027（内容寻址
交付——exec 取回临时路径后跑一次）、ADR-0015/0016（节点信任与驻留计时
对 bgi 实例不变）、probe 归属裁决（probe 执行交付的代码；spawn 就是
执行）。
