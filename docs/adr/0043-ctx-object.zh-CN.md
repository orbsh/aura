# 0043 — ctx 对象：唯一的执行上下文（整体取代 ADR-0011 的安置制，破坏式）

> **Languages:** [English](0043-ctx-object.md)（主文档） · [中文](0043-ctx-object.zh-CN.md)

**状态**：Accepted（2026-10-10）——**改名波已落地（2026-10-10）**
（aura `8a2b85b` + effector `fdb9507`/`1c6fbdc`：分组桥 key、
python/steel 的 ctx 对象注入、bgi 帧臂、wasm 在 aura_host 下保持平铺
ABI）。python 注入被迫做成顺序无关：`store` 同时是叶子和 `store.frame`
的分组——第一趟物化分组（双角色前缀成为可调用的 AuraCtxNode），第二趟
绑叶子。仍未落地：`ctx.emit`、`ctx.on`（§3）、payload 上 ctx（§4）、
bgi 的 emit 臂。落地过程暴露了那个预先存在的 `bgi_booth_invoke_to_sibling`
失败的真实原因（默认构建不含 python 载体 + 夹具把 host_reply 错误吞成
Null）——已加 feature 门 + 错误上抛，`--features python` 下转绿。
**emit 波已落地（2026-10-10）**（aura `4635a60` + effector `badcccd`）：
Weak-realm 发布闭包在 ctx_for 注入（emitter=类型名）；bgi 的 `Emit` 臂
按契约**不应答**（fire-and-forget 是方法语义；WS 跳仍保留帧级应答配对
——配对是协议、不是业务）；nu 垫片 helper；dead-ring 边界经活体验收。
仍未落地：`ctx.on`（§3，含生命周期钩子折叠）、Rust
`Fn(Ctx,Value)`→`Fn(&Ctx,Value)`。
**形态纠正（2026-10-10）**：ctx 是装载期绑定一次的全局命名空间对象，不
是 handler 参数——此条撤回了最初的 §4（payload 上 ctx、单参 handler），
在该半落地之前；payload 走 handler 参数位，Rust 收 `&Ctx`。
整体取代 ADR-0011 的安置决定；保留其入口原生（return/失败）规则与对
"运行时动态订阅"的否决（重述为相位门），但据以准入的判据——实例身份 +
宿主治理（后被重读为"方向"）——由 §1 的提供者分界取代。用户裁决
（2026-10-10）：**全换、不兼容**——平铺 `ctx_*` 名字整体退役，不留别名。

## 背景

ADR-0011 用两条判据（绑实例身份 + 受宿主治理）决定能力能否上 ctx。这条
判据实际保护的两个不变量是真的——订阅事实在部署期可知（路由注册表、
`ctx_queue_depth`、积压治理全靠它）、每个能力有唯一治理点——但它把不变量
和实现的路径依赖混在了一起：摊位可见面被拆到**四套安置机制**上（注入的
平铺 `ctx_*` 函数、脚本级裸函数（emit/on 的裁决归宿）、脚本导出
（interface_schema、on_sleep/on_wake）、语言原生语法糖（装饰器）。判据
超载的证据就是我们建 mudra 宿主时踩的同名坑：`ctx_store_emit`（一条存储
指令）和从未落地的脚本级 `emit`（事件发布）共享一个动词——名字住在哪由
安置决定，不由语义决定。而整个能力面上唯一的结构性缺口（脚本 `emit` 在
0011 裁决了、任何 carrier 都没注入）恰恰源于它没有一个自然的 ctx 归宿。

重定义视角：`ctx` 不是"实例身份对象"——它是**宿主提供给脚本的一切名
字所在的全局命名空间**，装载期绑定一次（gRPC `context.Context` 的正确
读法：上下文对象是 ambient 的，请求数据走参数）。在这个读法下，身份绑
定不是准入考试，只是个别成员的可选内容；真正的成员资格规则是**提供者**
（宿主给脚本的名字住 ctx；脚本给宿主的定义住模块命名空间，只经注册原语
与 ctx 相遇）。

## 决策

### 1. 边界：宿主提供名字的全局命名空间

**ctx 是 carrier 在装载期绑定一次的全局对象——宿主提供给脚本的一切名
字的命名空间。** 成员资格规则取代 0011 的两腿判据：**宿主提供给脚本的
名字住 ctx；脚本提供给宿主的定义（handler——含生命周期 handler——以及
显式 `interface_schema`）住模块命名空间，只经注册原语（`@on` /
`ctx.on`）与 ctx 相遇。** 这就是"散装函数/对象收进统一命名空间"那一
步：一处可查（python 的 `dir(ctx)` 反射完整 API 面），没有散落的全局名。

ctx 是命名空间，不是调用方传入的对象：它不作为 handler 签名里要转发
的数据出现。host↔脚本的分界按**提供者**划，不按消息方向划——`on_` 前
缀的坑正是这个混淆：`ctx.on(...)` 是宿主名字（动词"注册"）；
`on_sleep`/`on_wake` 是脚本 handler（名词"睡着时跑什么"），即定义——所
以 §3 把生命周期钩子折叠进 `ctx.on`（保留事件名），而不是给它们 ctx 槽位。

两样东西留在 ctx 外，理由相同（都是每投递的入口数据，不是宿主提供的名
字）：**handler 的 `return`** 在 carrier 处填应答信封（`ctx.return` 永
不存在），**payload** 走 handler 的参数位（§4）。入口失败保持原生（语
言异常→外层错误值），与 0011 一致。`set(lang, script)`——今天的
`register(BoothType::script(name, lang, source))`——属部署面，执行中的
摊位永远看不见它。

### 2. 能力面（一个全局对象，方法分组）

装载期绑定一次（随会话存活）；handler 以裸名 `ctx` 引用它，不经签名：

```
ctx.self                      # InstanceId（结构键：Singleton|Named）
ctx.invoke(target, handler, args) -> Value          # 唯一受控调用面
ctx.iterate(target, handler, args) -> Cursor        # start/pull/dispose
ctx.store(op) -> Value        # 一条 okm Collection 指令作为数据（ADR-0026 §3）
ctx.emit(event, data)         # 发布进 MQ 平面（已落地，emit 波）
ctx.timer.register(at_ms, tag) -> id                # ADR-0016 §3b
ctx.timer.cancel(id)          # 幂等
ctx.queue.depth(event)        # 积压点读
ctx.queue.skip_to_head(event) # 泄压阀（`to_head` 是语义不是赘词：方向性丢弃）
ctx.schema                    # 持久化 interface_schema 的副本，作为数据（反射）
ctx.on(event, handler)        # 相位门控：仅 load/activation 期（§3）
ctx.receives                  # activation 注册表，可读（反射："我绑了什么"——§3）
```

平铺名字退役后 store/emit 的同名冲突随之消失：存储是 `ctx.store(op)`
（方法组），发布是 `ctx.emit(event, data)`——动词跟着语义走，不跟安置走。
整面的命名规则：进入 ctx 命名空间后，成员取**短且唯一可解析**的名字——
`interface_schema` 的长度是平铺命名空间的税（裸全局环境里必须自证身份），
`ctx.schema` 即退税。（核对过予以保留的例外：`ctx.queue.skip_to_head`——
`to_head` 承载方向性丢弃的语义（游标单调、跳过的积压不复活），不是赘词。）

`ctx.emit` 的 emitter 字段：宿主闭包捕获**类型名**（绝不捕实例 id——
发布与实例无关是 0011 自己的观察；审计线仍读得出发布者是谁）。

### 3. `ctx.on`：唯一注册原语，activation 门控，生命周期折叠

0011 否决动态订阅，否决保持——但重述得更锋利：**`ctx.on` 仅在
load/activation 期合法；在消息 handler 内调用=错误值**（相位门与系统既
有的"无绑定=错误值，从不静默"纪律同形）。这不是一条与内省竞争的注册新
通道：装配出的注册表**就是** `interface_schema.receives` 数据——唯一持
久化契约源（路由注册表、depth/skip 治理、mudra 的 hello-schema 全读
它）。装饰器（`@on`）是 **lowering 到** `ctx.on` 的语法糖；静态语言
（Rust 摊位、bgi binary）直接写 `ctx.on` 作为低层形态。注册 = activation
期把入口执行一遍（carrier 为收集装饰器绑定本来就要做这件事），不再另做
静态扫描。同一相位期注册表可读，即 **`ctx.receives`**——handler 想反射
"我绑了什么"就问这个全局对象；handler 期调 `ctx.on` 失败，handler 期读
`ctx.receives` 答出已冻结的注册表（读永不被门控）。

**生命周期钩子作为保留事件名折叠进来。** 今天 `on_sleep`/`on_wake` 是
Rust 侧的 `BoothType` 字段——脚本根本声明不了（每个脚本夹具都是
`None`）；字段模型让生命周期成了第二条注册路径还带语言缺口。折叠进唯一
原语后它们变成 `ctx.on("__sleep", h)` / `ctx.on("__wake", h)`（对齐定时
器平面的 `__on_timer` 保留名风格）：宿主的 eviction/reactivation 分发在
路由表读的同一个注册表里查这个保留绑定。`BoothType.on_sleep/on_wake`
字段退役（Rust 摊位经构造侧等价调用注册）。`on_` 前缀坑就此消解：
`ctx.on` 是动词、`__sleep` 是数据——JS 风格的 `on_sleep` 名字从能力面上
彻底消失。

### 4. payload 走参数；ctx 永不是参数

（本节最初的裁决——payload 上 ctx、handler 塌缩成单 `(ctx)` 参数——在
落地之前于同一天被撤回。两条独立理由，记下来因为 §1 现在正立在它们之
上。）

1. **提供者分界（§1）**：payload 是宿主每次投递下推给 handler 的入口数
   据——系统里最"参数"的东西。把它做成 ctx 成员，等于让一个会话级全局
   对象携带每次调用都在变的值：要么每投递重绑一个全局属性（任何存了引
   用的 handler 看到陈旧值），要么每次调用重建对象（全局就没了）。参数
   位天生就是这个每调用作用域的语义——何必另造。
2. **gRPC 类比读对了，反而同意**：`func Foo(ctx context.Context, req
   *Req)`——ctx 带 deadline/values，req 走参数。web 框架同理：Request
   是参数，DI 容器不是。

签名裁决：脚本 handler `(args)`；**Rust `Handler = Fn(&Ctx, Value)`**
（从 `Fn(Ctx, Value)` 改来——上下文是一个长生命周期全局的借用，不是
handler 拥有或转发的值）。不需要也不加 `ctx.payload_as::<T>()`；体内
`from_value::<T>(args)` 一直是 Rust 地道写法。类型化入参便利停留在
0011 留下的位置。

### 5. bgi：可调用 ctx 方法与 host 帧臂 1:1 映射

进程外载体的 `host: {type: …}` 臂镜像这个对象的**可调用成员**
（`invoke | iterate | store | emit | timer.register | timer.cancel |
queue.depth | queue.skip_to_head | schema`）——运行时往返集合。非可调用
成员没有臂：`self` 在 spawn 时由宿主下推进 guest 垫片（会话级稳定），
payload 骑在请求帧上（§4：参数——进程外时，帧就是参数通道），
`on`/`receives` 住在子进程自己的 load 期通道里——hello 帧携带装配好的
注册表，不存在运行时的 on/receives 帧（子进程 load 后调 ctx.on 会栽在
自己垫片侧的相位门上，与镜像一致）。`emit` 是 fire-and-forget：该臂**没有应答**（与其脚本孪生同形；挂死的扩
展只有在同时停止读 stdout 时才会卡住会话——管道的超时纪律已经覆盖）。
mudra 扩展协议（其 ADR-extension-protocol）骑同一词汇表、带自己的
profile 差异（那边无 `store` 臂——该裁决不变）。

### 6. 迁移（破坏式，不留别名）

- `ctx_invoke` → `ctx.invoke`；`ctx_store_emit` → `ctx.store`；
  `ctx_interface_schema` → `ctx.schema`；
  `ctx_timer_register/cancel` → `ctx.timer.register/cancel`；
  `ctx_queue_depth` / `ctx_skip_to_head` → `ctx.queue.depth` /
  `ctx.queue.skip_to_head`；`ctx_iter_start/next/dispose` → 游标形态
  `ctx.iterate`（没有游标类型的脚本载体保留三段调用形态，但改用分组名
  `ctx.iterate.start` 等——分组是命名，游标是 Rust 形态）。
- 平铺 `ctx_*` 注入表退役；每种语言的环境收到**一个全局对象，装载期绑
  定一次、随会话存活**（python：会话装载时绑定模块级 `ctx` 一次——不是
  每投递参数，handler 以裸名引用；steel：`ctx` 符号全局，今天 steel 文
  档里的 `ctx_*` 名字退役；wasm：host fn 保持平铺 ABI——对象是**源码
  级**形状，`aura_alloc` 式 ABI 允许平铺（ABI 是线，不是契约）。
- `docs/design/booth-api.md` 的 host 函数表与语言差异表按本面重写；
  wiki `aura-architecture.md` §5.3 随后（它本来就在写 `ctx.invoke()`
  ——对象读法让那句话变成字面真）。
- ADR-0011 挂 dated Update note（历史正文不动，依既定纪律）。

## 后果

- 一条准入考试取代两条：**这个名字是不是宿主提供的？** 是→ctx 成员；
  脚本自己定义的名字（任何 handler、显式 schema）住模块命名空间，只经
  注册原语与 ctx 相遇；每投递的入口数据（payload）与应答通道（return）
  留在调用路径上——"每调用作用域"本来就住在那里。
- 事件模型最后一个结构缺口（emit 从未落地）获得自然归宿——原打算造出
  第五套安置机制的补丁，现在变成加一个方法。
- 0011 里早已退役的 "On ctx" 条目（ctx.state、ctx.metadata）、它的裸脚
  本函数裁决、以及它的 off-ctx/生命周期钩子安置（`on_sleep`/`on_wake`
  字段模型——脚本从来够不着）一次替换代替逐条勘误；生命周期折叠进
  `ctx.on` 保留名。
- python 获得真正的自省面：`dir(ctx)` 答得出"宿主提供了哪些 API"——散
  落全局时代做不到，这也是 §3 的 `ctx.receives` 可反射的前提。
- 接受的成本：Rust `Handler` 签名 `Ctx` → `&Ctx`（注册点全在树内：
  booth trait、钩子、测试夹具）；`ctx.on` 让注册成为运行时工作（有界：
  activation 本来就执行入口一遍收集装饰器绑定）；存量脚本/测试夹具全部
  改名（量少——能力面年轻，这正是用户能拍不兼容的原因）。
- 全局读法唯一说"谎"的地方：进程外摊位没有共享内存——bgi 的"全局
  ctx"是垫片侧构造（§5：臂 1:1 镜像、self spawn 时下推、payload 上
  帧）。一致性保持在**契约**层而非运行时对象层；guest 不能 `dir()` 宿
  主，只能读自己的注册表。记录，不修——线就是线。
- 维持原状的欠账：timer durable 层（ADR-0016 的 durable 半边）、0012 的
  源码级 emit 收集（Windmill 判据仍把关——ctx.emit 不创造那个消费者）。
