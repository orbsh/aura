# 0043 — ctx 对象：唯一的执行上下文（整体取代 ADR-0011 的安置制，破坏式）

> **Languages:** [English](0043-ctx-object.md)（主文档） · [中文](0043-ctx-object.zh-CN.md)

**状态**：Accepted（2026-10-10）——设计定案，实现未开始。整体取代
ADR-0011 的安置决定，但保留它的方向规则（入口保持语言原生）与它对
"运行时动态订阅"的否决（重述为相位门）。用户裁决（2026-10-10）：
**全换、不兼容**——平铺 `ctx_*` 名字整体退役，不留别名。

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

重定义视角：`ctx` 不是"实例身份对象"——它是**宿主授予一次投递的
ambient 执行上下文**（gRPC `context.Context` / web 框架 Request 对象的形
状）。在这个读法下，身份绑定不是准入考试，只是个别成员的可选内容。真正
存活的边界是 0011 已经用于 `return` 和钩子的那条：**方向**。

## 决策

### 1. 边界：出口面 vs 入口契约

**ctx = booth→host 的出口能力面。** 摊位向外调用的一切都是一个对象上的
方法。**入口保持语言原生**（0011 这条规则原样有效）：handler 的
`return` 填应答信封（`ctx.return` 永远不存在）、失败以语言原生异常上抛
并转为外层错误值、`on_sleep`/`on_wake` 是 Host→Booth 方向的导出、
`set(lang, script)` 属部署面且执行中的摊位永远看不见它。

### 2. 能力面（一个对象，方法分组）

```
ctx.self                      # InstanceId（结构键：Singleton|Named）
ctx.payload                   # 本次调用的输入，只读（§4）
ctx.invoke(target, handler, args) -> Value          # 唯一受控调用面
ctx.iterate(target, handler, args) -> Cursor        # start/pull/dispose
ctx.store(op) -> Value        # 一条 okm Collection 指令作为数据（ADR-0026 §3）
ctx.emit(event, data)         # 落地 0011 那半座桥：发布进 MQ 平面
ctx.timer.register(at_ms, tag) -> id                # ADR-0016 §3b
ctx.timer.cancel(id)          # 幂等
ctx.queue.depth(event)        # 积压点读
ctx.queue.skip_to_head(event) # 泄压阀（`to_head` 是语义不是赘词：方向性丢弃）
ctx.schema                    # 持久化 interface_schema 的副本，作为数据（反射）
ctx.on(event, handler)        # 相位门控：仅 load/activation 期（§3）
```

平铺名字退役后 store/emit 的同名冲突随之消失：存储是 `ctx.store(op)`
（方法组），发布是 `ctx.emit(event, data)`——动词跟着语义走，不跟安置走。
整面的命名规则：进入 ctx 命名空间后，成员取**短且唯一可解析**的名字——
`interface_schema` 的长度是平铺命名空间的税（裸全局环境里必须自证身份），
`ctx.schema` 即退税。（核对过予以保留的例外：`ctx.queue.skip_to_head`——
`to_head` 承载方向性丢弃的语义（游标单调、跳过的积压不复活），不是赘词。）

`ctx.emit` 的 emitter 字段：宿主闭包捕获**类型名**（绝不捕实例 id——
发布与实例无关是 0011 自己的观察；审计线仍读得出发布者是谁）。

### 3. `ctx.on`：声明式 schema 仍是契约的家

0011 否决动态订阅，否决保持——但重述得更锋利：**`ctx.on` 仅在
load/activation 期合法；在消息 handler 内调用=错误值**（相位门与系统既
有的"无绑定=错误值，从不静默"纪律同形）。这不是一条与内省竞争的注册新
通道：`interface_schema.receives` 数据仍是**唯一持久化契约源**（路由注册
表、depth/skip 治理、mudra 的 hello-schema 全读它）。装饰器（`@on`）是
**lowering 到** `ctx.on` 的语法糖；静态语言（Rust 摊位、bgi binary）可以
直接写 `ctx.on` 作为低层形态——schema 由同一个 activation 期注册表装配，
两条写法殊途同归。`on` 上 ctx 改变的是数据流：注册 = activation 期把入
口执行一遍（carrier 为收集装饰器绑定本来就要做这件事），不再另做静态扫描。

### 4. payload 上上下文；handler 签名塌缩为单参数

`ctx.payload` 是执行上下文最本义的成员——本次投递的数据。推论：所有
carrier 的 handler 签名统一为 `(ctx)`（Rust：`Handler = Fn(Ctx)`——今天
是 `Fn(Ctx, Value)`；python/steel/nu/bgi 同为一参）。静态语言的类型化入
参便利从签名位移到体内（`let p = ctx.payload().into_value::<T>()?`）：
编译期→运行期，接受。

### 5. bgi：host 帧臂集合成为 ctx 方法的 1:1 映射

进程外载体的 `host: {type: …}` 臂镜像这个对象
（`invoke | iterate | store | emit | timer.register | timer.cancel |
queue.depth | queue.skip_to_head | schema`——`self`/`payload`
由宿主在 call 帧里下推，`on` 住进 hello schema，无帧）。
`emit` 是 fire-and-forget：该臂**没有应答**（与其脚本孪生同形；挂死的扩
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
- 平铺 `ctx_*` 注入表退役；每种语言的环境收到**一个对象**（python：每
  投递绑定一个模块级 `ctx`；steel：`ctx` 全局，今天 steel 文档里的
  `ctx_*` 名字退役；wasm：host fn 保持平铺 ABI——对象是**源码级**形状，
  `aura_alloc` 式 ABI 允许平铺（ABI 是线，不是契约）。
- `docs/design/booth-api.md` 的 host 函数表与语言差异表按本面重写；
  wiki `aura-architecture.md` §5.3 随后（它本来就在写 `ctx.invoke()`
  ——对象读法让那句话变成字面真）。
- ADR-0011 挂 dated Update note（历史正文不动，依既定纪律）。

## 后果

- 一条准入考试取代两条：**是不是 booth→host？** 是→ctx 方法；是
  host→booth→导出或返回值；属部署→两个面都不在。
- 事件模型最后一个结构缺口（emit 从未落地）获得自然归宿——原打算造出
  第五套安置机制的补丁，现在变成加一个方法。
- 0011 里早已退役的 "On ctx" 条目（ctx.state、ctx.metadata）和它的裸
  脚本函数裁决，一次替换代替逐条勘误。
- 接受的成本：Rust 的类型化入参退到运行期；`ctx.on` 让注册成为运行时工
  作（有界：activation 本来就执行入口一遍收集装饰器绑定）；存量脚本/
  测试夹具全部改名（量少——能力面年轻，这正是用户能拍不兼容的原因）。
- 维持原状的欠账：timer durable 层（ADR-0016 的 durable 半边）、0012 的
  源码级 emit 收集（Windmill 判据仍把关——ctx.emit 不创造那个消费者）。
