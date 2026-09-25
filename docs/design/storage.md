# 存储架构（设计细节）

> 自 `~/.hermes/wiki/aura-architecture.md` §3 迁入的实现细节；wiki 保留综述。
> 综述：wiki [Aura 架构 §3](https://github.com/orbsh/wiki/blob/main/aura-architecture.md)。

## 3. 存储架构

### 3.1 存储引擎抽象：Trait 分离

Aura 的存储层通过两个 trait 实现引擎可插拔——存储引擎（KV 读写）和分发层（多节点协调）正交组合。

#### AuraStorage：统一二进制存储接口

```rust
use async_trait::async_trait;

#[derive(Debug)]
pub enum StorageOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// 统一二进制存储抽象，抹平 Fjall（同步）与 SlateDB（异步）的差异
#[async_trait]
pub trait AuraStorage: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    async fn write_batch(&self, ops: Vec<StorageOp>) -> Result<(), StorageError>;
    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>;
}
```

#### 双轨实现

| 实现 | 引擎 | 包装方式 | 适用场景 |
|:--|:--|:--|:--|
| `FjallEngine` | Fjall → 本地 NVMe | `tokio::task::spawn_blocking` 包裹同步 I/O | 私有部署，亚毫秒延迟 |
| `SlateEngine` | SlateDB → S3 | 天生 `async/await`，直接对接 Tokio | 云原生，无状态计算 |

```rust
// FjallEngine：同步阻塞 → spawn_blocking 异步包装
pub struct FjallEngine { keyspace: fjall::Keyspace }

#[async_trait]
impl AuraStorage for FjallEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let ks = self.keyspace.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            ks.get(&key).map_err(|e| StorageError::Fjall(e.to_string()))
        }).await.map_err(|e| StorageError::TaskJoin(e.to_string()))?
    }
    // write_batch / prefix_scan 类似，均用 spawn_blocking 包裹
}

// SlateDB：原生异步，直接对接
pub struct SlateEngine { db: Arc<slatedb::Db> }

#[async_trait]
impl AuraStorage for SlateEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.db.get(key).await.map_err(|e| StorageError::Slate(e.to_string()))
    }
    // write_batch / prefix_scan 直接调用 SlateDB async API
}
```

#### AuraCollection：OKM 与存储引擎的绑定容器

OKM 的 `TypedCollection` 通过 `Arc<dyn AuraStorage>` 绑定到具体引擎。上层业务代码通过 `#[derive(KvEncode)]` 声明实体，宏在编译期生成 Key 编码，AuraCollection 负责与底层引擎的读写交互：

```rust
pub struct AuraCollection<T> {
    pub storage: Arc<dyn AuraStorage>,  // FjallEngine 或 SlateEngine，运行时切换
    _marker: PhantomData<T>,
}

impl<T> AuraCollection<T> where T: Serialize + DeserializeOwned + EntityKeyGenerator {
    pub async fn save(&self, entity: &T) -> Result<(), StorageError> {
        let key = entity.generate_compiled_key();
        let value = bincode::serialize(entity).unwrap();
        self.storage.write_batch(vec![StorageOp::Put(key, value)]).await
    }

    pub async fn find(&self, key_spec: &T) -> Result<Option<T>, StorageError> {
        let key = key_spec.generate_compiled_key();
        match self.storage.get(&key).await? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes).unwrap())),
            None => Ok(None),
        }
    }
}
```

→ OKM 的完整 proc macro 实现见 [OKM 项目](https://github.com/orbsh/okm)。

#### 分发层：已裁决不引入共识

多节点协调不做 Raft/Paxos（openraft 已明确暂停，无回归条件）。当前口径：

- **引擎内部元数据每节点独立**：actor 注册表、shard map、配置住在本节点自己的存储里（ADR-0025 后即数据面 okm 实例），由控制平面单点写入，节点缓存读取（见 [partitioning.md §5](partitioning.md)）——单一逻辑写入者使共识失去必要性。
- **联邦节点间走 well-known 协议认证**：节点互不共享存储、不共享共识日志；身份经 well-known 协议（公钥/证书，ADR-0015 节点身份形态）认证后按节点信任交互；数据面隔离只按 namespace 机制（绑定维度是应用决定，PLAN 4.10），不预设用户维度。
- **用户数据跟随所属节点**：用户登录到另一节点时，其历史数据不在该节点——数据主权绑定所属节点，不做登录信息的全局同步。

共识不通过「出现第二个写入者」触发——联邦内部没有这个路径：多控制面部署是方向性倒退（推翻联邦而非扩展点）；引擎内部元数据保持控制平面只写面（只有控制平面能写），写入者永远单一；「全局唯一配置」在联邦语义下不存在——每节点独立是裁决而非缺陷。若未来整体转向逻辑单集群架构，那是推翻 ADR-0013 的新裁决，不是本架构内的扩展点。原 `Distribution` trait（`propose(cmd)` Raft 提案接口）已随本裁决删除。

**Actor 完全不感知底层引擎**——`ctx.store.emit` 的指令形态不变，底层是 Fjall 同步返回还是 SlateDB 从 Block Cache 命中，对 Actor 透明。

→ 两条架构路径的完整对比见 [KV 存储引擎架构 §11](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#11-两条架构路径fjall-vs-slatedb)。三引擎（Fjall/SlateDB/SurrealKV）的 API 差异和选型指南见 [KV 存储引擎架构 §三引擎 API 对比](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#三引擎-api-对比fjall--slatedb--surrealkv)。

### 3.2 为什么不用 SQL/Redis

Actor 状态是 KV 模式（点查 + 前缀扫描），SQL 的关系代数和查询优化器是多余开销。嵌入式 KV 相比 SQL 的三个系统性优势：C 语言依赖与交叉编译地狱、双重缓存与内存浪费、写锁线程阻塞。详见 [KV 存储引擎 §9.6 SQLite vs 嵌入式 KV](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#106-sqlite-vs-嵌入式-kv开源项目的隐形代价)。

**分层选择**：

| 数据类型 | 默认方案 | 备选 | 理由 |
|:---|:---|:---|:---|
| Actor 状态（用户数据） | SlateDB + S3 | Fjall（单机/离线） | S3 自动复制，无需手动同步 |
| Actor 状态（延迟敏感 + 需强一致复制） | TiDB 模式（每个 Actor = Region） | — | **数据级 Raft 复制**，属 TiDB/TiKV 范畴（外部现成方案，非本架构默认路径） |
| 配置/引擎内部元数据 | 本节点存储（控制平面单写，ADR-0025 后即数据面 okm 实例） | — | 单一逻辑写入者使共识失去必要性；见 §分发层 |
| 脚本/图片（静态资产） | 文件系统同步（git / S3） | — | 静态资产不是数据，不需要共识 |

**Actor 状态的 TiDB 模式**：每个 Actor 天然是一个 shard 边界。Actor:user:alice 独立一个 Raft Group（3 副本），Actor:user:bob 独立另一个。写放大始终 3x，不随 Actor 数量增长。这和 TiDB 的 Region 模型一致——Actor 是天然的分片边界。**但属数据级 Raft 复制**——此分支需要 actor 数据跨节点复制，是「分片 + Raft」强一致路径，应直接落现成的 TiKV / TiDB 分片机制（或 FoundationDB 得全局事务），非此架构的默认路径（默认 SlateDB+S3 / Fjall+落湖；元数据每节点独立，不引入共识）。

**大部分场景用 SlateDB + S3**：除非真的需要 <1ms 写延迟 + 强一致，否则 SlateDB + S3 更简单。S3 处理复制，成本低 20 倍，运维无 Raft/PD/Region 调度。

通过将 Fjall + 多模态嵌入式运行时揉进同一个单体二进制文件中，消除了现代架构中常见的冗余和嵌套。如果把这套架构里的 Fjall 剔除换成 Redis，整个系统将发生严重的**底层架构退化（Structural Regression）**：

| 核心维度 | Fjall + 多模态嵌入沙箱（原架构） | 替换为 Redis 的退化形态 |
|---------|---------------------------------------------|---------------------------|
| **集群内聚度** | 纯 Rust 单体。Actor 注册/路由的元数据由控制平面单写 + 节点缓存（数据落 Fjall+湖 或 SlateDB+S3）。 | 割裂的应用服务器矩阵 + 外部独立的 Redis 实例 + 复杂的 Redis Sentinel/Cluster 运维线。 |
| **计算局部性** | 计算紧贴存储（Compute Near Data）。多语言虚拟机内存指针直接映射磁盘 Buffer。零网络 RTT，走 CPU 总线速度。 | 计算远离存储。网关每次收请求必须打开 TCP 连接，数据打包成文本型 RESP 协议跨进程传输，重新背负 1.0ms–3.0ms 的网络往返延迟（RTT）。 |
| **零拷贝** | Rust 生命周期系统（`serde(borrow)`）让多语言虚拟机直接用指针读取磁盘 Buffer，无新内存申请。 | 数据必须在 Redis 侧打包、经 Socket 传输、在 Rust 客户端解包，在堆内存申请新空间大块拷贝。高频内存分配与 GC 开销。 |
| **多线程并行** | 元数据单写 + Tokio 异步协程多核高并发，Fjall 多线程 LSM 异步刷盘，Steel/PyO3 各走独立 OS 线程，全网无中心化吞吐卡死。 | Redis 单线程事件循环，一旦运行复杂 Lua 脚本或重度 CPU 计算，全球所有其他读写请求瞬间死锁卡死。 |
| **Scale-to-Zero** | Fjall LSM-Tree 将不活跃冷状态高度压缩为磁盘 SSTables。Agent 睡着时 RAM 消耗 0 字节。百万级 Agent 也无内存压力。 | Redis 纯内存数据库，所有数据全量躺在物理内存里。智能体扩大到 1 万或 100 万个时，硬件账单指数级爆炸。 |
| **Token 优化** | 后台静默触发"环境梦境整理（Ambient Consolidation）"，自动将长时文本 Sink 进 S3。 | 必须在应用端写复杂的定时任务（Cron Jobs），高频跨网络去捞内存数据再执行归档。 |
| **系统复杂度** | 极致极简。1 个可执行文件，0 个外部数据库配置文件，解压即组网。 | 高运维负荷。需要维护多套发布流水线、外部连接池监控以及缓存击穿/雪崩的防御代码。 |

**一句话总结**：把 Fjall 换成 Redis，是用系统长期的"运行期高延迟、带宽开销、内存账单膨胀以及单线程死锁风险"，去仅仅换取"在第一周开发时少写几行存储适配代码"的短暂偷懒。没有 Redis 集群的心跳同步紊乱，没有 PostgreSQL 昂贵的连接池耗尽与 SQL 树解析开销，没有 JavaScript（Rivet）运行时的冗余与弱类型妥协。在 Rust 语言的底层安全原语之上，构建了一套元数据单写自治、数据存算分离（Fjall+湖 / SlateDB+S3）的分布式智能体系统。

**Lua 脚本的工程断层**：Redis 为挽救吞吐量引入的 Lua 脚本，除了单线程死锁风险外，还导致主技术栈（Rust/Go）与脚本层发生工程学与调试断层——失去强类型保护、单元测试和 IDE 感知提示。

### 3.3 Actor 读写 API：ctx.store.emit（单 API）

Actor 的持久化面只有一套 API——`ctx.store.emit(op)`，携带 okm Collection 指令作用于**本类型声明的 collections**（ADR-0026 §3）：存储隔离在类型层，每个 Actor 类型占一个真实 okm ns，类型声明的 collections 的 schema 随 interface_schema 上传持久化；实例是类型 ns 内的 document。旧的 `ctx.state` 每实例一份平铺字段文档（`ctx_state_get/set/delete` 点读写）已退役——点模型无法承载 scan/index/reduce，被 collection 接口面取代。独立 meta 实例与 `ctx.metadata` 已随 ADR-0025 撤销：actor 定义是数据面 okm 实例里的 `ActorDef` 表（ns 41），注册表/分片映射等是引擎内部结构，不对 Actor 暴露读写面。**登录状态不在别处**：用户数据（含登录态/历史）绑定所属节点，登录其它节点 = 该节点没有此用户的数据，不做全局同步——跨节点只按 well-known 协议认证身份。

```rust
// Actor 状态（数据面 okm 实例：本地 Fjall 或 SlateDB+S3）
// handler 经 ctx bridge 发一条存储指令（wire 上是 JSON，realm 转成 DynamicValue）
ctx.store.emit(put_document("counters", key, doc))   // 写一个 document
ctx.store.emit(get_document("counters", key))        // 读一个 document
// 还有 scan（schema 声明的 AccessMethod）与 reduce（count/high_water/low_water 预设）
//——同类型跨实例聚合 = 类型 ns 内的普通 scan/reduce，不再需要 projection actor
```

| API | 数据类型 | 存储 | 复制 |
|:---|:---|:---|:---|
| `ctx.store.emit` | Actor 状态（类型声明的 collections） | 数据面 okm 实例：SlateDB + S3（默认）/ Fjall | S3 自动处理 / 无复制 |

### 3.4 分布式架构拓扑

```
┌─────────────────────────────────────────────┐
│ ctx.store.emit (Actor 状态) ──► 数据面 okm 实例 │ ← 本地 Fjall 或 SlateDB+S3
│ actor 定义/注册表等内部元数据 ──► 同一实例      │ ← 控制平面单写（ADR-0025）
└─────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────┐
│ Fjall/SlateDB 存储（单一 okm 实例，一个目录）   │
└─────────────────────────────────────────────┘

联邦节点之间：well-known 协议认证身份，无共享存储、无共识日志、无全局登录同步
```

### 3.5 Fjall vs SlateDB

- **Fjall 的定位**：纯 Rust LSM-Tree 存储引擎，进程内嵌入，零网络开销。ADR-0025 后为单一 okm 实例（actor 定义并入数据面）。

- **Actor 状态复制的正确方案**：
  - 默认：SlateDB + S3（S3 处理复制，成本低 20 倍）
  - 延迟敏感：TiDB 模式（每个 Actor = 一个 Raft Group，写放大固定 3x）——外部现成方案，非默认路径

- **单 API 单实例**：`ctx.store.emit` 是 Actor 唯一的持久化面（ADR-0026 §3）；actor 定义等引擎内部元数据住在同一个 okm 实例（ADR-0025），不对 Actor 暴露第二套 API。

→ 详见 [Redis 批判：RESP 协议 vs 二进制序列化](https://github.com/orbsh/wiki/blob/main/redis-critique.md#8-resp-协议-vs-二进制序列化嵌入式架构的物理优势)。Fjall 的 API 设计和与其他引擎的对比见 [KV 存储引擎架构 §三引擎 API 对比](https://github.com/orbsh/wiki/blob/main/kv-storage-engine.md#三引擎-api-对比fjall--slatedb--surrealkv)。

### 3.6 SlateDB + S3 模式（默认推荐）

```
[Client] → [无状态 gRPC Pod] → [SlateDB] → [S3 桶]
                ↑
          任意 Pod 可服务（S3 是真理源）
```

**为什么是默认推荐**：写入性能与 Fjall 相同（都是 MemTable 攒批），但 S3 处理复制（成本低 20 倍），计算节点无状态，运维最简单。Fjall 仅在不能用 S3 时（私有化、离线）考虑，且在大 Value 场景（KV 分离）、复杂本地事务、极致本地性能方面有结构性优势。Fjall 官方无 S3 支持计划。

**Actor 状态读写**：`ctx.store.emit` 指令形态不变。SlateDB 的 Block Cache 命中时延迟仍在 μs 级（热数据），未命中时退化为 ms（S3 Range Get）。Agent 场景的热数据（最近对话）天然驻留 Block Cache，冷数据（历史记录）的 ms 级延迟可接受。

**Durability**：SlateDB 的 WAL 在本地磁盘，节点磁盘丢失时需等 S3 flush 完成才能恢复——flush 前的窗口期存在数据丢失风险。对于 Agent 场景（对话数据可重建），这个风险通常可接受。

若未来某场景确需 Actor 状态强一致复制（TiDB 模式），直接落 TiKV/FoundationDB 等现成机制，不在本架构内自建——Openraft 状态机挂载 Fjall 的分析见 [共识协议文档](https://github.com/orbsh/wiki/blob/main/consensus-protocol.md)。

### 3.7 配置与工作量评估

**配置示例**（Phase 4 已落地的两实例模型）：

```kdl
// aura.kdl：node / data / meta 三平面；data 与 meta 是两个独立 okm 实例，引擎各自可选（fjall | slate）
// 单节点：两实例都跑 fjall，不同目录；无 distribution/raft 配置项
```

**实现工作量**（Engine 层已落地；Distribution 不再实现）：

| 任务 | 状态 | 说明 |
|:--|:--|:--|
| `AuraStorage` trait + FjallEngine / SlateEngine | ✅ 已落地 | okm 两实例绑定，引擎可选 |
| Distribution trait / RaftDist | ❌ 已删除 | ADR-0013：联邦架构内没有通向共识的路径 |
| 配置加载（aura.kdl 两实例） | ✅ 已落地 | knus 解析，未知引擎启动报错 |
| Actor 层适配 | 0 | ctx.store.emit 指令形态不变 |

### 3.8 用户意志主导的多模态路由机制（User-Driven Polyglot Routing）

在传统的 FaaS（如 Windmill）或重型智能体框架中，通常是由"系统架构或框架"死板地规定："这个步骤必须用 Python 跑，那个步骤必须用 JS 跑"。这本质上是对 AI 自由度和人类开发意志的束缚。

在本架构（Fjall + Polyglot Core）中，我们彻底打破这种死板的框架绑架。**用什么语言来执行决策、重构代码或处理数据，完全由"用户下达的指令"或"AI 智能体自发生成的策略"动态决定。** Rust 的主 Actor 控制台只负责提供一个没有任何偏见的、纯粹的多语言执行沙箱（Polyglot Engine Room）。

#### 用户驱动的状态指令协议

```rust
// src/store.rs - 用户驱动的多语言执行提案
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum EngineType {
    Steel,  // 嵌入式脚本：Lisp 策略引擎
    PyO3,   // 嵌入式脚本：Python AI/数据引擎
    Wasm,   // 沙箱运行时：第三方不信任代码（通常用 Rust 编写 .wasm）
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum EngineCommand {
    UpdateActorState {
        agent_id: String,
        serialized_context: Vec<u8>,
    },
    TerminateActor {
        agent_id: String,
    },
    // 用户或 AI 动态发起的任意语言执行提案
    DispatchUserScript {
        agent_id: String,
        engine: EngineType,      // 用户决定的语言类型
        script: String,          // 用户提交的动态脚本
        target_function: String, // 要调用的目标函数
        input_payload: Vec<u8>,  // CBOR 编码的动态输入数据
    },
}
```

当用户提交 `DispatchUserScript` 时，本地状态机根据用户选择，将物理内存指针映射到对应的语言虚拟机。运行期交接棒流程为：从 Fjall LSM-Tree 中读出 CBOR 编码的 Actor 状态（零网络延迟）→ 解码为 `ciborium::Value` → 根据用户选择的 EngineType 拉起对应的嵌入式虚拟机（Steel/PyO3/Wasm），Host 从 CBOR Value 中取出字段注入虚拟机执行 → 更新结果状态 → 写回本地 Fjall。Actor 状态不走网络路径。具体的多语言执行逻辑已在 [§2.2](#22-多语言网关纯-rust-混合-actor-实现) 的 `exec_steel_lisp` 和 `exec_embedded_python` 中完整实现，此处不再重复。

#### 用户驱动模式的工程爽点

1. **用户拥有"语法免冲突权"**：
   如果你要写一段需要跟大模型频繁交互、且在 Neovim 里进行深度协作的核心控制策略，你可以立刻下达指令："这一步我用 Steel Lisp 跑"。这能保证你的大脑完全沉浸在括号的几何边界里，彻底免受 Koto 那种隐式空格语义地雷的折磨。

2. **AI 智能体拥有"生态自选权"**：
   当你托管在云端的 Hermes 大脑发现："接下来的任务需要去读取一个复杂的深度学习 .bin 权重文件，或者分析一段遗留的 PyTorch 矩阵"时，AI 会自己在分布式提案里写明：`engine: EngineType::PyO3`。它通过纯粹的内存指针，直接在当前 Rust 进程里无缝吃掉 Python 的 AI 生态。

3. **多语言在 Fjall 磁盘里的统一**：
   不管用户刚才任性地选了 Lisp 还是 Python，它们对智能体状态的修改（Mutation），最终都会被反序列化回最基础的二进制内存块（`Vec<u8>`），写回本地 Fjall。

**框架不再是法官，框架只提供执行能力；用户和 AI 的动态意志决定哪种语言在这一毫秒登上多模态内存舞台。**


## 4. 序列化协议分析对比
