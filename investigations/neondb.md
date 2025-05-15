以下是 **Neon** 与 **MatrixOne** 的对比分析，从架构、技术实现、适用场景等角度总结两者的核心差异以及 **MatrixOne** 可以借鉴 **Neon** 的地方。

---

### **1. 基础对比**
| **特性**          | **MatrixOne**                          | **Neon**                                   |
|-------------------|---------------------------------------------|-------------------------------------------|
| **类型**          | 云原生 HTAP 数据库 （多模态）                   | 基于PG的云原生分离式架构（计算与存储分离） |
| **开源协议**      | 核心开源， 企业功能闭源                | 核心开源，企业功能可能闭源                |
| **底层存储**        | 基于对象存储的自研引擎      |              基于对象存储，适配 PG 的存储引擎           |
| **计算层**        |  完全自研     |              基于 PG 的计算层           |
| **云原生**        | 计算存储分离，计算无状态     |              计算存储分离，计算无状态           |
| **多租户**        | 原生支持多租户，隔离性强                                  | 原生支持多租户，隔离性强                 |
| **HTAP**        | 支持 HTAP 场景， 支持 OLTP 和 OLAP 场景     |              支持 OLTP 场景           |
| **自动扩展**      | 计算节点自动扩缩容                   | 计算节点自动扩缩容                         |
| **全局缓存**      | 分布式缓存 (目前不够成熟)                                | 分布式缓存 (比较成熟)                |
| **时间旅行**      |  条件支持，写放大较大                                 | 支持任意时间点回溯 |
| **分支功能**      | 部分支持,功能欠缺，性能差                                  | 即时分支功能完善，性能好 |
| **高并发读写**    | 事务密集型OLTP（目前有性能瓶颈待优化）              | 读多写少负载（计算层并行优化）     |
| **开发测试**      | PITR+Snapshot，操作比较重，不够快速，成本也较高                            | **分支+时间旅行**快速创建/回滚环境         |
| **突发流量**      | 计算层自动扩展应对峰值                                | 计算层自动扩展应对峰值                     |
| **稳定性成熟度**      |  完全自研,支持 HTAP， 稳定性和技术成熟度较差                               | 基于 PG 成熟， 技术成熟度高， 稳定性高 |

---

### **2.细节对比**

#### **4.1 模块对比**

**MatrixOne** 
- Compute Node
- Transaction Node
- LogService 
- Object Storage

**Neon**
- Compute Node: 和 **MatrixOne** 的 Compute Node 定位和功能类似:
  1. **Neon** 是基于 PG 的计算层，而 **MatrixOne** 是完全自研。
  2. **Neon** 的事务基于 PG 的机制，只需要 *Safekeeper* 的辅助即可。每个写都会在 *Safekeeper* 中记录日志，然后 *Safekeeper* 会异步将日志同步到 *PageServer* 中。**MatrixOne** 事务内的写不会写入 *WAL*, 会维护在一个 *Workspace* 中，当 *Workspace* 积攒到一定量后，会将数据写入 *Object Storage* 中，节省 Compute Node 的内存。在事务提交时，会将 *Workspace* 中的数据提交到 *Transaction Node*, 然后 *Transaction Node* 异步将数据写入 *Object Storage* 中。
  3. 都不支持分布式事务. **Neon** 的多个 *Compute Node* 之间不直接协调锁，而是通过 *PageServer* 协调，锁持久化在 *Safekeeper* 中，锁冲突的延迟比较大。**MatrixOne** 的锁不会持久化，锁冲突的延迟相对较小，但是异常场景下锁的状态迁移等比较复杂和困难。
  4. **Neon** CN 对于缓存不命中的 Page 会向 *PageServer* 请求数据页, 如果还是不命中，则向 *Object Storage* 请求数据页。**MatrixOne** CN 向 *Transaction Node* 订阅表的变更日志，然后以 Tail+Checkpoint 的方式读取数据。
- PageServer: 
  1. 异步将 *Safekeeper* 中的日志同步到 *PageServer* 中，生成 *delta* 文件。
  2. 缓存 *Page* 到内存中。为 *Compute Node* 提供缓存服务。
  3. 定期合并 *delta* 文件生成 *image* 文件, 提高读取性能，类似 LSM Tree, 但不是 LSM Tree。**MatrixOne** 的 TN 不会生成 *delta* 文件, 而是直接讲数据以数据文件和 Tombstone 文件的形式存储到 *Object Storage* 中。其中 *appendable file* 类似 **Neon** 的 *delta* 文件。**MatrixOne** 会做 Merge 操作，对应到 **Neon**，会定期合并 *image* 文件。这是支持 AP 系统必须的步骤，也是工程实现最复杂的地方。PG 有类似 *vacuum* 的机制，也是 PG 系统最晚支持和最难支持的优化，其频率非常的低，而 **MatrixOne** Merge 的频率非常高。
  4. PageServer 不会处理任何事务，但是会协调锁。单个 DB 不支持多个 PageServer 实例。
- Safekeeper
  1. 基于Paxos 协议的实现。**MatrixOne** 的 LogService 是基于 Raft 协议的实现。
  2. 保存事务日志，并异步同步到 PageServer 中。**MatrixOne** 的 LogService 只和 TN 交互，不和 CN 交互，并且只持久化提交的事务日志。
  3. 持久化锁。**MatrixOne** 的锁不会持久化。
- Object Storage
  1. PageServer 异步将日志转存成 *delta* 文件并持久化到 Object Storage 中。
  2. PageServer 异步将 *delta* 合并成 *image* 文件并持久化到 Object Storage 中。
  3. TN 异步将日志转存成 *appendable* 和 "nonappendable" 文件并持久化到 Object Storage 中。
  4. CN|TN 都可以通过一定的策略 Merge "nonappendable" 文件 成 新的 "nonappendable" 文件到 Object Storage 中。
  5. **Neon** 的数据不需要 *vacuum* 操作，因为增量变更会定期合并到 *image* 文件中。

#### **2.2 Neon 核心功能**

1. 时间旅行
 - **Neon** 支持任何时间点的回溯， 实现原理是保留所有的 *delta* 和 *image* 文件，需要查询历史数据时，找到离查询时间最近的 *image* 文件，然后叠加后续的 *delta* 文件，生成历史版本结果。其代价是保留了大量的历史数据。
 - **MatrixOne** 也支持时间旅行，但需要配合 "PITR" 或者 "Snapshot" 功能。用户需要主动的创建 PITR 或者 Snapshot，然后查询时指定到某个时间点。这样做的原因是 **MO** 支持 HTAP 场景，数据量通常很大，而且底层 Merge 机制会产生大量的历史数据，写放大更严重。
 
2. 即时分支
 - **Neon** 支持即时分支，其基础是时间旅行，其性能好的原因是通过 Copy-on-Write 技术，复用 *delta* 和 *image* 文件。
 - **MatrixOne** 支持即时分支，但存在几个问题：
    1. 同样依赖时间旅行，需要配合 "PITR" 或者 "Snapshot" 功能。使用上不如 **Neon** 方便。
    2. 因为 Merge 写放大，用户如果开启时间旅行，需要承担额外的存储成本。
    3. 功能开发不足，目前仅支持很基础的租户恢复，表恢复，数据库恢复等特定场景。相比 **Neon** 的即时分支，产品功能上欠缺很多。
    4. 分支创建的代价相对较高，之前实现为了开发速度，没有考虑性能，导致分支创建的代价比较高，完全可以做到和 **Neon** 一样，通过 Copy-on-Write 技术。

3. 计算存储分离和极致弹性
 - **Neon** 的弹性能力很强，支持计算节点和存储节点的弹性扩缩容。
 - **MatrixOne** 和 **Neon**的架构几乎一致，目前的工程实现阶段还没有达到 **Neon** 的能力，有待加强。

4. 生态
 - **Neon** Postgres 生态
 - **MatrixOne** MySQL 生态

---


### **2 推荐场景**
- **MatrixOne**：  
  ✅ 需要完全控制数据库（自托管）  
  ✅ 传统OLTP业务，追求稳定性和成熟生态  
  ✅ 预算有限（社区版免费，云托管成本低）  
  ✅ 需要 HTAP 场景， 需要支持 OLTP 和 OLAP 场景  
  ✅ 云原生环境，需快速弹性扩展  
  ✅ 读密集型分析负载（分离架构优化I/O）  
  ✅ 时序数据分析   
  ✅ 大数据分析  
  ✅ 多模态数据分析  

- **Neon**：  
  ✅ 云原生环境，需快速弹性扩展  
  ✅ 开发/测试场景（频繁克隆、分支）  
  ✅ 读密集型分析负载（分离架构优化I/O）  

---

### **3 不推荐场景**
- **MatrixOne**(现阶段)：  
  ❌ TPS要求极高的场景（待优化）  
  ❌ 数据规模极大的场景 (待优化)  
  ❌ 频繁开发测试场景 (待优化)  
  ❌ 低延迟写入场景(高频交易)  

- **Neon**：  
  ❌ 低延迟写入场景(高频交易)  
  ❌ 分析场景  
  ❌ 传统高并发写入OLTP场景  

**MatrixOne** 目前存在一些实现上的问题，有些场景目前还支持的不好，理论上 **MatrixOne** 不支持的场景只有 ❌ 低延迟写入场景(高频交易)  

---

### **4. 总结**

摘录 **Databricks** 官网的描述:

**Postgres open source ecosystem**: All frontier LLMs have been trained on the vast amount of public information available about the Postgres open source ecosystem, so all AI agents are experts in using Neon, which is built on Postgres.

**Speed**: Traditional databases were designed for humans to provision and operate. It was OK to take minutes to spin up a database. Given AI agents operate at machine speed, ultra rapid provisioning time becomes critical.

**Elastic scaling and pricing**: The decoupled storage from compute serverless architecture enables extremely low-cost Postgres instances. It’s now possible to launch thousands or even millions of agents with their own databases cost-effectively.

**Branching and forking**: AI agents can be non-deterministic, and “vibes” need to be checked and verified. Neon’s ability to instantly create a full copy of a database, not only for schema but also for the data, allows all these AI agents to be operating on their own isolated database instance in high fidelity for experimentation and validation.
