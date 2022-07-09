- Feature Name: TAE in **CN** (Computation Node)
- Status: In Progress
- Start Date: 2022-07-07
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Key Requirements

<details>
  <summary><b><font size=4>Remote S3 Compatitable Storage</b></font></summary>
          In computation node, all data is stored on remote object storage.
</details>
<details>
  <summary><b><font size=4>Local Staging Storage</b></font></summary>
          Disk and in-memory caching as an efficient and cost-effective medium between local clients and remote storage services.
</details>
<details>
  <summary><b><font size=4>Metadata Management</b></font></summary>
          Metadata is stored on remote object storage, local memory has a complete cache, and needs to be updated incrementally.
</details>
<details>
  <summary><b><font size=4>Distributed Transaction</b></font></summary>
          Distributed transactions implementing snapshot isolation isolation level.
</details>
<details>
  <summary><b><font size=4>Transactional Load</b></font></summary>
          Support transactional data load.
</details>
<details>
  <summary><b><font size=4>Transactional Compaction</b></font></summary>
          Support transactional data compaction.
</details>
<details>
  <summary><b><font size=4>Data Loading Pipeline | Prefetcher</b></font></summary>
          The data for the next batches can be load to staging storage while processing the current batch.
</details>
<details>
  <summary><b><font size=4>Data Uploader</b></font></summary>
          Upload data to remote object storage.
</details>

# Guide-level Design

## Local Staging Storage

Local staging storage is an important medium to improve data read and write performance and reduce usage costs. By uploading | downloading | prefetching asynchronously, the probability of accessing remote object storage data can be reduced, thereby reducing operation delay and improving throughput. Remote object storage is accessed less frequently, which also reduces usage costs.

- L1: In-Memory Cache
- L2: Disk Cache
- L3: Remote object storage

### Buffer Manager (L1)

See [Buffer Manager](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/20220503_tae_design.md#buffer-manager)

### Disk Cache (L2)

A fix-sized local disk space used to cache objects from remote storage.

```go
type State int8

const (
    CacheST_Remote State = iota
    CacheST_LocalDisk
)

type CacheItem struct {
    bucket string
    name string
    state State
    handle *os.File
}

type DiskCache struct {
    sync.RWMutex
    capacity int64
    usage int64
    objects map[string]*CacheItem
}
```

When the cache usage reaches a certain threshold, some cleanup jobs will be triggered, which will change the state of some cached items to be `Remote`.

<img src="https://user-images.githubusercontent.com/39627130/177914004-e1cb25ff-3591-4c1b-beda-375670a440be.png" height="50%" width="50%" />

### Distributed Staging Layer

A NUMA-like architecture.

<img src="https://user-images.githubusercontent.com/39627130/177916802-9bb248bc-ef9c-4982-9fd8-24fc565251ea.png" height="35%" width="35%" />

#### Cache Node

- A cache node must have a cache node id.
- A cache node contains many hash slots.
- The count of cache nodes should be a deloyment configuration and can be dynamically changed.
- The count of cache nodes and the id of a cache node can be used as a hash tag to force a certain objects to be stored in the same slot.
- Any `CN` node could be a cache node or a cacheless node.

#### Cacheless Node

- A cacheless node does not mean that no data is cached, just that the cached data will not be accessed by other nodes.
- A cacheless node can be changed dynamically to a cache node.
- Rebalance is needed after config changes.

### L1-L2-L3 Collaboration

**TODO**

## Data Uploader

<img src="https://user-images.githubusercontent.com/39627130/177835047-100b73a3-7516-4cc0-a670-e84e3ec52f48.png" height="70%" width="70%" />

## Data Loading Pipeline | Prefetcher

<img src="https://user-images.githubusercontent.com/39627130/177845304-cfcbb535-b02c-45eb-963f-2f5380dec3d9.png" height="70%" width="70%" />

## Distributed Transaction

### Components

#### TxnManager

**TxnManager** controls the coordination of transactions over one or more resources. It is responsible for creating **Txns** and managing their durability and atomicity.

#### Txn

Txn is a transaction handle kept in transaction session.
```go
type Txn interface {
    // Get the transaction context info. Used in Txn Engine
    GetCtx() []byte
    // Get the transaction id
    GetID() uint64
    // Commit the transaction
    Commit() error
    // Rollback the transaction
    Rollback() error
    // Get the final transaction error
    GetError() error
    // Get the transaction detailed info
    String() string
    // Get the transaction desc info
    Repr() string
}
```

A Txn object contains a **TxnStore** and each **TxnStore** contains a dedicated **TxnOperator**.
<img width="407" alt="image" src="https://user-images.githubusercontent.com/39627130/177816232-29c21541-3942-43c4-861c-a7f058a0c999.png">

#### TxnSession

A `Txn` session can only contains one active transaction handle at a time.

#### TxnClient

Distributed transaction client. Each `CN` node has a `TxnClient` singleton

#### TxnOperator

One-to-one relationship with `Txn`, created by `TxnClient`. Responsible for distributing transaction requests on `CN` to each associated `DN`.

#### TxnEngine | TxnDatabase | TxnRelation | TxnReader

`TxnEngine` is a singleton on `CN` and `Txn` is created by `TxnEngine::StartTxn()`. All read and write requests converted from DML|DDL at the computing layer will fall on the instance of `TxnDatabase` | `TxnRelation` | `TxnReader`.

### Sequence Diagram

<img src="https://user-images.githubusercontent.com/39627130/177819500-95ac95ac-9541-4f7c-8648-d8deab1836e4.png" height="90%" width="90%" />

## Transactional Load

- A data formatter will be used for loading
    - Customized uploader and policy
    - Customized data preprocessor: shard aware, nullable or other constraints check
- Do dedup in `CN` on the latest snapshot in `CN`.
- Send all write set to `DN` for log tail dedup.

### Flowchart

<img src="https://user-images.githubusercontent.com/39627130/178091112-9b5c30d6-32d1-4649-8dd5-d1b4945ef145.png" height="70%" width="70%" />

## Transactional Compaction

### Task Table

A dedicated table `TaskTable` used as a task queue. Any `CN` and `DN` can be a task producer and consumer.

```go
type TaskType int16
type TaskState int8

type TaskTable struct {
    id uint64
    // Specify task type: MergeBlocks, CompactBlock, Split etc.
    taskT TaskType
    // Task specification
    spec []byte
    // Task scope
    scope []byte
    // Task state: Pendding, Running, Done
    state TaskState
    // Task created time
    cTime int64
    // Task modification time
    mTime int64
    // Task Priority
    priority int8
    // If task state is running or done, it specify the executor id
    executorId uint64
}
```
- Triggered by regular timely background monitor
- Triggered by external command: admin command
- Triggered by events: slow query
- Producer
    1. Start a transaction
    2. Check scope confliction
    3. Insert task into table if no confliction
    4. Commit
- Consumer
    1. Start a transaction
    2. Get highest priority pending task
    3. Change the task state to `Running` and update the executorId as its node id
    4. Commit
    5. Execute task
    6. Start a transaction and update the task state to `Done`
- A monitor timely check the task table stats.
