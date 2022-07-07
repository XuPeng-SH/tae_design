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
  <summary><b><font size=4>Transactional Bulk Load</b></font></summary>
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

# Distributed Transaction

## Components

### TxnManager

**TxnManager** controls the coordination of transactions over one or more resources. It is responsible for creating **Txns** and managing their durability and atomicity.

### Txn

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

### TxnSession

A Txn session can only contains one active transaction handle at a time.

### TxnClient

Distributed transaction client. Each CN node has a TxnClient singleton

### TxnOperator

One-to-one relationship with Txn, created by TxnClient. Responsible for distributing transaction requests on **CN** to each associated **DN**.

### TxnEngine | TxnDatabase | TxnRelation | TxnReader

**TxnEngine** is a singleton on **CN**, and **Txn** is created by `TxnEngine::StartTxn()`. All read and write requests converted from DML|DDL at the computing layer will fall on the instance of **TxnDatabase** | **TxnRelation** | **TxnReader**.
