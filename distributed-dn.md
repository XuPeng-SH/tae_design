- Feature Name: TAE in **DN** (Data Node)
- Status: In Progress
- Start Date: 2022-07-10
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Summary

Here we will only discuss some design|concept changes compared to the stand-alone `TAE`, and only for `DN` nodes. Some things are basically the same as on cn, so they won't be mentioned.

# Key Requirements

<details>
  <summary><b><font size=4>New Driver for LogStore</b></font></summary>
          Use logservice as underlying driver for LogStore
</details>
<details>
  <summary><b><font size=4>Integrate With Distributed TxnCoordinator</b></font></summary>
         Work as a participant node in a distributed transaction.
</details>
<details>
  <summary><b><font size=4>Transaction Client</b></font></summary>
         Transaction can be issued both in `DN` and `CN`
</details>
<details>
  <summary><b><font size=4>Checkpoint</b></font></summary>
         Consider a checkpoint mechanism for remote data storage
</details>
<details>
  <summary><b><font size=4>Metadata Management</b></font></summary>
          Metadata is stored on remote object storage, local memory has a complete cache, and needs to be updated incrementally.
</details>

# Guide-level Design

## Transaction

The engine on `DN` is not responsible for generating new transactions, nor does it control the state of transactions, but only accepts relevant commands and executes them. Transactions are always created by transaction clients and can be on `CN` or `DN`.

<img src="https://user-images.githubusercontent.com/39627130/179884663-aa8bf01c-1f1b-41bf-a168-d366b01f9b48.png" height="45%" width="45%" />

### Workspace

We divide the commands into two categories, one is read-only and one is writable. The reason for distinguishing between these two categories is to reduce the interaction between the client and `DN` at the end of the read-only transaction.

A read-only transaction workspace is temporary, does not require much management, and is destroyed immediately when used up.
A writable transaction workspace is managed, and external commands are required to actively commit or abort, or to actively exit after timeout.

### Exception Handling

- Workspace timeout
- Network connection timeout

## Commands

> Snapshot context

```go
type SnapshotCtx struct {
    From []byte
    To []byte
}
```

> Request context

```go
type BaseRequestCtx struct {
    SyncMeta bool
    SyncTail bool
}
```

### Writable

- CreateDatabase
- DropDatabase
- CreateRelation
- DropRelation
- Trancate
- Append
- Delete
- Update
- TODOs

### Read-only

- GetDatabase
- Databases
- GetRelation
- Relations
- RelationCnt
- RelationRows
- Dedup
- TODOs

## New Driver for LogStore

Integrate log service as one of the underlying driver for LogStore. In the current implementation, the driver layer has not been abstracted, and the original internal logic of the driver is coupled with a lot of unrelated business logic.

```
  <LogEntry>
      |<---------------------------------------+
     \|/                                       |
+-----------+                                  |
|  Receiver |                                  |
+-----+-----+                                  |<Checkpoint LogEntry>
      |                                        |
      |------------------------+               |
     \|/                      \|/              |
+--------------+     +--------------------+    |
|    Driver    |     | Fuzzy Checkpointer |----+
+--------------+     +--------------------+
```

## Checkpoint

Refer [Checkpoint](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/20220503_tae_design.md#checkpoint) for basic concepts.

The change of checkpoint comes from the change of data persistence strategy. The data will be cached to the local disk first, and then persisted to the remote object storage in batches. Only when the data is successfully persisted and the corresponding metadata is persisted will it be considered checkpointed.

### Stand-alone

<img src="https://user-images.githubusercontent.com/39627130/179390091-fc751754-35df-49da-81d0-3ea04607cfcf.png" height="60%" width="60%" />

### Distributed

<img src="https://user-images.githubusercontent.com/39627130/179390266-4d050f62-ce71-4d0c-9900-38342b579992.png" height="60%" width="60%" />

## Metadata Management

**TODO**

## Integrate With Distributed TxnCoordinator

**TODO**

## Transaction Client

**TODO**
