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

## Metadata Management

**TODO**

## Integrate With Distributed TxnCoordinator

**TODO**

## Transaction Client

**TODO**
