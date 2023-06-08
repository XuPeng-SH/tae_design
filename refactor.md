# Refactor Table Data

## Table Data Layout

```
TableData:

|<---------- Checkpoint --------->|- Tail -|
|---------------------------------|--------|

Checkpoint:

|<-------- Gloabl -------->|- Incremetals... -|
|--------------------------|------------------|

| ------------------ Checkpoint ------------------|
| Object | Object | .....................| Object |

Object:

| ------------------ Object ------------------|
| Block | Block | ....................| Block |

Tail:

| ----------------- New Objects ------------------|
| Object | Object | .....................| Object |

| ----------------- Dirty Blocks -----------------|
| Block | Block | ........................| Block |

```

1. Table data is always consist of two parts: Checkpoint and Tail
2. Checkpoint is of two types: Global and Incremental
3. A Checkpoint is a list of Object entries
4. A Object entry is a list of Block entries
5. A Tail is always consist of two parts: New-Objects and Dirty-Blocks
   - New-Objects: newly created objects after the last checkpoint
   - Dirty-Blocks: deletes applied on the existed blocks after the last checkpoint

## Table Data Serving

1. Dedup
   ```
   Dedup with New Objects
   Dedup with Checkpointed-Objects

   When dedup with a object:
   1. Dedup with object meta, if negtive, just return
   2. Dedup with block metas, if negative, just return
   3. If positive with a block meta:
      1. Read block pk column data
      2. Binary search
         if not found, return negative
         else read dirty rows from Dirty-Blocks and judge again

   In dedeup scenario, it is very rare to get a positive result when dedup with
   the object|block meta. And it is always efficient to read dirty rows from Dirty-Blocks
   ```
2. Incremental Dedup

  ```
  We always dedup with Tail first and then Checkpoint if needed.
  Basicly, more than 99% chance that dedup will end at the stage of Tail scan.
  Incremental objects are only loaded into the LRU cache on-the-fly.
  ```

3. Transaction

   ```
   Insertion:
   1. It may add Block entry to a Object if it needs to add a new block
   2. It may append data to the data batch of a block entry and update related index

   Deletion:
   1. Append data to the Dirty-Blocks

   Merge:
   1. block and object insertion and deletion
   ```

4. Checkpoint

   ```
   We will lazy to atomic apply checkpoint to the table data state
   We will build some auxilary index to improve the query performance for checkpointed data
   We don't need to use any "Lock|atomic" when dealing with checkpointed data
   Maybe the checkpointed Object|Block Entry is different with the tail one
   ```
## Implementation

```go
type TxnNode struct {
    start, end types.TS
    txn        txnif.TxnReader
    aborted    bool
}

type VisibilityNode struct {
    CreateTS, DeleteTS types.TS
}

type BlockEntry struct {
    sync.RWMutex

    id objectio.Blockid
    state int8
    checkpointed bool

    pkZM atomic.Pointer[index.ZM]
}

type BlockEntryNode struct {
    *BlockEntry
    TxnNode
    VisibilityNode

    // may be mutated
    location  objectio.Location
    deltaLocs []objectio.Location
}

type ObjectEntry struct {
    VisibilityNode
    // persisted object meta location
    // for non-persisted object, location only contains the object name
    location objectio.Location

    // mutable will not be changed after creation
    // true:  object may be mutated, need to check if it is frozen
    // false: object will not be mutated, no need to check if it is frozen
    mutable bool
    // only valid when mutable is true
    // true: the object is frozen and will not be mutated
    // false: the object is not frozen and may be mutated
    frozen bool

    blocks struct {
        mu      sync.RWMutex
        nodes btree.BTreeG[BlockEntryNode]
    }
}

type Memtable struct {
    newObjects     *btree.BTreeG[ObjectEntry]
    dirtyBlocks *btree.BTreeG[BlockEntryNode] // ????
}

type Checkpoint struct {
    isIncremental bool
    start, end    types.TS
    locations     []string
}

type Checkpoints struct {
    points     []Checkpoint
    start, end types.TS
}

type TableData struct {
    id          uint64
    checkpoints Checkpoints
    memtable    Memtable
}
```
