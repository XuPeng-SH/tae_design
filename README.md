- Feature Name: Transactional Analytic Engine
- Status: In Progress
- Start Date: 2022-02-21
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Summary

**TAE** (Transactional Analytic Engine) is designed for hybrid transactional analytical query workloads, which can be used as the underlying storage engine of database management system (DBMS) for online analytical processing of queries (HTAP).

# Guilde-level design

## Terms
### Layout
- **Block**: Piece of a segment which is the minimum part of table data. The maximum number of rows of a block is fixed
- **Segment**: Piece of a table which is composed of blocks
- **Table**: Piece of a database which is composed of segments
- **Database**: A combination of tables, which shares the same log space

**TODO**

### State
- **Transient Block**: Block where the number of rows does not reach the upper limit and the blocks queued to be sorted and flushed
- **Sorted Block**: Sorted block
- **Unsorted Segment**: Segment that not merge sorted
- **Sorted Segment**: Segment that merge sorted.

**TODO**

### Container
- **Vector**: Data fragment of a column in memory
- **Batch**: A combination of vectors, and the number of rows in each vector is aligned

**TODO**

## Data storage
### Table
**TAE** stores data represented as tables. Each table is bound to a schema consisting of numbers of column definitions. A table data is organized as a log-structured merge-tree (LSM tree).

Currently, **TAE** is a three-level LSM tree, called L0, L1 and L2. L0 is small and can be entirely resident in memory, whereas L1 and L2 are both definitely resident on disk. In **TAE**, L0 consists of transient blocks and L1 consists of sorted blocks. The incoming new data is always inserted into the latest transient block. If the insertion causes the block to exceed the maximum row count of a block, the block will be sorted by primary key and flushed into L1 as sorted block. If the number of sorted blocks exceed the maximum number of a segment, the segment will be sorted by primary key using merge sort.

L1 and L2 are organized into sorted runs of data. Each run contains data sorted by the primary key, which can be represented on disk as a single file. There will be overlapping primary key ranges between sort runs. The difference of L1 and L2 is that a run in L1 is a **block** while a run in L2 is a **segment**.

A segment can be compacted into a new segment if it has many updates(deletions). Segments can be merged into a segment. The scheduling behind this has some customizable strategies, mainly the trade-off between write amplification and read amplification.

As described above, transient blocks can be entirely resident in memory, but not necessarily so. Because there will be many tables, each table has transient blocks. If they are always resident in memory, it will cause a huge waste. In **TAE**, transient blocks from all tables share a dedicated fixed-size LRU cache. A evicted transient block will be unloaded from memory and flushed as a transient block file. In practice, the transient blocks are constantly flowing to the L1 and the number of transient blocks per table at a certain time is very small, those active transient blocks will likly reside in memory even with a small-sized cache.

### Indexes
There's no table-level index in **TAE**, only segment and block-level indexes are available.

In **TAE**, there is a dedicated fixed-size LRU cache for all indexes. Compared with the original data, the index occupies a limited space, but the acceleration of the query is very obvious, and the index will be called very frequently. A dedicated cache can avoid a memory copy when being called.

#### Primary key index
**TAE** creates an index for each table's primary key by default. The main function is to deduplicate when inserting data and filter according to the primary key. Deduplication is the critical path for data insertion. We need to make trade-offs in the following three aspects:
- Query performance
- Memory usage
- Match with the underlying data store layout

From the granularity of the index, we divide the index into two categories, one is a table-level index, and the other is an index set composed of a series of partition indexes. For example, we can have a table-level B tree index, or each segment has a B tree index. The table data of **TAE** consists of multiple segments, and each segment must be unordered first and then ordered. Compaction, merging, or splitting may take place afterwards. This scenario is very unfriendly to the table-level index. So the index of TAE should be a segment-level index set.

The segment-level index in **TAE** is a two-level structure, bloomfilter and zonemap respectively. There are two options for bloomfilter, a segment-based bloomfilter, and a block-based bloomfilter. The Segment-based is a better choice when the index can be fully resident in memory. The workflow is as follows:

<img src="https://user-images.githubusercontent.com/39627130/154958080-495a461c-ea9f-4c43-ad0f-b57232720d50.png" height="30%" width="30%" />

**TODO**

#### Secondary index
**TODO**

### Compression
**TAE** is a column-oriented data store, very friendly to data compression. It supports per-column compression codecs and now only **LZ4** is used. You can easily obtain the meta information of compressed blocks. In **TAE**, the compression unit is a column of a block.

### Layout
#### Block
**TODO**

#### Segment
**TODO**

## Buffer manager
Buffer manager is responsible for the allocation of buffer space. It handles all requests for data pages and temporary blocks of the **TAE**.
1. Each page is bound to a buffer node with a unique node ID
2. A buffer node has two states:
   1) Loaded
   2) Unloaded
3. When a requestor **Pin** a node:
   1) If the node is in **Loaded** state, it will increase the node reference count by 1 and wrap a node handle with the page address in memory
   2) If the node is in **Unloaded** state, it will read the page from disk|remote first, increase the node reference count by 1 and wrap a node handle with the page address in memory. When there is no left room in the buffer, some victim node will be unloaded to make room. The current replacement strategy is **LRU**
4. When a requestor **Unpin** a node, just call **Close** of the node handle. It will decrease the node reference count by 1. If the reference count is 0, the node will be a candidate for eviction. Node with reference count greater than 0 never be evicted.

There are currently four buffer managers for different purposes in **TAE**
1. Mutation buffer manager: A dedicated fixed-size buffer used by L0 transient blocks. Each block corresponds to a node in the buffer
2. SST buffer manager: A dedicated fixed-size buffer used by L1 and L2 blocks. Each column within a block corresponds to a node in the buffer
3. Index buffer manager: A dedicated fixed-size buffer used by indexes. Each block or a segment index corresponds to a node in the buffer
4. Redo log buffer manager: A dedicated fixed-size buffer used by uncommitted transactions. Each transaction local storage consists of at least one buffer node.

## LogStore
An embedded log-structured data store. It is used as the underlying driver of **Catalog** and **WAL**.
**TODO**

## WAL
**Write-ahead logging** (WAL) is the key for providing **atomicity** and **durability**. All modifications should be written to a log before applied. In **TAE**, **REDO** log does not need to record every operation, but it must be recorded when the transaction is committed. We will reduce the usage of io by using the redo log buffer manager, and avoid any io events for those transactions that are not long and may need to be rolled back due to various conflicts. It can also support long or large transactions.

**TODO**

## Catalog
**Catalog** is **TAE**'s in-memory metadata manager that manages all states of the engine, and the underlying driver is an embedded **LogStore**. **Catalog** implements a simple memory transaction database, retains a complete version chain in memory, and is compacted when it is not referenced. **Catalog** can be fully replayed from the underlying **LogStore**.
1. DDL operation infos
2. Table Schema infos
3. Layout infos

**TODO**

## Database (Column Families)
In **TAE**, a **Table** is a **Column Family** while a **Database** is **Column Families**. The main idea behind **Column Families** is that they share the write-ahead log (Share **Log Space**), so that we can implement **Database-level** atomic writes. The old **WAL** cannot be compacted when the mutable buffer of a **Table** flushed since it may contains live data from other **Tables**. It can only be compacted when all related **Tables** mutable buffer are flushed.

**TAE** supports multiple **Databases**, that is, one **TAE** instance can work with multiple **Log Spaces**. Our **MatrixOne** DBMS is built upon multi-raft and each node only needs one **TAE** engine, and each raft group corresponds to a **Database**. It is complicated and what makes it more complicated is the engine shares the external **WAL** with **Raft** log.

## Multi-Version Concurrency Control (MVCC)
**TODO**

## Transaction
**TODO**

## Snapshot
**TODO**

## Split
**TODO**

## GC
1. Metadata compaction
   1) In-memory version chain compaction
   2) In-memory hard deleted metadata entry compaction
   3) Persisted data compaction
2. Stale data deletion
   1) Table data with reference count equal 0
   2) Log compaction

**TODO**
