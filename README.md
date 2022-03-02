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

From the granularity of the index, we divide the index into two categories, one is a table-level index, and the other is an index set composed of a series of partition indexes. For example, we can have a table-level B tree index, or each segment has a B tree index. The table data of **TAE** consists of multiple segments, and each segment must be unordered first and then ordered. Compaction, merging, or splitting may take place afterwards. This scenario is very unfriendly to the table-level index. So the index of **TAE** should be a segment-level index set.

There are two types of segment. One is appendable and the other is not. For non-appendable segment, the segment-level index is a two-level structure, bloomfilter and zonemap respectively. There are two options for bloomfilter, a segment-based bloomfilter, and a block-based bloomfilter. The Segment-based is a better choice when the index can be fully resident in memory. An appendable segment consists of at least one appendable block plus multiple non-appendable blocks. Appendable block index is a resident memory ART-tree plus zonemap while the non-appendable one is bloomfilter plus zonemap.

<img src="https://user-images.githubusercontent.com/39627130/155945090-945442eb-25f6-4624-8c5a-442475328e49.png" height="70%" width="70%" />

The query flow chart of segment index is as follow:

<img src="https://user-images.githubusercontent.com/39627130/155949839-3e771818-8fb0-41d5-8765-f81fdc650289.png" height="100%" width="100%" />

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
Write-ahead logging (WAL) is the key for providing atomicity and durability. All modifications should be written to a log before applied. In TAE,                 REDO log does not need to record every write operation, but it must be recorded when the transaction is committed. We will reduce the usage of io by using the     redo log buffer manager, and avoid any io events for those transactions that are not long and may need to be rolled back due to various conflicts. It can also support long or large transactions.

### Log Entry

#### Entry Layout

<img src="https://user-images.githubusercontent.com/39627130/156019975-ea156716-79fa-4682-ab63-a9cbe0c07f33.png" height="40%" width="40%" />

#### Entry Header Layout
| Item         | Size(Byte) | Scope| Description                                                |
| ------------ | -------- | -----| ------------------------------------------------------------ |
| `GroupId`      | 4        | `All` | Specify the group id          |
| `LSN`         | 8        | `All` | Specify the log sequence number        |
| `Length`     | 4        | `All` | Specify the length of the entry         |
| `Type`       | 1        | `All` | Specify the entry type         |

#### Entry Type

| Type       | Datatype | Value| Description                                                |
| ---------- | -------- | -----| ---------------------------------------------------------- |
| `AC`       | int8     | 0x10 | A committed transaction of complete write operations         |
| `PC`       | int8     | 0x11 | A committed transaction of partial write operations          |
| `UC`       | int8     | 0x12 | Partial write operations of a uncommitted transaction        |
| `RB`       | int8     | 0x13 | Rollback of a transaction                                    |
| `CKP`      | int8     | 0x40 | Checkpoint                                                   |

#### Transaction Payload

A transaction log entry includes multiple nodes, and there are multiple types of nodes. DML node, delete info node, append info node, update info node.

<img src="https://user-images.githubusercontent.com/39627130/156021428-3282eb20-4e69-4ba0-a4ed-ba5409ac7b71.png" height="40%" width="40%" />

##### Meta Layout
| Item         | Size(Byte) |  Scope |  Description                                                |
| ------------ | -------- | -------- | ------------------------------------------------------------ |
| `Id`      | 8      |  `All` |  Specify the transaction id          |
| `InfoLen` | 2      |  `All` |  Specify the length of transaction info   |
| `Info`    | -      |  `All` |  Specify the transaction info        |
| `PrevPtr` | 12     | `UC`,`PC`,`RB` | Specify the previous entry      |
| `NodeCnt` | 4      |  `All` |  Specify the count of nodes          |

##### Node Type
| Type       | Datatype | Value| Description                                                  |
| ---------- | -------- | -----| ------------------------------------------------------------ |
| `DML_A`   | int8     | 0x1  | Append info         |
| `DML_D`   | int8     | 0x2  | Delete info         |
| `DML_U`   | int8     | 0x3  | Update info         |
| `DDL_CT`  | int8     | 0x4  | Create table info       |
| `DDL_DT`  | int8     | 0x5  | Drop table info         |
| `DDL_CD`  | int8     | 0x6  | Create database info    |
| `DDL_DD`  | int8     | 0x7  | Drop database info         |
| `DDL_CI`  | int8     | 0x8  | Create index info         |
| `DDL_DI`  | int8     | 0x9  | Drop index info         |

##### Node Layout
| Item         | Size(Byte) |  Scope | Description                                                |
| ------------ | -------- |  -------- | ------------------------------------------------------------ |
| `Type`      | 1      | `All`     | Specify the type of node        |
| `Len`      | 4      | `All`  | Specify the length of node      |
| `Buf`      | -      | `All`  | Node payload      |

#### Example
Here are 6 concurrent transactions, arranged chronologically from left to right. <img src="https://latex.codecogs.com/svg.image?W_{1-2}" title="W_{1-2}" /> specify the second write operation of <img src="https://latex.codecogs.com/svg.image?Txn_{1}" title="Txn_{1}" />. <img src="https://latex.codecogs.com/svg.image?C_{1}" title="C_{1}" /> specify the commit of <img src="https://latex.codecogs.com/svg.image?Txn_{1}" title="Txn_{1}" />. <img src="https://latex.codecogs.com/svg.image?A_{2}" title="A_{2}" /> specify the rollback of <img src="https://latex.codecogs.com/svg.image?Txn_{2}" title="Txn_{2}" />.

<img src="https://user-images.githubusercontent.com/39627130/156098948-0c442ca1-6386-43a2-907e-22a55c012546.png" height="50%" width="50%" />

As mentioned in the chapter on transactions, all active transactions share a fixed-size memory space, which is managed by the buffer manager. When the remaining space is not enough, some transaction nodes will be unloaded. If it is the first time of the node unload, it will be saved to the redo log as a log entry, and when loaded, the corresponding log entry will be loaded from the redo log.

<img src="https://user-images.githubusercontent.com/39627130/156109116-eb2e5978-f8a1-491b-b13e-b14aa297d418.png" height="80%" width="80%" />

<img src="https://latex.codecogs.com/svg.image?TN_{1-1}" title="TN_{1-1}" /> specify the first transaction node of <img src="https://latex.codecogs.com/svg.image?Txn_{1}" title="Txn_{1}" />. At first, <img src="https://latex.codecogs.com/svg.image?Txn_{1}" title="Txn_{1}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{1-1}" title="TN_{1-1}" /> in buffer manager, and add <img src="https://latex.codecogs.com/svg.image?W_{1-1}" title="W_{1-1}" /> into it.

1. <img src="https://latex.codecogs.com/svg.image?Txn_{2}" title="Txn_{2}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{2-1}" title="TN_{2-1}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{2-1}" title="W_{2-1}" /> into it. Add <img src="https://latex.codecogs.com/svg.image?W_{1-2}" title="W_{1-2}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{1-1}" title="TN_{1-1}" />.
2. <img src="https://latex.codecogs.com/svg.image?Txn_{3}" title="Txn_{3}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{3-1}" title="TN_{3-1}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{3-1}" title="W_{3-1}" /> into it.
3. <img src="https://latex.codecogs.com/svg.image?Txn_{4}" title="Txn_{4}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{4-1}" title="TN_{4-1}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{4-1}" title="W_{4-1}" /> into it. Add <img src="https://latex.codecogs.com/svg.image?W_{2-2}" title="W_{2-2}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{2-1}" title="TN_{2-1}" />. Till now, no log entry generated.

<img src="https://user-images.githubusercontent.com/39627130/156114351-54b40ca9-358a-4df3-b46c-17ea583c963b.png" height="80%" width="80%" />

4. <img src="https://latex.codecogs.com/svg.image?Txn_{5}" title="Txn_{5}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{5-1}" title="TN_{5-1}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{5-1}" title="W_{5-1}" /> into it.
5. <img src="https://latex.codecogs.com/svg.image?Txn_{6}" title="Txn_{6}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{6-1}" title="TN_{6-1}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{6-1}" title="W_{6-1}" /> into it. Add <img src="https://latex.codecogs.com/svg.image?W_{3-2}" title="W_{3-2}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{3-1}" title="TN_{3-1}" />. Add <img src="https://latex.codecogs.com/svg.image?W_{2-3}" title="W_{2-3}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{2-1}" title="TN_{2-1}" />. Add <img src="https://latex.codecogs.com/svg.image?C_{5}" title="C_{5}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{5}" title="TN_{5}" /> and create a log entry for <img src="https://latex.codecogs.com/svg.image?TN_{5-1}" title="TN_{5-1}" />. Add <img src="https://latex.codecogs.com/svg.image?C_{4}" title="C_{4}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{4-1}" title="TN_{4-1}" /> and create a log entry for <img src="https://latex.codecogs.com/svg.image?TN_{4-1}" title="TN_{4-1}" />.
6. Unregister <img src="https://latex.codecogs.com/svg.image?TN_{4-1}" title="TN_{4-1}" /> and <img src="https://latex.codecogs.com/svg.image?TN_{5-1}" title="TN_{5-1}" /> from buffer manager. Before adding <img src="https://latex.codecogs.com/svg.image?W_{3-3}" title="W_{3-3}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{3-1}" title="TN_{3-1}" />, there is no space left. <img src="https://latex.codecogs.com/svg.image?TN_{2-1}" title="TN_{2-1}" /> is choosen as a candidate for eviction and it is unloaded as a log entry. Add <img src="https://latex.codecogs.com/svg.image?W_{3-3}" title="W_{3-3}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{3-1}" title="TN_{3-1}" />. <img src="https://latex.codecogs.com/svg.image?Txn_{2}" title="Txn_{2}" /> register a transaction node <img src="https://latex.codecogs.com/svg.image?TN_{2-2}" title="TN_{2-2}" /> in buffer manager and add <img src="https://latex.codecogs.com/svg.image?W_{2-4}" title="W_{2-4}" /> into it. Add <img src="https://latex.codecogs.com/svg.image?C_{1}" title="C_{1}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{1-1}" title="TN_{1-1}" /> and create a log entry for <img src="https://latex.codecogs.com/svg.image?TN_{1-1}" title="TN_{1-1}" />. Add <img src="https://latex.codecogs.com/svg.image?C_{6}" title="C_{6}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{6-1}" title="TN_{6-1}" /> and create a log entry for <img src="https://latex.codecogs.com/svg.image?TN_{6-1}" title="TN_{6-1}" />. Add <img src="https://latex.codecogs.com/svg.image?W_{2-5}" title="W_{2-5}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{2-2}" title="TN_{2-2}" />. Add <img src="https://latex.codecogs.com/svg.image?A_{2}" title="A_{2}" /> into <img src="https://latex.codecogs.com/svg.image?TN_{2-2}" title="TN_{2-2}" /> and create a log entry for <img src="https://latex.codecogs.com/svg.image?TN_{2-2}" title="TN_{2-2}" /> (For rolled back transaction, log entry is only necessary if some of the previous operations have been persisted).

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
**TAE** uses MVCC to provide snapshot isolation of individual transactions. For SI, the consistent read view of a transaction is determined by the transaction start time, so that data read within the transaction will never reflect changes made by other simultaneous transactions. For example, for <img src="https://latex.codecogs.com/svg.image?Txn-2" title="Txn-2" />, the read view includes <img src="https://latex.codecogs.com/svg.image?[seg-2,&space;seg-4]" title="[seg-2, seg-4]" />, and more fine-grained read view to the block level includes <img src="https://latex.codecogs.com/svg.image?[blk2-1,&space;blk2-3,&space;blk2-5,&space;blk4-1,&space;blk4-3,&space;blk4-5]" title="[blk2-1, blk2-3, blk2-5, blk4-1, blk4-3, blk4-5]" />.

<img src="https://user-images.githubusercontent.com/39627130/155067474-3303ef05-ad1c-4a4c-9bf5-50cb15e41c63.png" height="60%" width="60%" />

**TAE** provides value-level fine-grained optimistic concurrency control, only updates to the same row and same column will conflict. The transaction uses the value versions that exist when the transaction begins and no locks are placed on the data when it is read. When two transactions attempt to update the same value, the second transaction will fail due to write-write conflict.

### Read View

In **TAE**, a table includes multiple segments. A segment is the result of the combined action of multiple transactions. So a segment can be represented as <img src="https://latex.codecogs.com/svg.image?[T_{start},&space;T_{end}]" title="[T_{start}, T_{end}]" /> (<img src="https://latex.codecogs.com/svg.image?T_{start}" title="T_{start}" /> is the commit time of the oldest transaction while <img src="https://latex.codecogs.com/svg.image?T_{end}" /> is the commit time of the newest). Since segment can be compacted to a new segment and segments can be merged into a new segment, We need to add a dimension to the segment representation to distinguish versions <img src="https://latex.codecogs.com/svg.image?([T_{start},T_{end}],&space;[T_{create},T_{drop}])" title="([T_{start},T_{end}], [T_{create},T_{drop}])" /> (<img src="https://latex.codecogs.com/svg.image?T_{create}" title="T_{create}" /> is the segment create time while <img src="https://latex.codecogs.com/svg.image?T_{drop}" title="T_{drop}" /> is the segment drop time). <img src="https://latex.codecogs.com/svg.image?T_{drop}&space;=&space;0" title="T_{drop} = 0" /> means the segment is not dropped. The block representation is as same as the segment <img src="https://latex.codecogs.com/svg.image?([T_{start},T_{end}],&space;[T_{create},T_{drop}])" title="([T_{start},T_{end}], [T_{create},T_{drop}])" />.

A transaction can be represented as <img src="https://latex.codecogs.com/svg.image?[Txn_{start},&space;Txn_{commit}]" title="[Txn_{start}, Txn_{commit}]" /> (<img src="https://latex.codecogs.com/svg.image?Txn_{start}" /> is the transaction start time while <img src="https://latex.codecogs.com/svg.image?T_{commit}" /> is the commit time). The read view of a transaction can be determined by the following formula:

<img src="https://latex.codecogs.com/svg.image?(Txn_{start}&space;\geqslant&space;T_{create})&space;\bigcap&space;((T_{drop}=&space;0)\bigcup&space;(T_{drop}>Txn_{start}))" title="(Txn_{start} \geqslant T_{create}) \bigcap ((T_{drop} = 0)\bigcup (T_{drop}>Txn_{start}))" />

When a transaction is committed, it is necessary to obtain a read view related to the commit time for deduplication:

<img src="https://latex.codecogs.com/svg.image?(Txn_{commit}&space;\geqslant&space;T_{create})&space;\bigcap&space;((T_{drop}=&space;0)\bigcup&space;(T_{drop}>Txn_{commit}))" title="(Txn_{commit} \geqslant T_{create}) \bigcap ((T_{drop}= 0)\bigcup (T_{drop}>Txn_{commit}))" />

For example, the read view of <img src="https://latex.codecogs.com/svg.image?Txn-2" title="Txn-2" /> includes <img src="https://latex.codecogs.com/svg.image?[seg1]" title="[seg1]" /> while the read view during commit includes <img src="https://latex.codecogs.com/svg.image?[seg1,seg2,seg3]" title="[seg1,seg2,seg3]" />.

<img src="https://user-images.githubusercontent.com/39627130/154995795-2c367e33-bafa-4e47-812d-f80d82594613.png" height="100%" width="100%" />

The block read view is similar to segment.

### Concurrent Compaction
Compaction is needed for space efficiency, read efficiency, and timely data deletion. In **TAE**, the following scenarios require compaction：

- <img src="https://latex.codecogs.com/svg.image?Block_{L_{0}}&space;\overset{sort}{\rightarrow}&space;Block_{L_{1}}" title="Block_{L_{0}} \overset{sort}{\rightarrow} Block_{L_{1}}" />. When inserting data, it first flows to L0 in an unordered manner. After certain conditions are met, the data will be reorganized and flowed to L1, sorted by the primary key.
- <img src="https://latex.codecogs.com/svg.image?\{Block_{L_{1}},...\}&space;\overset{merge}{\rightarrow}Segment_{L_{2}}" title="\{Block_{L_{1}},...\} \overset{merge}{\rightarrow}Segment_{L_{2}}" />. Multiple L1 blocks are merge-sorted into a L2 segment.
- <img src="https://latex.codecogs.com/svg.image?Block_{L_{2}}&space;\overset{compact}{\rightarrow}&space;Block_{L_{2}}" title="Block_{L_{2}} \overset{compact}{\rightarrow} Block_{L_{2}}" />. If there are many updates to a L2 block and it is needed to compact the block to a new block to improve read efficiency.
- <img src="https://latex.codecogs.com/svg.image?Segment_{L_{2}}&space;\overset{compact}{\rightarrow}&space;Segment_{L_{2}}" title="Segment_{L_{2}} \overset{compact}{\rightarrow} Segment_{L_{2}}" />. If there are many updates to a L2 segment and it is needed to compact the block to a new segment to improve read efficiency.
- <img src="https://latex.codecogs.com/svg.image?\{Segment_{L_{2}}&space;...\}&space;\overset{merge}{\rightarrow}&space;Segment_{L_{2}}" title="\{Segment_{L_{2}} ...\} \overset{merge}{\rightarrow} Segment_{L_{2}}" />. Multiple L2 segments are merge-sorted into a L2 segment.

#### Block Sort Example

<img src="https://user-images.githubusercontent.com/39627130/155315545-0ec97d65-b716-4c30-9a00-e9bddcfaea2d.png" height="100%" width="100%" />

<img src="https://latex.codecogs.com/svg.image?Block1_{L_{0}}" title="Block1_{L_{0}}" /> is created @ <img src="https://latex.codecogs.com/svg.image?t_{1}" title="t_{1}" />, which contains data from <img src="https://latex.codecogs.com/svg.image?\{Txn1,Txn2,Txn3,Txn4\}" title="\{Txn1,Txn2,Txn3,Txn4\}" />. <img src="https://latex.codecogs.com/svg.image?Block1_{L_{0}}" title="Block1_{L_{0}}" /> starts to sort @ <img src="https://latex.codecogs.com/svg.image?t_{11}" title="t_{11}" />，and its block read view is the baseline plus an uncommitted update node, which will be skipped. Sort and persist a block may take a long time. There are two committed transactions <img src="https://latex.codecogs.com/svg.image?\{Txn5,Txn6\}" title="\{Txn5,Txn6\}" /> and one uncommitted <img src="https://latex.codecogs.com/svg.image?\{Txn7\}" title="\{Txn7\}" /> before commiting sorted <img src="https://latex.codecogs.com/svg.image?Block2_{L_{1}}" title="Block2_{L_{1}}" />. When commiting <img src="https://latex.codecogs.com/svg.image?\{Txn7\}" title="\{Txn7\}" /> @ <img src="https://latex.codecogs.com/svg.image?t_{16}" title="t_{16}" />, it will fail because <img src="https://latex.codecogs.com/svg.image?Block1_{L_{0}}" title="Block1_{L_{0}}" /> has been terminated. Update nodes <img src="https://latex.codecogs.com/svg.image?\{Txn5,Txn6\}" title="\{Txn5,Txn6\}" /> that were committed in between <img src="https://latex.codecogs.com/svg.image?(t_{11},&space;t_{16})" title="(t_{11}, t_{16})" /> will be merged into a new update node and it  will be committed together with <img src="https://latex.codecogs.com/svg.image?Block2_{L_{1}}" title="Block2_{L_{1}}" /> @ <img src="https://latex.codecogs.com/svg.image?t_{16}" title="t_{16}" />.

![image](https://user-images.githubusercontent.com/39627130/155317195-483f7b67-48b1-4474-8555-315805492204.png)

#### Compaction As A Transactional Schema Change
A compaction is the termination of a series of blocks or segments, while atomically creating a new one (building index). It usually takes a long time compared to normal transactions and we don't want to block update or delete transactions on involved blocks or segments. Here we extend the content of the **read view** to include the metadata of blocks and segments into it. When commiting a normal transaction, once it is detected that the metadata of blocks (segments) corresponding to write operation has been changed (committed), it will fail.

For a compaction transaction, write operations include block (segment) soft deletion and addition. During the execution of the transaction, each write will detect a write-write conflict. Once there is a conflict, the transaction will be terminated in advance.

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
