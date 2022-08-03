# Metadata

Metadata records fragmentation information of table data
```
                     +---------------------------+
                     |           MetaInfo        |
                     +----------+-------+--------+
                     |Id(uint64)|BaseLoc|DeltaLoc|
                     +----------+---+---+---+----+
                                    |       |
                                    +---+---+
                                        |
              +-------------------------+-----------------------------------+
              |                         Location                            |
              +-----------+-----------+-----------+------------+------------+
              |Key(string)|Off(uint32)|Len(uint32)|OLen(uint32)|Shared(bool)|
              +-----------+-----------+-----------+------------+------------+

              MetaInfo: table data fragmentation information
              Id = fragnentation id
              BaseLoc = fragmentation data detailed metadata location
              DeltaLoc = fragmentation delta data location

              Location: data location info
              Key = location key
              Off = offset in the buffer
              Len = io size
              OLen = original data size
              Shared = shared buffer

                          +-------------------+
                          |      IO Entry     |
 +----------+             +-------------------+                   +----------+
 | MetaInfo |-----------> | Detailed Metadata | ----------+---+-->| IO Entry |
 +----------+             +-------------------+           |   |   +----------+
                                                          |   |   +----------+
                                                          |   +-->| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          +------>| IO Entry |
                                                                  +----------+
```

## Detailed Metadata

```
 +-------------------------------------------------------------------------------+
 |                                  BlockMeta                                    |
 +---------------+-----------------+---------------+------------+---+------------+
 |TableId(uint64)|SegmentId(uint64)|BlockId(uint64)|<ColumnMeta>|...|<ColumnMeta>|
 +---------------+-----------------+---------------+------+-----+---+------------+
                                                          |
                                                          |   +---------------------------------------------------------------+
                                                          |   |                       SegmentMeta                             |
                                                          |   +---------------+-----------------+------------+---+------------+
                                                          |   |TableId(uint64)|SegmentId(uint64)|<ColumnMeta>|...|<ColumnMeta>|
                                                          |   +---------------+-----------------+------+-----+---+------------+
                                                          |                                            |
                                                          |                                            |
                                                          |--------------------------------------------+
                                                          |
                                                          |
                      +-----------------------------------+-------------------------------------+
                      |                               ColumnMeta                                |
                      +-----------+-----------------+-------------+-------------+---------------+
                      |Idx(uint16)|DataLoc(Location)|Min([32]byte)|Max([32]byte)|BFLoc(Location)|
                      +-----------+-----------------+-------------+-------------+---------------+

BlockMeta: Block data meta info

SegmentMeta: Segment data meta info

ColumnMeta: Column data meta info
Idx = Column index
DataLoc = Column data location
Min = Column min value
Max = Column max value
BFLoc = Bloomfilter data location
```

## Shared Metadata

> Non-shared

```
                          +-------------------+
                          |      IO Entry     |
 +----------+             +-------------------+                   +----------+
 | MetaInfo |-----------> | Detailed Metadata | ----------+---+-->| IO Entry |
 +----------+             +-------------------+           |   |   +----------+
                                                          |   |   +----------+
                                                          |   +-->| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          +------>| IO Entry |
                                                                  +----------+
                          +-------------------+
                          |      IO Entry     |
 +----------+             +-------------------+                   +----------+
 | MetaInfo |-----------> | Detailed Metadata | ----------+---+-->| IO Entry |
 +----------+             +-------------------+           |   |   +----------+
                                                          |   |   +----------+
                                                          |   +-->| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          +------>| IO Entry |
                                                                  +----------+

                          +-------------------+
                          |      IO Entry     |
 +----------+             +-------------------+                   +----------+
 | MetaInfo |-----------> | Detailed Metadata | ----------+---+-->| IO Entry |
 +----------+             +-------------------+           |   |   +----------+
                                                          |   |   +----------+
                                                          |   +-->| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          +------>| IO Entry |
                                                                  +----------+
```

> Shared

```
                                                                  +----------+
                                                          +------>| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          |------>| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          |------>| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          |------>| IO Entry |
                                                          |       +----------+
 +----------+                    Shared                   |       +----------+
 | MetaInfo |-----+       +-------------------+           |------>| IO Entry |
 +----------+     |       |      IO Entry     |           |       +----------+
 +----------+     |       +-------------------+           |       +----------+
 | MetaInfo |-----+-----> | Detailed Metadata | ----------+------>| IO Entry |
 +----------+     |       +-------------------+           |       +----------+
                  |                                       |       +----------+
 +----------+     |                                       |------>| IO Entry |
 | MetaInfo |-----+                                       |       +----------+
 +----------+                                             |       +----------+
                                                          |------>| IO Entry |
                                                          |       +----------+
                                                          |       +----------+
                                                          +------>| IO Entry |
                                                                  +----------+
```

## Metadata Snapshot

Metadata snapshot is a collection of meta info of data fragmentations.

### Option 1

It mainly consists of two parts:
1. Metadata checkpoint
2. Delta from checkpoint

`DN` find a checkpoint closest to the snapshot, and then collect all modifications from the timestamp of the checkpoint to the snapshot.

```
  <Checkpoint>
+==============+
|   IO Entry   |
+==============+
|   MetaInfo   |
+--------------+        <Delta>
|   MetaInfo   |     +-----------+
+--------------+  +  |  MetaInfo |
|     .....    |     +-----------+
+--------------+     |  .....    |
|     .....    |     +-----------+
+--------------+     |  MetaInfo |
|   MetaInfo   |     +-----------+
+--------------+
```

### Option 2

`DN` collect all metainfo of a specified snapshot and send it to `CN`

## Metadata in CN

1. No global metadata is maintained in `CN` and it always fetches metadata on demand from `DN`
2. For any transaction, a metadata snapshot is maintained in the transaction workspace
3. Checkpoint in snapshot is shared by many transactions and it should not be updated in-place
4. How to use a snapshot without in-place updates and copy? - **TODO**

## Metadata in DN

1. A global metadata is maintained in `DN`
2. It retains various checkpoints info in memory and provides query services to `CN`
3. `DN` should retains latest metadata commands in memory at least from the last checkpoint. How?- **TODO**
