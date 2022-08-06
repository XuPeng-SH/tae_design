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

Metadata snapshot is a collection of meta info of data fragmentations. It mainly consists of two parts:
1. Metadata checkpoint
2. Delta from checkpoint

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
