- Feature Name: Distributed TAE
- Status: In Progress
- Start Date: 2022-08-06
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Summary

Here we will only discuss some design|concept changes compared to the stand-alone `TAE`.

# Guild-level Design

## Metadata

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

### Detailed Metadata

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

### Shared Metadata

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

### Metadata Snapshot

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

## Object Key

### Key Prefix

- `10`: Checkpoint prefix
- `20`: Data range prefix

### Key Encoding

#### Checkpoint
```
  10/$shard/$ckpTs
  |   |      |
  |   |      +---- Checkpoint timestamp
  |   +----------- Shard id
  +--------------- Prefix
```
#### Data Range
```
  20/$shard/$ckpTs/$startTs_$endTs
  |   |       |      |        |
  |   |       |      |        +------- Range end timestamp
  |   |       |      +---------------- Range start timestamp
  |   |       +----------------------- Last checkpoint timestamp
  |   +------------------------------- Shard id
  +----------------------------------- Prefix
```
### Booting

1. List all checkpoints of shard 1 `10/1`
```
|-- 10/1
|    |-- 1
|    |-- 30
|    |-- 60

Max checkpoint is 10/1/60
```
2. List `11/1/60`
```
|-- 11/1/60
|      |-- 61_70
|      |-- 71_80
|      |-- 81_90

Max range is 11/1/60/81_90
```
3. Load checkpoint `11/1/60` and relevant ranges
4. Apply all catalog and metadata related changes from the ranges to the checkpoint
5. Start replay from WAL

## DN Engine

### Mutable|Immutable Buffer
A buffer is a representation of a range of the log. Imutable buffer is a frozen mutable buffer
```
                       <Freeze>
[MutableBuffer[0,10]] ---------> [ImmutableBuffer[10,10]]

```
1. At most one mutable buffer and all buffers are orgnized into a sorted list
   ```
   [MutableBuffer[100,110]]
         |
        \|/
   [ImmutableBuffer[60,99]]
         |
        \|/
   [ImmutableBuffer[30,59]]
   ```
2. Components in a buffer
   ```
      [MutableBuffer|ImutableBuffer]
                    |
                    |
       +------------+---------------+
       |            |               |
   +---+-----+ +----------+   +-----+-----------+
   |  Cmds   | |  ABlock  |   | PerBlk DelNodes |
   +---+-----+ +----+-----+   +-----+-----------+
       |            |
   +-------+     +--+-----+-------+
   |       |     |        |       |
   +---+---+ +---+---+ +--+---+ +-+---+
   |Pointer| |Command| |ANodes| |Index|
   +-------+ +-------+ +------+ +-----+
   ```
3. The immutable buffer will be enqueued to the checkpoint queue
   ```
   [MutableBuffer[100,110]]
         |
        \|/
   [ImmutableBuffer[60,99]]
         |
        \|/                     ------------
   [ImmutableBuffer[30,59]] -> ( )  queue   )
                                ------------
   ```
4. If a immutable buffer is checkpointed, delete the buffer from the buffer list
   ```
   [MutableBuffer[100,110]]
         |
        \|/
   [ImmutableBuffer[60,99]]
         |
         x
   ```
### Buffer Checkpoint
1. Prepare object contents
   - Merge sort all appendable blocks into some new blocks per table
   - Marshal all block deletes
     ```
     [Block-1 ] --- [Delete Buffer]
     [Block-4 ] --- [Delete Buffer]
     [Block-10] --- [Delete Buffer]
     ```
   - Marshal commands
2. Push the prepared object as a range object to the store
   ```
   20/$shard/30_59
   ```
3. Commit
4. Checkpoint relevant LSNs

### Checkpoint
1. Select a existed buffer checkpoint `[100,130]`
2. Prepare a checkpoint object
   - Catalog and metadata snapshot
3. Push to the store
   ```
   10/$shard/130
   ```
4. Update the local checkpoint list
   ```
   [130] --> [90] --> [20]
   ```
### Snapshot Read
1. Find the closest checkpoint to the snapshot timestamp
   ```
   Checkpoints:        [130] --> [90] --> [20]

   Snapshot:           120
   Working Checkpoint: [90]

   Snapshot:           150
   Working Checkpoint: [130]
   ```
2. Collect the checkpointed ranges
   ```
   Checkpoints:        [130] --> [90] --> [20]
   Ranges:             [131-140] --> [111-130] --> [91-110] --> [51-90] --> [31-50] --> [21-30]

   Snapshot:           120
   Working Checkpoint: [90]
   Working Ranges:     {[91-110],[111-130]}, MaxRange=130

   Snapshot:           150
   Working Checkpoint: [130]
   Working Ranges:     {[131-140]}, MaxRange=140
   ```
3. If the snapshot is less than the `MaxRange`. Return
   ```
   Snapshot:          120
   Working Checkpint: [90]
   Working Ranges:    {[91-110],[111-130]}, MaxRange=130
   ```

4. If the snapshot is large than the `MaxRange`
   ```
   Snapshot:           150
   Working Checkpoint: [130]
   Working Ranges:     {[131-140]}, MaxRange=140
   ```
   - Collect the active log tail
   ```
   [MutableBuffer[161-170]] --> [ImmutableBuffer[141-160]]
   Working Buffers: [141-160]

   Collect tail from 141 to 150 as Commands[141-150]. Expensitive operation!
   ```
   - Return
   ```
   Snapshot:           150
   Working Checkpint:  [130]
   Working Ranges:     {[131-140]}, MaxRange=140
   Commands:           [141-150]
   ```
### Range Read
```go
type RangeRequest struct {
    Snapshot Timestamp
    FromTS   Timestamp
}
```
1. If the snapshot is less than the checkpointed `MaxRange`
   ```
   Working Checkpint:         [90]
   Working Ranges Candidates: {[91-110],[111-130]}, MaxRange=130

   Request:                   Snapshot=120,FromTs=100
   Working Ranges:            {[91-110],[111-130]}

   Request:                   Snapshot=120,FromTs=115
   Working Ranges:            {[111-130]}
   ```
2. If the snapshot is larger than the checkpointed
   ```
   Working Checkpint:         [130]
   Working Ranges Candidates: {[131-140]}, MaxRange=140
   Commands Candidates:       [141-150]

   Request:                   Snapshot=150,FromTs=132
   Working Ranges:            {[131-140]}
   Commands:                  [141-150]

   Request:                   Snapshot=150,FromTs=142
   Working Ranges:            {}
   Commands:                  [142-150]
   ```
### Workspace
Workspace is only created on committing.
1. `PrePrepareCommit`: try push changes to statemachine
   - Any error, go to `PrepareRollback`
   - Else, go to `PrepareCommit`
2. `PrepareCommit`
   - Bind prepare timestamp
   - Confliction check. Any error, go to `PrepareRollback`
   - Build WAL entry
   - Append to WAL
   - Enqueue flush waiting queue
3. `PrepareRollback`
   - Notify coordinator aborted
   - Enqueue commit waiting queue
4. Wait WAL
   - Notify coordinator prepared
   - Enqueue commit waiting queue
5. Wait Committed|Aborted
   - `ApplyCommit` if committed
   - `ApplyRollback` if aborted

### Logstore

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

## CN Engine

### Shard Checkpoints

Each `CN` maintains visible checkpoint timestamps
```
// Timestamp is sorted from new to old
[Shard1]: [TS10]-->[TS5]-->[TS1]
[Shard2]: [TS15]-->[TS8]-->[TS1]
```
Once a transaction see a checkpoint timestamp larger than the max timestamp, insert into the list, which will be asynchronously pruned when appropriate.

One cache item per checkpoint timestamp:
```
CackeKey: $shard/CKP/$timestamp
CacheObject: Buffer Object
```
A checkpoint cache item is first inited with checkpoint data from `10/$shard/$timestamp`
```
BufferObject [$startTs,$endTs]
   |            |        |
   |            |        +--- The ending timestamp of the data representation. It is the checkpoint timetsamp when inited
   |            +------------ The starting timestamp of the data representation. Always zero here
   +------------------------- Cache item runtime data structure
```

A checkpoint cache item can apply data ranges
```
// A bufferObject applies a data range
BufferObject[Ckp=80][0, 100] + Range[Ckp=80][90, 110] = BufferObject[Ckp=80][0, 110]
BufferObject[Ckp=80][0, 100] + Range[Ckp=80][85,  95] = BufferObject[Ckp=80][0, 100]
BufferObject[Ckp=80][0, 100] + Range[Ckp=80][110,120] ==> Error
```
### Workspace

#### Scenarios

##### S-A

###### Description

Database name is `DBA`, Table name is `TBLA`. Snapshot is `100`, which is of `PK=1`.
1. Try delete `PK=1`
   - Scan and find physical address of `PK=1`
   - Delete by physical address
2. Insert a tuple with `PK=1`
3. Scan one column
4. Commit

###### Steps in Workspace
1. Scan shard checkpoints and decide a checkpoint timestamp
   ```
   [Shard1]: [80]-->[60]-->[30]-->[20]

   Snapshot:                    70
   Shard1 Checkpoint timestamp: 60

   Snapshot:                    100
   Shard1 Checkpoint timestamp: 80
   ```
2. Get the checkpoint[80] cache item. If exists, pin it.
   ```
   CacheKey:      '$shard:CKP:80'
   CacheObject:   Buffer Object
   Current Range: [81,90]
   ```
3. Range read [91,100] to `DN` to collect the log tail

   Refer to [Range Read](#range-read) for details.
   ```
   Range Read Response[91,100]:

   Working Checkpoint: [80]
   Working Ranges:     [91-95]
   Commands:           [96,100]
   ```
4. Get the range read response `[91,100]` and try to update the cache item `$shard:CKP:80`
   ```
   1. Get commands[91-95] from checkpinted ranges [91-95]
   2. Apply commands[91,95] to the cache item
   3. Apply commands[96,100] in the range read response to the cache item
   ```
5. Scan on the `$shard:CKP:80` cache item

   Refer [metadata](#metadata) for details.
   Metadata Snapshot
   ```
   +----------------------------------------------------------------------+
   |                           Metadata Snapshot                          |
   +--------------+   +--------------+   +--------------+  +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |  |   MetaInfo   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |xx/3|yy/3 |  | 4 |    |     |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+

   +-------------+
   | Deletes Map |
   +-------------+            +---------+
   |      1      |----------> | DelMask |
   +-------------+            +---------+
   |      3      |----------> | DelMask |
   +-------------+            +---------+

   +-------------+
   |   Data Map  |
   +-------------+          +---------+
   |      4      |--------> |  Batch  |
   +-------------+    |     +---------+
                      |     +-----------+
                      |     |  Zonemap  |
                      +---> +-----+-----+
                            | Min | Max |
                            +-----+-----+
   ```
   Pseudocode
   ```
   def ScanCol($colIdx):
     for $metaInfo in $snapshot:
       if $metaInfo.BaseLoc != "":
         $blkMeta = $cache.Get($metaInfo.BaseLoc)
         $data = $cache.Get($blkMeta[$colIdx].DataLoc)
       else:
         $data = $dataMap.Load($metaInfo.Id)
       if $metaInfo.DeltaLoc != "":
         $roDels = $cache.Get($metaInfo.DeltaLoc)
       $deletes = $deletesMap.Load($metaInfo.Id)
       $deletes = $deletes.Or($roDels)
       if $deletes == None:
         return $data
       $cloned = $data.Clone()
       $cloned.ApplyDeletes($deletes)
       return $cloned
   ```
4. Delete by physical address

   For example, delete row 10 on block 2
   ```
   +-------------+
   | Deletes Map |
   +-------------+            +---------+
   |      1      |----------> | DelMask |
   +-------------+            +---------+
   |      3      |----------> | DelMask |
   +-------------+            +---------+
   |      2      |----------> |   [10]  | --------- Newly added
   +-------------+            +---------+
   ```
5. Dedup
   ```
   def Dedup($pk):
     for $metaInfo in $snapshot:
        if $metaInfo.BaseLoc != "":
          $blkMeta = $cache.Get($metaInfo.BaseLoc)
          $pkMeta = $blkMeta[$pkIdx]
          if $pk < $pkMeta.Min or $pk > $pkMeta.Max:
            continue
          $bf = $cache.Get($pkMeta.BfLoc)
          if not $bf.MayContains($pk):
            continue
          $data = $cache.Get($pkMeta.DataLoc)
          if not $data.Find($pk):
            continue
          if $pk is in deletes map:
            continue
          return Duplicated
        else:
          $data = $dataMap.Load($metaInfo.Id)
          $data apply deletes
          if not $data.Find($pk):
            continue
          return Duplicated

        return Ok
   ```
6. Append a new tuple
   - Add a transient block in the workspace
   ```
   +----------------------------------------------------------------------------------------+
   |                                    Metadata Snapshot                                   |
   +--------------+   +--------------+   +--------------+  +--------------+  +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |  |   MetaInfo   |  |  MetaInfo    |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+  +-----+----+---+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |xx/3|yy/3 |  | 4 |    |     |  |Tid+0|    |   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+  +-----+-+--+---+
                                                                                     |
                                                                                     |
                                                                              Transient block
   ```
   - Append the tuple into the transient block
   ```
   +-------------+
   |   Data Map  |
   +-------------+            +---------+
   |      4      |------+---> |  Batch  |
   +-------------+      |     +---------+
   |    Tid+0    |---+  |     +-----------+
   +-------------+   |  |     |  Zonemap  |
                     |  +---> +-----+-----+
                     |        | Min | Max |
                     |        +-----+-----+
                     |        +---------+
                     |------> |  Batch  |
                     |        +---------+
                     |        +-----------+
                     |        |   Zonmap  |
                     +------> +-----+-----+
                              | Min | Max |
                              +-----+-----+
   ```
7. Scan one column. Same as step 3.
8. Commit
   - PreCommit
     - Collect delete nodes and transient blocks as commands
     - Send collected commands to the relevant `DN`
   - DoCommit

#### S-B

##### Description

Database name is "DBA", table name is "TBLA".
1. Insert tuples
2. Bulk load a data block
3. Delete a tuple
4. Commit

##### Steps In Workspace

1. Work on a snapshot as same as `S-A`
   ```
   +----------------------------------------------------+
   |                Metadata Snapshot                   |
   +--------------+   +--------------+   +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |    |     |
   +---+----+-----+   +---+----+-----+   +---+----+-----+

   +-------------+
   | Deletes Map |
   +-------------+            +---------+
   |      1      |----------> | DelMask |
   +-------------+            +---------+

   +-------------+
   |   Data Map  |
   +-------------+          +---------+
   |      3      |--------> |  Batch  |
   +-------------+    |     +---------+
                      |     +-----------+
                      |     |  Zonemap  |
                      +---> +-----+-----+
                            | Min | Max |
                            +-----+-----+
   ```
2. Dedup
3. Append tuples
   ```
   +-----------------------------------------------------------------------+
   |                           Metadata Snapshot                           |
   +--------------+   +--------------+   +--------------+   +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+   +-----+---+----+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |    |     |   |Tid+0|   |    |
   +---+----+-----+   +---+----+-----+   +---+----+-----+   +-----+---+----+

   +-------------+
   |   Data Map  |
   +-------------+            +---------+
   |      3      |------+---> |  Batch  |
   +-------------+      |     +---------+
   |    Tid+0    |----+ |     +-----------+
   +-------------+    | |     |  Zonemap  |
                      | +---> +-----+-----+
                      |       | Min | Max |
                      |       +-----+-----+
                      |       +---------+
                      |-----> |  Batch  |
                      |       +---------+
                      |       +-----------+
                      |       |   Zonmap  |
                      +-----> +-----+-----+
                              | Min | Max |
                              +-----+-----+
   ```
5. Load a data block
   - Dedup
   ```
    1. Fetch the block zonemap and bloomfilter
    2. Dedup on each block of a snapshot
   ```
   - Add into the metadata snapshot
   ```
   +------------------------------------------------------------------------------------------+
   |                                       Metadata Snapshot                                  |
   +--------------+   +--------------+   +--------------+   +--------------+   +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+   +-----+---+----+   +-----+--+-----+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |    |     |   |Tid+0|   |    |   |Tid+1|  |     |
   +---+----+-----+   +---+----+-----+   +---+----+-----+   +-----+---+----+   +-----+--+-----+

   +-------------+
   |   Data Map  |
   +-------------+            +---------+
   |      3      |------+---> |  Batch  |
   +-------------+      |     +---------+
   |    Tid+0    |----+ |     +-----------+
   +-------------+    | |     |  Zonemap  |
   |    Tid+1    |-+  | +---> +-----+-----+
   +-------------+ |  |       | Min | Max |
                   |  |       +-----+-----+
                   |  |       +---------+
                   |  |-----> |  Batch  |
                   |  |       +---------+
                   |  |       +-----------+
                   |  |       |   Zonmap  |
                   |  +-----> +-----+-----+
                   |          | Min | Max |
                   |          +-----+-----+
                   |          +---------+
                   |--------> |  Batch  |
                   |          +---------+
                   |          +-----------+
                   |          |   Zonmap  |
                   |--------> +-----+-----+
                   |          | Min | Max |
                   |          +-----+-----+
                   |          +-----------+
                   +--------> |  BFIndex  |
                              +-----------+
   ```
6. Delete a tuple
   - Scan by filter and get a matched tuple on block `Tid+1`
   - Delete by physical address
   ```
   +-------------+
   | Deletes Map |
   +-------------+            +---------+
   |    Tid+1    |----------> | DelMask |
   +-------------+            +---------+
   ```

## BufferObject
**TODO**

## Open Questions

### CN sync the log tail at table granularity?

#### Pros
1. Less data to sync
2. Avoid some unnecessary blocking waiting
   ```
   DN:
   TableA ------ [Txn100 Preparing] --> [Txn90 Committed] --> [Txn75 Committed] --> [Txn60 Aborted]
   TableB ------ [Txn93 Committed]  --> [Txn55 Committed] --> [Txn30 Aborted]   --> [Txn10 Committed]

   // Return immediately if only read TableB
   Range Read:         Snapshot=120, FromTs=95, Tables=[TableB]
   Commands:           []

   // Wait Txn100 committed|aborted if read all
   Range Read:         Snapshot=120, FromTs=95, Tables=[*]
   Commands:           [?]
   ```
#### Cons
1. Need fine-grained checkpoint cache implemetation in `CN`
   ```
   BufferObject[Ckp=80]
   Catalog                [0, 120]
   TableA                 [0,  90]
   TableB                 [0, 120]
   TableC                 [0, 110]
   ```
2. Multiple interactions are required between `CN` and `DN`
   - Bind different tables during the execution of statements
