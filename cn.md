# CN Engine

## Shard Checkpoints

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
## Workspace

### Scenarios

#### S-A

##### Description

Database name is `DBA`, Table name is `TBLA`. Snapshot is `100`, which is of `PK=1`.
1. Try delete `PK=1`
   - Scan and find physical address of `PK=1`
   - Delete by physical address
2. Insert a tuple with `PK=1`
3. Scan one column
4. Commit

##### Steps in Workspace
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
3. Range read [91,100] `DN` to collect the log tail

   Refer to [Range Read]() for details.
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

   Refer [metadata]() for details.
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

### S-B

#### Description

Database name is "DBA", table name is "TBLA".
1. Insert tuples
2. Bulk load a data block
3. Delete a tuple
4. Commit

#### Steps In Workspace

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
