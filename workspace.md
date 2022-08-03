## Workspace
A workspace cache all historical operations of this transaction.

### CN
1. Bind a workspace for each transaction
2. Before committing a transaction, any active abort only cleanup this workspace.
3. On committing a transaction, push all accumulated changes to the relevant `DN` and execute 2PC commit process.

### DN
Workspace is created on committing.
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

## DDL

### Create Database

1. Fetch the catalog snapshot from a `DN` at the first time
```
       +-----+         +-----+         +-----+
       | DB1 |         | DB2 |         | DB3 |
       +--+--+         +-----+         +-----+
```
2. Check unique constraints base on catalog the snapshot. Return duplicated error if violiated.
3. Fetch a unique database id and create a database entry
```go
type DBEntry struct {
    // Unique identity
    Id uint64
    // Database name: should be unique
    Name string
    // Create timestamp
    CreatedAt []byte
    // Delete timestamp
    DeletedAt []byte
}
```
4. Actively Abort
   Cleanup workspace only
5. Commit
   2PC commit process. Push all accumulated changes to the relevant `DN`

### Drop Database

1. Fetch the catalog snapshot from a `DN` at the first time
2. Find the database entry base on the catalog snapshot. Return not-found error if not found.
3. Update the entry as deleted
4. Actively Abort
   Cleanup workspace only
5. Commit
   2PC commit process. Push all accumulated changes to the relevant `DN`

### Create|Drop table

Almost same as Create|Drop database

## DML

### Insert

> CN-Workspace
1. Fetch the metadata snapshot and all cached data from the relevant `DN` at the first time
2. Dedup on the workspace local store
3. Dedup on the snapshot
4. Append to the workspace local store
5. Actively Abort
   Cleanup workspace only
6. Commit
   2PC commit process. Push all accumulated changes to the relevant `DN`
> DN-Workspace
1. Cache all writes
2. In PrePrepareCommit, push all append nodes to the statemachine. Do delta dedup.

### Delete

> CN-Workspace
1. Add delete node to the workspace local store
2. Actively Abort
   Cleanup workspace only
3. Commit
   2PC commit process. Push all accumulated changes to the relevant `DN`
> DN-Workspace
1. Cache all writes
2. In PrePrepareCommit, push all delete nodes to the statemachine.

## DQL

### Scan Table

> CN-Workspace
1. Fetch the metadata snapshot and all cached data from the relevant `DN` at the first time
2. Provide a block-iterator
   - Workspace local store block
   - Snapshot blocks
     - In-memory block
     - Remote block (base block + delete file)
     - Remote block + in-memory delta

## Scenarios

### S-A

#### Description

Database name is "DBA", Table name is "TBLA". Snapshot is of PK="1".
1. Try delete PK="1"
   - Scan and find physical address of PK="1"
   - Delete by physical address
2. Insert a tuple with PK="1"
3. Scan
4. Commit

#### Steps In Workspace
1. Get the database snapshot from one `DN` when build the plan. Store it in the transaction workspace
```
                                         +----------+
                                 +------>|   TBLA   |
                                 |       +----------+
                                 |       +----------+
                                 |------>|   TBLB   |
                                 |       +----------+
                                 |       +----------+
                                 |------>|   TBLC   |
                                 |       +----------+
    +----------------+           |       +----------+
    |       DBA      | ----------+------>|   TBLD   |
    +----------------+           |       +----------+
                                 |       +----------+
                                 |------>|   TBLE   |
                                 |       +----------+
                                 |       +----------+
                                 |------>|   TBLF   |
                                 |       +----------+
                                 |       +----------+
                                 +------>|   TBLG   |
                                         +----------+

```
2. Get the metadata snapshot and log tail of `TBLA` from `DN`. Store them in the transaction workspace
3. Scan on the metadata snapshot
   Refer [metadata]() for details.
   Snapshot
   ```
   +----------------------------------------------------------------------+
   |                           Metadata Snapshot                          |
   +--------------+   +--------------+   +--------------+  +--------------+
   |   MetaInfo   |   |   MetaInfo   |   |   MetaInfo   |  |   MetaInfo   |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+
   | 1 |xx/1|     |   | 2 |xx/2|yy/2 |   | 3 |xx/3|yy/3 |  | 4 |    |     |
   +---+----+-----+   +---+----+-----+   +---+----+-----+  +---+----+-----+
   ```
   Log tail
   ```
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
   **TODO**: Build ART tree for unsorted block in `CN`?
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
