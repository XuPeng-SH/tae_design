# DN Engine

## Catalog
1. Keep the latest catalog in memory
2. Collect changes from last checkpoint and persists to object store
   ```
   11/$shard/$ckpTs/$starts_$endTs
   ```
3. Checkpoint to object store
   ```
   10/$shard/$ckpTs
```
## Metadata
1. Keep the latest metadata in memory
2. Collect changes from last checkpoint and persists to object store
   ```
   21/$shard/$ckpTs/$starts_$endTs
   ```
3. Checkpoint to object store
   ```
   20/$shard/$ckpTs
   ```
## Mutable Buffer
1. Per-table mutable buffer
   ```
   [TableA] ---- [MutableBuffer]
   ```
2. A representation of the log tail of a table
   ```
                                         EndTs is mutable
                                          |
   [TableA] ---- [MutableBuffer[StartTs, EndTs]]
   ```
3. Components in a buffer
   ```
              [MutableBuffer]
                    |
                    |
       +------------+---------------+
       |            |               |
   +---+-----+ +----------+   +-----+-----------+
   |   Cmds  | |  ABlock  |   | PerBlk DelNodes |
   +---+-----+ +----+-----+   +-----+-----------+
       |            |
   +-------+     +--+-----+-------+
   |       |     |        |       |
   +---+---+ +---+---+ +--+---+ +-+---+
   |Pointer| |Command| |ANodes| |Index|
   +-------+ +-------+ +------+ +-----+
```

## Immutable Buffer
1. Per-table immutable buffer
2. A immutable buffer is a frozen mutable buffer and orgnized in a list
   ```
   [TableA] ---- [MutableBuffer[100, 110]]
                     |
                    \|/
                 [ImmutableBuffer[60, 99]]
                     |
                    \|/
                 [ImmutableBuffer[30, 59]]
   ```
3. The immutable buffer will be enqueued to the checkpoint queue
4. If a immutable buffer is checkpointed, delete the buffer from the buffer list

## Buffer Checkpoint
1. Prepare object contents
   - Merge sort all appendable blocks into some new blocks (same segment)
   - Marshal all block deletes
     ```
     [Block-1 ] --- [Delete Buffer]
     [Block-4 ] --- [Delete Buffer]
     [Block-10] --- [Delete Buffer]
     ```
   - Marshal commands
2. Push the prepared object as the table range to the store
   ```
   21/$shard/$tableId/30_59
   ```
3. Commit
4. Checkpoint relevant LSNs

## Table Checkpoint
1. Select a existed buffer checkpoint `[100, 130]`
2. Prepare a table checkpoint object
3. Push to the store
   ```
   20/$shard/$tableId/130
   ```
4. Update the local table checkpoint list
   ```
   [130] --> [90] --> [20]
   ```
## Snapshot Table Read
1. Find the closest table checkpoint to the snapshot timestamp
   ```
   Checkpoints:        [130] --> [90] --> [20]

   Snapshot:           120
   Working Checkpoint: [90]

   Snapshot:           150
   Working Checkpoint: [130]
   ```
2. Collect the checkpointed log tail
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

   Collect tail from 141 to 150 as MemBuffer[141-150]
   ```
   - Return
   ```
   Snapshot:           150
   Working Checkpint:  [130]
   Working Ranges:     {[131-140]}, MaxRange=140
   MemBuffer:          [141-150]
   ```
