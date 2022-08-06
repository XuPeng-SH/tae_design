# DN Engine

## Mutable Buffer
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
## Buffer Checkpoint
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

## Checkpoint
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
## Snapshot Read
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
## Range Read
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
