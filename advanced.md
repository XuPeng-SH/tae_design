# Optional Optimizations

## Global Metadata In CN

1. Maintain a global metadata cache in `CN` per table
2. The cache item key is `1:{$shardId}:{$tableId}`
3. Cache item
   ```
   TableMeta[Shard=0][Id=1][Range=[0,TS1]
   ```
4. A transaction in `CN` @`TS2`:
   - Pin `1:{$shardId}:{$tableId}`
   - Get the current cache max ts `TS1`
   - Query `DN` to collect the table's metadata from `TS1+1` to `TS2`
   - Try update cache with collected `Delta(TS1, TS2]`
     - If cache's max ts is larger than `TS2`, do nothing

## Global Log Tail In CN

1. Maintain a global log tail cache in `CN` per table
2. The cache item key is `2:{$shardId}:{$tableId}:{$checkpointTs}`
3. Cache item
   ```
   TableLogTail[Shard=0][Id=1][CkpTS1, TS1]
   ```
4. A transaction in `CN` @`TS2`, which > `TS1`
   - Pin `2:{$shardId}:{$tableId}:{$checkpointTs}`
   - Get the current cache max ts `TS1`
   - Query `DN` to collect the table's log tail from `TS1+1` to `TS2`
   - Try update cache with collected `Delta(TS1, TS2]`

## Global Checkpoint History

Each `CN` maintains visible checkpoint timestamps
   ```
   // Timestamp is sorted from new to old

   [Shard1] --------- [TS10]-->[TS5]-->[TS1]

   [Shard2] --------- [TS15]-->[TS8]-->[TS1]
   ```
## Protocol

1. Fetch a checkpoint timestamp
   ```
   If current is TS8, which is > TS5 and < TS10. Use TS5 as the checkpoint timestamp

   If current is TS100, which is > TS10. Use TS10 as the checkpoint timestamp
   ```
2. Get the local log tail max timestamp
   ```
   If current is TS8 and TableLogTail[Shard=1][Id=1][TS5, TS9]. Use TS9 as max timestamp.

   If current is TS100 and TableLogTail[Shard=1][Id=1][TS10, TS13]. Use TS13 as max timestamp.
   ```

3. Query `DN` to collect the table's log tail
   ```
   If current is TS8 and the local max timestamp is TS9. No need to query `DN`

   If current is TS100 and the local max timesamp is TS13, collect log tail in range (TS13,TS100]. If there are 2 checkpoints in between (TS13, TS100]:

   [TS120] ---> [TS80] ---> [TS50] ---> [TS10] ---> [TS5] ---> [TS1]
                  |           |
                  +-----------+
   Change the checkpoint from TS10 to TS80 and collect the log tail from (TS80, TS100]. And Also collect all checkpoint timestamps
   ```
4. Apply the collectted log tail from `DN` to the local log tail cache
   - If there are new checkpoint timestamps collectted. Apply them to the local checkpoint history.
   - Get the active checkpoint
   - If it is not the pinned log tail cache, unpin the cache and try pin the active log tail cache. If cache not existed, try to add a new cache. Apply collectted log tail to the cache
