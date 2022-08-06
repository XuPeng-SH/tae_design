# ObjectKey

## Key Prefix

- `10`: Checkpoint prefix
- `20`: Data range prefix

## Key Encoding

### Checkpoint
```
  10/$shard/$ckpTs
  |   |      |
  |   |      +---- Checkpoint timestamp
  |   +----------- Shard id
  +--------------- Prefix
```
### Data Range
```
  20/$shard/$ckpTs/$startTs_$endTs
  |   |       |      |        |
  |   |       |      |        +------- Range end timestamp
  |   |       |      +---------------- Range start timestamp
  |   |       +----------------------- Last checkpoint timestamp
  |   +------------------------------- Shard id
  +----------------------------------- Prefix
```
## Booting

1. List shard 1 `10/1`
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
