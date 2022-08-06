# ObjectKey

## Key Prefix

- `10`: Catalog checkpoints prefix
- `11`: Catalog commands prefix
- `20`: Table checkpoint prefix
- `21`: Table data range prefix

## Key Encoding

### Catalog Checkpoints
```
  10/$shard/$ckpTs
```
### Catalog Commands
```
  11/$shard/$ckpTs/$startTs_$endTs
```
### Table Checkpoint
```
  20/$shard/$tableId/$ckpTs
```
### Table Data Range
```
  21/$shard/$tableId/$startTs_$endTs
```
## Booting Catalog

1. List shard 1 `10/1`
```
|-- 10/1
|   |-- 1
|   |-- 30
|   |-- 60

Max checkpoint is 10/1/60
```

2. List `11/1/60`
```
|-- 11/1/60
|     |-- 61_70
|     |-- 71_80
|     |-- 81_90

Max range of commands is 11/1/60/81_90
```
