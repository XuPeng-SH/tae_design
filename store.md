# ObjectKey

## Key Prefix

- `10`: Catalog checkpoints prefix
- `11`: Catalog commands prefix
- `20`: Metadata checkpoints prefix
- `21`: Metadata commands prefix
- `30`: Default data prefix

## Key Encoding

### Catalog Checkpoints
```
  10/$ckpTs
```
### Catalog Commands
```
  11/$ckpTs/$startTs_$endTs
```
### Metadata Checkpoints
```
  20/$ckpTs
```
### Metadata Commands
```
  21/$ckpTs/$startTs_$endTs
```
### Default Data
```
  30/$name
```
## Booting Catalog

1. List `10/`
```
|-- 10/
|   |-- 1
|   |-- 30
|   |-- 60

Max checkpoint is 10/60
```

2. List `11/60`
```
|-- 11/60
|     |-- 61_70
|     |-- 71_80
|     |-- 81_90

Max range of commands is 11/60/81_90
```
