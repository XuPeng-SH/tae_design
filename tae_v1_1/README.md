- Feature Name: Transactional Analytic Engine Version 2
- Status: Design
- Start Date: 2023-09-04
- Authors: Xu Peng
- Implementation PR:
- Issue for this RFC:

# Summary

Transactional Analytic Engine is designed for hybrid transactional analytical query workloads, which can be used as the underlying storage engine of dat
abase management system (DBMS) for online analytical processing of queries (HTAP). Compared with the previous version, the new design incorporates our experience in cloud-native data processing, making the whole design more suitable for cloud-native architecture. There will be some major changes involved here. There are many basic concepts, such as descriptions related to transaction timestamps. You need to refer to the design documents of previous versions.

# Guide-level design

## Terms
### Data Layout
- **Table**: A table is a collection of related data held in a table format within a database. In our system, a table's data is always composed of a collection of persistent objects plus a memory memtable.
- **Object**: An object is a readonly file and any metadata that describe the file. This file can save table data, tombstones, index, etc. All persisted data are in the form of objects.
- **Block**: A block is a subset of rows. In our system, the maximum number of rows is 8192 rows. The data of the block will be persisted to the object. Usually an object will contain multiple blocks.
- **Column Block**: A block is usually saved in the form of column blocks. And the column block is the smallest unit of our data io.

## Data Storage
### Table
TAE stores data represented as tables. Each table is bound to a schema consisting of numbers of column definitions. A table data is organized as a log-structu
red merge-tree (LSM tree).

Currently, TAE is a two-level LSM tree, called L0, L1. L0 is small and can be entirely resident in memory, whereas L1 is definitely resi
dent on disk. In TAE, L0 consists of transient objects and L1 consists of persisted objects. The incoming new data is always inserted into the latest transient object. If the insertion causes the object to exceed the maximum row count of a object, the object will be sorted by primary key and flushed into L1 as a new sorted persisted object.

L1 is organized into sorted runs of data. Each run contains data sorted by the primary key, which can be represented on disk as a single file. There will be overlapping primary key ranges between sort runs.

A object can be compacted into a new object if it has many row tombstones. Objects can be merged into new objects. The scheduling behind this has some customizable strategies, mainly the trade-off between write amplification and read amplification.

### Primary key index
As mentioned before, the persisted table data is in the form of objects. We will persist zonemap information for all column blocks in the object, and also persist object-level column zonemap information. For primary key, we also persist the block-based bloom filter in the object.

### Table Data Management
Table data is composed of persisted object list and memtable in memory.
```
Data Objects:             Object-data1, Object-data2, Object-data3, ..... Object-dataM    [ M Objects ] \
                                                                                                         | => PERSISTED
Row Tombstone Objects:    Object-ts1, Object-ts2, Object-ts3, ... Object-tsN              [ N Objects ] /

In-memory Rows:              [Memory Store] // In-memory store   \
In-memory Row Tombstones:    [Memory Store] // In-memory store   | ==> In-memory
In-memory Object Tombstones: [Memory Store] // In-memory store   /
```

#### Data Objects
A collection of objects of table data in the form of blocks.
```go
type ObjectEntry struct {
    CreatedAt types.TS    // Specify the object create timestamp
    DeletedAt types.TS    // Specify the object tombstone timestamp
    location objectio.Location // Object location
    is_tombstone bool // specify whether it is tombstone object
}
```

#### Row Tombstone Objects
A collection of objects of the row tombstones. The row timbstones are persisted in the object in the form of blocks with some specified schemas. Currently, the row tombstone block has two schemas, one is for the tombstones that persist before the transaction is committed, and the other for the committed tombstones.
```go
// Schema Of Uncommitted Tomestones
type TombstoneSchema1 struct {
    row_id types.Rowid
    primary_key any
}

// Schema Of Committed Tomestones
type TombstoneSchema2 struct {
    row_id types.Rowid
    commit_time types.TS
    primary_key any
    aborted bool
}
```
Any object with uncommitted tomestone schema, all the `commit_time` of row tomestones in the object are the same with the object entry `CreatedAt`. And all the `aborted` of row tomestones in the object are `false`.

The row tombstone object visibility rule maybe very special. We can add a `min_commit_ts` for row tombstone object.
```go
type ObjectEntry struct {
    CreatedAt types.TS    // Specify the object create timestamp
    DeletedAt types.TS    // Specify the object tombstone timestamp
    location objectio.Location // Object location
    is_tombstone bool // specify whether it is tombstone object
    min_commit_ts types.TS // specify the min commit ts in the object
}
```
```
There are 3 row tombstone objects: A, B, C
A: CreateTS 100, DeleteTS 200,  min_commit_ts 50
B: CreateTS 80,  DeleteTS 200,  min_commit_ts 80
C: CreateTS 200, DeletedAt inf, min_commit_ts 50

A and B was merged into C at timestamp 200.
-----------------------------------------------------------
  Now    |    Snapshot     |   Visible Tombstone Objects
-----------------------------------------------------------
 <200    |      <80        |       []
 <200    |    >=80,<100    |       [B]
 <200    |    >=100,<200   |       [A,B]
 >=200   |      <50        |       []
 >=200   |      >=50       |       [C]
```

```go
// Special visibility rule
func (e *ObjectEntry) IsVisible(ts types.TS) bool {
    if e.DeletedAt.Valid() {
        return false
    }
    return e.min_commit_ts.LessEq(ts)
}
```

Or, we can create object first and then apply changes to the object. This way, we can avoid some special visibility rules. This will be covered in detail in the tombstone chapter.

#### In-memory Rows
All row data that are not persisted will be stored in the memory store.
```go
type RowEntry struct {
    CreatedAt types.TS
    DeletedAt types.TS
    object_name objectio.Location,
    offset uint32
    values []byte
}

type InMemoryDataStoreIterator interface {
    HasNext() bool
    Next() *RowEntry
}

type InMemoryDataStore inteface {
    SelectByPK(ts types.TS, pk any) *RowEntry
    SelectByRowid(ts types.TS, id types.Rowid) *RowEntry
    Iter(types.TS, object.ObjectName) InMemoryDataStoreIterator
}
```

#### In-memory Row Tomestones
All row tombstones that are not persisted will be stored in the memory store.
Each row tombstone represents the deletion record of a row.

```go
type RowTombstoneEntry struct {
    CreatedAt types.TS
    DeletedAt types.TS
    pk_value any
    rowid types.Rowid
}


type InMemoryTombstoneStoreIterator interface {
    HasNext() bool
    Next() *RowTombstoneEntry
}

type InMemoryTombstoneStore interface {
    SelectByPK(ts types.TS, pk any) *RowTombstoneEntry
    SelectByRowid(ts types.TS,id types.Rowid) *RowTombstoneEntry
    Iter(types.TS,object.ObjectName) InMemoryTombstoneStoreIterator
}
```

#### In-memory Object Tombstones
All object tombstones that are not persisted will be stored in the memory store.
```go
type ObjectTombstoneStore interface {
    HasObject(types.TS,objectio.ObjectName) bool
}
```

### Table Readers

```
    1 +----> | Determine data to read |
      |                 |___________________________________________________________
      |                /                                                             \
      |               /                                                               \
      |        Equal-Filter On PK                                                    No Equal-Filter On PK
      |              |                                                                           |
      |        1. Select from In-memory rows and tombstones with PK.                1. Select objects from the object list only by zonemap
      |           If not empty, return with in-memory only                                  var $objs []objectio.Location
      |        2. Select objects from the object list by zonemap and bloom filter           for $obj in range $data_objects {
      |           var $objs []objectio.Location                                                 if !obj.IsVisible($timestamp) {
      |           for $obj in range $data_objects {                                                 continue
      |                 if !obj.IsVisible($timestamp) {                                         }
      |                     continue                                                            $metadata = LoadMetadata($obj)
      |                 }                                                                       if !EvalZonemapByFilterExpr($exprs, $metadata) {
      |                 $metadata = LoadMetadata($obj)                                              continue
      |                 if !$metadata.Zonemap.Contains($pk_val) {                               }
      |                     continue                                                            if $objectTomestoneStore.HasObject($timestamp, $obj) {}
      |                 }                                                                           continue
      |                 for !metadata.BloomFilter.Contains($pk_val) {                           }
      |                     continue                                                            $objs = append($objs, $obj)
      |                 }                                                                   }
      |                 if $objectTomestoneStore.HasObject($timestamp, $obj) {
      |                     continue
      |                 }
      |                 $objs = append($objs, $obj)
      |           }
      |
      |
     \|/  (object_list, in-memory-rows, in-memory-row-tombstones)
    2 +----> | Reader Orchestration |
                    |
         check the object list length
                   / \_____________________________
                  /                                 \
         object list is short                     object list is long
                 |                                          |
         create reader in the current compute node      shuffle objects and collect related in-memory-rows and in-memory
                                                \       row tombstones of the specified object
                                                 \       /
                                                  \     /
                                            ($object_list, in-memory rows, in-memory row tombstones) => Create Reader
```

```go
type Reader struct {
    // Persisted object list to read
    object_list []objectio.Location
    // in-memory rows
    rows InMemoryDataStore
    // in-memory row tombstones
    tombstones InMemoryTombstoneStore
}
```

## Transaction

### Transaction Nodes
```go
type TransactionNode interface {
    MarshalBinary() ([]byte, error)
    UnmashalBinary([]byte) error
    GetType() uint32
    GetVersion() uint32
}

// StorageBase is for storage compatibility
type StorageBase struct {
    // storage node type
    Type uint32
    // storage node version
    Version uint32
}

type Txn struct {
    ID []byte
    // transaction start timestamp
    StartTS types.TS
    // transaction commit timestamp
    CommitTS types.TS
    // transaction state
    State uint8
}

type TxnState struct {
    Txn *Txn
    // transaction start timestamp
    StartTS types.TS
    // transaction commit timestamp
    CommitTS types.TS
    // whether it was aborted
    Aborted bool
}

type AppendNode struct {
    StorageBase
    TxnState
    // target object to append data
    object_name objectio.ObjectName
    // specify the offset of the target object
    offset uint32
    // append data payload
    data *batch.Batch
}

type RowTombstoneNode struct {
    StorageBase
    TxnState
    // specify the target block id
    block_id objectio.Blockid
    // specify the rows to be deleted
    rows []uint32
}

type ObjectTombstoneNode struct {
    StorageBase
    TxnState
    // specify the target object name
    object_name objectio.ObjectName
}

type CreateObjectNode struct {
    StorageBase
    TxnState
    // specify the target object location
    location objectio.Location
    // check whether it is a row tombstone object
    is_tombstone bool
}
```

### DML
```go
type KVStore[T any] interface{
    Get([]byte) (T, error)
    Set([]byte, T) error
    Seek([]byte) (KVIter[T], error)
}

type KVIter[T any] interface {
    HasNext() bool
    Next() T
}

type Visibility struct {
    CreatedAt types.TS
    DeletedAt types.TS
}

type CheckpointedObjectEntry struct {
    Visibility
    location objectio.Location

    // store primary key zonemap in the object entry if any
    pkZonemap objectio.Zonemap
    // specify whether it is of row tombstone
    is_tombstone bool
}

type ObjectCreateTxnEntry struct {
    TxnState
    location objectio.Location
    pkZonemap objectio.Zonemap
    is_tombstone bool
}

type RowTxnEntry struct {
    TxnState
    RowEntry
}

type RowTombstoneTxnEntry struct {
    TxnState
    RowTombstoneEntry
}

type ObjectTombstoneTxnEntry struct {
    TxnState
    ObjectEntry
}

// type MemoryRows = KVStore[*RowTxnEntry]
type MemoryRowTombstones = KVStore[*RowTombstoneTxnEntry]
type MemoryObjectTombstones = KVStore[*ObjectTombstoneTxnEntry]
type MemoryObjectCreates = KVStore[*ObjectCreateTxnEntry]
type CheckpointedObjects = KVStore[*CheckpointedObjectEntry]

type MemoryObjectRows struct {
    name objectio.ObjectName
    data []*batch.Batch
    nodes []*AppendNode
    freezed atomic.Bool
}

type MemoryTableRows = KVStore[*MemoryObjectRows]
```
```
[Snapshot|Workspace]
   |--- [in-memory-rows]
   |--- [in-memory-tombstones]
   |--- [in-memory-object-tombstones]
   |--- [data-objects]
   |--- [row-tombstone-objects]
```

#### Insert Row
```
Execute Statement:
    1. Dedup on snapshot and workspace
       for each pk {
         scan with equal filter expr on the primary column
         if return any row, report duplication
       }
    2. if too many rows, dump them into object and save as data object in the workspace
       else save rows as in-memory rows in the workspace

Commit With Conflict Check:
1. Write in-memory row intents
   conflict check with the in-memory rows with commit ts after the transaction snapshot timestamp.
   conflict check with the data objects with the max commit ts after the transaction snapshot timestamp.
       for each object in range of the data objects with the max commit ts > snapshot ts {
            for row in intents {
                if the pk is not in the column's zonemap or bloom filter, continue
                else conflict check the object
            }
       }

2. Write row tombstone object intents
   for each object in the row object intents {
      conflict check with the in-memory rows with commit ts after the transaction snapshot timestamp
      conflict check with the row objects with the max commit ts after the transaction snapshot timestamp
   }

```

#### Delete Rows
```
Execute Statement:
    Query on snapshot and workspace
             |
     a collection of (pk, rowid)
            /___________________________
           |                            \
   too many rowids                    few rowids
          |                                 |
dump them into object                   save as in-memory-tombstones in the workspace
and save as row-tombstone-objects
in the workspace


Commit With Incremental Conflict Check:
1. Write in-memory row tombstone intents
  Using rowid to conflict check with in-memory row tombstones
        |
  Using rowid to conflict check with row tombstone objects
     for $object in range $objects {
        if max commit ts of the object is before the transaction snapshot ts, skip this object
        if the rowid is not in the zonemap of the rowid column in the tombstone object, skip this object
        if the primary_key is not in the zonemap of the primary key column in the tombstone object, skip this object

        load rowid column of the object to conflict check
     }

2. Write row tombstone object intents
  for object in range of objects to be committed {
        for row-tombstone in in-memory tombstones with commit ts after the transaction snapshot ts {
            if the row-tombstone is not in the zonemap of the rowid column in the object, skip this check
            else load the object rowid column, conflict check
        }

        for object in the tombstone objects {
            if there is no overlap between 2 objects, skip this check
            else load the rowid column of 2 objects, conflict check
        }
  }
```

To speed up conflict detection, we can add several fields to the Object entry.
```go
type ObjectEntry struct {
    CreatedAt types.TS    // Specify the object create timestamp
    DeletedAt types.TS    // Specify the object tombstone timestamp
    location objectio.Location // Object location
    is_tombstone bool // specify whether it is tombstone object
    min_commit_ts types.TS // specify the min commit ts in the object

    // specify the primary key zonemap
    pkZonemap objectio.Zonemap
    // tombstone only. specify the rowid zonemap
    rowidZonemap objectio.Zonemap
}
```

## Advanced

### Logtail Protocol Extension

#### Rows
```go
// schema
type rows_schema struct {
    commit_ts types.TS
    rowid types.Rowid
}

type Rows struct {
    database_id uint64
    database_name string
    table_id uint64
    table_name string
    bat *batch.Batch
}
```

#### Row Objects & Tombstone Objects

```go
// schema
type row_objects_schema struct {
    location objectio.Location
    min_commit_ts types.TS
    commit_ts types.TS
    pk_zonemap objectio.Zonemap
    rowid_zonemap ojectio.Zonemap
}

type Objects struct {
    database_id uint64
    database_name string
    table_id uint64
    table_name string
    is_tombstone bool
    bat *batch.Batch
}
```

#### Row Tombstones

```go
// schema
type row_tombstone_schema struct {
    pk any
    rowid types.Rowid
    commit_ts types.TS
}

type RowTombstones struct {
    database_id uint64
    database_name string
    table_id uint64
    table_name string
    bat *batch.Batch
}
```

#### Object Tombstones

```go
// schema
type object_tombstone_schema struct {
    location objectio.Location
    commit_ts types.TS
}

type ObjectTimbstones struct {
    database_id uint64
    database_name string
    table_id uint64
    table_name string
    bat *batch.Batch
}
```

### Storage Optimization

```
In-memory Rows            ==>     Row Objects               \
                                                             | ==> Flush (Trigger based on time and memory)
In-memory Row Tombstones  ==>     Row Tombstone Objects     /

Main Target of Flush:
1. Save memory
2. Truncate Log

Row Objects + Row Tombstone Objects          ==>  Row Objects + Object Tombstones            \
                                                                                              | ==> Storage Optimization
Row Tombstone Objects + Object Tombstones    ==>  Row Tombstone Objects + Object Tombstones  /

Main Target of Storage Optimization:
1. Reduce overlap between objects
2. Reduce the number of row objects
3. Reduce the number of row tombstone objects
```

### Table Data Management

```
Table Data = Table Checkpoint + Tail
```

#### Checkpoint

```go
// schema of row and tombstone objects
type Object struct {
    // specify the create ts of the object
    created_at types.TS

    // specify the soft delete ts of the object
    // if it was not deleted, keep it as the max value. This only means there is no soft
    // deletion in the checkpoint, but it does not mean that there is no soft deletion in the tail.
    deleted_at types.TS

    // specify the location of the row or tombstone object
    location objectio.Location

    // pk_zonemap and rowid_zonemap are used for performance optimization
    pk_zonemap objectio.Zonemap
    rowid_zonemap objectio.Zonemap

    // hint to specify whether the object has related tombstones in the checkpoint
    has_tombstone bool
}

type TableCheckpoint struct {
    table_id uint64

    // row_locations specify all persisted blocks holding the row object information
    // the block schema is as defined above
    row_locations []objectio.Location

    // tombstone_locations specify all persisted blocks holding the tombstone row object information
    // the block schema is as defined above
    tombstone_locations []objectio.Location

    // Each bloomfilter combines data from multiple objects
    bloom_filter_locations []objectio.Location
    // Each zonemap combines data from multiple objects
    zonemap_locations []objectio.Location
}
```

#### Tail
```
In-memory Rows
In-memory Tombstones
Object Tombstones
Row Objects
Tombstone Objects
```

#### Table Readers

```
    1 +----> | Determine data to read |
      |                /
      |               /
      |        1. Select objects from tail in-memory rows and row objects with PK.
      |           var objs []objectio.Location
      |           for $obj in tail row objects {
      |                 if !obj.IsVisible($timestamp) {
      |                     continue
      |                 }
      |
      |                 if there is object tombstone for $obj and it is visible to $timestamp
      |                     continue
      |
      |                 $metadata = LoadMetadata($obj)
      |                 if !EvalFilterExpr($obj, $metadata) {
      |                      continue
      |                 }
      |
      |                 $objs = append($objs, $obj)
      |           }
      |        2. Select objects from checkpoint
      |           for $location in range row_locations {
      |               info_bat := LoadObjectInfo($location)
      |               location_col := info_bat["location"]
      |               for $row_object_loc in range location_col {
      |                  if there is object tombstone for $obj and it is visible to $timestamp
      |                     continue
      |                  $metadata = LoadMetadata($row_object_loc)
      |                  if !EvalFilterExpr($obj, $metadata) {
      |                     continue
      |                  }
      |
      |                  $objs = append($objs, $obj)
      |               }
      |           }
      |
      |
     \|/  (object_list, in-memory-rows, in-memory-row-tombstones)
    2 +----> | Reader Orchestration |
                    |
         check the object list length
                   / \_____________________________
                  /                                 \
         object list is short                     object list is long
                 |                                          |
         create reader in the current compute node      shuffle objects and collect related in-memory-rows and in-memory
                                                \       row tombstones of the specified object
                                                 \       /
                                                  \     /
                                            ($object_list, in-memory rows, in-memory row tombstones) => Create Reader
```


### Secondary key index
**TODO**
