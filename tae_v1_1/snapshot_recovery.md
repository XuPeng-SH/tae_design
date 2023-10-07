# Snapshot-based Recovery

## 需求分析
- 租户级别的快照创建，查询，备份与回滚
- 在线处理
- 租户隔离

## 租户快照

- 每个租户加一张快照表 `mo_snapshots`
  ```go
  type Schema struct {
    timestamp types.TS // primary key
  }
  ```
- 创建快照就是往 `mo_snapshots` 里面插入一行记录
- 查询快照也是普通的 `select` 查询
- 删除快照就是 `delete`

```
DQL 没有问题。DML 需要通过特殊语法的 SQL:
增加快照:
create snapshot;
删除快照:
drop snapshot {Filter Expr}
```

## 租户快照数据在线恢复

### 逻辑恢复方式

> 具体细节参考前端设计文档

- 选择快照并基于该快照时间 `dump` 该租户所有的 `databases`
- 创建新租户，并将之前导出的数据导入到新的租户
- 删除老租户

### 物理恢复方式(是逻辑恢复方式的优化路径)

> 具体细节参考前端设计文档

- 选择快照并基于该快照 `dump` 该租户的所有 `DDL`
- 创建新租户，并导入所有 `DDL`
- 将原快照所有表的数据以 `S3 Object` 的形式加入到事务的 `workspace`
- 删除老租户
- 提交事务

## 技术细节

### 逻辑恢复之基于快照的 `dump`

该功能依赖 `disttae` 的快照读。正常的读方式:
```
Table-Data -+
            |---- Checkpoint(Snapshot)
            |---- Tail
```

快照读发生在快照时间戳小于表数据的最小快照，此时需要加载较小的快照数据作为表数据，目前没有实现。
```
Table-Data -+
            |---- Checkpoint(Snapshot)
```

### 物理恢复之表数据恢复

#### 流程
- 快照数据的物理布局
  ```
  Table1 -+
          |
          |---- Data-Object-1 ----------------------- [t100~t200]
          |---- Data-Object-3 ----------------------- [t201~t300]
          |---- Tombstone-Object-1 ------------------ [t150~t200]
          |---- Tombstone-Object-3 ------------------ [t201~t300]

  Table2 -+
          |
          |---- Data-Object-2 ----------------------- [t100~t220]
          |---- Data-Object-4 ----------------------- [t221~t320]
          |---- Tombstone-Object-2 ------------------ [t180~t220]
          |---- Tombstone-Object-4 ------------------ [t221~t320]

  假设某租户有两张表 Table1 和 Table2, 表数据最后都保存在 S3 对象上。因为创建快照是用户发起的，
  时间戳不可控，所以永远存在一些 S3 对象上的部分数据在快照内，部分数据不在。比如针对以上两张表，
  如果快照时间取到t250, 那么至少有4个S3对象的数据存在多数据的情况，如果我们选择让回滚后创建的新
  表引用这些S3 Objects,那么就会出现数据不一致的情况(不一定多数据，如果是Tombstone Object, 也可能
  少数据，而且这些不一致的数据数据来自于某些事务的部分数据，也会破坏事务的原子性)
  ```
- 快照数据部分S3 Objects 改写
  ```
  原因如上所叙，对于那些快照时间戳不是 S3 Object 数据唯一时间戳的 Object, 需要重写一个新的S3 Object.
  这种重写的Object数量,最坏情况2倍于租户的表数量，正常情况下，会比较的少，这也是最大开销
  ```

- 写表数据
  ```
  走正常的写表数据流程，所有数据都是以 S3 Objects 的形式存在，将快照里对应表的所有Data Objects 和
  Tombstone Objects 以及改写的一些 Objects 都写入到事务的 Workspace 中
  ```
- 提交事务

#### Storage Optimization
需要考虑那些改写后的 `Data Objects`, 这些 `Objects` 没有按照主键排序，查询效率比较低。

#### Insert & Delete 以及 Workspace 的修改
支持直接 `Install` `Data Objects` 和 `Tombstone Objects` 的方式，不做去重等检测

### 开发评估

#### 逻辑方式
依赖快照读。快照读目前还没有相关的设计和排期，需要了解快照读的一些基本需求后才能评估.

#### 物理方式
- 依赖当前的表数据管理方式重构，之后才能启动
- S3 Objects 改写,1~2周
- Install 表数据相关的修改，2~3周
