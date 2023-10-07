# Quick Cluster Backup & Recovery

## 前提说明
- 不依赖用户快照：只通过配置GC 的周期保留相关的历史数据，可以恢复到期间的任一时间点
- 依赖用户快照: 需要等到快照功能实现后才能启动。 可以选择恢复到任一快照时间点
- 回滚时，集群需要重启
- 物理备份

## 使用方式
- 正常使用时用户需要配置 GC 的时间控制可以恢复到任意时刻的最大时长
- 如果依赖用户快照，用户需要手动创建快照
- 回滚
  1) 关闭集群
  2) 用`mo-tool` (名字再议)
  ```shell
  >> mo-tool snapshot show 3
     1. 2023-10-07 20:30:10:89789          // 最新的3个快照
     2. 2023-10-06 20:30:10:89789
     3. 2023-10-05 20:30:10:89777
     ...
     2023-10-06 20:30:10:89789 ~ now       // 时间段内可以恢复到任一时间点
  >> mo-tool snapshot recovery -cfg xxx.toml "2023-10-06 20:30:10:89789"
  ```
- 启动集群

## 技术细节

### 内核修改
> 基于用户快照的可以参考快照的设计文档, 本设计文档不提及任何用户快照相关的技术细节.
强制创建一个新的 Checkpoint, 该 Checkpint 的时间戳是指定快照的时间戳, 之后从该 Checkpoint 开始 Replay

#### Checkpoint

1. 提供一些查询的接口
```go
type Checkpoints interface {
    Snapshots(int) []types.TS
}
```

2. Checkpoint 的存储元信息需要修改
```go
type CheckPointEntry struct {
    ...
    From types.TS
    To types.TS

    // newly added
    Sequence int32     // 快照的序列号

    Kind int8          // 快照的类型：Internal, External
    Timestamp types.TS // 指示该快照的截止时间
}
```

3. 消费 Checkpoint 的逻辑需要修改
当前 Checkpoint 的创建时序和内部数据的时序是一致的，逻辑大概是这样的:
```
当前每一个 Checkpoint 按照内容的 From 排序
Checkpoints:
    |----- (t1000, t1200]
    |----- (t801, t1000]
    |----- [t801, t801]
    |----- (t600, t800]
    |----- (t400, t600]
    +----- (t0, t400]

之后需要按照 Checkpoint 的序列号排序, 比如后面准备回滚到t900:
Checkpoints:
    |----- 7. [t900, t900]     External
    |----- 6. (t1000, t1200]   Internal
    |----- 5. (t801, t1000]    Internal
    |----- 4. [t801, t801]     Internal
    |----- 3. (t600, t800]     Internal
    |----- 2. (t400, t600]     Internal
    +----- 1. (t0, t400]       Internal

影响最大的地方是 Pull 的逻辑
```

#### Replay
1. 从最新的全量 Checkpoint 开始 `Replay`, 唯一的区别就是前文所说的 Checkpoint 的时序问题
2. `Replay` 日志时，按照 Checkpoint 的Timestamp 做日志截断
3. `Truncate` 日志时，也是按照 Checkpoint 的 Timestamp 做日志截断

#### GC
`GC` 需要重构，只统计每个快照的 `S3` 引用，不需要和现在一样，感知 `S3` 的软删逻辑

### 工具开发
- 基于 `engine/tae` 包装出一个二进制文件
- 需要启动`logservice`,因为如果需要回滚的点还在 WAL 里，需要从 WAL 里加载数据
- 调用内核的一些接口
