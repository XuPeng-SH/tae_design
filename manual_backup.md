1. 选择一个全量备份的数据，清空mo-data/shared以外的所有数据,或者新建一个空的目录也可以
  ```shell
  >> cd $backup_dir
  >> rm -rf mo-data/dn-data
  >> rm -rf mo-data/etl
  >> rm -rf mo-data/local
  >> rm -rf mo-data/logservice-data
  >> rm -rf mo-data/trace
  ```
2. 非停机同步mo-data/shared
  ```shell
  >> cd $source_dir
  # 同步mo-data/shared
  # debug情况下可以用-av
  >> rsync -a --ignore-existing mo-data/shared $backup_dir/mo-data/
  # 这个rsync如果持续时间比较长，可以反复执行多次
  ```
3. 停机同步
  ```shell
  # 确保mo-data/shared/ckp为空
  # 确保mo-data只有一个shared目录
  >> cd $backup_dir
  >> ls mo-data/
  >> rm -rf mo-data/shared/ckp
  >> rm -rf mo-data/shared/gc
  # 同步mo-data
  >> cd $source_dir
  >> rsync -av --ignore-existing mo-data/ $backup_dir/mo-data
  ```
4. 启动
