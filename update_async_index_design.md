- Update Async Index Design
- Status: Draft
- Start Date: 2025-06-18
- Author: [XuPeng](https://github.com/XuPeng-SH)

# Design Goals

This document describes the design of the **Update Async Index** feature in the **MatrixOne DBMS**. Traditionally, the index is updated synchronously, which means the index is updated after the data is committed (Strong consistency). However, in some cases, the index update is not critical, and the data can be committed without waiting for the index update to complete(Weak consistency). This feature is designed to support this scenario.

# Design Overview

## Background

In analytical scenarios, the consistency is not critical, and the index could be updated asynchronously:
1. Some indexes take too long to build synchronously
2. Some indexes achieve better performance when built with larger datasets.

Full-text and vector indexes are good examples.

## Challenges

1. The total number of indexes may be too large, which may affect the performance of the system.
2. Most of the indexes are not modified and how to find the indexes that need to be updated with low cost.
3. Tenant isolation and charging.

## Design

1. Update async index is a task per tenant. The following design is based on this assumption.

2. A new table `mo_async_index_log` is added to record the async index update information.
```sql
CREATE TABLE mo_async_index_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    table_id INT NOT NULL,
    index_id INT NOT NULL,
    last_sync_txn_ts VARCHAR(32)  NOT NULL,
    err_code INT NOT NULL,
    error_msg VARCHAR(255) NOT NULL,
    info VARCHAR(255) NOT NULL,
    drop_at VARCHAR(32) NULL,
);
```
- When a table is created with async indexes, a record will be inserted into `mo_async_index_log` for each async index.
- When a index is updated, the `last_sync_txn_ts` will be updated.
- When the index is dropped, the `drop_at` will be updated and the record will be deleted asynchronously.
- When any error occurs, the `err_code` and `error_msg` will be updated.
  - err_code: 0 means success, 1-9999 means temporary error, which will be retried in the next iteration, 10000+ means permanent error, which need to be repaired manually.

3. The task is monitoring DML events for all non-index tables. It can collect all table IDs with DML events within a period at a low cost. 
- [start, end, accId) ==Collect=> [tid,tid,...] ==Filter=> [tid,tid,...] 

4. After collecting the table list, it starts to update index tables according to the table list, which will be called a `iteration`. In a iteration:
```sql
CREATE TABLE mo_async_index_iterations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    start_at VARCHAR(32) NOT NULL,
    end_at VARCHAR(32) NOT NULL,
    err_code INT NOT NULL,
    error_msg VARCHAR(255) NOT NULL,
    info VARCHAR(255) NOT NULL,
);
```
- If the list of tables is too large, it will be executed in multiple executors.
- For each executor, it handle one index table(or index table group) at a time.
- When handling one index table (group), it will be executed in a transaction. In the transaction, it will update the `mo_async_index_log` for the index table (group).
- It's better to avoid transferring the diff data to SQL.
- At the end of the iteration, a record will be inserted into `mo_async_index_iterations` for the iteration.

5. The task periodically handle the `GC` of the `mo_async_index_log` table.
- It will clean up the `mo_async_index_log` table for the tables that with `drop_at` is not empty and one day has passed.
- It could be very low frequency.








