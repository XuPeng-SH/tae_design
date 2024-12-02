- TN Migration Design
- Status: Draft
- Start Date: 2024-11-25
- Author: [XuPeng](https://github.com/XuPeng-SH)

# Design Goals

> Note: This feature does not reduce the system unavailability time for unanticipated TN reboots

This document describes the design of the **TN** migration feature in the **MatrixOne DBMS**. In a rolling upgrade scenario, there is a period of service unavailability during the upgrade process, curently up to minute level. The **TN** migration feature is designed to reduce the service unavailability time to seconds level.

# Design Overview

## Background

**TN** as the only transaction node in the **MatrixOne DBMS** is responsible for processing transactions and maintaining the global transaction state, which is a single point of failure in the system. In the current design, when the **TN** is upgraded, the service is unavailable during the upgrade process.

### CN-TN Communications
```
+-----------------+      Write       +-----------------+
|  CNs            | ---------------> |  TN             |
+-----------------+ <--------------- +-----------------+
```

```
+-----------------+    Subscribe     +-----------------+
|  CNs            | ---------------> |  TN             |
+-----------------+ <--------------- +-----------------+
                         Push
```
```
+-----------------+    LockTable(?)  +-----------------+
|  CNs            | ---------------> |  TN             |
+-----------------+ <--------------- +-----------------+
```

### TN Recovery Main Steps

1. Open connections to the **LogService**
2. Replay all **Checkpoint** files
3. Replay all **LogEntry** from the **LogService**

## Migration Process

### Definitions

#### What is write mode?
1. It receives WriteRequest from **CN** and commit the transaction
2. It writes **LogEntry** to the **LogService**
3. It flushes the memory table to the disk
4. It receives table subscription from **CN** and push the logtail to **CN**
5. It runs the **Checkpoint** periodically
6. It runs merge manager
7. It runs disk cleaner periodically
8. It replays checkpoint files and **LogEntry** from the **LogService** at startup

#### What is read-only mode?
1. It replays checkpoint files at the startup
2. It replays **LogEntry** from the **LogService** all the time
3. It can not receive WriteRequest from **CN** if there is no forward tunnel. If it has a forward tunnel, it forwards the WriteRequest to the **TN** in write mode
4. It can not commit the transaction
5. It can not write **LogEntry** to the **LogService**
6. It can not handle table subscription from **CN** if there is no forward tunnel. If it has a forward tunnel, it forwards the table subscription to the **TN** in write mode
7. It can not push the logtail to **CN** without a forward tunnel
8. It can not run the **Checkpoint**
9. It can not run merge manager but can run merge tasks
10. It can not run disk cleaner
11. It can not flush the memory table to the disk

### Main Steps

1. Given one running **TN** to be migrated as **TN0**
```
+-----------------+     Write        +-----------------+
|  CN0,CN1        | ---------------> |  TN0            |
+-----------------+                  +-----------------+
+-----------------+    Subscribe     +-----------------+
|  CN0,CN1        | ---------------> |  TN0            |
+-----------------+                  +-----------------+
```

2. Start a new **TN** as **TN1**
```
+-----------------+     Write        +-----------------+   Write     +-----------------+
|  CN0,CN1        | ---------------> |  TN0(W MODE)    | ----------> |    LogService   |
+-----------------+                  +-----------------+             +-----------------+
+-----------------+    Subscribe     +-----------------+                     /|\
|  CN0,CN1        | ---------------> |  TN0(W MODE)    |                      |
+-----------------+                  +-----------------+                      |
                                     +-----------------+        Read          |
                                     |  TN1(RO MODE)   | ---------------------+
                                     +-----------------+
```

3. Migration

```
1. Freeze the TN0 WriteRequest Queue
2. Flush the TN0 Commit Queue
3. Write a Change-Config LogEntry to the LogService to switch the writer to TN1

                                    +---------------------------+
                                    |   +---------------------+ |
                               + ---+-->| WriteRequest Queue  | |
                               |    |   +--------+------------+ |
                               |    |            |            | |
                               |    |            |            | |
                               |    |           \|/           | |
                               |    | +----------+------------+ |
                               |    | | Commit Queue          |-+--------+
                               |    | +-----------------------+ |        |
                               |    +---+-----------------------+        |
                               |        |                               \|/
+-----------------+     Write  |     +--+--------------+   Write     +-----------------+
|  CN0,CN1        | ---------------> |                 | ----------> |    LogService   |
+-----------------+                  |   TN0(W MODE)   |             +-----------------+
+-----------------+    Subscribe     |                 |                     /|\
|  CN0,CN1        | ---------------> |                 |                      |
+-----------------+                  +-----------------+                      |
                                                                              |
                                     +-----------------+        Read          |
                                     | TN1(RO Mode)    | ---------------------+
                                     +-----------------+

4. TN0 switch to RO Mode and TN1 switch to W Mode when it receives the Change-Config LogEntry
5. TN0 build two tunnels to TN1, one for WriteRequest and the other for Subscribe

+-----------------+     Write        +-----------------+
|  CN0,CN1        | ---------------> |  TN0(RO MODE)   |
|                 |    Subscribe     |                 |
|                 | ---------------> |                 |
+-----------------+                  +-----------------+
                                        |       |
                                  Write |       | Subscribe
                                        |       |
                                       \|/     \|/
                                     +-----------------+     Write     +-----------------+
                                     |  TN1(W MODE)    | ------------> |    LogService   |
                                     +-----------------+               +-----------------+

6. TN0 forward WriteRequest and Subscribe to TN1
7. New WriteRequest and Subscription are sent to TN1 directly
8. Migrate the TN0 subscription table to TN1
9. TN0 shutdown
```

### Switch Steps

#### Write-To-Read with Forward Tunnel

1. Build a forward tunnel from current **TN** to the new **TN**
2. Stop the Merge|DiskCleaner|Checkpointer
3. Freeze the WriteRequest Queue
4. Freeze the Commit Queue
5. Flush the Commit Queue
6. Send a Change-Config LogEntry to the **LogService** to switch the writer to the new **TN**
7. Freeze the logtail
8. Unfreeze the WriteRequest queue and forward the WriteRequest to the new **TN**
9. Replay the logEntry from the **LogService**

#### Read-To-Write

1. When it receives the Change-Config LogEntry, switch to Write Mode
2. Stop replaying the **LogEntry** from the **LogService**
3. Start receiving WriteRequest from **CN**
4. Start receiving table subscription from **CN**
5. Start commit queue
6. Start Flusher|Merge Manager|Disk Cleaner|Checkpointer

### Subtasks

1. **TN** supports write mode and read-only mode
2. **TN** can be switched between W Mode and RO Mode on the fly
3. **TN** can replay **LogEntry** from the **LogService** in runtime
4. **TN** can build tunnels to other **TN**s to forward WriteRequest and Subscription
5. **LogService** can switch the writer on the fly
6. **CN** can subscribe to multiple **TN**
7. LockTable migration?
