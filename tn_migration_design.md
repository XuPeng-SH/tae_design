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

### Subtasks

1. **TN** supports write mode and read-only mode
2. **TN** can be switched between W Mode and RO Mode on the fly
3. **TN** can replay **LogEntry** from the **LogService** in runtime
4. **TN** can build tunnels to other **TN**s to forward WriteRequest and Subscription
5. **LogService** can switch the writer on the fly
6. **CN** can subscribe to multiple **TN**
7. LockTable migration?
