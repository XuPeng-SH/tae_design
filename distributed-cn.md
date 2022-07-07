- Feature Name: TAE in **CN** (Computation Node)
- Status: In Progress
- Start Date: 2022-07-07
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Key Requirements

<details>
  <summary><b><font size=4>Remote S3 Compatitable Storage</b></font></summary>
          In computation node, all data is stored on remote object storage.
</details>
<details>
  <summary><b><font size=4>Local Staging Storage</b></font></summary>
          Disk and in-memory caching as an efficient and cost-effective medium between local clients and remote storage services.
</details>
<details>
  <summary><b><font size=4>Metadata Management</b></font></summary>
          Metadata is stored on remote object storage, local memory has a complete cache, and needs to be updated incrementally.
</details>
<details>
  <summary><b><font size=4>Distributed Transaction</b></font></summary>
          Distributed transactions implementing snapshot isolation isolation level.
</details>
<details>
  <summary><b><font size=4>Transactional Bulk Load</b></font></summary>
          Support transactional data load.
</details>
<details>
  <summary><b><font size=4>Transactional Compaction</b></font></summary>
          Support transactional data compaction.
</details>
<details>
  <summary><b><font size=4>Data Loading Pipeline | Prefetcher</b></font></summary>
          The data for the next batches can be load to staging storage while processing the current batch.
</details>
