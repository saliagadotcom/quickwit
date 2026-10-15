---
title: Metastore configuration
sidebar_position: 4
---

Quickwit needs a place to store meta-information about its indexes.

For instance:

- The index configuration.
- Meta-information about its splits. For instance, their IDs, the number of documents they contain, their sizes, their min/max timestamp, and the set of tags present in the split.
- The different sources checkpoints.
- Some extra information such as the index creation time.

The metastore is entirely defined by a single URI. One can set it by editing the `metastore_uri` parameter of the [node configuration file](./node-config.md) (often named `quickwit.yaml`).

Currently, Quickwit offers two implementations:

- **PostgreSQL**: recommended for distributed usage.
- **File-backed implementation**.

# PostgreSQL Metastore

We recommend the PostgreSQL metastore for any distributed usage.

The PostgreSQL metastore can be configured by setting a PostgreSQL URI in the `metastore_uri` parameter of the Quickwit configuration file. The URI takes the following format:

```
postgres://[user]:[password]@[host]:[port]/[dbname]
```

Some of those parameters can be omitted. The following PostgreSQL URIs are for instance valid:

```
postgres://localhost/mydb
postgres://user@localhost
postgres://user:secret@localhost
```

The database has to be created in advance.

On its first execution, Quickwit will transparently create the necessary tables.

Likewise, if you upgrade Quickwit to a version that includes some changes in the PostgreSQL schema, Quickwit will transparently operate the migration startup.

## PostgreSQL connection pool

Each Quickwit node running the `metastore` service maintains its own PostgreSQL connection pool. Database-backed metastore nodes admit at most `2 * metastore.postgres.max_connections` in-flight requests, so the default is `20`.

When sizing the pool, keep `metastore_nodes * metastore.postgres.max_connections` below the PostgreSQL connection limit with operational headroom. If Quickwit logs `pool timed out while waiting for an open connection`, check `quickwit_metastore_active_connections`, `quickwit_metastore_idle_connections`, and `quickwit_metastore_acquire_connections`.

# File-backed metastore

For convenience, Quickwit also makes it possible to store its metadata in files using a file-backed metastore. In that case, Quickwit will write one file per index.

The metastore is then configured by passing a [storage URI](storage-config#storage-uris) that will serve as the root of the metastore storage.

The metadata file associated with a given index will then be stored under

  `[storage_uri]/[index_id]/metastore.json`

For the moment, Quickwit supports two types of storage types:

- a local file system URI (e.g., `file:///opt/toto`). It is also valid to pass a file path directly (without file://). `/var/quickwit`. Relative paths will be resolved with respect to the current working directory.
- S3-compatible storage URI (e.g., `s3://my-bucket/some-path`). See the [storage config](storage-config) documentation to configure S3 or S3-compatible storage providers.

### Polling configuration

By default, the File-Backed Metastore is only read once when you start a Quickwit process (searcher, indexer, ...).

You can also configure it to poll the File-Backed Metastore periodically to keep a fresh view of it. This is useful for a Searcher instance that needs to be aware of new splits published by an Indexer running in parallel.

To configure the polling interval (in seconds), add a URI fragment to the storage URI as follows: `s3://quickwit/my-indexes#polling_interval=30s`

The polling interval can also be set with the `metastore.file.polling_interval` parameter of the [node configuration file](./node-config.md#file-backed-metastore-configuration). If both the URI fragment and the parameter are set, they must agree.

:::note
The polling interval can be configured in seconds only in the URI fragment; other units, such as minutes or hours, are not supported there.
:::

:::tip
Amazon S3 charges $0.0004 per 1000 GET requests. Polling a metastore every 30 seconds costs $0.04 per month and index.
:::

### Examples

The following file-backed metastore URIs for instance are valid:

```markdown
s3://my-indexes
s3://quickwit/my-indexes
s3://quickwit/my-indexes#polling_interval=30s
file:///local/indices
file:///local/indices#polling_interval=30s
/local/indices
./quickwit-metastores
```

:::caution
By default, the file-backed metastore does not support multiple instances running at the same time because their writes overwrite each other. Ensure that only one file-backed metastore instance is running at all times, or enable [concurrent writers](#concurrent-writers).
:::

### Concurrent writers

Setting `metastore.file.concurrent_writers: true` in the [node configuration file](./node-config.md#file-backed-metastore-configuration) makes it safe to run the `metastore` service on several nodes sharing the same `metastore_uri`, for instance for high availability.

In that mode, every write to a metastore file is a conditional write (compare-and-swap) against the version of the file the node last read. A write that lost a race against another node is rejected by the storage; the node then reloads the file, re-applies the operation and writes again. Updates are never lost, at the cost of extra requests when two nodes write to the same index at the same time.

```yaml
metastore_uri: s3://my-bucket/indexes
metastore:
  file:
    concurrent_writers: true
    polling_interval: 30s
```

Concurrent writers require a storage that supports **and enforces** conditional writes:

- Amazon S3 supports conditional writes natively. Among S3-compatible providers, make sure that the `If-Match` and `If-None-Match` headers are honored, not merely accepted. LocalStack enforces them from version 4.14.
- Local file system URIs (`file://`) rely on advisory file locks (`flock`). They are safe for several processes on the same host or on a shared file system with working locks, but not on a network file system that ignores locks.

At startup, a node configured with `concurrent_writers: true` writes and deletes a probe object under the metastore URI to verify that the storage enforces conditional writes, and refuses to start otherwise.

A node only observes the index files written by other nodes when it reads them again. Set a [polling interval](#polling-configuration) so that every node keeps a fresh view of the indexes created, deleted and updated by its peers. Until the next poll, a node that lost a race to create an index rejects requests on that index as if its creation were still in progress.

Conflicts are exposed by the `quickwit_metastore_conditional_write_conflicts_total` and `quickwit_metastore_conditional_write_retries_exhausted_total` [metrics](../reference/metrics.md#metastore-metrics). An operation that keeps losing races gives up after a few retries and returns an unavailable error; the client should retry it.
