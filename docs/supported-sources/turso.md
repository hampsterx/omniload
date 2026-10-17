# Turso

[Turso](https://turso.tech/) is an in-process SQL database engine compatible with
SQLite, with a sync protocol for replicating a database to and from a server: Turso
Cloud, or a self-hosted `tursodb --sync-server`.

omniload supports Turso as a source and a destination, both for a local database
file and for a remote database.

## Installation

```shell
pip install 'omniload[turso]'
```

The extra installs [pyturso] and its SQLAlchemy dialects. pyturso publishes no
Windows wheel, so the extra is empty on Windows.

## URI format

A local database file, read and written in process:

```text
turso:///<database-file>
```

A remote database, Turso Cloud or a self-hosted sync server:

```text
turso://<host>[:<port>]?authToken=<token>
```

URI parameters:

- `database-file`: the path to the database file. As with `sqlite://`, three slashes
  start a relative path and four an absolute one (`turso:////var/data/app.db`).
- `host`, `port`: the database's host. For Turso Cloud, this is the hostname of the
  database's URL, such as `my-db-my-org.turso.io`.
- `authToken` (or `auth_token`): the database's auth token, sent as an
  `Authorization: Bearer` header. A self-hosted sync server may need none.
- `tls`: `true` (the default) connects over HTTPS; `tls=false` uses plain HTTP, for a
  local sync server.

## Examples

Load a table from a local Turso file into DuckDB:

```shell
omniload ingest \
  --source-uri 'turso:///app.db' \
  --source-table 'people' \
  --dest-uri 'duckdb:///warehouse.duckdb' \
  --dest-table 'raw.people'
```

Load a PostgreSQL table into a Turso Cloud database:

```shell
omniload ingest \
  --source-uri 'postgresql://user:password@localhost:5432/app' \
  --source-table 'public.people' \
  --dest-uri 'turso://my-db-my-org.turso.io?authToken=TOKEN' \
  --dest-table 'people'
```

Copy between a self-hosted sync server and a local file:

```shell
tursodb --sync-server 127.0.0.1:8080 server.db

omniload ingest \
  --source-uri 'turso:///app.db' \
  --source-table 'people' \
  --dest-uri 'turso://127.0.0.1:8080?tls=false' \
  --dest-table 'people' \
  --incremental-strategy append
```

## How a remote database is read and written

omniload reads and writes a remote database through a replica it bootstraps for the
run, from the whole database, and discards afterwards. A destination run loads into
the replica and pushes it to the server once the load has committed. If the push
fails, omniload cannot tell whether some rows reached the server, and says so:
re-running an `append` load may then duplicate rows, while a `replace` load is safe to
repeat.

The pipeline state, including incremental cursors, travels with the data: with the
default temporary pipelines directory, each run restores it from the server.

:::{note}
- **Dispositions**: the destination takes `append` and `replace` only. `merge`,
  `delete+insert` and `scd2` are refused before anything is written, as are sources
  that set a merge disposition on their own resources. dlt merges through a staging
  database it `ATTACH`es, which Turso treats as experimental, and with that enabled
  Turso resolves the merge's database-qualified correlated subquery against the wrong
  table and deletes rows it should keep ([tursodatabase/turso#6279]).
- **`--full-refresh`**: refused, on a first load too. dlt would drop the tables, then
  fail on the staging database it `ATTACH`es. To reload from scratch, load into a new
  destination table: the pipeline, and with it any incremental cursor, is keyed on
  the destination table, so a new one starts without state.
- **Load jobs**: they run one at a time, because Turso refuses a second concurrent
  writer instead of waiting for it. A load that writes many tables takes longer for
  it.
- **Schema**: tables land in the `main` schema. Name the destination table as
  `<table>` or `main.<table>`.
- **`--pipelines-dir`**: a remote destination cannot be combined with it. dlt saves
  the run's state before the push, so a failed push would leave a persistent
  pipelines directory ahead of the server.
- **Database size**: every run downloads the whole remote database into its replica,
  for a read of one table as for an append of one row. The replica goes under
  `remote_database_staging_root` when the Python API sets one. Otherwise a
  destination's replica goes to the system temporary directory, and a source's to
  the pipelines directory.
- **A server that stops answering**: pyturso's sync requests have no timeout, so a
  run against a server that accepts the connection and never replies waits
  indefinitely.
:::

[pyturso]: https://pypi.org/project/pyturso/
[tursodatabase/turso#6279]: https://github.com/tursodatabase/turso/issues/6279
