(feather)=

# Feather

`omniload` reads and writes [Feather] V2 files. Feather V2 is the [Apache Arrow]
IPC file format: a columnar on-disk layout that stores a schema and zero or more
record batches, and that every current Arrow implementation opens.

Feather is supported for reads on shared filesystem sources and for writes
through the local `file://` destination.

## Where it works

Feather is available on every source that uses the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

Remote reads use the source's existing fsspec handle. They use its existing
authentication. No separate Feather storage configuration is required.

A file is read as Feather when its extension is `.feather`, `.arrow` or `.ipc`,
optionally followed by `.gz`. All three name the same container. You can also
append the `#feather` {ref}`format hint <format-hint>` to a file with a
different extension. `omniload` decompresses gzipped files automatically.

For details about format selection, see {ref}`file-format-routing`.

:::{note}
Feather **V1** is a different container, and is not supported in either
direction. Rewrite a V1 file as V2 to read it; the reader says so rather than
reporting the file as damaged.
:::

## Examples

### Load a local Feather file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.feather' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load a Feather file from S3

Use `#feather` if the object name does not end in one of the known extensions.

```sh
omniload ingest \
    --source-uri 's3://' \
    --source-table 'my_bucket/events/day.data#feather' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load multiple Feather files

Use a glob to load rows from all matching Feather files.

```sh
omniload ingest \
    --source-uri 'file://events/*.arrow' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Write a source table to a local Feather file

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/db' \
    --source-table 'public.events' \
    --dest-uri 'file://export/events.feather' \
    --dest-table 'public.events'
```

Feather output uses PyArrow and is available through the local `file://`
destination. Columns that are absent from an individual source row are written
as null values.

## Extended-type handling

The reader uses PyArrow's `ipc.open_file()` and reads one record batch at a
time, the Arrow IPC analogue of ORC's stripes. Large batches are sliced into
chunks according to the `chunksize` format hint.

Strings, integers, floating-point values, booleans, dates, timestamps (with or
without a time zone), times, binary values, decimals, lists, structs and
all-null columns all survive a write followed by a read.

:::{note}
Rows are Python values, so **nanosecond** time, timestamp and duration columns
narrow to microseconds on the way through. The reader itself keeps nanosecond
timestamps and durations, which arrive as `pandas.Timestamp` and
`pandas.Timedelta`; a `time64[ns]` narrows at the read, because `datetime.time`
has no nanoseconds, and writing any of the three back emits a microsecond
column. This is a property of the row pipeline rather than of Feather: the ORC
and Parquet readers narrow the same values the same way, and the Feather file
itself stores whatever precision it was written with.
:::

[Apache Arrow]: https://arrow.apache.org/
[Feather]: https://arrow.apache.org/docs/python/feather.html
