(parquet)=

# Parquet

`omniload` reads [Apache Parquet] files. Parquet is a column-oriented binary
format for structured data. It stores a schema with the data and supports
nested values, typed columns, compression, and row groups.

Parquet is available for read operations on every supported filesystem source.
Parquet is also available for write operations through the local `file://`
destination.

## Installation

Parquet support is included in the base `omniload` installation. It uses
[`pyarrow`] to read Parquet files and [`polars`] to write them.

```sh
pip install omniload
```

Do not install an optional extra to use Parquet.

## Where it works

Every source that uses the shared filesystem readers can read Parquet:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...
- HTTP and HTTPS URLs: {ref}`http`

The source determines the storage connection and authentication. Parquet adds
no storage-specific configuration.

`omniload` selects the Parquet reader in one of these cases:

- The filename ends in `.parquet`.
- The filename ends in `.parquet.gz`.
- The source path has an explicit `#parquet` {ref}`format hint <format-hint>`.

For example, use `#parquet` when the file has no `.parquet` extension:

```text
file://data/events.bin#parquet
```

Gzipped Parquet files are decompressed before `pyarrow` reads them:

```text
s3://my-bucket/events/2026-09-01.parquet.gz
```

See {ref}`filesystem` for format detection, glob patterns, compression, and
incremental file selection.

## Reading behavior

`omniload` reads Parquet files with `pyarrow.parquet.ParquetFile`. The reader
returns rows in batches. The default batch size is 10 rows.

The reader converts each Arrow batch to Python dictionaries before it passes
the rows to the loader. The Parquet schema controls the decoded column types.

Parquet is not a streaming format over a remote transport. `pyarrow` reads the
file footer and can request the complete data section. A large remote Parquet
file can therefore require substantial memory and network transfer even when
the reader returns rows in batches.

If the Parquet file is corrupt, truncated, encrypted without the required
configuration, or uses an unsupported codec, `pyarrow` raises an error during
the load. Validate files upstream when a partial or failed load is not
acceptable.

## Examples

### Load a local Parquet file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://data/events.parquet' \
    --source-table 'events' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

When the URI already contains the file path, `--source-table` does not select a
table inside the Parquet file. The destination table is set by `--dest-table`.

### Load a Parquet file from S3 into DuckDB

```sh
omniload ingest \
    --source-uri 's3://my-bucket?access_key_id=YOUR_ACCESS_KEY&secret_access_key=YOUR_SECRET_KEY' \
    --source-table 'events/2026-09-01.parquet' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

Use the documentation for the selected filesystem source to configure
authentication and source paths.

### Read a file with a non-standard extension

```sh
omniload ingest \
    --source-uri 'file://data/events.data#parquet' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

The `#parquet` fragment is not part of the filename. It instructs `omniload`
to use the Parquet reader.

## Write a local Parquet file

Use a `file://` destination path that ends in `.parquet`, or use `#parquet`:

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/app' \
    --source-table 'public.events' \
    --dest-uri 'file://export/events.parquet' \
    --dest-table 'public.events'
```

The `file://` destination writes one Parquet file at the requested path. It
creates missing parent directories and overwrites an existing output file.

The destination removes dlt bookkeeping columns before it writes the file. It
collects all loaded rows before it writes the Parquet table. This makes a
single-file output reliable, but it is not suitable for data that cannot fit in
memory.

The column types in that file are decided by the staging format rather than by
Parquet: under the default staging a timestamp and a decimal both arrive as
text, so they are written as string columns. Pass `--loader-file-format parquet`
for typed columns. See {ref}`file-load-types`.

See {ref}`file-destination` for the complete URI and destination-table rules
for the `file://` destination.

## Extended-type handling

This section describes the reader and the writer called directly. An ingest
stages rows between the two, and what a load delivers to a file is decided there
rather than here: see {ref}`file-load-types`.

Read directly, strings, integers, floating-point values, booleans, dates,
timestamps, times, binary values, decimals, lists, structs and all-null columns
all come back as themselves. A time zone survives the read: a
`timestamp[us, tz=UTC]` column arrives as a timezone-aware `datetime`, and a
naive one stays naive.

Nanosecond columns are the exception, because a row carries Python values rather
than Arrow ones. A `time64[ns]` narrows at the read, `datetime.time` having no
nanoseconds, so `00:00:00.123456789` arrives as `00:00:00.123456`; passed back
to the writer it becomes a `time64[ns]` column again, carrying the narrowed
value. Nanosecond timestamps and durations keep their precision at the read, the
pandas types carrying it, and lose it on the way back out, where the writer emits
microsecond columns. The Feather reader answers the same way, which the test
suite pins; the file itself stores whatever precision it was written with.

The writer has limits of its own, and they follow from the library split: the
reader is `pyarrow`, the writer is [`polars`]. Polars widens an integer above the
signed 64-bit range to a 128-bit one, and Parquet has no type for that, so the
writer narrows such a column to an unsigned 64-bit integer before writing it,
inside a struct or a list of structs as well as at the top level. A value in `0..2**64-1` is written
as `uint64` and reads back digit for digit; anything else, a value past
`2**64-1` or a negative in a column another row widened, is refused by value and
by the column holding it, and no file is written. Inside a bare list the value is
named and the column is not, which is Polars' own error reporting.
`write_feather` and `write_orc` build a PyArrow table from the same Python
values, where inference stops at a signed 64-bit, and raise `OverflowError` on
the whole range instead. The JSON, JSONL, CSV and YAML writers keep any of these
values digit for digit.

A decimal too wide for a 128-bit store goes the other way, and it is the value
that decides rather than the column's declared type: a row carries a Python
`Decimal`, and every writer infers from that one. A `decimal256(41, 2)` column
holding `3.14` reads and writes without complaint, as `decimal128(38, 2)`. One
holding a 41-digit value reads back exactly and then fails the Parquet write
with `Decimal is too large to fit in Decimal128`, where `write_feather` takes it
and `write_orc` refuses it too.

## Parquet files and Parquet loader files

The Parquet source format is independent of the Parquet loader format that
`omniload` can select for some warehouse destinations. A Parquet source
controls how `omniload` reads input files. A Parquet loader controls how
`omniload` stages rows for a destination.

You do not need to set a loader option to read a Parquet source file.

[Apache Parquet]: https://parquet.apache.org/
[`polars`]: https://docs.pola.rs/
[`pyarrow`]: https://arrow.apache.org/docs/python/
