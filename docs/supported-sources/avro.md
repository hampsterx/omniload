(avro)=

# Apache Avro

`omniload` reads [Apache Avro] object container files. Like BSON, CBOR and MessagePack it
is a **read format**: it is decoded through the shared filesystem readers, so any source
that reads files can read Avro.

Avro is currently supported for read operations only.

## Installation

Avro support ships with the base install, so there is nothing extra to install:

```sh
pip install omniload
```

Avro is decoded with `polars`, which is already a core dependency; see
{ref}`file-format-routing` about how omniload chooses a reader per format.

## Where it works

Avro is available on every source that goes through the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

Remote reads go through the source's own fsspec handle, so they reuse its existing
authentication (no separate Avro storage configuration). A file is read as Avro when its
extension is `.avro` (optionally `.avro.gz`), or when an explicit `#avro`
{ref}`format hint <format-hint>` is appended to a name that carries no extension.
Gzipped files are decompressed automatically.

The whole file is read and decoded at once: Avro carries its schema in the container
header, but the reader behind it has no lazy scan, so `#chunksize=` bounds how many rows
are handed downstream at a time rather than how much is held in memory.

## Examples

### Load a local Avro file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.avro' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load an Avro file from S3

Use `#avro` if the object name does not end in `.avro`.

```sh
omniload ingest \
    --source-uri 's3://?access_key_id=KEY&secret_access_key=SECRET' \
    --source-table 'my_bucket/events/day.data#avro' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load selected columns

`#columns=` takes one bare name, or a JSON list for several.

```sh
omniload ingest \
    --source-uri 'file://events/day.avro#columns=["id","name"]' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

## Type handling

Avro's types map onto the loader's as follows. Every row of this table is covered by the
test suite rather than inferred from the specification.

| Avro type | Loaded as |
| :--- | :--- |
| `int`, `long` | integer |
| `float`, `double` | float |
| `string` | string |
| `boolean` | boolean |
| `bytes`, `fixed` | bytes |
| `enum` | string (the symbol) |
| `array` | list |
| `record` | nested object |
| `["null", T]` union | the value, or null |
| logical `date` | `datetime.date` |
| logical `time-micros` | `datetime.time` |
| logical `timestamp-micros` / `timestamp-millis` | UTC-aware `datetime.datetime` |
| logical `decimal` (in `bytes` or `fixed`) | `decimal.Decimal` |
| logical `uuid` | string |

Timestamps load as timezone-aware UTC datetimes, which is what the Avro specification
says they are: the encoded value is an offset from the epoch and carries no local zone.

## Limitations

Three Avro schemas have no mapping onto the reader's in-memory representation, and a file
using one is rejected rather than partially loaded:

- A **`map` field** (`{"type": "map", "values": ...}`). This is the one to watch for,
  because `map` is an ordinary Avro type that plenty of writers emit. If you control the
  writer, emit a `record` with declared fields instead.
- A **field typed `null`** outright, rather than as one branch of a union.
- A **union of more than two branches**, or one whose branches are not `null` plus a
  single type.

Empty and malformed files raise rather than loading partial data, except when the **tail**
of the file is missing. Two separate things are going on there, and only the first is
inherent to the format:

- **A tail cut exactly at a block boundary is undetectable.** An Avro container is a
  sequence of self-describing blocks and carries no trailing index or record count, so a
  file that lost its last blocks is a shorter valid file and nothing distinguishes the two.
  Feather, ORC and Parquet each carry a footer that a truncation destroys, so they raise on
  the same damage; Avro is closer to {ref}`msgpack` here.
- **A tail cut one byte into the next block's header is detectable, and is currently
  accepted anyway.** That single leading byte of an unfinished record count is corruption a
  reader could reject, and the reader behind this format treats it as end of file instead.
  Two or more bytes into that header does raise, so the window is one byte wide.

Validate file integrity upstream if a partial load would be a problem.

## Why Avro is a read format

The `file://` destination writes several formats, and Avro is not one of them. That is not
a gap waiting to be filled in: the Avro writer available here mis-frames a list column
that holds an empty list *before* a non-empty one, so a load carrying
`{"tags": []}` followed by `{"tags": ["x"]}` would produce a file whose records overrun
their own block. Depending on the other columns in the row, reading such a file back
fails outright or, worse, succeeds with wrong values.

That is a defect in the underlying library rather than a design choice here, and it
behaves identically across every version of it that `omniload` supports. Registering the
writer would mean offering an export that silently corrupts an ordinary shape, so the
format stays read-only until it is fixed upstream. Export to {ref}`parquet`,
{ref}`feather` or {ref}`orc` in the meantime; all three are columnar and all three
round-trip nested columns.

[Apache Avro]: https://avro.apache.org/
