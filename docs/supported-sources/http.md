(http)=

# HTTP

`omniload` reads a file addressed by an [HTTP] or HTTPS URL, using the same reader
stack as every other {ref}`filesystem <filesystem>` source. See that page for the
supported formats, format and reader hints, and type normalization.

## URI format

The URI is the URL of the file, exactly as you would open it.

```text
https://example.org/path/to/data.csv
http://example.org/path/to/data.parquet
```

## The query string is part of the address

Unlike the other schemes in this family, an HTTP URL's query string is **not**
connection configuration: it is sent to the server verbatim. That is what makes a
presigned URL work, since the signature lives in the query and is computed over its
encoded form:

```sh
omniload ingest \
    --source-uri   'https://bucket.s3.amazonaws.com/exports/orders.csv?X-Amz-Signature=...&X-Amz-Expires=900' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.orders'
```

Quote the URI in your shell: `&` would otherwise background the command.

:::{note}
Because the query is addressing information, connection options cannot be passed
in it. Pass them as keyword arguments to `run_ingest` (the Python API) instead:
`block_size`, `simple_links`, `headers`, `client_kwargs` and `ssl` reach the
underlying [fsspec HTTP filesystem].
:::

## Authentication

Credentials in the URL's userinfo become an HTTP basic-authentication header.
Percent-encode any character that a URI cannot carry literally, `@` and `/`
included:

```text
https://<USERNAME>:<PASSWORD>@example.org/private/data.csv
```

Environment proxy settings and `.netrc` are honoured.

## Extended type support

| Type    | Support | Remarks                                                     |
|:--------|:--------|:------------------------------------------------------------|
| Formats | ✅      | Every format the {ref}`filesystem <filesystem>` page lists. |
| Ranges  | ✅      | A file is read in ranges where the server serves them.      |
| Globs   | ❌      | One concrete URL per source; see Limitations.               |

## Examples

### Load a public CSV file into DuckDB

```sh
omniload ingest \
    --source-uri   'https://example.org/path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

### Name the format for a URL that has no useful extension

An API endpoint rarely ends in `.csv`. Append a `#format` fragment:

```sh
omniload ingest \
    --source-uri   'https://api.example.org/exports/latest#csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

## How much is transferred

Where the server honours byte ranges, the file is read in ranges rather than
downloaded whole, so a line-delimited document starts producing rows before all of
it has arrived. Two cases are read whole instead, because they cannot be read any
other way:

- a server that ignores `Range` and answers with the entire body;
- a response with no `Content-Length` (ordinary `Transfer-Encoding: chunked`),
  which leaves the file with no known size and therefore no seekable form.

Parquet is read whole in every case: pyarrow asks for a file's entire data section
in one read, independently of the transport.

## Limitations

- **One concrete URL per source.** Wildcards are not supported: HTTP has no
  listing operation, only whatever links a server happens to render, so a glob
  cannot be resolved reliably.
- **No incremental file selection.** `--filesystem-incremental` is refused for
  HTTP. A response need not carry a `Last-Modified` header, and a missing one
  would read as "just now", so every file would be reloaded on every run while the
  run reported that it had filtered.
- **Read only.** There is no portable way to write a file over plain HTTP.

Both of the first two are the subject of ongoing work; see the {ref}`WebDAV
<webdav>` source for an HTTP-based transport that does support listing.

[fsspec HTTP filesystem]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem
[HTTP]: https://developer.mozilla.org/en-US/docs/Web/HTTP
