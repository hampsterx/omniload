(nextcloud)=

# Nextcloud

[Nextcloud Files] is a secure cloud storage and file sharing software for easy
sync, sharing and collaboration on your files.
`omniload` supports Nextcloud as a data source.

## URI format

The URI for connecting to Nextcloud is structured as follows.

```text
https+webdav://<USERNAME>:<PASSWORD>@cloud.example.org
```

Include the WebDAV prefix in the selected path, for example
`--source-table 'remote.php/webdav/path/to/data.csv'`.
A non-root URI path selects the resource itself and takes precedence over
`--source-table`; the two paths are not concatenated.

## URI parameters

The Nextcloud connected uses the {ref}`webdav` connector, so all of its
URL parameters are also available here.

## Authentication

To integrate `omniload` with Nextcloud, you need to authenticate like you
do with any HTTP server.

## Example: Load CSV file from Nextcloud into DuckDB

```sh
omniload ingest \
    --source-uri   'https+webdav://<USERNAME>:<PASSWORD>@cloud.example.org' \
    --source-table 'remote.php/webdav/path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.

[Nextcloud Files]: https://nextcloud.com/files/
