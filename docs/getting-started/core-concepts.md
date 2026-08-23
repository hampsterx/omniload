---
outline: deep
---

# Concepts
omniload has a few simple concepts that you should understand before you start using it.

## Source & destination URIs
The source and destination are the two main components of omniload. The source is the place from where you want to ingest the data, hence the name "source" and the destination is the place where you want to store the data.

The sources and destinations are identified with [URIs](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier). A URI is a simple string that contains the credentials used to connect to the source or destination.

Here's an example URI for a Postgres database:
```
postgresql://admin:admin@localhost:8837/web?sslmode=disable
```

The URI is composed of the following parts:
- `postgresql`: the scheme, identifying the source or destination type (here, a PostgreSQL database)
- `admin:admin`: the username and password
- `localhost:8837`: the host and port
- `web`: the database name
- `sslmode=disable`: the query parameters

omniload can connect to any source or destination using this structure across all databases.

:::{note}
omniload uses [dlt](https://github.com/dlt-hub/dlt) & [SQLAlchemy](https://www.sqlalchemy.org/) libraries internally, which means you can get connection URIs by following their documentation as well, they are supposed to work right away in omniload.
:::

## Source & destination tables
The source and destination tables are the tables from the source and destination databases, respectively. The source table is the table from where you want to ingest the data from, and the destination table is the table where you want to store the data.

omniload uses the `--source-table` and `--dest-table` flags to specify the source and destination tables, respectively. The `--dest-table` is optional, if you don't specify it, omniload will use the same table name as the source table.

Qualified SQL names are parsed from the right: the last component is the table, the component before it is the schema or dataset, and an optional third component is the catalog, database, or project. Quote an individual identifier with square brackets, double quotes, or backticks when it contains a dot. For example, `[dbo].[my.table]` selects the table `my.table` in the `dbo` schema. Quote each component separately; a whole-path spelling such as `` `project.dataset.table` `` is not accepted.

SQL sources accept `schema.table`. Select their catalog or database in `--source-uri`. Destinations can additionally route a three-component name when their dlt configuration exposes the catalog:

| Destination | Three-component form | Catalog routing |
|---|---|---|
| Athena | `catalog.schema.table` | `aws_data_catalog` |
| BigQuery | `project.dataset.table` | `project_id` |
| Databricks | `catalog.schema.table` | Databricks credentials |
| MotherDuck | `database.schema.table` | MotherDuck credentials |
| MSSQL, Postgres, Redshift, Snowflake, Synapse | `database.schema.table` | Database selected by the connection URI |
| Trino | `catalog.schema.table` | First path component of the connection URI |

The catalog in `--dest-table` takes precedence over the corresponding value in `--dest-uri`. DuckDB and CrateDB remain two-level destinations. MySQL uses `database.table`, ClickHouse uses `database.table`, and SQLite uses `schema.table` with `main` as the default schema. Elasticsearch indexes and MongoDB destination collection names are opaque, so dots in those names are preserved rather than treated as qualification.

A defaulted `--dest-table` carries the source's spelling, which a destination that splits on dots would read as its own components. So a name of three or more components only defaults through to a destination whose names are opaque; a qualifying destination asks for `--dest-table` explicitly rather than choosing a schema the source never named. A dot inside a quoted identifier does not count towards that, so `public."order.items"` still defaults.


## Incremental loading
omniload supports incremental loading, which means you can choose to append, merge or delete+insert data into the destination table. Incremental loading allows you to ingest only the new rows from the source table into the destination table, which means that you don't have to ingest the entire table every time you run omniload.

Incremental loading requires various identifiers in the source table to understand what has changed when, so that the new rows can be ingested into the destination table. Read more in the [Incremental Loading](/getting-started/incremental-loading.md) section.

