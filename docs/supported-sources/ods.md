(ods)=

# ODS

`omniload` reads [OpenDocument spreadsheet (ODS)] files,
used by [OpenOffice], [LibreOffice], and other spreadsheet applications.
By default, every nonempty worksheet is loaded into its own destination table.

ODS is currently supported for read operations only.

## Example: load a workbook into DuckDB

```sh
omniload ingest \
    --source-uri 'file://path/to/workbook.ods' \
    --dest-uri   'duckdb:///local.duckdb' \
    --dest-table 'public.workbook'
```

The first part of `--dest-table` selects the destination dataset. The second
part is a required placeholder for a plural workbook load. Worksheet names
replace it as the destination table names.

To load one worksheet into the table named by `--dest-table`, select it by name
or one-based number:

```sh
omniload ingest \
    --source-uri 'file://path/to/workbook.ods#sheet_name=events' \
    --dest-uri   'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

Select several worksheets by name or one-based position with a JSON array:

```text
file://path/to/workbook.ods#sheet_name=["events","inventory"]
file://path/to/workbook.ods#sheet_id=[1,2]
```

## Where it works

OpenOffice and LibreOffice ODS files can be accessed on every source that
goes through the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

A file is read as ODS when its extension is `.ods` (optionally `.ods.gz`),
or when an explicit `#ods` {ref}`format hint <format-hint>` is appended.
Gzipped files are decompressed automatically.

## How it works

The whole file is read into memory and decoded at once (ODS is not a streaming
format); a corrupt or truncated file raises rather than loading partial data.
Map keys are expected to be strings.
During plural loads, worksheets without data rows are skipped because dlt has
no row from which to create a destination table. This includes header-only sheets.

## Options

Options can be defined by using reader hints. The loader is using
[polars.read_ods], please consult its documentation about all available
parameters and their descriptions.

See {ref}`Workbook tables <workbook-tables>` for naming, glob, collision, and
destination compatibility rules shared by XLSX and ODS.


[LibreOffice]: https://en.wikipedia.org/wiki/LibreOffice
[OpenDocument spreadsheet (ODS)]: https://en.wikipedia.org/wiki/OpenDocument
[OpenOffice]: https://en.wikipedia.org/wiki/Apache_OpenOffice
[polars.read_ods]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_ods.html
