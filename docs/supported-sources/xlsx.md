(xlsx)=

# XLSX

`omniload` reads [Excel Workbook] XLSX spreadsheet files. By default, every
nonempty worksheet is loaded into its own destination table.

XLSX is currently supported for read operations only.

## Example: load a workbook into DuckDB

```sh
omniload ingest \
    --source-uri 'file://path/to/workbook.xlsx' \
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
    --source-uri 'file://path/to/workbook.xlsx#sheet_name=events' \
    --dest-uri   'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

Select several worksheets by name or one-based position with a JSON array:

```text
file://path/to/workbook.xlsx#sheet_name=["events","inventory"]
file://path/to/workbook.xlsx#sheet_id=[1,2]
```

## Where it works

Excel XLSX files can be accessed on every source that goes through the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

A file is read as XLSX when its extension is `.xlsx` (optionally `.xlsx.gz`),
or when an explicit `#xlsx` {ref}`format hint <format-hint>` is appended.
Gzipped files are decompressed automatically.

## How it works

The whole file is read into memory and decoded at once (XLSX is not a streaming
format); a corrupt or truncated file raises rather than loading partial data.
Map keys are expected to be strings.
During plural loads, worksheets without data rows are skipped because dlt has
no row from which to create a destination table. This includes header-only sheets.

## Options

Options can be defined by using reader hints. The loader is using
[polars.read_excel], please consult its documentation about all available
parameters and their descriptions.

See {ref}`Workbook tables <workbook-tables>` for naming, glob, collision, and
destination compatibility rules shared by XLSX and ODS.


[Excel workbook (XLSX)]: https://en.wikipedia.org/wiki/Microsoft_Excel#Current_file_extensions
[polars.read_excel]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_excel.html
