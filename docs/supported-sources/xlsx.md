(xlsx)=

# XLSX

`omniload` reads [Excel workbook (XLSX)] spreadsheet files. By default, every
nonempty worksheet is loaded into its own destination table.

XLSX is supported for reads on shared filesystem sources and for writes through
the local `file://` destination.

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

## Example: write a table to an XLSX file

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/db' \
    --source-table 'public.events' \
    --dest-uri   'file://export/events.xlsx' \
    --dest-table 'public.events'
```

The workbook holds one worksheet, named after the table part of `--dest-table`
(`events` here), or after the file name when no table is named. Reading the file
back finds the table by that name, as `file://export/events.xlsx#sheet_name=events`.
Excel limits a worksheet name to 31 characters, without any of `[]:*?/\`, so a
longer name is shortened and those characters become `_`, with a warning.

Numbers, booleans and strings are written as themselves, and dates, timestamps and
times as date cells. Excel has no timezone, so a timestamp with one is written as the
same instant in UTC. Nested values, binary and decimals are written as text, the way
CSV writes them: JSON text, base64, and a decimal keeping its scale.

:::{note}
A value that an Excel cell would change is refused, naming the column, and no file
is written. That includes an integer beyond 2^53, NaN or infinity, a string (or the
text a nested value becomes) longer than 32,767 characters, a date before
1900-03-01, where Excel counts a leap day that 1900 did not have, and a load with
more rows or columns than one worksheet holds. Times are refused where Excel
would change them too: a time of day with a timezone, a time at or after
23:59:59.9995, which reads back as midnight, and a timestamp after
9999-12-31 23:59:59.999, which can read back in year 10000. Where another format keeps the value, the error names it.

Two limits are Excel's own precision and are not refused: a float is written to 16
significant digits, and readers round a time to the millisecond. A time of day has
no cell type of its own, so reading one back through omniload with the default
calamine engine gives a timestamp on 1899-12-31, Excel's day zero. A row whose every
field is null is written as an empty row, which the reader skips. A load with no rows
is a worksheet with no cells, and reading it back with a worksheet selector needs
`raise_if_empty=false`, as for any blank worksheet.
:::

:::{note}
dlt stages the rows between the source and the writer, and the staging format
decides what the writer receives: a load to a local `file://` destination
writes a timestamp or a decimal as text unless `--loader-file-format parquet`
is passed. See {ref}`file-load-types`.
:::

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
