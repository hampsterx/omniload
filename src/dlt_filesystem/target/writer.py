"""Format writers for the local ``file://`` destination.

Every writer takes ``(path, rows)`` and emits one file. Output is UTF-8 whatever the
process locale is, because the readers decode as UTF-8 unconditionally (``json.loadb``
rejects anything else, and Polars defaults to it), so a locale-encoded export would not
read back on the machine that wrote it.
"""

from dlt_filesystem.source.error import MissingDecoderError


def _column_union(rows: list[dict]) -> list[str]:
    """Union of keys in first-seen order.

    dlt omits null keys per row, so a later row can carry a column the first row lacked.
    First-seen order preserves the source column order (rather than sorting), which is
    what an export is expected to look like.
    """
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    return fieldnames


def _frame(rows: list[dict]):
    """Build a Polars frame over every column any row carries.

    ``infer_schema_length=None`` reads every row rather than the first 100, because
    dlt omits null keys per row: a column can first appear anywhere in the load, and
    the default window would drop one that first appears past it. Polars fills the
    gaps with null and keeps first-seen key order, which is what ``_column_union``
    was built by hand to do for the writers that predate this.

    An empty load has no keys to infer from, so it becomes a frame with no columns
    rather than an error.
    """
    import polars as pl

    if not rows:
        return pl.DataFrame()
    return pl.from_dicts(rows, infer_schema_length=None)


def _spell_for_csv(value):
    """Spell a value CSV cannot hold the way the JSON writers spell it.

    The same rule ``_yaml_dumper`` follows for the types PyYAML refuses: ask dlt's own
    serializer. A nested document becomes its JSON text and ``bytes`` become the base64
    string ``.json`` and ``.jsonl`` already write for it, so one value reads the same
    whichever format the export names.
    """
    from dlt.common import json

    if value is None:
        return None
    spelled = json.loads(json.dumps(value))
    # A scalar dlt spells as a string (base64 bytes, an ISO timestamp) is that string,
    # not a quoted JSON document; anything structural keeps its JSON text.
    return spelled if isinstance(spelled, str) else json.dumps(spelled)


def write_csv(path: str, rows: list[dict]) -> None:
    """CSV writer using Polars.

    ``line_terminator`` is CRLF because that is what this destination has always
    written (``csv.DictWriter`` defaults to it, as does RFC 4180) and Polars defaults
    to LF; the migration is about the column union, not about changing the bytes of
    every existing export.

    CSV is flat, so Polars refuses a struct, list or binary column outright where
    ``csv.DictWriter`` accepted one and wrote ``str()`` of it: ``{'a': 1}`` for a nested
    document and ``b'hi'`` for binary, neither valid JSON nor readable back. A nested
    source reaches this on the default load path, so those columns are encoded rather
    than left to abort the export.
    """
    import polars as pl

    frame = _frame(rows)
    flat = [
        # `.to_list()` rather than iterating the column: a list column yields a
        # `Series` per element, which dlt's serializer does not know.
        pl.Series(
            name, [_spell_for_csv(v) for v in frame[name].to_list()], dtype=pl.String
        )
        for name, dtype in frame.schema.items()
        if dtype.base_type() in {pl.Struct, pl.List, pl.Array, pl.Binary, pl.Object}
    ]
    if flat:
        frame = frame.with_columns(flat)
    frame.write_csv(path, line_terminator="\r\n")


def write_json(path: str, rows: list[dict]) -> None:
    """JSON writer emitting one array document.

    One document rather than one per row, because that is the shape ``read_json``
    expects: it parses the whole file as a single value and expands an array to one row
    per element. A stream of ``---``-free concatenated documents would only load through
    the line-delimited fallback, which is what ``.jsonl`` is for.
    """
    from dlt.common import json

    with open(path, "wb") as handle:
        json.dump(rows, handle)


def write_jsonl(path: str, rows: list[dict]) -> None:
    """JSONL writer using json.dumps"""
    # dlt's json handles datetime/Decimal/etc. that dlt may have produced; stdlib json
    # would choke on them.
    from dlt.common import json

    with open(path, "wb") as handle:
        for row in rows:
            handle.write(json.dumpb(row) + b"\n")


def write_orc(path: str, rows: list[dict]) -> None:
    """Write rows as an ORC file with PyArrow."""
    import pyarrow as pa
    from pyarrow import orc

    fieldnames = _column_union(rows)
    if rows and not fieldnames:
        raise ValueError("ORC output requires at least one column for nonempty rows")
    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    orc.write_table(pa.table(columns), path)


def write_parquet(path: str, rows: list[dict]) -> None:
    """Parquet writer using Polars.

    ``compression`` is Snappy because that is what this destination has always written
    (PyArrow's default) and Polars defaults to Zstd. Zstd is the smaller of the two and
    every current reader handles it, but a codec is a thing a consumer either supports
    or fails on, so it is named here rather than changed as a side effect of moving
    libraries.
    """
    _frame(rows).write_parquet(path, compression="snappy")


def write_yaml(path: str, rows: list[dict]) -> None:
    """YAML writer emitting one sequence document.

    One document holding a list, not a ``---``-separated stream of one document per
    row: ``read_yaml`` expands a list document to one row per element and yields any
    other document as a single row, so both shapes round-trip, and the list is the one
    that reads as a table rather than as a concatenation.

    ``sort_keys=False`` keeps each row in the column order the load produced, matching
    the other writers. ``allow_unicode=True`` writes non-ASCII as itself rather than as
    a ``\\xNN`` escape; the file is UTF-8 either way, but escaped output would be a
    gratuitous difference from what every other writer here emits.
    """
    try:
        import yaml
    except ImportError as e:
        raise MissingDecoderError(
            "Writing YAML needs the PyYAML package. "
            "Install it with: pip install 'omniload[iterable]'"
        ) from e

    with open(path, "wb") as handle:
        yaml.dump(
            rows,
            handle,
            Dumper=_yaml_dumper(yaml),
            encoding="utf-8",
            allow_unicode=True,
            sort_keys=False,
        )


def _yaml_dumper(yaml_module) -> type:
    """``SafeDumper`` plus a fallback for the types dlt produces and YAML does not know.

    ``SafeDumper`` covers strings, numbers, booleans, null, sequences and mappings, and
    also ``bytes`` (as ``!!binary``) and ``date`` / ``datetime`` (as timestamps); it
    raises ``RepresenterError`` on anything else. That is the whole vocabulary of the
    default load path, where dlt stages gzip-JSONL and every value reaches a writer
    already JSON-typed. It is not the vocabulary of ``--loader-file-format parquet``,
    where a decimal column arrives as ``Decimal`` and a time column as
    ``datetime.time``, and an export would abort after the entire load had run.

    So an unknown type is spelled the way ``write_json`` and ``write_jsonl`` spell it,
    by asking dlt's own serializer: a ``Decimal`` writes as the string ``'1.50'``,
    keeping the scale a float would drop. The types YAML does know are left to
    ``SafeDumper``, so a timestamp still reads back as a datetime rather than as text.
    """

    class Dumper(yaml_module.SafeDumper):
        pass

    def represent_via_dlt_json(dumper, data):
        from dlt.common import json

        return dumper.represent_data(json.loads(json.dumps(data)))

    # ``None`` is PyYAML's key for the catch-all representer, the one that would
    # otherwise raise ``RepresenterError``.
    Dumper.add_representer(None, represent_via_dlt_json)
    return Dumper
