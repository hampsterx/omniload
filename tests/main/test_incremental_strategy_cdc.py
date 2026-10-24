"""``--incremental-strategy cdc`` and ``upsert``: dlt merge strategies keyed on the
primary key.

Like ``scd2``, both are a dlt merge *strategy* rather than a write disposition, so they
are unflattened onto a ``merge`` disposition that names them. ``upsert`` updates and
inserts by primary key and never deletes. ``cdc`` also deletes the destination records a
load leaves out, so it shares scd2's refusal of partial reads, and it is allowed only for
SQL database sources, the one family omniload can be sure it reads in full.

Mock-only unit lane (no Docker, no credentials): a real ``sqlite://`` source loads into a
real embedded duckdb. Sources that are not SQL are small fakes.
"""

import sqlite3

import dlt
import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory


def _make_sqlite(path, rows, *, keyed=True):
    """(Re)create a `people` table holding `rows`, standing in for a changing source."""
    conn = sqlite3.connect(str(path))
    conn.execute("DROP TABLE IF EXISTS people")
    key = "PRIMARY KEY" if keyed else ""
    conn.execute(
        f"CREATE TABLE people (id INTEGER {key}, name TEXT, city TEXT, seen INTEGER)"
    )
    conn.executemany("INSERT INTO people VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _load(src, dest, strategy, **kwargs):
    kwargs.setdefault("source_table", "main.people")
    run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri=f"duckdb:///{dest}",
        dest_table="out.people",
        incremental_strategy=strategy,
        progress="log",
        **kwargs,
    )


def _records(dest, table="out.people"):
    con = duckdb.connect(str(dest))
    try:
        return con.sql(f"select id, name, city from {table} order by id").fetchall()
    finally:
        con.close()


THREE = [
    (1, "Alice", "Auckland", 1),
    (2, "Bob", "Wellington", 1),
    (3, "Carol", "Dunedin", 1),
]
# Bob is gone and Carol has moved.
TWO = [(1, "Alice", "Auckland", 1), (3, "Carol", "Nelson", 2)]


def test_cdc_deletes_a_vanished_record(tmp_path):
    """The point of cdc: after a full read the destination equals the source, so a row
    the source no longer holds is deleted and a changed one is updated in place."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"

    _make_sqlite(src, THREE)
    _load(src, dest, "cdc")
    _make_sqlite(src, TWO)
    _load(src, dest, "cdc")

    assert _records(dest) == [(1, "Alice", "Auckland"), (3, "Carol", "Nelson")]


def test_upsert_keeps_a_vanished_record(tmp_path):
    """upsert updates and inserts by primary key and deletes nothing."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"

    _make_sqlite(src, THREE)
    _load(src, dest, "upsert")
    _make_sqlite(src, TWO)
    _load(src, dest, "upsert")

    assert _records(dest) == [
        (1, "Alice", "Auckland"),
        (2, "Bob", "Wellington"),
        (3, "Carol", "Nelson"),
    ]


def test_upsert_allows_an_incremental_key(tmp_path):
    """upsert reads absence as nothing at all, so a windowed read is safe: the second run
    reads only Carol's changed row, updates it, and leaves the rest alone."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    pipelines_dir = tmp_path / "pipelines"

    _make_sqlite(src, THREE)
    _load(src, dest, "upsert", incremental_key="seen", pipelines_dir=str(pipelines_dir))
    _make_sqlite(src, TWO)
    _load(src, dest, "upsert", incremental_key="seen", pipelines_dir=str(pipelines_dir))

    assert _records(dest) == [
        (1, "Alice", "Auckland"),
        (2, "Bob", "Wellington"),
        (3, "Carol", "Nelson"),
    ]


INTERVAL_QUERY = (
    "query:SELECT id, name, city FROM people "
    "WHERE seen >= :interval_start AND seen <= :interval_end"
)


@pytest.mark.parametrize("strategy", ["scd2", "cdc"])
@pytest.mark.parametrize(
    ("option", "kwargs"),
    [
        ("--incremental-key", {"incremental_key": "seen"}),
        ("--sql-limit", {"sql_limit": 1}),
        ("--yield-limit", {"yield_limit": 1}),
        (
            "--interval-start",
            {"source_table": INTERVAL_QUERY, "interval_start": "2026-01-01"},
        ),
        (
            "--interval-end",
            {"source_table": INTERVAL_QUERY, "interval_end": "2026-01-01"},
        ),
    ],
)
def test_rejects_a_partial_read(tmp_path, strategy, option, kwargs):
    """scd2 retires and cdc deletes every record a run leaves out, so anything that reads
    only part of the table is refused before the read. A `query:` table binds the
    interval into its placeholders with no incremental key, which windows it all the
    same. One check serves both strategies, so each option is tried with each."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE)

    with pytest.raises(ValidationError, match=f"cannot be combined with '{option}'"):
        _load(src, dest, strategy, primary_key=["id"], **kwargs)
    assert not dest.exists()


@pytest.mark.parametrize("strategy", ["scd2", "cdc"])
def test_an_interval_on_a_plain_table_is_not_a_partial_read(tmp_path, strategy):
    """Without an incremental key, an interval binds only into a `query:` table's
    placeholders, so on a plain table it reads the whole table and is not refused."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE)

    _load(src, dest, strategy, primary_key=["id"], interval_start="2026-01-01")
    assert len(_records(dest)) == 3


def test_cdc_after_delete_insert_keeps_its_merge_key(tmp_path):
    """Current behaviour, pinned so a change shows up here: `delete+insert` marks the
    incremental key as the table's merge key, dlt keeps that hint in the stored schema,
    and a later cdc run then deletes only inside the merge-key values it loaded. A
    partition that vanished from the source wholesale survives in the destination. The
    documented remedy is a `--full-refresh` when switching a table to cdc."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, part INTEGER)")
    conn.executemany("INSERT INTO people VALUES (?, ?)", [(1, 1), (2, 1), (3, 2)])
    conn.commit()

    def load(strategy, **kwargs):
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="main.people",
            dest_table="out.people",
            incremental_strategy=strategy,
            progress="log",
            **kwargs,
        )

    def ids():
        con = duckdb.connect(str(dest))
        try:
            return [
                r[0]
                for r in con.sql("select id from out.people order by id").fetchall()
            ]
        finally:
            con.close()

    load("delete+insert", incremental_key="part")
    conn.execute("DELETE FROM people WHERE id IN (2, 3)")
    conn.commit()
    conn.close()

    load("cdc")
    assert ids() == [1, 3]  # 2 deleted inside partition 1; partition 2 left alone

    load("cdc", full_refresh=True)
    assert ids() == [1]


class _DictSource:
    """A source that is not SQL and keeps a cursor of its own, as several SaaS sources do,
    while reporting that it leaves incrementality to omniload."""

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri, table, **kwargs):
        @dlt.resource(name="rows", primary_key="id")
        def rows(
            updated=dlt.sources.incremental("updated", initial_value=0),  # noqa: B008
        ):
            yield from [{"id": 1, "updated": 1}, {"id": 2, "updated": 2}]

        return rows()


class _ManagedSource:
    """A ``handles_incrementality`` source that sets its own resource-level disposition,
    like the SaaS/streaming sources."""

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri, table, **kwargs):
        @dlt.resource(name="rows")
        def rows():
            yield from [{"id": 1}, {"id": 2}, {"id": 3}]

        return rows()


def _ingest_stub(monkeypatch, stub, dest, strategy, **kwargs):
    monkeypatch.setattr(SourceDestinationFactory, "get_source", lambda self: stub)
    run_ingest(
        source_uri=kwargs.pop("source_uri", "mongodb://placeholder/db"),
        dest_uri=f"duckdb:///{dest}",
        source_table="rows",
        dest_table="out.rows",
        incremental_strategy=strategy,
        progress="log",
        **kwargs,
    )


def _count(dest, table="out.rows"):
    con = duckdb.connect(str(dest))
    try:
        return con.sql(f"select count(*) from {table}").fetchall()[0][0]
    finally:
        con.close()


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize(
    ("stub", "source_uri"),
    [
        (_DictSource(), "mongodb://placeholder/db"),
        (_ManagedSource(), "influxdb://placeholder"),
    ],
    ids=["own-cursor", "managed"],
)
def test_cdc_rejects_a_non_sql_source(tmp_path, monkeypatch, stub, source_uri, dry_run):
    """A source that is not SQL can window its read with a cursor or a default range of
    its own that omniload cannot see before reading, so cdc is refused for it, on a dry
    run too. A source that owns its disposition would otherwise drop cdc to `none`
    without a word, so it is refused rather than ignored."""
    dest = tmp_path / "wh.duckdb"
    with pytest.raises(
        ValidationError, match="only supported for SQL database sources"
    ):
        _ingest_stub(
            monkeypatch,
            stub,
            dest,
            "cdc",
            source_uri=source_uri,
            primary_key=["id"],
            dry_run=dry_run,
        )
    assert not dest.exists()


def test_cdc_rejects_an_arrow_source(tmp_path):
    """`mmap://` is not a SQL source either."""
    src = tmp_path / "people.arrow"
    table = pa.table({"id": [1, 2], "city": ["Auckland", "Wellington"]})
    with pa.OSFile(str(src), "wb") as sink:
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

    with pytest.raises(ValidationError, match="'mmap' is not one"):
        run_ingest(
            source_uri=f"mmap://{src}",
            dest_uri=f"duckdb:///{tmp_path / 'wh.duckdb'}",
            source_table="arrow_mmap",
            dest_table="out.people",
            incremental_strategy="cdc",
            primary_key=["id"],
            progress="log",
        )


def test_upsert_accepts_a_non_sql_source(tmp_path, monkeypatch):
    """upsert deletes nothing, so a source's own cursor cannot cost it a record, and the
    SQL-only rule does not apply."""
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        _ingest_stub(monkeypatch, _DictSource(), dest, "upsert")
    assert _count(dest) == 2


def test_managed_source_still_ignores_upsert(tmp_path, monkeypatch):
    """A source that owns its disposition keeps ignoring upsert, as it does scd2: its
    resource-level append stands, so rows accumulate across runs."""
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        _ingest_stub(
            monkeypatch,
            _ManagedSource(),
            dest,
            "upsert",
            source_uri="file://placeholder.csv",
        )
    assert _count(dest) == 6


@pytest.mark.parametrize(("primary_key", "expected"), [(None, 6), (["id"], 3)])
def test_influxdb_upsert_keeps_its_primary_key_merge(
    tmp_path, monkeypatch, primary_key, expected
):
    """InfluxDB owns its incrementality, so upsert is suppressed there like any strategy,
    and its `--primary-key` override still turns the load into a plain merge."""
    dest = tmp_path / "wh.duckdb"
    kwargs = {"primary_key": primary_key} if primary_key else {}
    for _ in range(2):
        _ingest_stub(
            monkeypatch,
            _ManagedSource(),
            dest,
            "upsert",
            source_uri="influxdb://placeholder",
            **kwargs,
        )
    assert _count(dest) == expected


@pytest.mark.parametrize("backend", ["pyarrow", "default"])
def test_cdc_accepts_an_arrow_backend(tmp_path, backend):
    """cdc matches destination rows on the primary key in SQL and needs no row hash, so
    unlike scd2 it works on data read as Arrow tables."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"

    _make_sqlite(src, THREE)
    _load(src, dest, "cdc", sql_backend=backend)
    _make_sqlite(src, TWO)
    _load(src, dest, "cdc", sql_backend=backend)

    assert _records(dest) == [(1, "Alice", "Auckland"), (3, "Carol", "Nelson")]


@pytest.mark.parametrize("strategy", ["cdc", "upsert"])
def test_requires_a_primary_key(tmp_path, strategy):
    """Both strategies match records on the primary key. dlt says so only at the load
    step, as a traceback, so a table with none is refused first with a pointer to the
    option that supplies one."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE, keyed=False)

    with pytest.raises(
        ValidationError, match="Name its key columns with '--primary-key'"
    ):
        _load(src, dest, strategy)
    assert not dest.exists()


@pytest.mark.parametrize("strategy", ["cdc", "upsert"])
def test_a_primary_key_option_satisfies_the_check(tmp_path, strategy):
    """`--primary-key` names the key a keyless table lacks."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE, keyed=False)

    _load(src, dest, strategy, primary_key=["id"])
    assert len(_records(dest)) == 3


def test_cdc_takes_a_reflected_primary_key(tmp_path):
    """A SQL table's declared key is reflected onto the resource, so cdc needs no
    `--primary-key` for it (the other tests here rely on that too)."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE)

    _load(src, dest, "cdc")
    assert len(_records(dest)) == 3


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_cdc_on_an_empty_source_deletes_nothing(tmp_path, backend):
    """Current dlt behaviour, pinned so a change shows up here: a table read on these
    backends yields no item at all when it is empty, and dlt runs no merge for a load with
    no rows (it synthesizes an empty job for `replace` only), so nothing is deleted. If
    this starts failing, dlt has started treating an empty read as an empty table, which
    is what cdc means, and the documented limitation can go."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"

    _make_sqlite(src, THREE)
    _load(src, dest, "cdc", sql_backend=backend)
    _make_sqlite(src, [])
    _load(src, dest, "cdc", sql_backend=backend)

    assert len(_records(dest)) == 3


def test_cdc_on_an_empty_source_clears_it_with_connectorx(tmp_path):
    """ConnectorX hands dlt an empty Arrow table for an empty source table, which does
    reach the merge, so cdc deletes every destination record."""
    pytest.importorskip("connectorx")
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"

    _make_sqlite(src, THREE)
    _load(src, dest, "cdc", sql_backend="connectorx")
    _make_sqlite(src, [])
    _load(src, dest, "cdc", sql_backend="connectorx")

    assert _records(dest) == []


@pytest.mark.parametrize(
    ("strategy", "kwargs"),
    [
        ("merge", {"primary_key": ["id"]}),
        ("delete+insert", {"incremental_key": "id"}),
    ],
)
def test_a_later_merge_does_not_inherit_cdc(tmp_path, strategy, kwargs):
    """dlt keeps a table's merge strategy in its stored schema when a run names none, so
    after a cdc load a plain `merge` would still run cdc and delete every row it did not
    read. Each run names its own strategy, so a limited read after cdc leaves the unread
    rows in place. The `delete+insert` case is a regression assertion only: its own merge
    key already scopes the stored cdc delete to the keys it loaded."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE)

    _load(src, dest, "cdc")
    _load(src, dest, strategy, sql_limit=1, **kwargs)

    assert [r[0] for r in _records(dest)] == [1, 2, 3]


def test_the_primary_key_error_names_a_query_as_such(tmp_path):
    """A `query:` table's resource has a placeholder name the user never wrote."""
    src, dest = tmp_path / "src.db", tmp_path / "wh.duckdb"
    _make_sqlite(src, THREE, keyed=False)

    with pytest.raises(ValidationError, match="and the query declares none"):
        _load(src, dest, "cdc", source_table="query:SELECT id, name, city FROM people")


def test_upsert_leaves_dynamic_hints_to_dlt(tmp_path, monkeypatch):
    """A resource whose table name depends on each item cannot be inspected before the
    read, so the primary-key check defers to dlt, which loads it with the key the
    resource declares."""

    class _DynamicSource:
        def handles_incrementality(self) -> bool:
            return False

        def dlt_source(self, uri, table, **kwargs):
            @dlt.resource(name="rows", table_name=lambda item: "rows", primary_key="id")
            def rows():
                yield from [{"id": 1}, {"id": 2}]

            return rows()

    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        _ingest_stub(monkeypatch, _DynamicSource(), dest, "upsert")
    assert _count(dest) == 2


def test_a_plain_merge_into_a_factory_destination_still_runs(tmp_path):
    """Naming the default merge strategy reads the capabilities of the destination the
    pipeline resolved, because a destination can hand dlt a factory callable, as MongoDB
    does, which has no capabilities of its own. A dry run stops before any connection."""
    src = tmp_path / "src.db"
    _make_sqlite(src, THREE)
    run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri="mongodb://localhost:1/db",
        source_table="main.people",
        dest_table="people",
        incremental_strategy="merge",
        primary_key=["id"],
        dry_run=True,
        progress="log",
    )


@pytest.mark.parametrize("strategy", ["cdc", "upsert"])
@pytest.mark.parametrize(
    ("dest_uri", "message"),
    [
        ("sqlite:///{tmp}/app.db", "Its merge strategies are: delete-insert, scd2"),
        ("mongodb://localhost:1/db", "It has no merge strategy for it"),
        (
            "s3://bucket/prefix?access_key_id=x&secret_access_key=y",
            "It has no merge strategy for it",
        ),
    ],
    ids=["sqlalchemy", "mongodb", "bucket"],
)
def test_rejects_a_destination_that_cannot_run_the_strategy(
    tmp_path, strategy, dest_uri, message
):
    """dlt refuses a strategy a SQLAlchemy destination lacks only at the load step, and
    one with no merge strategy at all is not refused: MongoDB upserts, a bucket appends,
    and `cdc` deletes nothing. Both are refused up front, on a dry run too."""
    src = tmp_path / "src.db"
    _make_sqlite(src, THREE)
    with pytest.raises(ValidationError, match=message):
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=dest_uri.format(tmp=tmp_path),
            source_table="main.people",
            dest_table="people",
            incremental_strategy=strategy,
            dry_run=True,
            progress="log",
        )


@pytest.mark.parametrize("strategy", ["cdc", "upsert"])
def test_a_delta_table_runs_both_strategies(tmp_path, strategy):
    """Delta Lake's table format carries every dlt merge strategy, so the check reads the
    destination's selector for the table rather than the bare capability list."""
    src = tmp_path / "src.db"
    _make_sqlite(src, THREE)
    run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri=f"file+delta://{tmp_path / 'lake'}",
        source_table="main.people",
        dest_table="out.people",
        incremental_strategy=strategy,
        dry_run=True,
        progress="log",
    )
