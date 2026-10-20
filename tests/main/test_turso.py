"""Turso, in-process: ``turso:///`` files on both sides, and what the destination refuses.

Mock-only unit lane (no Docker, no network): pyturso runs the database in process.
Merge into Turso is refused because Turso resolves the database-qualified correlated
subquery in dlt's merge against the wrong table and deletes every destination row,
so these tests pin that the refusal fires on every disposition dlt would apply, and
before anything is written.
"""

import sqlite3

import dlt
import duckdb
import pytest
from dlt.extract.hints import make_hints
from dlt.pipeline.exceptions import PipelineStepFailed

from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory

pytest.importorskip("turso")

PEOPLE = [(1, "Alice", "Auckland"), (2, "Bob", "Wellington")]


def _make_sqlite(path, rows=PEOPLE):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE people (id INTEGER, name TEXT, city TEXT)")
    conn.executemany("INSERT INTO people VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _rows(path, table="people"):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            f"SELECT id, name, city FROM {table} ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def _tables(path):
    """User tables in a database file, or none if nothing created the file."""
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    try:
        return sorted(
            name
            for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
            if not name.startswith("_dlt")
        )
    finally:
        conn.close()


def _ingest(source_uri, dest_uri, **kwargs):
    kwargs.setdefault("source_table", "people")
    kwargs.setdefault("dest_table", "people")
    return run_ingest(
        source_uri=source_uri, dest_uri=dest_uri, progress="log", **kwargs
    )


def test_round_trip_through_a_turso_file(tmp_path):
    """sqlite file -> turso file -> duckdb, row for row."""
    src, mid, wh = tmp_path / "src.db", tmp_path / "app.db", tmp_path / "wh.duckdb"
    _make_sqlite(src)

    _ingest(f"sqlite:///{src}", f"turso:///{mid}")
    _ingest(f"turso:///{mid}", f"duckdb:///{wh}", dest_table="out.people")

    con = duckdb.connect(str(wh))
    try:
        assert (
            con.sql("SELECT id, name, city FROM out.people ORDER BY id").fetchall()
            == PEOPLE
        )
    finally:
        con.close()


def test_append_accumulates_and_replace_resets(tmp_path):
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)

    for _ in range(2):
        _ingest(f"sqlite:///{src}", f"turso:///{dest}", incremental_strategy="append")
    assert _rows(dest) == sorted(PEOPLE * 2)

    _ingest(f"sqlite:///{src}", f"turso:///{dest}", incremental_strategy="replace")
    assert _rows(dest) == PEOPLE


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_full_refresh_is_refused_and_keeps_the_table(tmp_path, dry_run):
    """dlt would drop the table, then fail on the staging dataset's ATTACH."""
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    _ingest(f"sqlite:///{src}", f"turso:///{dest}")

    with pytest.raises(ValidationError, match="cannot run with '--full-refresh'"):
        _ingest(
            f"sqlite:///{src}", f"turso:///{dest}", full_refresh=True, dry_run=dry_run
        )
    assert _rows(dest) == PEOPLE


def test_a_named_schema_other_than_main_is_refused(tmp_path):
    src = tmp_path / "src.db"
    _make_sqlite(src)
    with pytest.raises(ValidationError, match="'main' schema only"):
        _ingest(
            f"sqlite:///{src}",
            f"turso:///{tmp_path / 'app.db'}",
            dest_table="other.people",
        )


def test_main_qualified_table_is_accepted(tmp_path):
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    _ingest(f"sqlite:///{src}", f"turso:///{dest}", dest_table="main.people")
    assert _rows(dest) == PEOPLE


def test_scd2_guard_covers_a_turso_source(tmp_path):
    """A `turso:///` source is a SQL source, so scd2's Arrow-backend refusal reaches it.
    duckdb takes merges, so only the source-side guard can produce this error."""
    src = tmp_path / "app.db"
    _make_sqlite(src)
    with pytest.raises(ValidationError, match="cannot use the 'pyarrow' SQL backend"):
        _ingest(
            f"turso:///{src}",
            f"duckdb:///{tmp_path / 'wh.duckdb'}",
            dest_table="out.people",
            incremental_strategy="scd2",
            sql_backend="pyarrow",
        )


MERGE_REFUSED = "cannot load with a merge write disposition"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"incremental_strategy": "merge", "primary_key": ["id"]},
        {"incremental_strategy": "delete+insert", "incremental_key": "id"},
        {"incremental_strategy": "scd2"},
    ],
    ids=["merge", "delete+insert", "scd2"],
)
def test_requested_merge_strategies_are_refused_before_loading(tmp_path, kwargs):
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    with pytest.raises(ValidationError, match=MERGE_REFUSED):
        _ingest(f"sqlite:///{src}", f"turso:///{dest}", **kwargs)
    assert _tables(dest) == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"incremental_strategy": "merge", "primary_key": ["id"]},
        {"incremental_strategy": "delete+insert", "incremental_key": "id"},
        {"incremental_strategy": "scd2"},
    ],
    ids=["merge", "delete+insert", "scd2"],
)
def test_a_dry_run_reports_a_requested_merge(tmp_path, kwargs):
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    with pytest.raises(ValidationError, match=MERGE_REFUSED):
        _ingest(f"sqlite:///{src}", f"turso:///{dest}", dry_run=True, **kwargs)
    assert not dest.exists()


def _stub_source(monkeypatch, make_resource, *, owns_incrementality, honours=None):
    class _Stub:
        def handles_incrementality(self):
            return owns_incrementality

        def dlt_source(self, uri, table, **kwargs):
            return make_resource()

    class _Honouring(_Stub):
        def honours_run_disposition(self):
            return honours

    stub = _Stub() if honours is None else _Honouring()
    monkeypatch.setattr(SourceDestinationFactory, "get_source", lambda self: stub)


def _merge_resource():
    @dlt.resource(name="people", write_disposition="merge", primary_key="id")
    def people():
        yield [{"id": 1, "name": "Alice"}]

    return people()


def _append_resource():
    @dlt.resource(name="people")
    def people():
        yield [{"id": 1, "name": "Alice"}]

    return people()


def test_a_source_owned_merge_resource_is_refused(tmp_path, monkeypatch):
    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, _merge_resource, owns_incrementality=True)
    with pytest.raises(ValidationError, match=MERGE_REFUSED):
        _ingest("adjust://placeholder", f"turso:///{dest}")
    assert _tables(dest) == []


def test_explicit_append_does_not_rescue_a_source_that_ignores_it(
    tmp_path, monkeypatch
):
    """A source that owns its incrementality without honouring a run-level disposition
    (Adjust's shape) has the explicit append discarded, so its merge resource stands."""
    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, _merge_resource, owns_incrementality=True)
    with pytest.raises(ValidationError, match=MERGE_REFUSED) as exc_info:
        _ingest(
            "adjust://placeholder", f"turso:///{dest}", incremental_strategy="append"
        )
    # The advice must not be the strategy the source just discarded.
    assert "does not take '--incremental-strategy'" in str(exc_info.value)
    assert "Use '--incremental-strategy append'" not in str(exc_info.value)
    assert _tables(dest) == []


def test_explicit_append_overrides_the_merge_hint_of_a_source_that_honours_it(
    tmp_path, monkeypatch
):
    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, _merge_resource, owns_incrementality=True, honours=True)
    _ingest(
        "file+delta://placeholder", f"turso:///{dest}", incremental_strategy="append"
    )
    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT id, name FROM people").fetchall() == [(1, "Alice")]
    finally:
        conn.close()


def test_a_callable_disposition_is_refused(tmp_path, monkeypatch):
    def make():
        # dlt takes callable hints only alongside a callable table name.
        @dlt.resource(
            name="people",
            table_name=lambda item: "people",
            write_disposition=lambda item: "append",
        )
        def people():
            yield [{"id": 1}]

        return people()

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, make, owns_incrementality=True)
    with pytest.raises(ValidationError, match=MERGE_REFUSED):
        _ingest("adjust://placeholder", f"turso:///{dest}")
    assert _tables(dest) == []


@pytest.mark.parametrize("strategy", [None, "append", "merge"])
def test_the_influxdb_primary_key_override_is_refused(tmp_path, monkeypatch, strategy):
    """A strategy cannot undo it, so the message names the option that can. The
    registered influxdb source owns its incrementality and discards an explicit
    strategy, so only `--primary-key` merges there; the `merge` case uses a synthetic
    source that keeps the strategy, to pin that the message names both causes when
    both apply."""
    dest = tmp_path / "app.db"
    # A source that owns its incrementality has an explicit strategy discarded, so
    # only one that does not can carry `merge` alongside the primary key.
    _stub_source(monkeypatch, _append_resource, owns_incrementality=strategy != "merge")
    kwargs = {"incremental_strategy": strategy} if strategy else {}
    if strategy == "merge":
        kwargs["primary_key"] = ["id"]
    with pytest.raises(ValidationError, match=MERGE_REFUSED) as exc_info:
        _ingest(
            "influxdb://placeholder",
            f"turso:///{dest}",
            **{"primary_key": ["id"], **kwargs},
        )
    message = str(exc_info.value)
    assert "omit '--primary-key'" in message
    assert ("use '--incremental-strategy append'" in message) == (strategy == "merge")
    assert _tables(dest) == []


def test_dlt_reads_the_sqlalchemy_configuration_section(tmp_path, monkeypatch):
    """The Turso destination is dlt's sqlalchemy destination under its own client,
    and keeps reading the settings a user wrote for that one."""
    monkeypatch.setenv("DESTINATION__SQLALCHEMY__CREATE_PRIMARY_KEYS", "true")
    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    _ingest(
        f"sqlite:///{src}",
        f"turso:///{dest}",
        incremental_strategy="append",
        primary_key=["id"],
    )
    conn = sqlite3.connect(str(dest))
    try:
        keyed = [row[1] for row in conn.execute("PRAGMA table_info(people)") if row[5]]
    finally:
        conn.close()
    assert keyed == ["id"]


def test_a_replace_through_a_staging_dataset_is_refused(tmp_path, monkeypatch):
    """dlt's `insert-from-staging` replace ATTACHes a staging file too; only
    `truncate-and-insert` is offered, and the earlier rows survive the refusal."""
    from dlt.common.destination.exceptions import WriteDispositionNotSupported

    src, dest = tmp_path / "src.db", tmp_path / "app.db"
    _make_sqlite(src)
    _ingest(f"sqlite:///{src}", f"turso:///{dest}")

    monkeypatch.setenv("DESTINATION__REPLACE_STRATEGY", "insert-from-staging")
    with pytest.raises(PipelineStepFailed) as exc_info:
        _ingest(f"sqlite:///{src}", f"turso:///{dest}", incremental_strategy="replace")
    assert isinstance(_root_cause(exc_info.value), WriteDispositionNotSupported)
    assert _rows(dest) == PEOPLE


def _variant_resource():
    @dlt.resource(name="people")
    def people():
        yield dlt.mark.with_table_name([{"id": 1}], "people_merged")

    resource = people()
    resource.apply_hints(
        table_name="people_merged",
        write_disposition="merge",
        primary_key="id",
        create_table_variant=True,
    )
    return resource


def _yielded_hint_resource():
    @dlt.resource(name="people")
    def people():
        yield dlt.mark.with_hints(
            [{"id": 1}], make_hints(write_disposition="merge", primary_key="id")
        )

    return people()


def _nested_hint_resource():
    @dlt.resource(
        name="people",
        nested_hints={
            "items": dlt.mark.make_nested_hints(
                write_disposition="merge", primary_key="k"
            )
        },
    )
    def people():
        yield [{"id": 1, "items": [{"k": 1}]}]

    return people()


def _nested_hint_without_key_resource():
    # Without a primary key the child stays nested, which dlt's own merge check skips.
    @dlt.resource(
        name="people",
        write_disposition="replace",
        nested_hints={"items": dlt.mark.make_nested_hints(write_disposition="merge")},
    )
    def people():
        yield [{"id": 1, "items": [{"k": 1}]}]

    return people()


def _root_cause(exc: BaseException) -> BaseException:
    while (inner := exc.__cause__ or exc.__context__) is not None:
        exc = inner
    return exc


@pytest.mark.parametrize(
    "make_resource",
    [
        _variant_resource,
        _yielded_hint_resource,
        _nested_hint_resource,
        _nested_hint_without_key_resource,
    ],
    ids=["table-variant", "yielded-hint", "nested-hint", "nested-hint-without-key"],
)
@pytest.mark.parametrize("strategy", [None, "append"])
def test_merge_tables_the_early_check_cannot_see_are_refused_at_load(
    tmp_path, monkeypatch, make_resource, strategy
):
    """A table variant, a yielded hint and a nested table's own hint only show up in
    the schema once data is extracted, so the destination's job client refuses any
    merge table, nested ones included, when the load step starts."""
    from dlt.common.destination.exceptions import WriteDispositionNotSupported

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, make_resource, owns_incrementality=True, honours=True)
    kwargs = {"incremental_strategy": strategy} if strategy else {}
    with pytest.raises(PipelineStepFailed) as exc_info:
        _ingest("file+delta://placeholder", f"turso:///{dest}", **kwargs)

    assert isinstance(_root_cause(exc_info.value), WriteDispositionNotSupported)
    assert _tables(dest) == []


def test_a_merge_hint_on_a_nested_table_that_never_held_data_is_allowed(
    tmp_path, monkeypatch
):
    """dlt neither loads nor stages a nested table that has never held data, so a
    typed merge hint on one is no reason to refuse the parent's `replace`."""

    def make():
        @dlt.resource(
            name="people",
            write_disposition="replace",
            nested_hints={
                "items": dlt.mark.make_nested_hints(
                    write_disposition="merge",
                    columns=[{"name": "k", "data_type": "bigint"}],
                )
            },
        )
        def people():
            yield [{"id": 1, "items": []}]

        return people()

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, make, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}")
    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT id FROM people").fetchall() == [(1,)]
    finally:
        conn.close()


def test_a_merge_hint_on_a_nested_table_that_held_data_before_is_refused(
    tmp_path, monkeypatch
):
    """A nested table that held data in an earlier run is still in the chain a
    `replace` truncates, even when this run gives it no rows, and dlt restores that
    history into the fresh pipeline directory each run uses."""
    from dlt.common.destination.exceptions import WriteDispositionNotSupported

    def seed():
        @dlt.resource(name="people", write_disposition="replace")
        def people():
            yield [{"id": 123, "items": [{"k": 1}]}]

        return people()

    def empty_child_with_merge_hint():
        @dlt.resource(
            name="people",
            write_disposition="replace",
            nested_hints={
                "items": dlt.mark.make_nested_hints(
                    write_disposition="merge",
                    columns=[{"name": "k", "data_type": "bigint"}],
                )
            },
        )
        def people():
            yield [{"id": 456, "items": []}]

        return people()

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, seed, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}")

    _stub_source(
        monkeypatch, empty_child_with_merge_hint, owns_incrementality=True, honours=True
    )
    with pytest.raises(PipelineStepFailed) as exc_info:
        _ingest("file+delta://placeholder", f"turso:///{dest}")
    assert isinstance(_root_cause(exc_info.value), WriteDispositionNotSupported)

    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT id FROM people").fetchall() == [(123,)]
        assert conn.execute("SELECT k FROM people__items").fetchall() == [(1,)]
    finally:
        conn.close()


def test_a_jobless_nested_merge_table_under_an_append_root_is_refused(
    tmp_path, monkeypatch
):
    """dlt still initialises the staging dataset for a nested merge table that held
    data before, even under an `append` root and with no rows this run, and on Turso
    that is the ATTACH. So the run is refused up front, and the earlier rows stay."""
    from dlt.common.destination.exceptions import WriteDispositionNotSupported

    def seed():
        @dlt.resource(name="people", write_disposition="append")
        def people():
            yield [{"id": 1, "items": [{"k": 1}]}]

        return people()

    def empty_child_with_merge_hint():
        @dlt.resource(
            name="people",
            write_disposition="append",
            nested_hints={
                "items": dlt.mark.make_nested_hints(
                    write_disposition="merge",
                    columns=[{"name": "k", "data_type": "bigint"}],
                )
            },
        )
        def people():
            yield [{"id": 2, "items": []}]

        return people()

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, seed, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}")
    _stub_source(
        monkeypatch, empty_child_with_merge_hint, owns_incrementality=True, honours=True
    )
    with pytest.raises(PipelineStepFailed) as exc_info:
        _ingest("file+delta://placeholder", f"turso:///{dest}")
    assert isinstance(_root_cause(exc_info.value), WriteDispositionNotSupported)

    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT id FROM people").fetchall() == [(1,)]
        assert conn.execute("SELECT k FROM people__items").fetchall() == [(1,)]
    finally:
        conn.close()


def test_a_refused_nested_merge_keeps_the_rows_of_a_replace_parent(
    tmp_path, monkeypatch
):
    """The refusal comes before the load truncates a `replace` table chain, so the rows
    an earlier run loaded survive it. The earlier run creates the nested table too:
    that is the chain the truncation walks."""

    def seed():
        @dlt.resource(name="people", write_disposition="replace")
        def people():
            yield [{"id": 123, "items": [{"k": 1}]}]

        return people()

    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, seed, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}")

    _stub_source(
        monkeypatch,
        _nested_hint_without_key_resource,
        owns_incrementality=True,
        honours=True,
    )
    with pytest.raises(PipelineStepFailed):
        _ingest("file+delta://placeholder", f"turso:///{dest}")

    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT id FROM people").fetchall() == [(123,)]
        assert conn.execute("SELECT k FROM people__items").fetchall() == [(1,)]
    finally:
        conn.close()


def test_close_is_harmless_before_use_and_when_repeated():
    from omniload.target.turso import TursoDestination

    destination = TursoDestination()
    destination.close()
    destination.close()


def _wide_resource(children: int = 20, rows: int = 500):
    """One resource whose nested lists make ``children`` tables beside its own, the
    shape of nested JSON from a document store or an API."""

    @dlt.resource(name="wide", write_disposition="append")
    def wide():
        for i in range(rows):
            yield {"id": i, **{f"list_{c}": [{"v": i}] for c in range(children)}}

    return wide()


def test_a_load_that_writes_many_tables_at_once_succeeds(tmp_path, monkeypatch):
    """Turso refuses a second concurrent writer instead of waiting, so dlt's parallel
    load jobs would fail this load on "database is locked"; the destination loads
    one job at a time."""
    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, _wide_resource, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}", dest_table="wide")
    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT count(*) FROM wide").fetchone() == (500,)
        assert conn.execute("SELECT count(*) FROM wide__list_19").fetchone() == (500,)
    finally:
        conn.close()


@pytest.mark.parametrize("strategy", ["parallel", "table-sequential"])
def test_a_configured_parallel_strategy_does_not_unlock_parallel_jobs(
    tmp_path, monkeypatch, strategy
):
    """dlt's load configuration overrides the destination's sequential strategy;
    the job cap must still hold."""
    monkeypatch.setenv("LOAD__PARALLELISM_STRATEGY", strategy)
    dest = tmp_path / "app.db"
    _stub_source(monkeypatch, _wide_resource, owns_incrementality=True, honours=True)
    _ingest("file+delta://placeholder", f"turso:///{dest}", dest_table="wide")
    conn = sqlite3.connect(str(dest))
    try:
        assert conn.execute("SELECT count(*) FROM wide__list_19").fetchone() == (500,)
    finally:
        conn.close()
