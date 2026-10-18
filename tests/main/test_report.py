import dataclasses
import datetime as dt
import sqlite3
import uuid
from types import SimpleNamespace

import dlt
import pytest
from dlt.common.pipeline import LoadInfo
from dlt.pipeline.exceptions import PipelineStepFailed

import omniload
from omniload import IngestJobError, run_ingest
from omniload.core.report import (
    DLT_STEP_STAGES,
    Endpoint,
    JobReport,
    Stage,
    Status,
    build,
    render_scheme,
    render_selector,
)
from omniload.model import LoadRequest

SECRET = "SENTINEL7f3a"  # noqa: S105 - leak canary


def _now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def _request(**overrides) -> LoadRequest:
    request = LoadRequest(
        source_uri="csv://data.csv",
        dest_uri="duckdb:///warehouse.duckdb",
        source_table="main.t",
        dest_table="out.t",
    )
    return dataclasses.replace(request, **overrides)


def _ingest_kwargs(request: LoadRequest) -> dict:
    return {
        "source_uri": request.source_uri,
        "dest_uri": request.dest_uri,
        "source_table": request.source_table,
        "dest_table": request.dest_table,
    }


def _renderings(report: JobReport) -> list[str]:
    return [repr(report), report.title, report.as_markdown(), report.as_text()]


def _assert_no_secret(report: JobReport) -> None:
    for rendering in _renderings(report):
        assert SECRET.lower() not in rendering.lower()


@dlt.resource(name="t")
def _three_rows():
    yield from ({"a": i} for i in range(3))


@dlt.resource(name="t")
def _raising_resource():
    yield {"a": 1}
    raise RuntimeError(f"extract failed with token {SECRET}")


def _pipeline(tmp_path, destination):
    return dlt.pipeline(
        pipeline_name=f"report_{uuid.uuid4().hex[:8]}",
        pipelines_dir=str(tmp_path / "pipelines"),
        destination=destination,
        dataset_name="out",
    )


def _duckdb(tmp_path):
    return dlt.destinations.duckdb(str(tmp_path / "warehouse.duckdb"))


def test_success_from_a_real_run(tmp_path):
    """A CSV to DuckDB load reports rows normalized per table, from the in-memory trace."""
    source = tmp_path / "data.csv"
    source.write_text("id,name\n1,alpha\n2,beta\n3,gamma\n")
    request = LoadRequest(
        source_uri=f"file://{source}",
        dest_uri=f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
        dest_table="out.t",
    )
    started_at = _now()
    info = run_ingest(**_ingest_kwargs(request), progress="log")
    report = build(request, started_at, _now(), load_info=info)

    assert report.status is Status.success
    assert report.stage is None
    assert report.exception_type is None
    assert report.source == Endpoint("file", None)
    assert report.destination == Endpoint("duckdb", "out.t")
    assert report.rows == (("t", 3),)
    assert report.load_package_count == 1
    assert report.failed_job_count == 0
    assert report.failed_job_tables == ()
    assert report.title == "omniload success: file → duckdb"
    assert "- `t`: 3 rows" in report.as_markdown()
    assert "- t: 3 rows" in report.as_text()
    assert str(tmp_path) not in "".join(_renderings(report))


def test_setup_failure_has_no_counts(tmp_path):
    """A failure before any pipeline runs carries type and stage, every count unavailable."""
    database = tmp_path / "source.db"
    sqlite3.connect(database).close()
    request = _request(
        source_uri=f"sqlite:///{database}",
        dest_uri=f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
        source_table="main.missing",
        dest_table="out.missing",
    )
    started_at = _now()
    with pytest.raises(Exception) as excinfo:
        run_ingest(**_ingest_kwargs(request))
    report = build(
        request, started_at, _now(), exception=excinfo.value, stage=Stage.setup
    )

    assert report.status is Status.failure
    assert report.stage is Stage.setup
    assert report.exception_type == "NoSuchTableError"
    assert report.rows is None
    assert report.load_package_count is None
    assert report.failed_job_count is None
    assert report.failed_job_tables is None
    assert report.title == "omniload failure at setup: sqlite → duckdb"
    text = report.as_text()
    assert "Rows normalized: unavailable" in text
    assert "Load packages: unavailable" in text
    assert "Failed jobs: unavailable" in text


def test_extract_failure_from_a_real_pipeline(tmp_path):
    """dlt's step sets the stage; an extract failure has no normalize info."""
    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    started_at = _now()
    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(_raising_resource())
    report = build(
        _request(), started_at, _now(), exception=excinfo.value, stage=Stage.load
    )

    assert report.stage is Stage.extract
    assert report.exception_type == "PipelineStepFailed (ResourceExtractionError)"
    assert report.rows is None
    assert report.load_package_count is None
    _assert_no_secret(report)


def test_load_failure_keeps_normalized_rows(tmp_path):
    """A terminal load failure still reports the rows that were normalized."""
    pipeline = _pipeline(tmp_path, dlt.destinations.dummy(fail_prob=1.0))
    started_at = _now()
    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(_three_rows())
    report = build(_request(), started_at, _now(), exception=excinfo.value)

    assert report.stage is Stage.load
    assert report.exception_type is not None
    assert report.exception_type.startswith("PipelineStepFailed (")
    assert report.rows == (("t", 3),)
    assert report.load_package_count == 1
    # The exception's `LoadInfo` is a pre-load snapshot: every job still "new".
    snapshot = excinfo.value.step_info.load_packages[0].jobs
    assert len(snapshot["failed_jobs"]) == 0
    assert len(snapshot["new_jobs"]) > 0
    assert report.failed_job_count is None
    assert report.failed_job_tables is None
    assert "Failed jobs: unavailable" in report.as_text()
    assert "Stage: `load`" in report.as_markdown()


def test_returned_failed_jobs_without_and_with_load_info(tmp_path, monkeypatch):
    """`IngestJobError` alone gives job counts but no package count; with `LoadInfo`, both."""
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    pipeline = _pipeline(tmp_path, dlt.destinations.dummy(fail_prob=1.0))
    started_at = _now()
    info = pipeline.run(_three_rows())
    # The dummy destination fails every job, dlt's own state table included.
    failed_jobs = info.load_packages[0].jobs["failed_jobs"]
    assert len(failed_jobs) == 2
    error = IngestJobError(failed_jobs)

    alone = build(_request(), started_at, _now(), exception=error, stage=Stage.load)
    assert alone.stage is Stage.load
    assert alone.exception_type == "IngestJobError"
    assert alone.failed_job_count == 2
    assert alone.failed_job_tables == ("_dlt_pipeline_state", "t")
    assert alone.load_package_count is None
    assert alone.rows is None

    with_info = build(
        _request(),
        started_at,
        _now(),
        load_info=info,
        exception=error,
        stage=Stage.load,
    )
    assert with_info.load_package_count == 1
    assert with_info.failed_job_count == 2
    assert with_info.rows == (("t", 3),)
    for report in (alone, with_info):
        for rendering in _renderings(report):
            assert "Traceback" not in rendering
            assert "job_id" not in rendering


def test_stale_persisted_trace_is_unavailable(tmp_path):
    """A trace dlt loads from disk for an earlier run is not reported as this run's rows."""
    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    before_first_run = _now()
    info = pipeline.run(_three_rows())
    after_first_run = _now()

    attached = dlt.attach(
        pipeline_name=pipeline.pipeline_name,
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    stale = info._replace(pipeline=attached)
    assert attached.last_trace is not None  # loaded from the working directory

    current = build(_request(), after_first_run, _now(), load_info=stale)
    assert current.rows is None
    assert "Rows normalized: unavailable" in current.as_text()

    # Control: the same persisted trace is accepted when it belongs to this run.
    earlier = build(_request(), before_first_run, _now(), load_info=stale)
    assert earlier.rows == (("t", 3),)


def test_trace_that_cannot_be_read_is_unavailable(tmp_path):
    class Unreadable:
        @property
        def last_trace(self):
            raise FileNotFoundError("working dir is gone")

    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    info = pipeline.run(_three_rows())
    report = build(
        _request(), _now(), _now(), load_info=info._replace(pipeline=Unreadable())
    )
    assert report.rows is None


def _with_row_counts(tmp_path, row_counts: dict[str, int]):
    """A real `LoadInfo` whose pipeline reports a trace with these row counts."""
    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    info = pipeline.run(_three_rows())
    trace = SimpleNamespace(
        started_at=_now() + dt.timedelta(days=1),
        last_normalize_info=SimpleNamespace(row_counts=row_counts),
        last_pipeline_step_trace=lambda step: SimpleNamespace(step_exception=None),
    )
    return info._replace(pipeline=SimpleNamespace(last_trace=trace))


def test_non_identifier_table_names_render_by_position(tmp_path):
    """Names dlt did not reduce to an identifier (direct naming) are not shown."""
    info = _with_row_counts(tmp_path, {"t": 1, "t▶contact jane@example.com": 2})
    report = build(_request(), _now(), _now(), load_info=info)
    assert report.rows == (("t", 1), ("table 2", 2))
    assert "jane" not in report.as_text()


@pytest.mark.parametrize(
    ("dest_table", "rows", "failed"),
    [
        ("out.t", (("b", 2), ("c", 1)), ("a", "b")),
        (
            f"out.t?token={SECRET}",
            (("table 2", 2), ("table 3", 1)),
            ("table 1", "table 2"),
        ),
    ],
)
def test_rows_and_failed_jobs_share_one_numbering(tmp_path, dest_table, rows, failed):
    """A table only in rows, only in failed jobs, or in both gets one label in each."""
    info = _with_row_counts(tmp_path, {"b": 2, "c": 1})
    jobs = [
        SimpleNamespace(job_file_info=SimpleNamespace(table_name=name))
        for name in ("a", "b")
    ]
    info = info._replace(load_packages=[SimpleNamespace(jobs={"failed_jobs": jobs})])
    report = build(_request(dest_table=dest_table), _now(), _now(), load_info=info)
    assert report.rows == rows
    assert report.failed_job_tables == failed
    _assert_no_secret(report)


def test_only_dlt_own_tables_are_dropped(tmp_path):
    """dlt's fixed tables are dropped by name, not by a `_dlt_` prefix a selector can hit."""
    info = _with_row_counts(
        tmp_path,
        {"_dlt_loads": 1, "_dlt_pipeline_state": 1, "_dlt_version": 1, "_dlt_x": 4},
    )
    report = build(_request(), _now(), _now(), load_info=info)
    assert report.rows == (("_dlt_x", 4),)


def test_unexpected_exception_class_names_are_not_shown():
    oddly_named = type("Token SENTINEL7f3a!", (Exception,), {})
    report = build(
        _request(), _now(), _now(), exception=oddly_named(), stage=Stage.setup
    )
    assert report.exception_type == "unknown"
    _assert_no_secret(report)


def test_unexpected_version_strings_are_not_shown(monkeypatch):
    monkeypatch.setattr(omniload, "__version__", f"1.0 <{SECRET}>")
    report = build(_request(), _now(), _now())
    assert report.omniload_version == "unknown"
    _assert_no_secret(report)


def test_a_normalize_failure_has_no_row_counts(tmp_path):
    """dlt records an empty normalize info for a failed normalize step: unavailable."""

    @dlt.resource(name="t", schema_contract={"columns": "freeze"})
    def frozen(columns):
        yield dict.fromkeys(columns, 1)

    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    pipeline.run(frozen(["a"]))
    started_at = _now()
    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(frozen(["a", "b"]))
    assert excinfo.value.step == "normalize"
    report = build(_request(), started_at, _now(), exception=excinfo.value)

    assert report.stage is Stage.normalize
    assert report.rows is None
    assert "Rows normalized: unavailable" in report.as_text()


def test_normalize_with_no_data_tables(tmp_path):
    """Normalize ran but produced only dlt's own tables: no data tables, not unavailable."""

    @dlt.resource(name="t")
    def nothing():
        yield from ()

    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    started_at = _now()
    info = pipeline.run(nothing())
    report = build(_request(), started_at, _now(), load_info=info)
    assert report.rows == ()
    assert "Rows normalized: no data tables" in report.as_text()


@pytest.mark.parametrize(
    ("rows", "packages", "failed", "expected"),
    [
        (
            None,
            None,
            None,
            [
                "Rows normalized: unavailable",
                "Load packages: unavailable",
                "Failed jobs: unavailable",
            ],
        ),
        (
            (),
            0,
            0,
            ["Rows normalized: no data tables", "Load packages: 0", "Failed jobs: 0"],
        ),
        ((("t", 0),), 1, 0, ["- t: 0 rows", "Load packages: 1", "Failed jobs: 0"]),
    ],
)
def test_unavailable_is_distinct_from_zero(rows, packages, failed, expected):
    report = JobReport(
        status=Status.success,
        source=Endpoint("csv", "t"),
        destination=Endpoint("duckdb", "out.t"),
        started_at=_now(),
        finished_at=_now(),
        omniload_version="1.0",
        rows=rows,
        load_package_count=packages,
        failed_job_count=failed,
    )
    text = report.as_text()
    for line in expected:
        assert line in text


@pytest.mark.parametrize(
    "source_table",
    [
        f"query:SELECT * FROM t WHERE token = '{SECRET}'",
        f"sql:SELECT '{SECRET}'",
        f"query:12345:api_key={SECRET}",
        f"gaql_query:SELECT campaign.id FROM campaign WHERE x = '{SECRET}'",
        f'orders:{{"customer": "{SECRET}"}}',
        f"tickets:updated_since={SECRET}",
        f"custom:fields:{SECRET}",
        f"events?token={SECRET}",
        f"events#{SECRET}",
        f"weird table {SECRET}!",
    ],
)
def test_selector_payloads_never_reach_the_report(source_table):
    """Selector payloads are dropped, for both a given and a defaulted dest table."""
    for dest_table in (None, "out.t"):
        request = _request(
            source_uri=f"postgresql://user:{SECRET}@db.example.com:5432/app",
            dest_uri=f"duckdb:///{SECRET}/warehouse.duckdb",
            source_table=source_table,
            dest_table=dest_table,
        )
        report = build(
            request,
            _now(),
            _now(),
            exception=RuntimeError(f"connection refused: password={SECRET}"),
            stage=Stage.setup,
        )
        assert report.source.scheme == "postgresql"
        assert report.exception_type == "RuntimeError"
        _assert_no_secret(report)


def test_defaulted_dest_table_names_rows_by_position(tmp_path):
    """dlt names a defaulted destination after the selector, payload included."""
    pipeline = _pipeline(tmp_path, _duckdb(tmp_path))
    started_at = _now()
    info = pipeline.run(_three_rows(), table_name=f"sel_{SECRET}")
    request = _request(source_table=f"sql:SELECT '{SECRET}'", dest_table=None)
    report = build(request, started_at, _now(), load_info=info)

    assert report.rows == (("table 1", 3),)
    assert report.source.table == "sql (custom selector)"
    assert report.destination.table == "sql (custom selector)"
    _assert_no_secret(report)

    # Control: an explicit plain dest table keeps dlt's table names, padded or not.
    for dest_table in ("out.t", " out.t "):
        named = build(
            _request(dest_table=dest_table), started_at, _now(), load_info=info
        )
        assert named.rows == ((f"sel_{SECRET}".lower(), 3),)


def test_explicit_dest_table_with_a_stripped_part_names_tables_by_position(
    tmp_path, monkeypatch
):
    """An explicit destination selector's `?` part reaches dlt's table names too."""
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    pipeline = _pipeline(tmp_path, dlt.destinations.dummy(fail_prob=1.0))
    started_at = _now()
    # What dlt makes of `out.events?token=...` once omniload hands it the table part.
    info = pipeline.run(_three_rows(), table_name=f"events?token={SECRET}")
    request = _request(dest_table=f"out.events?token={SECRET}")
    report = build(request, started_at, _now(), load_info=info)

    assert report.destination.table == "out.events"
    # One numbering for both lists: `table 1` is the same table in each, and dlt's
    # own state table keeps its name.
    assert report.rows == (("table 1", 3),)
    assert report.failed_job_tables == ("_dlt_pipeline_state", "table 1")
    _assert_no_secret(report)


def test_step_info_passed_as_load_info_still_has_no_failed_jobs(tmp_path):
    """The pre-load snapshot gives no failed-job count, however the caller passes it."""
    pipeline = _pipeline(tmp_path, dlt.destinations.dummy(fail_prob=1.0))
    started_at = _now()
    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(_three_rows())
    snapshot = excinfo.value.step_info
    assert isinstance(snapshot, LoadInfo)
    report = build(
        _request(),
        started_at,
        _now(),
        load_info=snapshot,
        exception=excinfo.value,
    )
    assert report.load_package_count == 1
    assert report.failed_job_count is None
    assert report.failed_job_tables is None


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        (None, None),
        ("main.widgets", "main.widgets"),
        ('public."order.items"', "custom selector"),
        ("bucket/path/*.csv", "bucket/path/*.csv"),
        ("query:SELECT 1", "custom query"),
        ("messages:general,random", "messages (custom selector)"),
        ("events?x=1", "events"),
        ("", "custom selector"),
    ],
)
def test_render_selector(table, expected):
    assert render_selector(table) == expected


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("postgresql://user:pass@host/db", "postgresql"),
        ("Postgres://user:pass@host/db", "postgres"),
        ("mssql+pyodbc://user:pass@host/db", "mssql+pyodbc"),
        ("user:pass@host/db", "unknown"),
        ("user:pass@host://db", "unknown"),
        ("sk-live-abcdef", "unknown"),
        ("sk_live_abcdef", "unknown"),
        ("", "unknown"),
    ],
)
def test_render_scheme(uri, expected):
    assert render_scheme(uri) == expected


def test_dlt_steps_map_onto_stages():
    assert DLT_STEP_STAGES == {
        "sync": Stage.setup,
        "extract": Stage.extract,
        "normalize": Stage.normalize,
        "load": Stage.load,
    }


def test_naive_timestamps_are_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        build(_request(), dt.datetime.now(), _now())


def test_version_is_the_installed_one():
    report = build(_request(), _now(), _now())
    assert report.omniload_version == omniload.__version__
    assert f"omniload: `{omniload.__version__}`" in report.as_markdown()
