"""A destination's optional ``close()`` runs on every exit from ``run_ingest``.

A destination may hold run-scoped resources from ``dlt_dest()`` on, so the guard has to
cover a failure inside ``dlt_dest()`` itself, every failure after it, a dry run and a
successful load. Mock-only: a recording SQLite destination, no Docker.
"""

import sqlite3

import dlt
import pytest

import omniload.core.resource
from omniload import run_ingest
from omniload.core.factory import SourceDestinationFactory
from omniload.target.sqlite import SqliteDestination


class _RecordingDestination(SqliteDestination):
    def __init__(self, fail_in_dlt_dest=False):
        self.fail_in_dlt_dest = fail_in_dlt_dest
        self.fail_in_close = False
        self.allocated = False
        self.closed = 0

    def dlt_dest(self, uri, **kwargs):
        self.allocated = True
        if self.fail_in_dlt_dest:
            raise RuntimeError("dlt_dest failed after allocating")
        return super().dlt_dest(uri, **kwargs)

    def close(self):
        self.closed += 1
        if self.fail_in_close:
            raise RuntimeError("close failed")


@pytest.fixture
def destination(monkeypatch):
    recording = _RecordingDestination()
    monkeypatch.setattr(
        SourceDestinationFactory, "get_destination", lambda self: recording
    )
    return recording


@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "src.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE people (id INTEGER)")
    conn.execute("INSERT INTO people VALUES (1)")
    conn.commit()
    conn.close()
    return path


def _ingest(source_db, tmp_path, **kwargs):
    kwargs.setdefault("source_table", "people")
    return run_ingest(
        source_uri=f"sqlite:///{source_db}",
        dest_uri=f"sqlite:///{tmp_path / 'dest.db'}",
        dest_table="people",
        progress="log",
        **kwargs,
    )


def test_close_runs_after_a_successful_load(destination, source_db, tmp_path):
    _ingest(source_db, tmp_path)
    assert destination.closed == 1


def test_close_runs_after_a_dry_run(destination, source_db, tmp_path):
    assert _ingest(source_db, tmp_path, dry_run=True) is None
    assert destination.closed == 1


def test_close_runs_when_dlt_dest_fails_after_allocating(
    destination, source_db, tmp_path
):
    destination.fail_in_dlt_dest = True
    with pytest.raises(RuntimeError, match="after allocating"):
        _ingest(source_db, tmp_path)
    assert destination.allocated
    assert destination.closed == 1


def test_a_failing_close_does_not_replace_the_error_of_the_run(
    destination, source_db, tmp_path
):
    destination.fail_in_dlt_dest = True
    destination.fail_in_close = True
    with pytest.raises(RuntimeError, match="after allocating"):
        _ingest(source_db, tmp_path)
    assert destination.closed == 1


def test_a_failing_close_after_a_successful_run_is_reported(
    destination, source_db, tmp_path
):
    destination.fail_in_close = True
    with pytest.raises(RuntimeError, match="close failed"):
        _ingest(source_db, tmp_path)


def test_close_runs_when_registering_a_transform_fails(
    destination, source_db, tmp_path, monkeypatch
):
    def fail(*args, **kwargs):
        raise RuntimeError("transform registration failed")

    monkeypatch.setattr(omniload.core.resource, "for_each", fail)
    with pytest.raises(RuntimeError, match="transform registration failed"):
        _ingest(source_db, tmp_path)
    assert destination.closed == 1


def test_close_runs_when_reflection_fails(destination, source_db, tmp_path):
    with pytest.raises(Exception, match="no_such_table"):
        _ingest(source_db, tmp_path, source_table="no_such_table")
    assert destination.closed == 1


def test_close_runs_when_the_load_fails(destination, source_db, tmp_path, monkeypatch):
    class _FailingSource:
        def handles_incrementality(self):
            return False

        def dlt_source(self, uri, table, **kwargs):
            @dlt.resource(name="people")
            def people():
                yield {"id": 1}
                raise RuntimeError("source failed mid-load")

            return people()

    monkeypatch.setattr(
        SourceDestinationFactory, "get_source", lambda self: _FailingSource()
    )
    with pytest.raises(Exception, match="source failed mid-load"):
        _ingest(source_db, tmp_path)
    assert destination.closed == 1


def test_a_destination_without_close_is_left_alone(source_db, tmp_path):
    _ingest(source_db, tmp_path)
    conn = sqlite3.connect(str(tmp_path / "dest.db"))
    try:
        assert conn.execute("SELECT id FROM people").fetchall() == [(1,)]
    finally:
        conn.close()
