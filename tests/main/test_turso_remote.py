"""Remote ``turso://`` behaviour that needs no server.

Every path here either stops before connecting or connects to an address nothing
listens on, so it runs in the unit lane. The sync-server tests are in
``tests/warehouse/db/test_turso.py``.
"""

import glob
import logging
import socket
import sqlite3
import tempfile

import pytest

import omniload.core.resource
from omniload import ValidationError, run_ingest
from omniload.util.turso import TursoSyncError

pytest.importorskip("turso")

SENTINEL = "sentinel-secret-token-value"


def _unreachable(parameter: str = "auth_token") -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    # Nothing listens once the probe socket closes, so a connect is refused.
    return f"turso://127.0.0.1:{port}?tls=false&{parameter}={SENTINEL}"


@pytest.fixture(autouse=True)
def own_tempdir(tmp_path, monkeypatch):
    """Give each test a temp root of its own, so a replica another test (or another
    xdist worker) creates cannot move this test's leak check."""
    root = tmp_path / "tmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    return root


def _replica_dirs() -> set:
    return set(glob.glob(f"{tempfile.gettempdir()}/**/turso-replica-*", recursive=True))


@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "src.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE people (id INTEGER)")
    conn.execute("INSERT INTO people VALUES (1)")
    conn.commit()
    conn.close()
    return path


def _to_remote(source_db, **kwargs):
    kwargs.setdefault("source_table", "people")
    kwargs.setdefault("dest_table", "people")
    kwargs.setdefault("dest_uri", _unreachable())
    return run_ingest(source_uri=f"sqlite:///{source_db}", progress="log", **kwargs)


def _chain(exc: BaseException) -> list:
    seen: list = []
    current: BaseException | None = exc
    while current is not None and current not in seen:
        seen.append(current)
        current = current.__cause__ or current.__context__
    return seen


def _assert_redacted(exc_info, caplog):
    for exc in _chain(exc_info.value):
        assert SENTINEL not in str(exc), type(exc).__name__
        assert SENTINEL not in repr(exc), type(exc).__name__
    for record in caplog.records:
        assert SENTINEL not in record.getMessage()
        if record.exc_info:
            for exc in _chain(record.exc_info[1]):
                assert SENTINEL not in str(exc)


def test_a_persistent_pipelines_dir_is_refused_for_a_remote_destination(
    source_db, tmp_path
):
    before = _replica_dirs()
    with pytest.raises(
        ValidationError, match="cannot be combined with '--pipelines-dir'"
    ):
        _to_remote(source_db, pipelines_dir=str(tmp_path / "pipelines"))
    assert _replica_dirs() == before


def test_a_persistent_pipelines_dir_is_fine_for_a_local_file(source_db, tmp_path):
    _to_remote(
        source_db,
        dest_uri=f"turso:///{tmp_path / 'app.db'}",
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def test_a_dry_run_allocates_and_removes_the_replica_without_connecting(
    source_db, caplog
):
    before = _replica_dirs()
    with caplog.at_level(logging.DEBUG):
        assert _to_remote(source_db, dry_run=True) is None
    assert _replica_dirs() == before
    assert all(SENTINEL not in r.getMessage() for r in caplog.records)


def test_a_dry_run_of_a_remote_source_does_not_connect(tmp_path, caplog):
    with caplog.at_level(logging.DEBUG):
        run_ingest(
            source_uri=_unreachable(),
            dest_uri=f"duckdb:///{tmp_path / 'wh.duckdb'}",
            source_table="people",
            dest_table="out.people",
            dry_run=True,
            progress="log",
        )
    assert all(SENTINEL not in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("parameter", ["auth_token", "authToken"])
def test_a_failed_bootstrap_names_the_host_and_not_the_token(
    tmp_path, caplog, parameter
):
    dest = tmp_path / "wh.duckdb"
    with caplog.at_level(logging.DEBUG), pytest.raises(TursoSyncError) as exc_info:
        run_ingest(
            source_uri=_unreachable(parameter),
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            progress="log",
        )
    assert "Connecting to the Turso database at http://127.0.0.1:" in str(
        exc_info.value
    )
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_redacted(exc_info, caplog)
    assert _replica_dirs() == set()


@pytest.mark.parametrize("parameter", ["auth_token", "authToken"])
def test_a_failed_destination_connect_names_the_host_and_not_the_token(
    source_db, caplog, parameter
):
    before = _replica_dirs()
    with caplog.at_level(logging.DEBUG), pytest.raises(Exception) as exc_info:
        _to_remote(source_db, dest_uri=_unreachable(parameter))
    assert any(isinstance(e, TursoSyncError) for e in _chain(exc_info.value))
    _assert_redacted(exc_info, caplog)
    assert _replica_dirs() == before


def test_the_replica_is_removed_when_registering_a_transform_fails(
    source_db, monkeypatch
):
    def fail(*args, **kwargs):
        raise RuntimeError("transform registration failed")

    monkeypatch.setattr(omniload.core.resource, "for_each", fail)
    before = _replica_dirs()
    with pytest.raises(RuntimeError, match="transform registration failed"):
        _to_remote(source_db)
    assert _replica_dirs() == before


def test_the_replica_is_removed_when_reflection_fails(source_db):
    before = _replica_dirs()
    with pytest.raises(Exception, match="no_such_table"):
        _to_remote(source_db, source_table="no_such_table")
    assert _replica_dirs() == before


def test_the_replica_is_removed_when_dlt_dest_fails_after_allocating(
    source_db, monkeypatch
):
    from omniload.target import turso as turso_target

    real = turso_target.TursoDestination.dlt_dest

    def allocate_then_fail(self, uri, **kwargs):
        real(self, uri, **kwargs)
        raise RuntimeError("dlt_dest failed after allocating")

    monkeypatch.setattr(turso_target.TursoDestination, "dlt_dest", allocate_then_fail)
    before = _replica_dirs()
    with pytest.raises(RuntimeError, match="after allocating"):
        _to_remote(source_db)
    assert _replica_dirs() == before


def test_close_is_harmless_before_use_and_when_repeated():
    from omniload.target.turso import TursoDestination

    destination = TursoDestination()
    destination.close()
    destination.dlt_dest(_unreachable())
    destination.close()
    destination.close()


def test_a_staging_root_that_does_not_exist_yet_is_created(tmp_path):
    """The failure is the connection, not a missing directory."""
    staging = tmp_path / "not" / "there" / "yet"
    with pytest.raises(TursoSyncError, match="Connecting to the Turso database"):
        run_ingest(
            source_uri=_unreachable(),
            dest_uri=f"duckdb:///{tmp_path / 'wh.duckdb'}",
            source_table="people",
            dest_table="out.people",
            remote_database_staging_root=str(staging),
            progress="log",
        )
    assert staging.is_dir()
    assert list(staging.iterdir()) == []


@pytest.mark.parametrize("message", ["database is busy", "Database is LOCKED"])
def test_a_busy_replica_connection_is_retried(monkeypatch, message):
    import omniload.target.turso as turso_target

    calls = []

    def flaky(target, path):
        calls.append(path)
        if len(calls) < 3:
            raise TursoSyncError(f"Connecting to x failed: DatabaseError: {message}")
        return "connection"

    monkeypatch.setattr(turso_target, "sync_connect", flaky)
    monkeypatch.setattr(turso_target, "BUSY_BACKOFF", (0, 0, 0, 0))
    destination = turso_target.TursoDestination()
    factory = destination.dlt_dest(_unreachable())
    try:
        assert factory._creator() == "connection"
        assert len(calls) == 3
    finally:
        destination.close()


def test_any_other_connection_failure_is_not_retried(monkeypatch):
    import omniload.target.turso as turso_target

    calls = []

    def refused(target, path):
        calls.append(path)
        raise TursoSyncError("Connecting to x failed: network error: refused")

    monkeypatch.setattr(turso_target, "sync_connect", refused)
    monkeypatch.setattr(turso_target, "BUSY_BACKOFF", (0, 0, 0, 0))
    destination = turso_target.TursoDestination()
    factory = destination.dlt_dest(_unreachable())
    try:
        with pytest.raises(TursoSyncError, match="refused"):
            factory._creator()
        assert len(calls) == 1
    finally:
        destination.close()


def test_a_busy_connection_that_never_clears_gives_up_after_the_backoff(monkeypatch):
    import omniload.target.turso as turso_target

    calls = []

    def busy(target, path):
        calls.append(path)
        raise TursoSyncError("Connecting to x failed: DatabaseError: database is busy")

    monkeypatch.setattr(turso_target, "sync_connect", busy)
    monkeypatch.setattr(turso_target, "BUSY_BACKOFF", (0, 0, 0))
    destination = turso_target.TursoDestination()
    factory = destination.dlt_dest(_unreachable())
    try:
        with pytest.raises(TursoSyncError, match="database is busy"):
            factory._creator()
        assert len(calls) == 4
    finally:
        destination.close()


@pytest.mark.parametrize("key", ["ENGINE_KWARGS", "ENGINE_ARGS"])
def test_configured_engine_options_are_kept_beside_the_creator(monkeypatch, key):
    from omniload.target.turso import TursoDestination

    monkeypatch.setenv(f"DESTINATION__SQLALCHEMY__CREDENTIALS__{key}", '{"echo": true}')
    destination = TursoDestination()
    factory = destination.dlt_dest(_unreachable())
    try:
        credentials = factory.configuration(
            factory.spec(), accept_partial=True
        ).credentials
        assert credentials.engine_args is None
        assert credentials.engine_kwargs["echo"] is True
        assert credentials.engine_kwargs["creator"] is factory._creator
    finally:
        destination.close()
