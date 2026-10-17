"""Remote ``turso://`` against a self-hosted ``tursodb --sync-server``.

The server binary comes from ``OMNILOAD_TEST_TURSODB``; without it these tests skip.
Each test starts its own server on a free port over a fresh database, and checks
what reached the server through a replica of its own, never through the one the
load used. The Turso Cloud test needs ``OMNILOAD_TEST_TURSO_URL`` and
``OMNILOAD_TEST_TURSO_AUTH_TOKEN``.
"""

import glob
import logging
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
import uuid

import dlt
import duckdb
import pytest

from omniload import run_ingest
from omniload.core.factory import SourceDestinationFactory

turso_sync = pytest.importorskip("turso.sync")

SENTINEL = "sentinel-secret-token-value"
PEOPLE = [(1, "Alice"), (2, "Bob")]


@pytest.fixture
def server(tmp_path):
    binary = os.environ.get("OMNILOAD_TEST_TURSODB")
    if not binary:
        pytest.skip("OMNILOAD_TEST_TURSODB (path to a tursodb binary) is not set")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    # The binary is the one the operator named in the environment, run with fixed
    # arguments and no shell.
    process = subprocess.Popen(  # noqa: S603
        [binary, "--sync-server", f"127.0.0.1:{port}", str(tmp_path / "server.db")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 15
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            if time.time() > deadline:
                process.kill()
                pytest.fail(f"tursodb did not listen on {port}")
            time.sleep(0.1)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        process.terminate()
        process.wait(timeout=10)


def _uri(remote: str, token: str = SENTINEL, parameter: str = "auth_token") -> str:
    return f"turso://{remote.split('://', 1)[1]}?tls=false&{parameter}={token}"


def _server_rows(remote: str, table: str = "people") -> list:
    """Read the server through a replica that only this check uses."""
    path = os.path.join(tempfile.mkdtemp(prefix="turso-check-"), "check.db")
    connection = turso_sync.connect(path, remote_url=remote)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchall()
        if not exists:
            return []
        return connection.execute(
            f"SELECT id, name FROM {table} ORDER BY id"
        ).fetchall()
    finally:
        connection.close()


def _write_server(remote: str, statements: list) -> None:
    """Write to the server as another client would: through a replica, then push."""
    path = os.path.join(tempfile.mkdtemp(prefix="turso-writer-"), "writer.db")
    connection = turso_sync.connect(path, remote_url=remote)
    try:
        for statement in statements:
            connection.execute(statement)
        connection.commit()
        connection.push()
    finally:
        connection.close()


def _sqlite(path, rows=PEOPLE):
    conn = sqlite3.connect(str(path))
    conn.execute("DROP TABLE IF EXISTS people")
    conn.execute("CREATE TABLE people (id INTEGER, name TEXT)")
    conn.executemany("INSERT INTO people VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def own_tempdir(tmp_path, monkeypatch):
    """Give each test a temp root of its own for the leak checks."""
    root = tmp_path / "tmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    return root


def _replica_dirs() -> set:
    return set(glob.glob(f"{tempfile.gettempdir()}/**/turso-replica-*", recursive=True))


def _load(src, remote, **kwargs):
    kwargs.setdefault("source_table", "people")
    kwargs.setdefault("dest_table", "people")
    return run_ingest(
        source_uri=f"sqlite:///{src}", dest_uri=_uri(remote), progress="log", **kwargs
    )


def test_append_and_replace_reach_the_server(server, tmp_path):
    src = tmp_path / "src.db"
    _sqlite(src)
    before = _replica_dirs()

    for _ in range(2):
        _load(src, server, incremental_strategy="append")
    assert _server_rows(server) == sorted(PEOPLE * 2)

    _load(src, server, incremental_strategy="replace")
    assert _server_rows(server) == PEOPLE
    assert _replica_dirs() == before


def test_each_source_read_sees_what_the_server_holds_now(server, tmp_path):
    """A replica kept from an earlier read would miss another writer's push."""
    _write_server(
        server,
        [
            "CREATE TABLE people (id INTEGER, name TEXT)",
            "INSERT INTO people VALUES (1, 'Alice')",
        ],
    )

    def read():
        wh = tmp_path / f"{uuid.uuid4().hex}.duckdb"
        run_ingest(
            source_uri=_uri(server),
            dest_uri=f"duckdb:///{wh}",
            source_table="people",
            dest_table="out.people",
            progress="log",
        )
        con = duckdb.connect(str(wh))
        try:
            return con.sql("SELECT id, name FROM out.people ORDER BY id").fetchall()
        finally:
            con.close()

    before = _replica_dirs()
    assert read() == [(1, "Alice")]
    _write_server(server, ["INSERT INTO people VALUES (2, 'Bob')"])
    assert read() == PEOPLE
    assert _replica_dirs() == before


def test_a_failed_load_pushes_nothing_and_removes_the_replica(
    server, tmp_path, monkeypatch
):
    class _FailingSource:
        def handles_incrementality(self):
            return False

        def dlt_source(self, uri, table, **kwargs):
            @dlt.resource(name="people")
            def people():
                yield {"id": 1, "name": "Alice"}
                raise RuntimeError("source failed mid-load")

            return people()

    monkeypatch.setattr(
        SourceDestinationFactory, "get_source", lambda self: _FailingSource()
    )
    before = _replica_dirs()
    with pytest.raises(Exception, match="source failed mid-load"):
        _load(tmp_path / "unused.db", server)
    assert _server_rows(server) == []
    assert _replica_dirs() == before


def _fail_push(monkeypatch):
    def push(self):
        # Client errors can quote the request, so the injected one carries the token.
        raise OSError(f"connection reset while sending Bearer {SENTINEL}")

    monkeypatch.setattr(turso_sync.ConnectionSync, "push", push)


def _chain(exc):
    seen = []
    while exc is not None and exc not in seen:
        seen.append(exc)
        exc = exc.__cause__ or exc.__context__
    return seen


@pytest.mark.parametrize("parameter", ["auth_token", "authToken"])
def test_a_failed_push_says_delivery_is_unconfirmed_and_hides_the_token(
    server, tmp_path, monkeypatch, caplog, parameter
):
    src = tmp_path / "src.db"
    _sqlite(src)
    _fail_push(monkeypatch)
    with caplog.at_level(logging.DEBUG), pytest.raises(Exception) as exc_info:
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=_uri(server, parameter=parameter),
            source_table="people",
            dest_table="people",
            incremental_strategy="replace",
            progress="log",
        )
    message = str(exc_info.value)
    assert "Delivery could not be confirmed" in message
    assert "'replace' load is safe to repeat" in message
    for exc in _chain(exc_info.value):
        assert SENTINEL not in str(exc)
    for record in caplog.records:
        assert SENTINEL not in record.getMessage()

    monkeypatch.undo()
    _load(src, server, incremental_strategy="replace")
    assert _server_rows(server) == PEOPLE


def _incremental(src, server):
    return _load(src, server, incremental_strategy="append", incremental_key="id")


def test_an_incremental_append_recovers_from_a_push_that_never_landed(
    server, tmp_path, monkeypatch
):
    """The cursor the failed run advanced lived only in its discarded pipeline
    directory, so the next run restores the server's older state and loads the rows
    again, once."""
    src = tmp_path / "src.db"
    _sqlite(src)
    _fail_push(monkeypatch)
    with pytest.raises(Exception, match="Delivery could not be confirmed"):
        _incremental(src, server)
    assert _server_rows(server) == []

    monkeypatch.undo()
    _incremental(src, server)
    assert _server_rows(server) == PEOPLE

    _sqlite(src, PEOPLE + [(3, "Carol")])
    _incremental(src, server)
    assert _server_rows(server) == PEOPLE + [(3, "Carol")]


def test_an_incremental_append_does_not_reload_after_a_push_that_landed(
    server, tmp_path, monkeypatch
):
    """When the push lands and the run fails after it, the server holds the rows and
    the advanced cursor, which the next run restores, so nothing is loaded twice."""
    from omniload.target.turso import TursoDestination

    src = tmp_path / "src.db"
    _sqlite(src)
    real = TursoDestination.post_load

    def push_then_fail(self):
        real(self)
        raise RuntimeError("failed after the push")

    monkeypatch.setattr(TursoDestination, "post_load", push_then_fail)
    with pytest.raises(RuntimeError, match="failed after the push"):
        _incremental(src, server)
    assert _server_rows(server) == PEOPLE

    monkeypatch.undo()
    _incremental(src, server)
    assert _server_rows(server) == PEOPLE

    _sqlite(src, PEOPLE + [(3, "Carol")])
    _incremental(src, server)
    assert _server_rows(server) == PEOPLE + [(3, "Carol")]


def test_configured_engine_options_do_not_bypass_the_connection_creator(
    server, tmp_path, monkeypatch
):
    """Engine options a user set for the sqlalchemy destination are merged with the
    creator rather than replacing it, so every connection still carries the token."""
    import omniload.target.turso as turso_target

    real = turso_target.sync_connect
    real_post_load = turso_target.TursoDestination.post_load
    seen = []
    pushing = []

    def spy(target, path):
        seen.append((bool(pushing), target.auth_token))
        return real(target, path)

    def post_load(self):
        pushing.append(True)
        try:
            real_post_load(self)
        finally:
            pushing.clear()

    monkeypatch.setattr(turso_target, "sync_connect", spy)
    monkeypatch.setattr(turso_target.TursoDestination, "post_load", post_load)
    monkeypatch.setenv(
        "DESTINATION__SQLALCHEMY__CREDENTIALS__ENGINE_KWARGS", '{"echo": false}'
    )
    src = tmp_path / "src.db"
    _sqlite(src)
    _load(src, server, incremental_strategy="append")
    assert _server_rows(server) == PEOPLE
    load_connections = [token for during_push, token in seen if not during_push]
    assert load_connections, "the load opened no connection through the creator"
    assert set(load_connections) == {SENTINEL}


def test_a_load_that_writes_many_tables_reaches_the_server(
    server, tmp_path, monkeypatch
):
    """A load writing many tables at once reaches the server whole. The concurrent
    writer lock it would otherwise hit is pinned by the local-file test in
    tests/main/test_turso.py; against a sync replica it does not reproduce reliably."""

    @dlt.resource(name="wide", write_disposition="append")
    def wide():
        for i in range(2000):
            yield {"id": i, **{f"list_{c}": [{"v": i}] for c in range(20)}}

    class _WideSource:
        def handles_incrementality(self):
            return True

        def honours_run_disposition(self):
            return True

        def dlt_source(self, uri, table, **kwargs):
            return wide()

    monkeypatch.setattr(
        SourceDestinationFactory, "get_source", lambda self: _WideSource()
    )
    run_ingest(
        source_uri="file+delta://placeholder",
        dest_uri=_uri(server),
        source_table="wide",
        dest_table="wide",
        progress="log",
    )
    path = os.path.join(tempfile.mkdtemp(prefix="turso-check-"), "check.db")
    connection = turso_sync.connect(path, remote_url=server)
    try:
        assert connection.execute("SELECT count(*) FROM wide").fetchone() == (2000,)
        assert connection.execute("SELECT count(*) FROM wide__list_19").fetchone() == (
            2000,
        )
    finally:
        connection.close()


def test_turso_cloud_round_trip(tmp_path):
    url = os.environ.get("OMNILOAD_TEST_TURSO_URL")
    token = os.environ.get("OMNILOAD_TEST_TURSO_AUTH_TOKEN")
    if not url or not token:
        pytest.skip("OMNILOAD_TEST_TURSO_URL / OMNILOAD_TEST_TURSO_AUTH_TOKEN not set")
    host = url.split("://", 1)[-1].rstrip("/")
    table = f"omniload_{uuid.uuid4().hex[:12]}"
    src = tmp_path / "src.db"
    _sqlite(src)

    try:
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=f"turso://{host}?authToken={token}",
            source_table="people",
            dest_table=table,
            incremental_strategy="replace",
            progress="log",
        )
        wh = tmp_path / "wh.duckdb"
        run_ingest(
            source_uri=f"turso://{host}?authToken={token}",
            dest_uri=f"duckdb:///{wh}",
            source_table=table,
            dest_table="out.people",
            progress="log",
        )
        con = duckdb.connect(str(wh))
        try:
            rows = con.sql("SELECT id, name FROM out.people ORDER BY id").fetchall()
        finally:
            con.close()
        assert rows == PEOPLE
    finally:
        # Each run writes a table of its own; drop it. dlt's own state tables
        # (`_dlt_loads` and the rest) are shared by every run and stay.
        path = os.path.join(tempfile.mkdtemp(prefix="turso-cleanup-"), "cleanup.db")
        connection = turso_sync.connect(path, remote_url=url, auth_token=token)
        try:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
            connection.commit()
            connection.push()
        finally:
            connection.close()
