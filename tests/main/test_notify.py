"""End-of-run notifications through apprise.

Every test posts to a local HTTP recorder through apprise's ``json://`` target, so the
suite needs no third-party service and runs in the Docker-free unit lane.
"""

import http.server
import json
import logging
import os
import subprocess
import sys
import textwrap
import threading
from types import SimpleNamespace
from typing import Any

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed
from sqlalchemy.exc import NoSuchTableError
from typer.testing import CliRunner

import omniload.api
import omniload.util.notify as notify_module
from omniload import IngestJobError, ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory

apprise = pytest.importorskip("apprise")

SECRET = "SENTINEL9c2e"  # noqa: S105 - leak canary


class Recorder:
    """A local HTTP endpoint that records notification POSTs and serves configs."""

    def __init__(self):
        self.posts: list[tuple[str, dict]] = []
        self.fetches: list[str] = []
        self.events: list[str] = []
        self.status: dict[str, int] = {}
        self.configs: dict[str, tuple[str, str]] = {}
        self.block: dict[str, threading.Event] = {}
        recorder = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length)
                if self.path in recorder.configs:
                    body, content_type = recorder.configs[self.path]
                    recorder.fetches.append(self.path)
                    self.send_response(200)
                    self.send_header("Content-Type", content_type)
                    self.end_headers()
                    self.wfile.write(body.encode())
                    return
                if self.path in recorder.block:
                    recorder.block[self.path].wait(10)
                recorder.posts.append((self.path, json.loads(raw)))
                recorder.events.append("notify")
                self.send_response(recorder.status.get(self.path, 200))
                self.end_headers()

            def log_message(self, format, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self, path: str = "/hook", scheme: str = "json") -> str:
        return f"{scheme}://127.0.0.1:{self.port}{path}"

    def paths(self) -> list[str]:
        return [path for path, _ in self.posts]

    def payloads(self) -> str:
        return json.dumps([payload for _, payload in self.posts])


@pytest.fixture
def recorder():
    instance = Recorder()
    yield instance
    for event in instance.block.values():
        event.set()
    instance.server.shutdown()


@pytest.fixture
def csv_load(tmp_path):
    """Keyword arguments for a three-row CSV to DuckDB load."""
    source = tmp_path / "data.csv"
    source.write_text("id,name\n1,alpha\n2,beta\n3,gamma\n")
    return {
        "source_uri": f"file://{source}",
        "dest_uri": f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
        "dest_table": "out.t",
        "progress": "log",
    }


@pytest.fixture
def hooks(monkeypatch, recorder):
    """Spy on the source's and destination's commit hooks, and break them on request.

    ``fail`` names hooks that raise; ``resource`` replaces the source's dlt resource;
    ``dlt_dest`` replaces the dlt destination the DuckDB target hands over.
    """
    state = SimpleNamespace(fail=set(), resource=None, dlt_dest=None)
    get_source = SourceDestinationFactory.get_source
    get_destination = SourceDestinationFactory.get_destination

    def spy(name):
        def hook():
            recorder.events.append(name)
            if name in state.fail:
                raise RuntimeError(f"{name} failed with token {SECRET}")

        return hook

    def spied_source(self):
        source: Any = get_source(self)
        source.post_load = spy("source.post_load")
        source.release = spy("source.release")
        if state.resource is not None:
            source.dlt_source = lambda *args, **kwargs: state.resource()
        return source

    def spied_destination(self):
        destination: Any = get_destination(self)
        original_post_load = destination.post_load
        spy_post_load = spy("destination.post_load")

        def post_load():
            spy_post_load()
            original_post_load()

        destination.post_load = post_load
        if state.dlt_dest is not None:
            destination.dlt_dest = lambda *args, **kwargs: state.dlt_dest()
        return destination

    monkeypatch.setattr(SourceDestinationFactory, "get_source", spied_source)
    monkeypatch.setattr(SourceDestinationFactory, "get_destination", spied_destination)
    return state


def _write(tmp_path, name: str, text: str):
    path = tmp_path / name
    path.write_text(textwrap.dedent(text).lstrip())
    return path


def _routed_yaml(recorder, tmp_path):
    return _write(
        tmp_path,
        "notify.yml",
        f"""
        urls:
          - {recorder.url("/success")}:
              tag: success
          - {recorder.url("/failure")}:
              tag: failure
          - {recorder.url("/always")}:
              tag: always
          - {recorder.url("/untagged")}
        """,
    )


def _missing_table(tmp_path, **kwargs):
    """Keyword arguments for a load that fails in setup: the SQLite table is missing."""
    import sqlite3

    database = tmp_path / "empty.db"
    sqlite3.connect(database).close()
    return {
        "source_uri": f"sqlite:///{database}",
        "source_table": "main.missing",
        "dest_uri": f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
        "dest_table": "out.missing",
        **kwargs,
    }


@dlt.resource(name="t")
def _raising_resource():
    yield {"a": 1}
    raise RuntimeError(f"extract failed with token {SECRET}")


def _failing_destination():
    """A dlt destination whose every load job fails terminally."""
    return dlt.destinations.dummy(fail_prob=1.0, loader_file_format="parquet")


# Outcomes and stages.


def test_success_sends_one_report(recorder, csv_load):
    info = run_ingest(**csv_load, notify=[recorder.url()])

    assert info is not None
    assert len(recorder.posts) == 1
    payload = recorder.posts[0][1]
    assert payload["type"] == "success"
    assert payload["title"] == "omniload success: file → duckdb"
    assert "`t`: 3 rows" in payload["message"]


def test_setup_failure_reports_and_reraises(recorder, tmp_path):
    with pytest.raises(NoSuchTableError):
        run_ingest(**_missing_table(tmp_path), notify=[recorder.url()])

    assert len(recorder.posts) == 1
    payload = recorder.posts[0][1]
    assert payload["type"] == "failure"
    assert "Stage: `setup`" in payload["message"]
    assert "Exception: `NoSuchTableError`" in payload["message"]


def test_load_failure_names_the_stage_and_exception_type(recorder, csv_load, hooks):
    hooks.dlt_dest = _failing_destination
    with pytest.raises(PipelineStepFailed):
        run_ingest(**csv_load, notify=[recorder.url()])

    payload = recorder.posts[0][1]
    assert len(recorder.posts) == 1
    assert payload["type"] == "failure"
    assert "Stage: `load`" in payload["message"]
    assert "Exception: `PipelineStepFailed (" in payload["message"]
    assert "random fail" not in payload["message"]


def test_failed_jobs_in_a_returned_load_info(recorder, csv_load, hooks, monkeypatch):
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    hooks.dlt_dest = _failing_destination
    with pytest.raises(IngestJobError):
        run_ingest(**csv_load, notify=[recorder.url()])

    payload = recorder.posts[0][1]
    assert len(recorder.posts) == 1
    assert "Stage: `load`" in payload["message"]
    assert "Exception: `IngestJobError`" in payload["message"]
    assert "Failed jobs: 2" in payload["message"]


@pytest.mark.parametrize("databricks", [True, False])
def test_databricks_retry_reports_the_terminal_outcome_once(
    recorder, csv_load, monkeypatch, databricks
):
    """A retried run reports once, for its final outcome; the retry is Databricks-only."""
    calls = []
    run = dlt.Pipeline.run

    def flaky_run(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise PipelineStepFailed(
                self, "load", "", RuntimeError("SCHEMA_ALREADY_EXISTS")
            )
        return run(self, *args, **kwargs)

    get_destination = SourceDestinationFactory.get_destination

    def as_databricks(self):
        destination = get_destination(self)
        self.destination_scheme = "databricks"
        return destination

    monkeypatch.setattr(dlt.Pipeline, "run", flaky_run)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    if databricks:
        monkeypatch.setattr(SourceDestinationFactory, "get_destination", as_databricks)
        run_ingest(**csv_load, notify=[recorder.url()])
        assert len(calls) == 2
        assert [payload["type"] for _, payload in recorder.posts] == ["success"]
    else:
        with pytest.raises(PipelineStepFailed):
            run_ingest(**csv_load, notify=[recorder.url()])
        assert len(calls) == 1
        assert [payload["type"] for _, payload in recorder.posts] == ["failure"]


def test_a_staged_run_that_fails_in_setup_reports_setup(
    recorder, tmp_path, monkeypatch
):
    """Once the download is done, a failure is no longer a staging failure."""
    import contextlib
    import sqlite3

    import omniload.source.sql_database.remote as remote

    database = tmp_path / "staged.db"
    sqlite3.connect(database).close()

    @contextlib.contextmanager
    def staged(*args, **kwargs):
        yield f"sqlite:///{database}"

    monkeypatch.setattr(remote, "stage_remote_database", staged)
    with pytest.raises(NoSuchTableError):
        run_ingest(
            source_uri="s3://bucket/snapshots/events.duckdb",
            source_table="main.missing",
            dest_uri=f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
            dest_table="out.missing",
            notify=[recorder.url()],
        )
    assert "Stage: `setup`" in recorder.posts[0][1]["message"]


def test_a_bare_inline_value_is_not_labelled_a_file(csv_load):
    with pytest.raises(ValidationError, match=r"--notify #1 \(unknown\)"):
        run_ingest(**csv_load, notify=["not-a-url"])


def test_staging_failure_reports_before_any_source_exists(recorder, monkeypatch):
    import omniload.source.sql_database.remote as remote

    def failing_download(*args, **kwargs):
        raise RuntimeError(f"download failed with token {SECRET}")

    def unreachable(*args, **kwargs):
        raise AssertionError("_run_ingest must not run after a staging failure")

    monkeypatch.setattr(remote, "stage_remote_database", failing_download)
    monkeypatch.setattr(omniload.api, "_run_ingest", unreachable)
    with pytest.raises(RuntimeError, match="download failed"):
        run_ingest(
            source_uri=f"s3://bucket/{SECRET}/events.duckdb?key=id&secret={SECRET}",
            source_table="main.events",
            dest_uri="duckdb:///unused.duckdb",
            dest_table="out.events",
            notify=[recorder.url()],
        )

    payload = recorder.posts[0][1]
    assert len(recorder.posts) == 1
    assert "Stage: `staging`" in payload["message"]
    assert SECRET not in recorder.payloads()


@pytest.mark.parametrize(
    ("break_it", "stage", "events"),
    [
        ("extract", "extract", ["source.release", "notify"]),
        ("normalize", "normalize", ["source.release", "notify"]),
        ("load", "load", ["source.release", "notify"]),
        (
            "destination.post_load",
            "destination_post_load",
            ["destination.post_load", "source.release", "notify"],
        ),
        (
            "source.post_load",
            "source_post_load",
            [
                "destination.post_load",
                "source.post_load",
                "source.release",
                "notify",
            ],
        ),
    ],
)
def test_failures_after_setup_release_first_and_name_the_stage(
    recorder, csv_load, hooks, monkeypatch, break_it, stage, events
):
    if break_it == "extract":
        hooks.resource = _raising_resource
    elif break_it == "normalize":
        from dlt.normalize import Normalize

        def failing_normalize(self, pool):
            raise RuntimeError(f"normalize failed with token {SECRET}")

        monkeypatch.setattr(Normalize, "run", failing_normalize)
    elif break_it == "load":
        hooks.dlt_dest = _failing_destination
    else:
        hooks.fail.add(break_it)

    with pytest.raises(Exception) as excinfo:
        run_ingest(**csv_load, notify=[recorder.url()])

    assert not isinstance(excinfo.value, AssertionError)
    assert recorder.events == events
    payload = recorder.posts[0][1]
    assert len(recorder.posts) == 1
    assert f"Stage: `{stage}`" in payload["message"]
    if stage in ("extract", "normalize"):
        assert "Rows normalized: unavailable" in payload["message"]
    if stage.endswith("post_load"):
        # The load itself committed, so the report carries what it loaded.
        assert "Load packages: 1" in payload["message"]
        assert "`t`: 3 rows" in payload["message"]
    assert SECRET not in recorder.payloads()


def test_a_release_that_raises_keeps_the_original_stage(recorder, csv_load, hooks):
    hooks.dlt_dest = _failing_destination
    hooks.fail.add("source.release")
    with pytest.raises(RuntimeError, match="source.release failed"):
        run_ingest(**csv_load, notify=[recorder.url()])

    assert "Stage: `load`" in recorder.posts[0][1]["message"]


def test_success_notifies_after_both_commit_hooks(recorder, csv_load, hooks):
    run_ingest(**csv_load, notify=[recorder.url()])
    assert recorder.events == ["destination.post_load", "source.post_load", "notify"]


# A report that cannot be sent changes nothing.


@pytest.mark.parametrize("breakage", ["endpoint_500", "notifier_raises", "renderer"])
def test_a_broken_notification_changes_nothing(
    recorder, csv_load, tmp_path, monkeypatch, caplog, breakage
):
    if breakage == "endpoint_500":
        recorder.status["/hook"] = 500
    elif breakage == "notifier_raises":

        def raising_notify(self, *args, **kwargs):
            raise RuntimeError(f"apprise broke on {SECRET}")

        monkeypatch.setattr(apprise.Apprise, "notify", raising_notify)
    else:
        from omniload.core.report import JobReport

        def raising_render(self):
            raise RuntimeError(f"render broke on {SECRET}")

        monkeypatch.setattr(JobReport, "as_markdown", raising_render)

    url = recorder.url(f"/hook-{SECRET}")
    if breakage == "endpoint_500":
        url = recorder.url("/hook")
    with caplog.at_level(logging.WARNING, logger="omniload"):
        info = run_ingest(**csv_load, notify=[url])
        with pytest.raises(NoSuchTableError):
            run_ingest(**_missing_table(tmp_path), notify=[url])

    assert info is not None
    warnings = [r for r in caplog.records if r.name == "omniload.util.notify"]
    assert len(warnings) == 2
    for record in warnings:
        assert "127.0.0.1" not in record.getMessage()
        assert SECRET not in record.getMessage()


def test_the_cli_exit_status_ignores_a_broken_notification(recorder, csv_load):
    from omniload.main import app

    recorder.status["/hook"] = 500
    args = [
        "ingest",
        "--source-uri",
        csv_load["source_uri"],
        "--dest-uri",
        csv_load["dest_uri"],
        "--dest-table",
        csv_load["dest_table"],
        "--notify",
        recorder.url(),
    ]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert len(recorder.posts) == 1


# Validation before the load.


@pytest.mark.parametrize("url", ["not-a-url", "slakc://x/y/z", ""])
@pytest.mark.parametrize("dry_run", [False, True])
def test_a_bad_inline_url_fails_before_staging(csv_load, monkeypatch, url, dry_run):
    def unreachable(*args, **kwargs):
        raise AssertionError("the load must not start")

    monkeypatch.setattr(omniload.api, "_run_ingest_in_pipelines_dir", unreachable)
    with pytest.raises(ValidationError, match=r"--notify #1"):
        run_ingest(**csv_load, notify=[url], dry_run=dry_run)


def test_a_valid_dry_run_sends_nothing(recorder, csv_load):
    assert run_ingest(**csv_load, notify=[recorder.url()], dry_run=True) is None
    assert recorder.posts == []


def test_a_failing_dry_run_sends_nothing(recorder, csv_load):
    with pytest.raises(NotImplementedError):
        run_ingest(
            **{**csv_load, "dest_uri": "nope://x"},
            notify=[recorder.url()],
            dry_run=True,
        )
    assert recorder.posts == []


@pytest.mark.parametrize("raised", [KeyboardInterrupt, SystemExit(3)])
def test_an_interrupted_load_sends_no_report(recorder, csv_load, monkeypatch, raised):
    """An interrupted run exits now instead of waiting on a delivery."""

    def interrupted(*args, **kwargs):
        raise raised

    monkeypatch.setattr(omniload.api, "_run_ingest_in_pipelines_dir", interrupted)
    with pytest.raises((KeyboardInterrupt, SystemExit)):
        run_ingest(**csv_load, notify=[recorder.url()])
    assert recorder.posts == []


@pytest.mark.parametrize("option", ["notify", "notify_config"])
def test_missing_extra_names_it(csv_load, tmp_path, monkeypatch, option):
    monkeypatch.setitem(sys.modules, "apprise", None)
    value = "json://127.0.0.1/" if option == "notify" else str(tmp_path / "c.yml")
    with pytest.raises(ValidationError, match=r"omniload\[notify\]"):
        run_ingest(**csv_load, **{option: [value]})


def test_no_notification_input_does_not_import_apprise(tmp_path):
    script = textwrap.dedent(
        f"""
        import sys
        from omniload import run_ingest
        open({str(tmp_path / "d.csv")!r}, "w").write("id\\n1\\n")
        run_ingest(
            source_uri="file://{tmp_path / "d.csv"}",
            dest_uri="duckdb:///{tmp_path / "w.duckdb"}",
            dest_table="out.t",
            progress="log",
        )
        print("apprise" in sys.modules, "omniload.util.notify" in sys.modules)
        """
    )
    result = _run_python(script, tmp_path)
    assert result.stdout.splitlines()[-1] == "False False"


def _run_python(script: str, tmp_path, env: dict | None = None):
    """Run ``script`` in a fresh interpreter, where nothing is imported yet."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={**os.environ, **(env or {})},
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result


# Secrets.


def test_no_secret_reaches_a_report_or_a_warning(
    recorder, tmp_path, monkeypatch, caplog
):
    """URI credentials, query text, exception messages and target URLs stay out."""
    import sqlite3

    database = tmp_path / "source.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE t (id INTEGER)")
    connection.execute("INSERT INTO t VALUES (1)")
    connection.commit()
    connection.close()
    recorder.configs[f"/cfg-{SECRET}.yml"] = (
        f"urls:\n  - {recorder.url(f'/remote-{SECRET}')}:\n      tag: always\n",
        "text/yaml",
    )
    local = _write(
        tmp_path,
        "notify.yml",
        f"""
        urls:
          - {recorder.url(f"/local-{SECRET}")}:
              tag: always
          - {recorder.url(f"/down-{SECRET}")}:
              tag: failure
        """,
    )
    recorder.status[f"/down-{SECRET}"] = 500
    targets = {
        "notify": [recorder.url(f"/inline-{SECRET}")],
        "notify_config": [
            str(local),
            f"http://127.0.0.1:{recorder.port}/cfg-{SECRET}.yml",
        ],
    }
    dest = f"duckdb:///{tmp_path / 'warehouse.duckdb'}?password={SECRET}"
    # Reading a `query:` table swaps dlt's `table_rows` for the rest of the process;
    # put it back afterwards, so later SQL loads in this worker read real tables.
    import dlt.sources.sql_database as sql_database

    monkeypatch.setattr(sql_database, "table_rows", sql_database.table_rows)
    with caplog.at_level(logging.DEBUG):
        run_ingest(
            source_uri=f"sqlite:///{database}?password={SECRET}",
            source_table=f"query:SELECT id FROM t WHERE '{SECRET}' != ''",
            dest_uri=dest,
            dest_table="out.t",
            **targets,
        )
        with pytest.raises(RuntimeError):
            monkeypatch.setattr(
                SourceDestinationFactory,
                "get_source",
                lambda self: (_ for _ in ()).throw(
                    RuntimeError(f"connection refused, password={SECRET}")
                ),
            )
            run_ingest(
                source_uri=f"sqlite:///{database}",
                source_table="main.t",
                dest_uri=dest,
                dest_table="out.t",
                **targets,
            )

    # Success: inline, local `always`, remote. Failure: those three and local `failure`.
    assert len(recorder.posts) == 7
    titles_and_bodies = json.dumps(
        [(payload["title"], payload["message"]) for _, payload in recorder.posts]
    )
    assert SECRET not in titles_and_bodies
    # Every warning, and everything the notifier logs. (omniload's existing INFO line
    # naming the source table prints a `query:` selector as given; that is not the
    # report and stays as it is.)
    for record in caplog.records:
        if record.levelno >= logging.WARNING or record.name == notify_module.__name__:
            assert SECRET not in record.getMessage(), record.name


# Inputs, routing and precedence.


def test_repeated_inline_urls_each_get_the_report(recorder, csv_load):
    run_ingest(**csv_load, notify=[recorder.url("/a"), recorder.url("/b")])
    assert sorted(recorder.paths()) == ["/a", "/b"]


def _cli(args: list[str], env: dict | None = None):
    from omniload.main import app

    return CliRunner().invoke(app, ["ingest", *args], env=env)


def _cli_load(csv_load) -> list[str]:
    return [
        "--source-uri",
        csv_load["source_uri"],
        "--dest-uri",
        csv_load["dest_uri"],
        "--dest-table",
        csv_load["dest_table"],
    ]


def test_the_notify_flag_repeats(recorder, csv_load):
    result = _cli(
        [
            *_cli_load(csv_load),
            "--notify",
            recorder.url("/a"),
            "--notify",
            recorder.url("/b"),
        ]
    )
    assert result.exit_code == 0, result.output
    assert sorted(recorder.paths()) == ["/a", "/b"]


def test_envvar_and_flag_precedence(recorder, csv_load, tmp_path):
    env = {"OMNILOAD_NOTIFY": f"{recorder.url('/env-a')} {recorder.url('/env-b')}"}
    result = _cli(_cli_load(csv_load), env=env)
    assert result.exit_code == 0, result.output
    assert sorted(recorder.paths()) == ["/env-a", "/env-b"]

    recorder.posts.clear()
    result = _cli([*_cli_load(csv_load), "--notify", recorder.url("/flag")], env=env)
    assert result.exit_code == 0, result.output
    assert recorder.paths() == ["/flag"]


def test_config_entries_route_by_tag(recorder, csv_load, tmp_path):
    config = _routed_yaml(recorder, tmp_path)
    run_ingest(**csv_load, notify_config=[str(config)])
    assert sorted(recorder.paths()) == ["/always", "/success"]

    recorder.posts.clear()
    with pytest.raises(NoSuchTableError):
        run_ingest(**_missing_table(tmp_path), notify_config=[str(config)])
    assert sorted(recorder.paths()) == ["/always", "/failure"]


def test_text_config_routes_the_same(recorder, csv_load, tmp_path):
    config = _write(
        tmp_path,
        "notify.txt",
        f"""
        success={recorder.url("/success")}
        failure={recorder.url("/failure")}
        always={recorder.url("/always")}
        {recorder.url("/untagged")}
        """,
    )
    run_ingest(**csv_load, notify_config=[str(config)])
    assert sorted(recorder.paths()) == ["/always", "/success"]


def test_inline_and_config_add_up_in_one_call(
    recorder, csv_load, tmp_path, monkeypatch
):
    calls = []
    notify = apprise.Apprise.notify

    def counting_notify(self, *args, **kwargs):
        calls.append(kwargs.get("tag"))
        return notify(self, *args, **kwargs)

    monkeypatch.setattr(apprise.Apprise, "notify", counting_notify)
    config = _routed_yaml(recorder, tmp_path)
    run_ingest(
        **csv_load, notify=[recorder.url("/inline")], notify_config=[str(config)]
    )
    assert sorted(recorder.paths()) == ["/always", "/inline", "/success"]
    assert calls == ["success"]

    recorder.posts.clear()
    with pytest.raises(NoSuchTableError):
        run_ingest(
            **_missing_table(tmp_path),
            notify=[recorder.url("/inline")],
            notify_config=[str(config)],
        )
    assert sorted(recorder.paths()) == ["/always", "/failure", "/inline"]
    assert calls == ["success", "failure"]


def test_config_envvar_and_flags(recorder, csv_load, tmp_path):
    env_config = _write(
        tmp_path, "env.yml", f"urls:\n  - {recorder.url('/env')}:\n      tag: always\n"
    )
    flag_config = _write(
        tmp_path,
        "flag.yml",
        f"urls:\n  - {recorder.url('/flag')}:\n      tag: always\n",
    )
    env = {"OMNILOAD_NOTIFY_CONFIG": str(env_config)}

    result = _cli([*_cli_load(csv_load), "--notify", recorder.url("/inline")], env=env)
    assert result.exit_code == 0, result.output
    assert sorted(recorder.paths()) == ["/env", "/inline"]

    recorder.posts.clear()
    result = _cli([*_cli_load(csv_load), "--notify-config", str(flag_config)], env=env)
    assert result.exit_code == 0, result.output
    assert recorder.paths() == ["/flag"]


def test_notify_on_failure_gates_every_target(recorder, csv_load, tmp_path):
    config = _write(
        tmp_path, "c.yml", f"urls:\n  - {recorder.url('/always')}:\n      tag: always\n"
    )
    run_ingest(
        **csv_load,
        notify=[recorder.url("/inline")],
        notify_config=[str(config)],
        notify_on="failure",
    )
    assert recorder.posts == []
    with pytest.raises(NoSuchTableError):
        run_ingest(
            **_missing_table(tmp_path), notify_config=[str(config)], notify_on="failure"
        )
    assert recorder.paths() == ["/always"]


@pytest.mark.parametrize("notify", [["json://127.0.0.1/"], None])
def test_an_unknown_notify_on_is_refused(csv_load, notify):
    with pytest.raises(ValidationError, match="--notify-on"):
        run_ingest(**csv_load, notify=notify, notify_on="sometimes")


# Config validation.


def _hook_url(recorder) -> str:
    """A target URL whose path is the apprise template value ``${hook}``."""
    return recorder.url("/") + "${hook}"


def _bad_configs(recorder, tmp_path) -> dict[str, str]:
    return {
        "unsupported scheme": "foo://localhost/c.yml",
        "missing file": str(tmp_path / "missing.yml"),
        "malformed yaml": str(_write(tmp_path, "bad.yml", "urls: [unclosed\n")),
        "unknown scheme only": str(
            _write(
                tmp_path, "slakc.yml", "urls:\n  - slakc://x/y/z:\n      tag: failure\n"
            )
        ),
        "empty file": str(_write(tmp_path, "empty.yml", "")),
        "untagged or foreign tags only": str(
            _write(
                tmp_path,
                "untagged.yml",
                f"urls:\n  - {recorder.url('/a')}\n  - {recorder.url('/b')}:\n      tag: ops\n",
            )
        ),
        "unset template": str(
            _write(
                tmp_path,
                "template.yml",
                f"template:\n  - hook\nurls:\n  - {_hook_url(recorder)}:\n      tag: failure\n",
            )
        ),
    }


@pytest.mark.parametrize("dry_run", [False, True])
def test_unusable_configs_fail_before_staging(
    recorder, csv_load, tmp_path, monkeypatch, dry_run
):
    def unreachable(*args, **kwargs):
        raise AssertionError("the load must not start")

    monkeypatch.setattr(omniload.api, "_run_ingest_in_pipelines_dir", unreachable)
    for case, source in _bad_configs(recorder, tmp_path).items():
        with pytest.raises(ValidationError, match=r"--notify-config #1"):
            run_ingest(**csv_load, notify_config=[source], dry_run=dry_run)
        assert recorder.posts == [], case


def test_a_missing_config_file_is_named_as_such(csv_load, tmp_path):
    with pytest.raises(
        ValidationError, match=r"--notify-config #1 \(file\): no such file"
    ):
        run_ingest(**csv_load, notify_config=[str(tmp_path / f"{SECRET}.yml")])


def test_an_unexpected_apprise_error_is_reported_by_type_only(
    csv_load, tmp_path, monkeypatch
):
    config = _write(tmp_path, "c.yml", "urls: []\n")

    def raising_add(self, *args, **kwargs):
        raise RuntimeError(f"apprise could not read json://127.0.0.1/{SECRET}")

    monkeypatch.setattr(apprise.AppriseConfig, "add", raising_add)
    with pytest.raises(ValidationError) as excinfo:
        run_ingest(**csv_load, notify_config=[str(config)])
    assert str(excinfo.value) == "Reading the notification targets failed: RuntimeError"
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__


def test_a_template_with_a_value_validates_and_delivers(
    recorder, csv_load, tmp_path, monkeypatch
):
    with_default = _write(
        tmp_path,
        "default.yml",
        f"template:\n  hook: from-default\nurls:\n  - {_hook_url(recorder)}:\n      tag: always\n",
    )
    run_ingest(**csv_load, notify_config=[str(with_default)])
    assert recorder.paths() == ["/from-default"]

    recorder.posts.clear()
    unset = _bad_configs(recorder, tmp_path)["unset template"]
    monkeypatch.setenv("APPRISE_TEMPLATE_HOOK", "from-env")
    with pytest.raises(NoSuchTableError):
        run_ingest(**_missing_table(tmp_path), notify_config=[unset])
    assert recorder.paths() == ["/from-env"]


def test_one_bad_entry_among_good_ones_is_skipped(recorder, csv_load, tmp_path):
    config = _write(
        tmp_path,
        "mixed.yml",
        f"urls:\n  - {recorder.url('/ok')}:\n      tag: always\n  - slakc://x/y/z:\n      tag: failure\n",
    )
    run_ingest(**csv_load, notify_config=[str(config)])
    assert recorder.paths() == ["/ok"]


def test_one_bad_source_among_good_ones_is_refused(recorder, csv_load, tmp_path):
    good = _write(
        tmp_path, "good.yml", f"urls:\n  - {recorder.url('/ok')}:\n      tag: always\n"
    )
    bad = str(_write(tmp_path, "empty.yml", ""))
    for sources, position in (([str(good), bad], 2), ([bad, str(good)], 1)):
        with pytest.raises(ValidationError, match=rf"--notify-config #{position}"):
            run_ingest(**csv_load, notify=[recorder.url()], notify_config=sources)
    assert recorder.posts == []


def test_an_unreachable_remote_config(recorder, csv_load, caplog):
    closed = http.server.HTTPServer(
        ("127.0.0.1", 0), http.server.BaseHTTPRequestHandler
    )
    port = closed.server_address[1]
    closed.server_close()
    remote = f"http://127.0.0.1:{port}/c-{SECRET}.yml"

    with pytest.raises(ValidationError, match=r"--notify-config #1 \(http\)"):
        run_ingest(**csv_load, notify_config=[remote], dry_run=True)

    with caplog.at_level(logging.WARNING, logger="omniload"):
        info = run_ingest(**csv_load, notify=[recorder.url()], notify_config=[remote])
    assert info is not None
    assert recorder.paths() == ["/hook"]
    messages = [r.getMessage() for r in caplog.records if r.name.startswith("omniload")]
    assert any("--notify-config #1 (http)" in message for message in messages)
    assert not any(SECRET in message for message in messages)


def test_a_failure_only_config_on_success_is_quiet(
    recorder, csv_load, tmp_path, caplog
):
    config = _write(
        tmp_path, "c.yml", f"urls:\n  - {recorder.url('/f')}:\n      tag: failure\n"
    )
    with caplog.at_level(logging.WARNING, logger="omniload"):
        run_ingest(**csv_load, notify_config=[str(config)])
    assert recorder.posts == []
    assert not [r for r in caplog.records if r.name.startswith("omniload.util")]


# Delivery outcomes.


def test_partial_delivery_warns_once(recorder, csv_load, caplog):
    recorder.status["/down"] = 500
    with caplog.at_level(logging.WARNING, logger="omniload"):
        info = run_ingest(
            **csv_load, notify=[recorder.url("/ok"), recorder.url("/down")]
        )
    assert info is not None
    warnings = [
        r.getMessage() for r in caplog.records if r.name == "omniload.util.notify"
    ]
    assert warnings == ["Job report delivery partial (json)"]


def test_timeout_warns_once(recorder, csv_load, caplog, monkeypatch):
    monkeypatch.setattr(notify_module, "NOTIFY_TIMEOUT", 0.5)
    recorder.block["/slow"] = threading.Event()
    with caplog.at_level(logging.WARNING, logger="omniload"):
        info = run_ingest(**csv_load, notify=[recorder.url("/slow")])
    recorder.block["/slow"].set()
    assert info is not None
    warnings = [
        r.getMessage() for r in caplog.records if r.name == "omniload.util.notify"
    ]
    assert warnings == ["Job report delivery timeout (json)"]


def test_the_run_sends_to_the_targets_it_validated(
    recorder, csv_load, tmp_path, monkeypatch
):
    config = _write(
        tmp_path, "c.yml", f"urls:\n  - {recorder.url('/before')}:\n      tag: always\n"
    )
    run = omniload.api._run_ingest_in_pipelines_dir

    def rewrite_then_run(*args, **kwargs):
        config.write_text(f"urls:\n  - {recorder.url('/after')}:\n      tag: always\n")
        return run(*args, **kwargs)

    monkeypatch.setattr(omniload.api, "_run_ingest_in_pipelines_dir", rewrite_then_run)
    run_ingest(**csv_load, notify_config=[str(config)])
    assert recorder.paths() == ["/before"]

    monkeypatch.setattr(omniload.api, "_run_ingest_in_pipelines_dir", run)
    recorder.posts.clear()
    run_ingest(**csv_load, notify_config=[str(config)])
    assert recorder.paths() == ["/after"]


@pytest.mark.parametrize(
    ("first_status", "expected"), [(200, ["/p0"]), (500, ["/p0", "/p1"])]
)
def test_priorities_escalate_as_in_apprise(recorder, tmp_path, first_status, expected):
    recorder.status["/p0"] = first_status
    config = _write(
        tmp_path,
        "c.yml",
        f"""
        urls:
          - {recorder.url("/p0")}:
              tag: failure
          - {recorder.url("/p1")}:
              tag: "1:failure"
        """,
    )
    with pytest.raises(NoSuchTableError):
        run_ingest(**_missing_table(tmp_path), notify_config=[str(config)])
    assert recorder.paths() == expected


def test_inline_urls_come_before_prioritised_entries(recorder, tmp_path):
    """Inline URLs are priority 0, so while they succeed, `1:failure` waits."""
    config = _write(
        tmp_path,
        "c.yml",
        f"""
        urls:
          - {recorder.url("/primary")}:
              tag: "1:failure"
          - {recorder.url("/fallback")}:
              tag: "2:failure"
        """,
    )
    with pytest.raises(NoSuchTableError):
        run_ingest(**_missing_table(tmp_path), notify_config=[str(config)])
    assert recorder.paths() == ["/primary"]

    recorder.posts.clear()
    with pytest.raises(NoSuchTableError):
        run_ingest(
            **_missing_table(tmp_path),
            notify=[recorder.url("/inline")],
            notify_config=[str(config)],
        )
    assert recorder.paths() == ["/inline"]


def test_a_remote_config_is_fetched_once_at_validation(recorder, csv_load):
    recorder.configs["/cfg.yml"] = (
        f"urls:\n  - {recorder.url('/remote')}:\n      tag: always\n",
        "text/yaml",
    )
    remote = f"http://127.0.0.1:{recorder.port}/cfg.yml"
    run_ingest(**csv_load, notify_config=[remote])
    assert recorder.fetches == ["/cfg.yml"]
    assert recorder.paths() == ["/remote"]

    recorder.fetches.clear()
    recorder.posts.clear()
    run_ingest(**csv_load, notify_config=[remote], dry_run=True)
    assert recorder.fetches == ["/cfg.yml"]
    assert recorder.posts == []


def test_include_follows_one_level(recorder, csv_load, tmp_path):
    _write(
        tmp_path,
        "grandchild.yml",
        f"urls:\n  - {recorder.url('/grandchild')}:\n      tag: always\n",
    )
    _write(
        tmp_path,
        "child.yml",
        f"include:\n  - grandchild.yml\nurls:\n  - {recorder.url('/child')}:\n      tag: always\n",
    )
    parent = _write(
        tmp_path,
        "parent.yml",
        f"include:\n  - child.yml\nurls:\n  - {recorder.url('/parent')}:\n      tag: always\n",
    )
    run_ingest(**csv_load, notify_config=[str(parent)])
    assert sorted(recorder.paths()) == ["/child", "/parent"]


def test_a_remote_config_cannot_include_a_local_one(recorder, csv_load, tmp_path):
    local = _write(
        tmp_path,
        "local.yml",
        f"urls:\n  - {recorder.url('/local')}:\n      tag: always\n",
    )
    recorder.configs["/cfg.yml"] = (
        f"include:\n  - file://{local}\nurls:\n  - {recorder.url('/remote')}:\n      tag: always\n",
        "text/yaml",
    )
    run_ingest(**csv_load, notify_config=[f"http://127.0.0.1:{recorder.port}/cfg.yml"])
    assert recorder.paths() == ["/remote"]


# apprise's own logging.


def _all_loggers_at_debug(monkeypatch):
    for name in list(logging.root.manager.loggerDict):
        candidate = logging.getLogger(name)
        monkeypatch.setattr(candidate, "level", logging.DEBUG)
    monkeypatch.setattr(logging.root, "level", logging.DEBUG)


def test_apprise_logs_nothing_secret(
    recorder, csv_load, tmp_path, monkeypatch, caplog, capfd
):
    """apprise formats raw URLs into warnings and urllib3 logs request paths at DEBUG."""
    _all_loggers_at_debug(monkeypatch)
    recorder.status[f"/down-a-{SECRET}"] = 500
    recorder.status[f"/down-b-{SECRET}"] = 500
    recorder.configs[f"/cfg-{SECRET}.yml"] = (
        f"urls:\n  - {recorder.url(f'/remote-{SECRET}')}:\n      tag: always\n",
        "text/yaml",
    )
    both_keys = _write(
        tmp_path,
        "both.yml",
        f"""
        urls:
          - {recorder.url(f"/entry-{SECRET}")}:
              tag: always
              tags: ops
          - {recorder.url(f"/down-a-{SECRET}")}:
              tag: always
          - {recorder.url(f"/down-b-{SECRET}")}:
              tag: always
        """,
    )
    broken = _write(
        tmp_path, "broken.yml", f"urls:\n  - slakc://{SECRET}/x/y:\n      tag: always\n"
    )
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ValidationError):
            run_ingest(**csv_load, notify_config=[str(broken)])
        run_ingest(
            **csv_load,
            notify=[recorder.url(f"/inline-{SECRET}")],
            notify_config=[
                str(both_keys),
                f"http://127.0.0.1:{recorder.port}/cfg-{SECRET}.yml",
            ],
        )
    assert any(f"/inline-{SECRET}" == path for path in recorder.paths())
    captured = capfd.readouterr()
    for record in caplog.records:
        assert SECRET not in record.getMessage(), record.name
    assert SECRET not in captured.err
    assert SECRET not in captured.out


def test_apprise_logs_nothing_secret_when_imported_fresh(recorder, tmp_path):
    (tmp_path / "d.csv").write_text("id\n1\n")
    script = textwrap.dedent(
        f"""
        import logging, sys
        logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
        from omniload import run_ingest
        run_ingest(
            source_uri="file://{tmp_path / "d.csv"}",
            dest_uri="duckdb:///{tmp_path / "w.duckdb"}",
            dest_table="out.t",
            progress="log",
            notify=["json://127.0.0.1:{recorder.port}/down-{SECRET}"],
        )
        print("apprise" in sys.modules)
        """
    )
    recorder.status[f"/down-{SECRET}"] = 500
    result = _run_python(script, tmp_path)
    assert result.stdout.splitlines()[-1] == "True"
    assert recorder.paths() == [f"/down-{SECRET}"]
    # The handler did capture this run's logging: omniload's own warning is there.
    assert "Job report delivery failure (json)" in result.stderr
    assert SECRET not in result.stdout + result.stderr


def test_a_worker_abandoned_on_timeout_stays_quiet(
    recorder, csv_load, monkeypatch, caplog, capfd
):
    from apprise.decorators import notify as apprise_plugin

    release = threading.Event()
    done = threading.Event()

    @apprise_plugin(on="omniloadlate")
    def late(body, title, notify_type, *args, **kwargs):
        release.wait(10)
        logging.getLogger("apprise").warning("late worker saw %s", SECRET)
        # A logger first created by the abandoned worker, after the call returned.
        logging.getLogger("omniload_test_late.lib").warning("late lib saw %s", SECRET)
        done.set()
        return True

    monkeypatch.setattr(notify_module, "NOTIFY_TIMEOUT", 0.3)
    with caplog.at_level(logging.DEBUG):
        info = run_ingest(**csv_load, notify=["omniloadlate://x"])
        # The run returned while the worker was still blocked: it was abandoned.
        assert not done.is_set()
        release.set()
        assert done.wait(10)
        logging.getLogger("apprise").warning("outside omniload")
    assert info is not None
    messages = [r.getMessage() for r in caplog.records]
    assert "Job report delivery timeout (omniloadlate)" in messages
    assert not any(SECRET in message for message in messages)
    assert SECRET not in capfd.readouterr().err
    assert "outside omniload" in messages


def _register_plugin(scheme: str, behaviour):
    """Register an apprise plugin for `scheme` that runs `behaviour()` on delivery."""
    from apprise.decorators import notify as apprise_plugin

    @apprise_plugin(on=scheme)
    def plugin(body, title, notify_type, *args, **kwargs):
        return behaviour()

    return plugin


@pytest.mark.parametrize("handlers", ["captured", "none"])
def test_loggers_created_during_a_send_stay_quiet(
    recorder, csv_load, monkeypatch, caplog, capfd, handlers
):
    """A logger that did not exist before the call, under any name, is still dropped."""
    fresh = f"omniload_test_plugin_{handlers}"

    def behaviour():
        logging.getLogger(f"{fresh}.lib").warning("plugin saw %s", SECRET)
        logging.getLogger(f"apprise.{fresh}").warning("apprise child saw %s", SECRET)
        return True

    _register_plugin(f"omniloadfresh{handlers}", behaviour)
    if handlers == "none":
        # Nothing to handle the record: it would fall through to `logging.lastResort`.
        monkeypatch.setattr(logging.root, "handlers", [])
        info = run_ingest(**csv_load, notify=[f"omniloadfresh{handlers}://x"])
    else:
        with caplog.at_level(logging.DEBUG):
            info = run_ingest(**csv_load, notify=[f"omniloadfresh{handlers}://x"])
        assert not any(SECRET in r.getMessage() for r in caplog.records)
    assert info is not None
    assert SECRET not in capfd.readouterr().err


@pytest.mark.parametrize("raised", [SystemExit(41), GeneratorExit()])
def test_a_plugin_raising_a_base_exception_changes_nothing(
    csv_load, tmp_path, caplog, raised
):
    def behaviour():
        raise raised

    scheme = f"omniloadexit{type(raised).__name__.lower()}"
    _register_plugin(scheme, behaviour)
    with caplog.at_level(logging.WARNING, logger="omniload"):
        info = run_ingest(**csv_load, notify=[f"{scheme}://x"])
        with pytest.raises(NoSuchTableError):
            run_ingest(**_missing_table(tmp_path), notify=[f"{scheme}://x"])
    assert info is not None
    failed = f"Sending the job report failed: {type(raised).__name__}"
    assert caplog.messages.count(failed) == 2


def test_a_failure_inside_report_itself_changes_nothing(
    recorder, csv_load, tmp_path, monkeypatch, caplog
):
    def raising_build(*args, **kwargs):
        raise SystemExit(7)

    monkeypatch.setattr(notify_module, "build", raising_build)
    with caplog.at_level(logging.WARNING, logger="omniload"):
        assert run_ingest(**csv_load, notify=[recorder.url()]) is not None
        with pytest.raises(NoSuchTableError):
            run_ingest(**_missing_table(tmp_path), notify=[recorder.url()])
    assert caplog.messages.count("Sending the job report failed: SystemExit") == 2


def test_a_keyboard_interrupt_while_sending_still_interrupts(csv_load):
    def behaviour():
        raise KeyboardInterrupt

    _register_plugin("omniloadinterrupt", behaviour)
    with pytest.raises(KeyboardInterrupt):
        run_ingest(**csv_load, notify=["omniloadinterrupt://x"])


@pytest.mark.parametrize("form", ["relative file://", "percent-escaped", "home"])
def test_config_paths_resolve_as_apprise_reads_them(
    recorder, csv_load, tmp_path, monkeypatch, form
):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    # Windows expands `~` from USERPROFILE and ignores HOME.
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.chdir(tmp_path)
    body = f"urls:\n  - {recorder.url('/ok')}:\n      tag: always\n"
    if form == "relative file://":
        (tmp_path / "notify.yml").write_text(body)
        source = "file://notify.yml"
    elif form == "percent-escaped":
        (tmp_path / "my notify.yml").write_text(body)
        source = f"file://{tmp_path}/my%20notify.yml"
    else:
        (home / "notify.yml").write_text(body)
        source = "~/notify.yml"
    run_ingest(**csv_load, notify_config=[source])
    assert recorder.paths() == ["/ok"]


def test_cli_notify_on(recorder, csv_load, tmp_path):
    args = [*_cli_load(csv_load), "--notify", recorder.url()]
    result = _cli([*args, "--notify-on", "failure"])
    assert result.exit_code == 0, result.output
    assert recorder.posts == []

    result = _cli(args, env={"OMNILOAD_NOTIFY_ON": "failure"})
    assert result.exit_code == 0, result.output
    assert recorder.posts == []

    result = _cli([*args, "--notify-on", "sometimes"])
    assert result.exit_code != 0
    assert "sometimes" in result.output


def test_apprise_discovery_is_never_used(recorder, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".apprise.yml").write_text(
        f"urls:\n  - {recorder.url('/home')}:\n      tag: always\n"
    )
    (tmp_path / "d.csv").write_text("id\n1\n")
    script = textwrap.dedent(
        f"""
        import sys
        from omniload import run_ingest
        run_ingest(
            source_uri="file://{tmp_path / "d.csv"}",
            dest_uri="duckdb:///{tmp_path / "w.duckdb"}",
            dest_table="out.t",
            progress="log",
        )
        print("apprise" in sys.modules)
        run_ingest(
            source_uri="file://{tmp_path / "d.csv"}",
            dest_uri="duckdb:///{tmp_path / "w.duckdb"}",
            dest_table="out.t",
            progress="log",
            notify=["{recorder.url("/inline")}"],
        )
        """
    )
    env = {
        "HOME": str(home),
        "APPRISE_URLS": recorder.url("/env-urls"),
        "APPRISE_CONFIG_PATH": str(home / ".apprise.yml"),
    }
    result = _run_python(script, tmp_path, env=env)
    assert "False" in result.stdout.splitlines()
    # Neither run reached the discovered targets, with or without an option given.
    assert recorder.paths() == ["/inline"]
