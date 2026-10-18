"""Job summary report for one `run_ingest` invocation.

A `JobReport` is built from what a finished (or failed) run already holds in memory:
the original `LoadRequest`, the wall-clock bounds, and the dlt `LoadInfo` or exception
when there is one. It is meant to leave the process (a notification target), so every
field is drawn from an allowlist and nothing free-form is copied in:

- URIs are reduced to their scheme. Credentials, hosts and paths never appear.
- Table selectors keep only their leading identifier. A selector with a payload
  (``query:<SQL>``, ``sql:<SQL>``, ``collection:<query JSON>``, ``custom:<fields>``
  and the like)
  or a ``?``/``#`` part is rendered as a custom selector, because the payload is a query
  or carries values.
- Exceptions are reported by type only. Their messages, dlt's failed-job messages and
  ``str(LoadInfo)`` all embed arbitrary text, including credentials, and are left out.

Counts that are not known for this run are `None` and render as "unavailable", never 0.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import re
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dlt.common.pipeline import LoadInfo

    from omniload.model import LoadRequest


class Status(str, Enum):
    """Terminal outcome of one invocation."""

    success = "success"
    failure = "failure"


class Stage(str, Enum):
    """Where a failed run stopped.

    ``staging`` is the remote-database download before the load, ``setup`` covers
    request validation, source and destination construction and dlt's state sync, and
    ``extract`` / ``normalize`` / ``load`` are dlt's pipeline steps (failed jobs in a
    returned `LoadInfo` count as ``load``). The two ``*_post_load`` stages are the
    commit hooks after a successful load. ``pipeline`` is a dlt step outside that map.
    """

    staging = "staging"
    setup = "setup"
    extract = "extract"
    normalize = "normalize"
    load = "load"
    destination_post_load = "destination_post_load"
    source_post_load = "source_post_load"
    pipeline = "pipeline"


#: dlt `TPipelineStep` values, mapped onto `Stage`. Anything else is `Stage.pipeline`.
DLT_STEP_STAGES: dict[str, Stage] = {
    "sync": Stage.setup,
    "extract": Stage.extract,
    "normalize": Stage.normalize,
    "load": Stage.load,
}

UNAVAILABLE = "unavailable"
CUSTOM_QUERY = "custom query"
CUSTOM_SELECTOR = "custom selector"

_SCHEME = re.compile(r"[a-z][a-z0-9+.-]{0,31}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.$*/-]{1,128}")
_TYPE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_VERSION = re.compile(r"[A-Za-z0-9_.+-]{1,64}")


@dataclasses.dataclass(frozen=True)
class Endpoint:
    """One side of the load: a URI scheme and a rendered table selector.

    ``scheme`` is ``"unknown"`` when the URI has no plain ``<scheme>://`` prefix.
    ``table`` is `None` when no table was given; otherwise it is an identifier, or
    ``"<kind> (custom selector)"``, ``"custom query"`` or ``"custom selector"``.
    """

    scheme: str
    table: str | None

    def label(self) -> str:
        """Return ``scheme`` or ``scheme / table`` for plain text."""
        return self.scheme if self.table is None else f"{self.scheme} / {self.table}"


@dataclasses.dataclass(frozen=True)
class JobReport:
    """The allowlisted summary of one invocation.

    Fields:

    - ``status``: success, or failure when the run raised.
    - ``source``, ``destination``: scheme and rendered table selector (see `Endpoint`).
      No URI, credential, host, path, query text or selector payload.
    - ``started_at``, ``finished_at``: timezone-aware wall-clock bounds of the whole
      invocation, remote staging included.
    - ``omniload_version``: the installed distribution version.
    - ``stage``: where a failed run stopped (`Stage`); `None` on success, and on a
      failure the caller could not place.
    - ``exception_type``: the raised exception's class name, plus the wrapped
      exception's class for dlt's ``PipelineStepFailed``. Never its message.
    - ``rows``: rows **normalized** per table (dlt's normalize metrics, not rows
      committed), dlt's own ``_dlt_*`` tables dropped. `None` when no normalize info
      exists for this run; an empty tuple when normalize ran and produced no data
      tables.
    - ``load_package_count``, ``failed_job_count``: `None` when no `LoadInfo` or
      failed-job list is known.
    - ``failed_job_tables``: table names of failed jobs, sorted, no messages; `None`
      alongside ``failed_job_count``.
    """

    status: Status
    source: Endpoint
    destination: Endpoint
    started_at: dt.datetime
    finished_at: dt.datetime
    omniload_version: str
    stage: Stage | None = None
    exception_type: str | None = None
    rows: tuple[tuple[str, int], ...] | None = None
    load_package_count: int | None = None
    failed_job_count: int | None = None
    failed_job_tables: tuple[str, ...] | None = None

    @property
    def duration(self) -> dt.timedelta:
        """Return the wall-clock duration of the invocation."""
        return self.finished_at - self.started_at

    @property
    def title(self) -> str:
        """Return a one-line title naming the outcome and both schemes."""
        outcome = self.status.value
        if self.status is Status.failure and self.stage is not None:
            outcome = f"{outcome} at {self.stage.value}"
        return f"omniload {outcome}: {self.source.scheme} → {self.destination.scheme}"

    def as_markdown(self) -> str:
        """Render the report as a Markdown body."""
        return "\n".join(self._lines(code=lambda value: f"`{value}`", bold=True))

    def as_text(self) -> str:
        """Render the report as a plain-text body."""
        return "\n".join(self._lines(code=str, bold=False))

    def _lines(self, *, code, bold: bool) -> list[str]:
        import humanize

        def endpoint(item: Endpoint) -> str:
            if item.table is None:
                return code(item.scheme)
            return f"{code(item.scheme)} / {code(item.table)}"

        def count(value: int | None) -> str:
            return UNAVAILABLE if value is None else str(value)

        status = f"**{self.status.value}**" if bold else self.status.value
        lines = [f"Status: {status}"]
        if self.stage is not None:
            lines.append(f"Stage: {code(self.stage.value)}")
        if self.exception_type is not None:
            lines.append(f"Exception: {code(self.exception_type)}")
        lines += [
            "",
            f"- Source: {endpoint(self.source)}",
            f"- Destination: {endpoint(self.destination)}",
            f"- Started: {self.started_at.isoformat(timespec='seconds')}",
            f"- Finished: {self.finished_at.isoformat(timespec='seconds')}",
            f"- Duration: {humanize.precisedelta(self.duration)}",
            f"- Load packages: {count(self.load_package_count)}",
            f"- Failed jobs: {count(self.failed_job_count)}",
        ]
        if self.failed_job_tables:
            tables = ", ".join(code(name) for name in self.failed_job_tables)
            lines.append(f"- Failed job tables: {tables}")
        lines.append(f"- omniload: {code(self.omniload_version)}")
        lines.append("")
        if self.rows is None:
            lines.append(f"Rows normalized: {UNAVAILABLE}")
        elif not self.rows:
            lines.append("Rows normalized: no data tables")
        else:
            lines.append("Rows normalized:")
            lines += [f"- {code(name)}: {rows} rows" for name, rows in self.rows]
        return lines


def build(
    request: LoadRequest,
    started_at: dt.datetime,
    finished_at: dt.datetime,
    *,
    load_info: LoadInfo | None = None,
    exception: BaseException | None = None,
    stage: Stage | None = None,
) -> JobReport:
    """Build the report for one invocation from explicit context.

    ``request`` is the caller's original `LoadRequest` (before any remote staging
    rewrote its source URI). ``started_at`` and ``finished_at`` must be timezone-aware.
    ``exception`` makes the report a failure. A dlt ``PipelineStepFailed`` sets the
    stage from its own step, overriding ``stage``, because the caller can only tell
    that the failure happened inside ``pipeline.run()``.

    Rows normalized come from the pipeline trace of ``load_info`` or of a
    ``PipelineStepFailed``, and only when that trace started at or after
    ``started_at``: dlt falls back to the trace persisted by an earlier run when the
    in-memory one is unset, and those numbers belong to that run. The package count
    comes from ``load_info``, else the ``LoadInfo`` a ``PipelineStepFailed`` carries.
    Failed jobs come from ``load_info``, else the failed jobs of an ``IngestJobError``;
    a ``PipelineStepFailed``'s own ``LoadInfo`` predates its jobs, so it gives none,
    also when passed as ``load_info`` (recognised by identity, so not as a copy).

    Table names from dlt (rows, failed jobs) are replaced by position (``table 1``)
    when the destination selector, given or defaulted, lost anything but surrounding
    whitespace in rendering, because dlt names the table after the raw selector.
    """
    from dlt.common.pipeline import LoadInfo
    from dlt.pipeline.exceptions import PipelineStepFailed

    from omniload import __version__
    from omniload.error import IngestJobError

    if started_at.tzinfo is None or finished_at.tzinfo is None:
        raise ValueError("JobReport timestamps must be timezone-aware")

    step_failure = exception if isinstance(exception, PipelineStepFailed) else None
    if step_failure is not None:
        stage = DLT_STEP_STAGES.get(step_failure.step, Stage.pipeline)

    # The `LoadInfo` a load-step `PipelineStepFailed` carries is taken before its jobs
    # ran (package still "normalized", every job "new"), so it gives a package count but
    # no failed-job count: reporting its zero would understate a failed load.
    # That holds when the caller passes it as `load_info` too.
    jobs_known = load_info is not None and not (
        step_failure is not None and load_info is step_failure.step_info
    )
    if load_info is None and step_failure is not None:
        if isinstance(step_failure.step_info, LoadInfo):
            load_info = step_failure.step_info

    source = Endpoint(
        scheme=render_scheme(request.source_uri),
        table=render_selector(request.source_table),
    )
    dest_defaulted = request.dest_table is None
    destination = Endpoint(
        scheme=render_scheme(request.dest_uri),
        table=source.table if dest_defaulted else render_selector(request.dest_table),
    )
    # dlt names the table after the destination selector (the source selector when it
    # defaulted), normalising whatever the rendering dropped into an identifier: a
    # `?token=...` or a query payload. Name tables by position whenever it dropped any.
    dest_selector = request.source_table if dest_defaulted else request.dest_table
    positional = (
        dest_selector is not None and destination.table != dest_selector.strip()
    )

    pipeline = None
    if load_info is not None:
        pipeline = load_info.pipeline
    elif step_failure is not None:
        pipeline = step_failure.pipeline
    rows = _rows(pipeline, started_at, positional=positional)

    package_count = None
    failed_jobs: list[Any] | None = None
    if load_info is not None:
        package_count = len(load_info.load_packages)
    if load_info is not None and jobs_known:
        failed_jobs = [
            job
            for package in load_info.load_packages
            for job in package.jobs["failed_jobs"]
        ]
    elif isinstance(exception, IngestJobError):
        failed_jobs = list(exception.failed_jobs)

    failed_tables = None
    if failed_jobs is not None:
        names = sorted({job.job_file_info.table_name for job in failed_jobs})
        failed_tables = tuple(
            _table_name(name, index, positional=positional)
            for index, name in enumerate(names, start=1)
        )

    return JobReport(
        status=Status.success if exception is None else Status.failure,
        source=source,
        destination=destination,
        started_at=started_at,
        finished_at=finished_at,
        omniload_version=__version__ if _VERSION.fullmatch(__version__) else "unknown",
        stage=None if exception is None else stage,
        exception_type=None if exception is None else _exception_type(exception),
        rows=rows,
        load_package_count=package_count,
        failed_job_count=None if failed_jobs is None else len(failed_jobs),
        failed_job_tables=failed_tables,
    )


def render_scheme(uri: str) -> str:
    """Return the URI's scheme, or ``"unknown"`` without a plain ``<scheme>://``."""
    head, separator, _ = uri.partition("://")
    head = head.lower()
    if separator and _SCHEME.fullmatch(head):
        return head
    return "unknown"


def render_selector(table: str | None) -> str | None:
    """Return a table selector reduced to its leading identifier.

    ``query:<SQL>`` becomes ``"custom query"``; any other ``<kind>:<payload>`` becomes
    ``"<kind> (custom selector)"``; a ``?``/``#`` part is dropped; anything left that is
    not a plain identifier becomes ``"custom selector"``.
    """
    if table is None:
        return None
    kind, colon, _ = table.partition(":")
    kind = re.split(r"[?#]", kind, maxsplit=1)[0].strip()
    if colon and kind == "query":
        return CUSTOM_QUERY
    if not _IDENTIFIER.fullmatch(kind):
        return CUSTOM_SELECTOR
    return f"{kind} ({CUSTOM_SELECTOR})" if colon else kind


def _table_name(name: str, index: int, *, positional: bool) -> str:
    if positional or not _IDENTIFIER.fullmatch(name):
        return f"table {index}"
    return name


def _exception_type(exception: BaseException) -> str:
    from dlt.pipeline.exceptions import PipelineStepFailed

    def name(item: BaseException) -> str:
        value = type(item).__name__
        return value if _TYPE_NAME.fullmatch(value) else "unknown"

    if isinstance(exception, PipelineStepFailed) and exception.exception is not None:
        return f"{name(exception)} ({name(exception.exception)})"
    return name(exception)


def _rows(
    pipeline, started_at: dt.datetime, *, positional: bool
) -> tuple[tuple[str, int], ...] | None:
    """Return this run's rows normalized per data table, or `None` if unknown."""
    if pipeline is None:
        return None
    try:
        trace = pipeline.last_trace
    except Exception:
        # `last_trace` reads the persisted trace from the working directory when the
        # in-memory one is unset; a deleted temporary directory must not fail the
        # report.
        return None
    if trace is None or trace.started_at is None or trace.started_at < started_at:
        return None
    normalize_info = trace.last_normalize_info
    if normalize_info is None:
        return None
    names = sorted(
        name for name in normalize_info.row_counts if not name.startswith("_dlt_")
    )
    return tuple(
        (
            _table_name(name, index, positional=positional),
            normalize_info.row_counts[name],
        )
        for index, name in enumerate(names, start=1)
    )
