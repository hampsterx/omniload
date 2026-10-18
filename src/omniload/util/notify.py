"""Send a run's job report through apprise.

Targets come from two inputs that add up into one `Apprise` object: inline apprise URLs
(``--notify``), which receive every report that goes out, and apprise config files or
URLs (``--notify-config``), whose entries route by their own tags (``success``,
``failure``, or apprise's ``always``). Each report is sent once, with the run's outcome
as both the tag and the notify type.

Everything is read and checked before the load starts, so a broken target fails the run
early (also under ``--dry-run``) instead of after the load. Once the load has run, a
notification that cannot be sent logs one warning and changes nothing else: the return
value, the exception and the exit status stay what they would have been.

Notification URLs carry secrets (webhook tokens, passwords), and apprise's own log lines
and its transport's debug lines print them. While omniload calls apprise, those loggers
are muted for the calling context (`quiet_apprise`), and omniload names each input by
its position and scheme only.
"""

from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import datetime as dt
import logging
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from omniload.core.report import Stage, Status, build, render_scheme
from omniload.error import ValidationError
from omniload.model import NotifyOn

if TYPE_CHECKING:
    from dlt.common.pipeline import LoadInfo

    from omniload.model import LoadRequest

logger = logging.getLogger(__name__)

#: How long one `notify()` call waits for delivery, in seconds. apprise's own default
#: (0) sets no bound, and a report must not hold a finished load open. A target still
#: running when this expires keeps running until its own socket timeouts end it.
NOTIFY_TIMEOUT = 30

#: Tags inline URLs are added with, so they receive every outcome.
OUTCOME_TAGS = [Status.success.value, Status.failure.value]

#: Config sources are read over HTTP from these schemes; everything else is local.
REMOTE_SCHEMES = frozenset({"http", "https"})

#: Loggers apprise and the libraries it sends through write to. Their records can
#: carry notification URLs (apprise formats raw URLs into some warnings, urllib3 logs
#: request paths at DEBUG), so they are muted while omniload calls apprise. A filter
#: applies only on the logger that creates a record, so every logger under these names
#: gets one.
QUIET_LOGGER_PREFIXES = (
    "apprise",
    "urllib3",
    "requests",
    "requests_oauthlib",
    "oauthlib",
    "charset_normalizer",
    "MARKDOWN",
    "asyncio",
    "concurrent.futures",
)

_QUIET = contextvars.ContextVar("omniload_quiet_apprise", default=False)


class _QuietFilter(logging.Filter):
    """Drop every record created while the calling context has `_QUIET` set.

    apprise runs each delivery in a worker thread with a copy of the caller's
    context, so a worker it abandons on timeout still carries the flag when it logs
    later. Outside such a context the filter passes everything through.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return not _QUIET.get()


_QUIET_FILTER = _QuietFilter()


def _install_quiet_filter() -> None:
    """Attach the filter to every existing logger under `QUIET_LOGGER_PREFIXES`."""
    names = list(QUIET_LOGGER_PREFIXES)
    names += [
        name
        for name in list(logging.root.manager.loggerDict)
        if name.startswith(tuple(f"{prefix}." for prefix in QUIET_LOGGER_PREFIXES))
    ]
    for name in names:
        target = logging.getLogger(name)
        if _QUIET_FILTER not in target.filters:
            target.addFilter(_QUIET_FILTER)


@contextlib.contextmanager
def quiet_apprise() -> Generator[None, None, None]:
    """Mute apprise and its transport for work done in this context.

    The filter is installed on entry and again on exit, so loggers apprise or its
    dependencies created during the call are covered for any worker still running.
    """
    _install_quiet_filter()
    token = _QUIET.set(True)
    try:
        yield
    finally:
        _QUIET.reset(token)
        _install_quiet_filter()


def _import_apprise():
    with quiet_apprise():
        try:
            import apprise
        except ImportError as exc:
            raise ValidationError(
                "Sending notifications needs apprise: install omniload[notify]"
            ) from exc
    return apprise


def _label(option: str, position: int, value: str) -> str:
    scheme = "file" if "://" not in value else render_scheme(value)
    return f"{option} #{position} ({scheme})"


def _local_path(source: str) -> Path | None:
    """Return the file a local config source names, or `None` for a remote one."""
    if "://" not in source:
        return Path(source)
    parts = urlsplit(source)
    if parts.scheme.lower() == "file":
        return Path(parts.path)
    return None


@dataclasses.dataclass
class Notifier:
    """The validated targets for one invocation, and the gate on outcomes."""

    apprise: Any
    sender: Any
    notify_on: NotifyOn
    target_count: int

    @classmethod
    def prepare(cls, request: LoadRequest) -> Notifier | None:
        """Read and check every notification input, before anything is loaded.

        Returns `None` when the request names no notification input, without
        importing apprise. Raises `ValidationError` for a bad inline URL, a missing
        extra, a config source apprise cannot open, and a config source that, once
        read, holds no usable entry tagged ``success``, ``failure`` or ``always``. A
        remote source that yields nothing only warns in a real run, so a config server
        that is briefly down does not stop the load; under ``--dry-run`` it is refused.
        """
        urls = list(request.notify or [])
        sources = list(request.notify_config or [])
        if not urls and not sources:
            return None
        try:
            notify_on = NotifyOn(request.notify_on)
        except ValueError as exc:
            choices = ", ".join(member.value for member in NotifyOn)
            raise ValidationError(f"--notify-on must be one of: {choices}") from exc

        apprise = _import_apprise()
        with quiet_apprise():
            try:
                sender = apprise.Apprise()
                count = 0
                for position, url in enumerate(urls, start=1):
                    probe = apprise.Apprise()
                    if not probe.add(url, tag=OUTCOME_TAGS):
                        raise ValidationError(
                            f"{_label('--notify', position, url)} is not a valid "
                            "apprise URL"
                        )
                    services = list(probe.find(tag=OUTCOME_TAGS))
                    sender.add(services)
                    count += len(services)
                for position, source in enumerate(sources, start=1):
                    services = cls._read_config(
                        apprise, source, position, dry_run=request.dry_run
                    )
                    sender.add(services)
                    count += len(services)
            except ValidationError:
                raise
            except Exception as exc:
                # apprise's own message can quote the URL it failed on.
                raise ValidationError(
                    f"Reading the notification targets failed: {type(exc).__name__}"
                ) from None
        return cls(
            apprise=apprise, sender=sender, notify_on=notify_on, target_count=count
        )

    @staticmethod
    def _read_config(apprise, source: str, position: int, *, dry_run: bool) -> list:
        """Read one config source on its own and return its usable services."""
        label = _label("--notify-config", position, source)
        path = _local_path(source)
        if path is not None and not path.is_file():
            raise ValidationError(f"{label}: no such file")
        config = apprise.AppriseConfig(recursion=1)
        if not config.add(source):
            raise ValidationError(f"{label} is not a config source apprise can read")
        # A throwaway object holding only this source, because `services()` flattens
        # every source and include together, and one good file would hide a broken
        # one. `find()` forces the read, resolves `${NAME}` templates without sending,
        # and skips entries it cannot resolve.
        probe = apprise.Apprise()
        probe.add(config)
        services = list(probe.find(tag=OUTCOME_TAGS, report=[]))
        if services:
            return services
        if path is None and not dry_run and render_scheme(source) in REMOTE_SCHEMES:
            logger.warning("%s yielded no usable target, skipped for this run", label)
            return []
        raise ValidationError(
            f"{label} has no usable entry tagged success, failure or always "
            "(check it with `apprise -c <config> --dry-run`)"
        )

    def report(
        self,
        request: LoadRequest,
        started_at: dt.datetime,
        *,
        load_info: LoadInfo | None = None,
        exception: BaseException | None = None,
        stage: Stage | None = None,
    ) -> None:
        """Build the run's report and send it. Never raises."""
        status = Status.success if exception is None else Status.failure
        if self.notify_on is NotifyOn.failure and status is Status.success:
            return
        try:
            report = build(
                request,
                started_at,
                dt.datetime.now(tz=dt.timezone.utc),
                load_info=load_info,
                exception=exception,
                stage=stage,
            )
            self._send(report.title, report.as_markdown(), status)
        except Exception as exc:
            logger.warning("Sending the job report failed: %s", type(exc).__name__)

    def _send(self, title: str, body: str, status: Status) -> None:
        apprise = self.apprise
        with quiet_apprise():
            result = self.sender.notify(
                body=body,
                title=title,
                notify_type=apprise.NotifyType(status.value),
                body_format=apprise.NotifyFormat.MARKDOWN,
                tag=status.value,
                timeout=NOTIFY_TIMEOUT,
            )
        outcome = result.status
        if outcome == apprise.AppriseResultStatus.SUCCESS:
            logger.debug("Job report sent")
        elif outcome == apprise.AppriseResultStatus.NOMATCH:
            logger.debug("No notification target is routed for %s", status.value)
        else:
            failed = sorted(
                {
                    render_scheme(item.url)
                    for item in result.results
                    if item.status != apprise.AppriseResultStatus.SUCCESS
                }
            )
            logger.warning(
                "Job report delivery %s%s",
                outcome.name.lower(),
                f" ({', '.join(failed)})" if failed else "",
            )
