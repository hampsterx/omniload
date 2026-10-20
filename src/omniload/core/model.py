import importlib
import typing
from typing import Any, Iterator, Mapping, Protocol

if typing.TYPE_CHECKING:
    from dlt.common.destination import Destination


class SourceProtocol(Protocol):
    """
    Note: an optional ``post_load(self) -> None`` hook may be defined by sources that ack
    their own offsets (e.g. mq-bridge). It is intentionally NOT part of this protocol so
    that ordinary sources need not implement it; run_ingest invokes it only when present
    via ``getattr(source, "post_load", lambda: None)()``.

    Likewise, an optional ``honours_run_disposition(self) -> bool`` hook may be defined by
    ``handles_incrementality`` sources that hold no resource-level write disposition (the
    filesystem family). It signals that an explicit run-level ``--incremental-strategy``
    append/replace is safe to honour. It is absent on sources that set their own
    resource-level disposition; run_ingest reads it via
    ``getattr(source, "honours_run_disposition", lambda: False)()``, so the default is False.

    Filesystem-family sources may also define
    ``supports_filesystem_incremental(self) -> bool`` to opt into file selection by
    modification time. It is optional for the same reason and defaults to False when
    absent.

    Sources that may dispatch several tables can optionally define
    ``produces_multiple_tables(uri, table) -> bool``. It defaults to False when absent.

    A source may optionally define ``consumed_run_options(self) -> frozenset[str]``,
    naming the subset of omniload's run vocabulary its ``dlt_source`` actually accepts.
    When present, run_ingest filters the run options it passes down to that subset
    before expanding them as keyword arguments, so a name the source does not declare
    never reaches a connector constructor as a stray keyword. Absent means the source
    receives every run option, unfiltered, which is the default for the ~90 sources
    that never subtract from it themselves.
    """  # noqa: E501

    def dlt_source(self, uri: str, table: str, **kwargs):
        """Build the dlt source or resource for the requested table."""
        pass

    def handles_incrementality(self) -> bool:
        """Return whether the source manages its own incremental state."""
        pass


class DestinationProtocol(Protocol):
    """Protocol implemented by destination adapters used by the factory.

    Destinations that only represent one table may optionally define
    ``supports_multiple_tables() -> bool`` and return ``False``. The ingest API
    treats an absent method as support for dlt's normal multi-table dispatch.

    A destination that cannot load with the ``merge`` write disposition may
    define ``supports_merge() -> bool`` and return ``False``; the ingest API then
    refuses a run whose effective disposition merges, before loading. An absent
    method means merge is supported.

    A destination holding run-scoped resources from ``dlt_dest()`` on may define
    ``close() -> None``. The ingest API calls it on every exit once the
    destination exists, including a failure inside ``dlt_dest()`` and a dry run,
    so it must be idempotent and safe before ``dlt_dest()`` has run.

    A destination whose state cannot be kept consistent with a persistent
    ``--pipelines-dir`` may define ``persistent_pipelines_dir_refusal(uri) ->
    str | None``; a returned reason refuses such a run before anything is loaded.
    One that cannot drop and reload its tables may likewise define
    ``full_refresh_refusal() -> str | None``, consulted for ``--full-refresh``.

    A destination that copies a remote database to local disk may define
    ``set_remote_database_staging_root(root: str | None) -> None``. The ingest API
    calls it before ``dlt_dest()`` with the run's ``remote_database_staging_root``,
    so the copy can go where the caller asked rather than the system temporary
    directory. Other destinations never see the option.
    """

    def dlt_dest(self, uri: str, **kwargs) -> "Destination":
        """Build the dlt destination for the given URI."""
        pass

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        """Return dlt pipeline run parameters for the destination table."""
        pass

    def post_load(self) -> None:
        """Run destination follow-up work after a successful load."""
        pass


class LazyRegistry(Mapping):
    """Mapping that imports a connector class only when its scheme is first accessed."""

    def __init__(self, paths: dict[str, str]) -> None:
        """Store scheme-to-class paths without importing connector modules."""
        self._paths = paths
        self._cache: dict[str, Any] = {}

    def _load(self, dotted_path: str) -> Any:
        """Import and return the connector class named by a dotted path."""
        module_path, cls_name = dotted_path.rsplit(":", 1)
        return getattr(importlib.import_module(module_path), cls_name)

    def __getitem__(self, key: str) -> Any:
        """Return the cached connector class for a scheme, importing on first use."""
        if key not in self._cache:
            if key not in self._paths:
                raise KeyError(key)
            self._cache[key] = self._load(self._paths[key])
        return self._cache[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered URI schemes without importing connectors."""
        return iter(self._paths)

    def __len__(self) -> int:
        """Return the number of registered URI schemes."""
        return len(self._paths)

    def __contains__(self, key: object) -> bool:  # O(1), no import triggered
        """Return whether a scheme is registered without importing it."""
        return key in self._paths
