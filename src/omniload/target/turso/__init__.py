import logging
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from omniload.core.tablename import Defaults, TableName, two_level
from omniload.error import ValidationError
from omniload.target.model import GenericSqlDestination
from omniload.util.turso import (
    RemoteTurso,
    TursoSyncError,
    call_redacted,
    parse_turso_uri,
    sync_connect,
)

logger = logging.getLogger(__name__)

#: Seconds to wait between attempts to open a replica connection that the sync engine
#: reports busy: about 12 seconds in all before the last attempt's error stands.
BUSY_BACKOFF = (0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4)


def _is_busy(exc: Exception) -> bool:
    message = str(exc).lower()
    return "database is busy" in message or "database is locked" in message


def _remove(directory: Path) -> None:
    """Remove a replica directory, saying so when that fails: each one is a copy of
    the remote database, so one left behind is worth a line in the log."""
    try:
        shutil.rmtree(directory)
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("Could not remove the Turso replica at %s: %s", directory, exc)


class TursoDestination(GenericSqlDestination):
    """Load into a Turso database through pyturso's SQLAlchemy dialects.

    ``turso:///path/to/app.db`` writes an in-process database file.
    ``turso://host`` writes a remote database (Turso Cloud, or a self-hosted
    ``tursodb --sync-server``) through a replica bootstrapped for this run alone,
    pushed to the server once the load has committed, and then discarded.

    Tables land in the ``main`` schema only, and only with the ``append`` and
    ``replace`` dispositions: dlt reaches any other schema, and the staging tables
    a merge needs, by ``ATTACH``ing a sibling file, which Turso treats as
    experimental. Enabling it is no fix either, since Turso then resolves the
    merge's database-qualified correlated subquery against the wrong table and
    deletes every destination row (tursodatabase/turso#6279).
    """

    table_capability = two_level("turso", min_components=1)

    def __init__(self) -> None:
        self._remote: Optional[RemoteTurso] = None
        self._replica_dir: Optional[Path] = None

    def dlt_dest(self, uri: str, **kwargs):
        from omniload.target.turso.client import turso_sqlalchemy

        target = parse_turso_uri(uri)
        creator = None
        if isinstance(target, RemoteTurso):
            self._remote = target
            # Beside the source's replica when the caller names a staging root:
            # each one is a copy of the whole remote database.
            staging_root = kwargs.get("remote_database_staging_root")
            if staging_root:
                Path(staging_root).mkdir(parents=True, exist_ok=True)
            self._replica_dir = Path(
                tempfile.mkdtemp(prefix="turso-replica-", dir=staging_root or None)
            )
            replica = str(self._replica_dir / "replica.db")
            # The URL names the replica and the server, never the token. Every
            # connection dlt opens, load workers included, goes through
            # `sync_connect`, whose failures carry the host and not the token.
            credentials = (
                f"sqlite+turso_sync:///{Path(replica).as_posix()}"
                f"?remote_url={quote(target.url, safe='')}"
            )

            # Each connection starts the replica's sync engine, which reports
            # "database is busy" while another connection still holds it, as when a
            # load worker opens its own beside the one dlt keeps for the schema.
            # It clears once that one is returned, so wait for it here rather than
            # fail the load job and leave dlt to retry it.
            def connect():
                for delay in BUSY_BACKOFF:
                    try:
                        return sync_connect(target, replica)
                    except TursoSyncError as exc:
                        if not _is_busy(exc):
                            raise
                    time.sleep(delay)
                return sync_connect(target, replica)

            creator = connect
        else:
            credentials = target.url

        # No merge strategies, and a client that refuses every merge table, nested
        # ones included, at the start of the load step, before it writes anything.
        # That covers what the check in `run_ingest` cannot see ahead of
        # extraction: table variants, nested tables with their own hints, and
        # hints a resource yields with its data.
        # Replace only by truncating in place: dlt's other strategy goes through a
        # staging dataset, which is the same ATTACH. Load jobs run one at a time:
        # Turso refuses a second writer outright ("database is locked") instead of
        # waiting for the first, so parallel jobs fail a load that writes many
        # tables. The name keeps dlt reading the `destination.sqlalchemy`
        # configuration section, as the stock factory does.
        factory = turso_sqlalchemy(
            credentials=credentials,
            destination_name="sqlalchemy",
            supported_merge_strategies=[],
            supported_replace_strategies=["truncate-and-insert"],
            loader_parallelism_strategy="sequential",
        )
        factory._creator = creator
        return factory

    def table_defaults(self, uri: str) -> Defaults:
        return Defaults(schema="main")

    def parse_table(self, uri: str, table: str) -> TableName:
        parsed = super().parse_table(uri, table)
        if parsed.schema != "main":
            raise ValidationError(
                f"The 'turso' destination writes to the 'main' schema only, not "
                f"{parsed.schema!r}: dlt would reach another schema through ATTACH, "
                "which Turso does not support. Name the table as '<table>' or "
                "'main.<table>'."
            )
        return parsed

    def supports_merge(self) -> bool:
        """Return ``False``: a merge load into Turso loses rows, so it is refused.

        ``run_ingest`` checks every selected resource's effective write
        disposition against this before extracting anything, so the common
        cases fail early with a message that names the fix. The destination's
        empty merge strategy list is the backstop for the rest.
        """
        return False

    def persistent_pipelines_dir_refusal(self, uri: str) -> Optional[str]:
        """Refuse a persistent ``--pipelines-dir`` for a remote database.

        dlt saves the run's incremental state locally before ``post_load()``
        pushes the replica. If that push fails, the replica is discarded but a
        persistent pipelines directory keeps the advanced cursor, and the next
        run would skip the rows that never reached the server. The default
        temporary directory is discarded with the replica, and the next run
        restores its state from the server instead.
        """
        if isinstance(parse_turso_uri(uri), RemoteTurso):
            return (
                "A remote Turso destination keeps no local replica between runs, so "
                "it cannot be combined with '--pipelines-dir': a failed push would "
                "leave that directory's incremental state ahead of the server. Omit "
                "'--pipelines-dir'; dlt restores the state from the server instead."
            )
        return None

    def post_load(self) -> None:
        """Push the replica's committed writes to the remote database."""
        if self._remote is None or self._replica_dir is None:
            return
        target, replica = self._remote, str(self._replica_dir / "replica.db")
        connection = sync_connect(target, replica)
        try:
            call_redacted(
                target,
                "Pushing the load to",
                connection.push,
                advice=(
                    "Delivery could not be confirmed, so some or all rows may have "
                    "reached the server. Re-running an 'append' load may duplicate "
                    "rows; a 'replace' load is safe to repeat."
                ),
            )
        finally:
            # The push's own error is the one to report.
            try:
                connection.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Closing the Turso replica connection failed: %s", exc)

    def close(self) -> None:
        """Remove the run's replica, if any.

        ``run_ingest`` calls this on every exit once the destination exists, so
        it must be safe to call before ``dlt_dest()`` and more than once. A local
        ``turso:///`` file is the user's own database and is never touched.
        """
        replica_dir, self._replica_dir = self._replica_dir, None
        if replica_dir is not None:
            _remove(replica_dir)
