"""Parse ``turso://`` URIs into the two databases the Turso connector reaches.

One scheme names both variants, told apart by whether the URI has a host:

- ``turso:///path/to/app.db`` is an in-process database file, read and written
  through pyturso's ``sqlite+turso`` SQLAlchemy dialect. The path follows the
  ``sqlite://`` convention: three slashes for a relative path, four for an
  absolute one.
- ``turso://host[:port]?auth_token=...`` is a remote database reached over
  Turso's sync protocol: Turso Cloud, or a self-hosted ``tursodb --sync-server``.
  ``tls=false`` selects plain HTTP, for a local sync server.

The auth token never appears in anything this module returns as text, so a
caller can log or interpolate a target's URL without leaking it.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import parse_qs, urlsplit

from omniload.error import ValidationError

logger = logging.getLogger(__name__)

SCHEME = "turso"
LOCAL_DIALECT = "sqlite+turso"

#: Both spellings are accepted: ``auth_token`` is pyturso's, ``authToken`` the
#: one Turso's own connection URLs carry.
TOKEN_PARAMETERS = ("auth_token", "authToken")
REMOTE_PARAMETERS = frozenset({*TOKEN_PARAMETERS, "tls"})

#: Characters an HTTP header value cannot carry. pyturso sends the token as a
#: header, so one of these would fail inside the HTTP client, whose error text
#: may quote the header back.
HEADER_INVALID = ("\r", "\n", "\x00")


@dataclass(frozen=True)
class LocalTurso:
    """An in-process Turso database file."""

    #: The file's ``sqlite+turso://`` SQLAlchemy URL.
    url: str


@dataclass(frozen=True)
class RemoteTurso:
    """A remote Turso database, reached over Turso's sync protocol."""

    #: The sync endpoint, ``https://host[:port]`` (``http://`` with ``tls=false``).
    url: str
    auth_token: Optional[str] = field(default=None, repr=False)


def is_turso_uri(uri: str) -> bool:
    """Return whether ``uri`` uses the ``turso`` scheme."""
    return urlsplit(uri).scheme.lower() == SCHEME


def parse_turso_uri(uri: str) -> LocalTurso | RemoteTurso:
    """Split a ``turso://`` URI into the local file or remote database it names."""
    parsed = urlsplit(uri)
    if parsed.scheme.lower() != SCHEME:
        raise ValidationError(f"Not a Turso URI, its scheme is {parsed.scheme!r}.")

    query = parse_qs(parsed.query, keep_blank_values=True)

    if not parsed.netloc:
        if not parsed.path.strip("/"):
            raise ValidationError(
                "A local Turso URI names a database file, as 'turso:///app.db' "
                "(relative) or 'turso:////var/data/app.db' (absolute)."
            )
        remote_only = sorted(REMOTE_PARAMETERS.intersection(query))
        if remote_only:
            raise ValidationError(
                f"A local Turso database file takes no {', '.join(remote_only)}: "
                "they apply to a remote database, 'turso://host'."
            )
        # Everything after the scheme carries over unchanged, so the path keeps
        # sqlite's slash convention and dialect options such as
        # `experimental_features` still reach pyturso.
        return LocalTurso(url=LOCAL_DIALECT + uri[len(parsed.scheme) :])

    return _parse_remote(parsed.netloc, parsed.path, query)


def _parse_remote(netloc: str, path: str, query: dict[str, list[str]]) -> RemoteTurso:
    if "@" in netloc:
        raise ValidationError(
            "A remote Turso URI takes no user or password; pass the token as "
            "'?auth_token=...'."
        )
    if path.strip("/"):
        raise ValidationError(
            "A remote Turso URI names a database by its host alone, as "
            "'turso://my-db-my-org.turso.io'; it takes no path."
        )

    unknown = sorted(set(query) - REMOTE_PARAMETERS)
    if unknown:
        raise ValidationError(
            f"Unknown remote Turso URI parameter(s): {', '.join(unknown)}. "
            f"Supported: {', '.join(sorted(REMOTE_PARAMETERS))}."
        )
    repeated = sorted(name for name, values in query.items() if len(values) > 1)
    given_tokens = [name for name in TOKEN_PARAMETERS if name in query]
    if repeated or len(given_tokens) > 1:
        raise ValidationError(
            "Give the Turso auth token once, as either 'auth_token' or 'authToken', "
            "and 'tls' at most once."
        )

    token = query[given_tokens[0]][0] if given_tokens else ""
    if any(character in token for character in HEADER_INVALID):
        # Fixed text: the token itself must not reach the message.
        raise ValidationError(
            "The Turso auth token contains a line break or NUL character, which "
            "an HTTP header cannot carry."
        )

    tls_values = query.get("tls", ["true"])
    tls = tls_values[0].lower()
    if tls not in ("true", "false"):
        raise ValidationError(
            "The Turso URI parameter 'tls' takes 'true' or 'false', "
            f"not {tls_values[0]!r}."
        )

    protocol = "https" if tls == "true" else "http"
    return RemoteTurso(
        url=f"{protocol}://{netloc}",
        auth_token=token or None,
    )


class TursoSyncError(Exception):
    """A request to a remote Turso database failed.

    Raised from outside the failing call's `except` block, so neither its cause nor
    its context carries the original exception, and with the auth token redacted
    from the text: client errors may quote the request they were making.
    """


def call_redacted(target: RemoteTurso, operation: str, call, advice: str = ""):
    """Run ``call`` and re-raise any failure as a `TursoSyncError` that names
    ``operation`` and the host, never the token, followed by ``advice``."""
    failure: Optional[BaseException] = None
    try:
        return call()
    except Exception as exc:  # noqa: BLE001 - every client failure is reported alike
        failure = exc
    detail = f"{type(failure).__name__}: {failure}"
    if target.auth_token:
        detail = detail.replace(target.auth_token, "***")
    raise TursoSyncError(
        f"{operation} the Turso database at {target.url} failed: {detail}"
        + (f" {advice}" if advice else "")
    )


def sync_connect(target: RemoteTurso, path: str):
    """Open a sync replica connection at ``path``, bootstrapping it if empty."""
    import turso.sync

    return call_redacted(
        target,
        "Connecting to",
        lambda: turso.sync.connect(
            path, remote_url=target.url, auth_token=target.auth_token
        ),
    )


@contextmanager
def stage_turso_replica(
    target: RemoteTurso, *, staging_root: Optional[str] = None
) -> Generator[str, None, None]:
    """Yield a ``sqlite+turso`` URI for a run-scoped replica of a remote database.

    The replica is bootstrapped from the server, its sync connection closed, and
    the file read in place as a local Turso database. The directory holding it,
    with every sibling the sync engine writes (``-wal``, ``-info``, ``-changes``),
    is removed on exit.
    """
    from omniload.source.sql_database.remote import _database_uri

    if staging_root is not None:
        # The staging root may be a pipelines directory dlt has not created yet.
        Path(staging_root).mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="turso-replica-", dir=staging_root))
    try:
        replica = directory / "replica.db"
        connection = sync_connect(target, str(replica))
        connection.close()
        yield _database_uri(LOCAL_DIALECT, replica)
    finally:
        try:
            shutil.rmtree(directory)
        except OSError as exc:
            logger.warning(
                "Could not remove the Turso replica at %s: %s", directory, exc
            )
