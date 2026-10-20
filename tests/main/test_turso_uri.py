"""The ``turso://`` URI parser shared by the Turso source and destination."""

import pytest

from omniload import ValidationError
from omniload.util.turso import LocalTurso, RemoteTurso, parse_turso_uri

SENTINEL = "sentinel-token-value"


@pytest.mark.parametrize(
    ("uri", "local_url"),
    [
        ("turso:///app.db", "sqlite+turso:///app.db"),
        ("turso:////var/data/app.db", "sqlite+turso:////var/data/app.db"),
        (
            "turso:///app.db?experimental_features=index_method",
            "sqlite+turso:///app.db?experimental_features=index_method",
        ),
    ],
)
def test_local_form_keeps_the_sqlite_path_convention(uri, local_url):
    assert parse_turso_uri(uri) == LocalTurso(url=local_url)


def test_local_form_needs_a_path():
    with pytest.raises(ValidationError, match="names a database file"):
        parse_turso_uri("turso:///")


def test_local_form_refuses_remote_parameters():
    with pytest.raises(ValidationError, match="auth_token, tls"):
        parse_turso_uri(f"turso:///app.db?auth_token={SENTINEL}&tls=false")


@pytest.mark.parametrize("parameter", ["auth_token", "authToken"])
def test_remote_form_takes_the_token_under_either_name(parameter):
    target = parse_turso_uri(f"turso://my-db-my-org.turso.io?{parameter}={SENTINEL}")
    assert target == RemoteTurso(
        url="https://my-db-my-org.turso.io", auth_token=SENTINEL
    )
    assert SENTINEL not in repr(target)


def test_remote_form_without_tls_uses_http_and_keeps_the_port():
    assert parse_turso_uri("turso://localhost:8080?tls=false") == RemoteTurso(
        url="http://localhost:8080"
    )


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("turso://host/db", "takes no path"),
        (f"turso://user:{SENTINEL}@host", "takes no user or password"),
        ("turso://host?token=x", "Unknown remote Turso URI parameter"),
        ("turso://host?tls=maybe", "takes 'true' or 'false'"),
        (f"turso://host?auth_token={SENTINEL}&authToken={SENTINEL}", "once"),
        (f"turso://host?auth_token={SENTINEL}&auth_token={SENTINEL}", "once"),
    ],
)
def test_remote_form_refuses_what_it_cannot_honour(uri, message):
    with pytest.raises(ValidationError, match=message) as exc_info:
        parse_turso_uri(uri)
    assert SENTINEL not in str(exc_info.value)


@pytest.mark.parametrize("parameter", ["auth_token", "authToken"])
@pytest.mark.parametrize("character", ["%0D", "%0A", "%00"])
def test_a_token_no_http_header_can_carry_is_refused_without_quoting_it(
    parameter, character
):
    uri = f"turso://host?{parameter}={SENTINEL}{character}x"
    with pytest.raises(ValidationError, match="line break or NUL") as exc_info:
        parse_turso_uri(uri)
    assert SENTINEL not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
