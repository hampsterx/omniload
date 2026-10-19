import datetime
import importlib
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from dlt.common.pendulum import pendulum

from omniload.util.time import ensure_datetime_utc


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            datetime.datetime(2026, 1, 2, 3, 4, 5),
            datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            datetime.date(2026, 1, 2),
            datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
        ),
        (
            datetime.datetime(
                2026,
                1,
                2,
                3,
                4,
                5,
                tzinfo=datetime.timezone(datetime.timedelta(hours=13)),
            ),
            datetime.datetime(2026, 1, 1, 14, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            "2026-01-02T03:04:05-05:00",
            datetime.datetime(2026, 1, 2, 8, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (0, datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)),
        (0.5, datetime.datetime(1970, 1, 1, 0, 0, 0, 500000, datetime.timezone.utc)),
    ],
)
def test_ensure_datetime_utc(value, expected):
    result = ensure_datetime_utc(value)

    assert isinstance(result, pendulum.DateTime)
    assert result == expected
    assert result.tzinfo is pendulum.UTC


def test_ensure_datetime_utc_rejects_invalid_value():
    with pytest.raises(ValueError):
        ensure_datetime_utc("03:04:05")


def test_ensure_datetime_utc_ignores_dlt_context_timezone():
    dlt_time = importlib.import_module("dlt.common.time")
    get_context_timezone = getattr(dlt_time, "get_context_timezone", None)
    set_context_timezone = getattr(dlt_time, "set_context_timezone", None)
    if get_context_timezone is None or set_context_timezone is None:
        pytest.skip("dlt release has no context timezone API")

    previous_timezone = get_context_timezone()
    try:
        set_context_timezone(ZoneInfo("Pacific/Auckland"))
        context_result = dlt_time.ensure_pendulum_datetime(
            datetime.datetime(2026, 1, 2, 3, 4, 5)
        )
        assert context_result.utcoffset() == datetime.timedelta(hours=13)

        result = ensure_datetime_utc(datetime.datetime(2026, 1, 2, 3, 4, 5))
        assert result.utcoffset() == datetime.timedelta(0)
    finally:
        set_context_timezone(previous_timezone)


def test_project_source_does_not_use_removed_dlt_datetime_helper():
    source_root = Path(__file__).parents[2] / "src" / "omniload"
    compatibility_module = source_root / "util" / "time.py"
    matches = [
        path.relative_to(source_root)
        for path in source_root.rglob("*.py")
        if path != compatibility_module
        if "ensure_pendulum_datetime_utc" in path.read_text()
    ]

    assert matches == []


def test_mongodb_source_imports():
    importlib.import_module("omniload.source.mongodb.api")
