"""Destination capabilities are checked through a real, resolved dlt pipeline."""

import sqlite3
from unittest.mock import MagicMock

import duckdb
import pytest

from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory


@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "source.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO people VALUES (1, 'Alice')")
    return path


def _ingest(source_db, dest_uri, **kwargs):
    kwargs.setdefault("dest_table", "people")
    return run_ingest(
        source_uri=f"sqlite:///{source_db}",
        source_table="main.people",
        dest_uri=dest_uri,
        incremental_strategy="replace",
        progress="log",
        **kwargs,
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_mongodb_refuses_jsonl_before_constructing_the_source(
    source_db, monkeypatch, dry_run
):
    source = SourceDestinationFactory(
        f"sqlite:///{source_db}", "mongodb://localhost:1/db"
    ).get_source()
    construct = MagicMock(side_effect=AssertionError("source must not be constructed"))
    monkeypatch.setattr(source, "dlt_source", construct)
    monkeypatch.setattr(SourceDestinationFactory, "get_source", lambda self: source)

    with pytest.raises(
        ValidationError, match="Loader file format jsonl is not supported"
    ):
        _ingest(
            source_db,
            "mongodb://localhost:1/db",
            loader_file_format="jsonl",
            dry_run=dry_run,
        )

    construct.assert_not_called()


@pytest.mark.parametrize("loader_file_format", [None, "parquet"])
def test_mongodb_loads_rows_with_a_resolved_destination(
    tmp_path, monkeypatch, loader_file_format
):
    """Keep the factory, pipeline, loader file and MongoDB sink; fake only the server."""
    source = tmp_path / "people.csv"
    source.write_text("id,name\n1,Alice\n")
    client = MagicMock()
    client.__enter__.return_value = client
    monkeypatch.setattr("pymongo.MongoClient", lambda uri: client)

    run_ingest(
        source_uri=f"csv://{source}",
        source_table="people",
        dest_uri="mongodb://localhost:1/db",
        dest_table="people",
        incremental_strategy="replace",
        loader_file_format=loader_file_format,
        progress="log",
    )

    collection = client["db"]["people"]
    collection.delete_many.assert_called_once_with({})
    documents = [
        document
        for call in collection.insert_many.call_args_list
        for document in call.args[0]
    ]
    assert [(doc["id"], doc["name"]) for doc in documents] == [(1, "Alice")]


def test_an_instantiated_destination_accepts_its_loader_format(source_db, tmp_path):
    dest = tmp_path / "dest.duckdb"
    _ingest(
        source_db,
        f"duckdb:///{dest}",
        dest_table="out.people",
        loader_file_format="parquet",
    )
    with duckdb.connect(str(dest)) as conn:
        assert conn.sql("SELECT id, name FROM out.people").fetchall() == [(1, "Alice")]


@pytest.mark.parametrize("dry_run", [False, True])
def test_an_instantiated_destination_refuses_an_unsupported_loader_format(
    source_db, tmp_path, dry_run
):
    with pytest.raises(
        ValidationError, match="Loader file format insert_values is not supported"
    ):
        _ingest(
            source_db,
            f"sqlite:///{tmp_path / 'dest.db'}",
            loader_file_format="insert_values",
            dry_run=dry_run,
        )
