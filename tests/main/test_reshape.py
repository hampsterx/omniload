"""Docker-free unit tests for the reshape codec engine dispatch (issue #112 harness).

The end-to-end behaviour of each lane is covered by the integration tests in
``tests/warehouse/db/test_sample_airbnb.py``; these check only ``create_reshape_mapper``'s
spec parsing, the per-row vs batch classification, and the error contract the CLI relies on
(spec errors are ``ValueError``, which ``omniload/api.py`` converts to ``ValidationError``).
"""

import pytest

from omniload.codec.reshape import ReshapeMapper, create_reshape_mapper


def test_callable_passes_through_as_per_row():
    def reshape(doc):
        return doc

    mapper = create_reshape_mapper(reshape)
    assert isinstance(mapper, ReshapeMapper)
    assert mapper.batch is False
    assert mapper.fn is reshape


def test_python_engine_is_per_row():
    mapper = create_reshape_mapper("python:tests.warehouse.db.sample_airbnb:reshape")
    assert mapper.batch is False
    assert callable(mapper.fn)


def test_polars_engine_is_batch():
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")
    mapper = create_reshape_mapper("polars:select:_id,name")
    assert mapper.batch is True
    assert callable(mapper.fn)


def test_jq_engine_is_per_row():
    pytest.importorskip("tikray")
    mapper = create_reshape_mapper("jq:{_id: ._id}")
    assert mapper.batch is False
    assert callable(mapper.fn)


def test_missing_engine_separator_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("just-a-string-no-colon")


def test_unknown_engine_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("nope:whatever")


def test_empty_polars_spec_raises_valueerror():
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")
    # The CLI relies on this being a ValueError (api.py wraps it as ValidationError).
    with pytest.raises(ValueError):
        create_reshape_mapper("polars:")


def test_python_spec_without_callable_part_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("python:no_callable_after_module")


def test_missing_attribute_raises_valueerror_not_attributeerror():
    """A named callable that is not there is a spec error, not a traceback.

    `api.py` converts only ValueError and ImportError into ValidationError, so an
    AttributeError escaping here reaches the CLI as a stack trace.
    """
    with pytest.raises(ValueError):
        create_reshape_mapper("python:builtins:not_present")


def test_extra_colon_in_python_spec_raises_valueerror():
    """`partition` puts the tail in the attribute name, which must not raise."""
    with pytest.raises(ValueError):
        create_reshape_mapper("python:builtins:print:extra")


def test_polars_recipe_registration_is_idempotent_across_threads():
    """Two threads building their first polars mapper must not race the registry.

    The guard is check-then-act against macropipe's process-global registry, so
    without a lock one thread sees the name absent, the other registers it first,
    and the loser raises `ValueError` from deep inside mapper construction.
    """
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")

    import threading

    spec = "polars:select:a"
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def build():
        barrier.wait()
        try:
            create_reshape_mapper(spec)
        except BaseException as exc:  # noqa: BLE001 - the assertion is "none escaped"
            errors.append(exc)

    threads = [threading.Thread(target=build) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []


def test_as_yield_map_rowifies_an_arrow_batch():
    """A per-row mapper must survive an item that is an Arrow table, not a dict.

    dlt's `MapItem` enumerates a Python list and passes anything else through
    whole, so a source on the default `pyarrow` SQL backend (or `mmap`) hands the
    mapper a `pyarrow.Table`. Without rowification the mapper sees one Table where
    it expects one document.
    """
    pa = pytest.importorskip("pyarrow")

    seen = []

    def per_row(doc):
        seen.append(type(doc).__name__)
        return {"id": doc["id"], "doubled": doc["id"] * 2}

    mapper = create_reshape_mapper(per_row)
    table = pa.table({"id": [1, 2, 3]})

    out = list(mapper.as_yield_map()(table))

    assert seen == ["dict", "dict", "dict"]
    assert out == [
        {"id": 1, "doubled": 2},
        {"id": 2, "doubled": 4},
        {"id": 3, "doubled": 6},
    ]


def test_as_yield_map_passes_a_plain_document_through():
    """The dict path stays one document in, one out."""
    mapper = create_reshape_mapper(lambda doc: {"id": doc["id"]})

    assert list(mapper.as_yield_map()({"id": 7, "drop": "me"})) == [{"id": 7}]
