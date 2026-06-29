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
