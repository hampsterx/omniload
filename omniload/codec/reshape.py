"""Reshape transforms (issue #112 "fast transformations" harness).

A reshape restructures a source document into a flat-plus-child-tables target: flatten
nested objects, coerce types, drop/rename fields, and leave arrays as real lists so dlt's
normalizer turns them into child tables. There are two execution models:

  - **per-row** (``python``, ``jq``): a ``(doc: dict) -> dict`` mapper applied via dlt
    ``add_map``, one document at a time.
  - **batch** (``polars``): a columnar mapper over an Apache Arrow batch, applied via dlt
    ``add_yield_map`` (it yields N dicts per Arrow table). This needs the source to deliver
    Arrow (``data_item_format="arrow"``); only the MongoDB source supports that today, so a
    batch reshape over any other source is rejected upstream in ``omniload/api.py``.

``create_reshape_mapper`` returns a :class:`ReshapeMapper` carrying both the callable and
which model it uses, so the caller knows whether to wire it as ``add_map`` (per-row) or
``add_yield_map`` over an Arrow source (batch).

For Mongo sources the caller skips ``TypeHintMap`` while a reshape is active (it would
otherwise json-hint top-level arrays into JSON columns, which prevents child tables from
forming). See ``omniload/api.py`` and ``omniload/core/resource.py``.

Spec format mirrors ``--mask``: ``<engine>:<spec>`` (split on the first colon only, so a
jq program or a macropipe recipe may itself contain colons).
  - ``python:<module>:<callable>``  resolve a ``(doc: dict) -> dict`` mapper
  - ``jq:<program>``                a jq program, run per document via Tikray
  - ``polars:<recipes>``            macropipe recipes (one per line), run over Arrow batches

A ``(doc: dict) -> dict`` callable may also be passed directly via the Python API.

jq fidelity note (issue #112 finding): jq operates on JSON, so it cannot serialize BSON
``Decimal128``/``ObjectId`` (the engine coerces them to JSON-native values first) and it
collapses decimal money to a plain number (``120.00`` -> ``120``). The pure-Python lane
preserves ``Decimal``; the jq lane does not. The Polars lane behaves like jq here (money is
cast to ``Float64``, losing ``Decimal`` typing). Cross-lane comparison therefore normalizes
numerics rather than expecting identical column types.
"""

from dataclasses import dataclass
from typing import Callable


@dataclass
class ReshapeMapper:
    """A reshape callable plus how dlt should apply it.

    ``batch`` mappers take a pyarrow Table and yield many dicts (wired via
    ``add_yield_map`` over an Arrow-mode source); per-row mappers take one dict and
    return one dict (wired via ``add_map``).
    """

    fn: Callable
    batch: bool = False


def create_reshape_mapper(
    reshape: "str | Callable[[dict], dict]",
) -> ReshapeMapper:
    """Return a :class:`ReshapeMapper` for ``reshape`` (a spec string or a callable)."""
    if not isinstance(reshape, str):
        # already a (doc: dict) -> dict callable (Python API path)
        return ReshapeMapper(fn=reshape, batch=False)

    if ":" not in reshape:
        raise ValueError(
            "reshape must be a callable or a '<engine>:<spec>' string "
            "(e.g. 'python:my.module:reshape')"
        )

    engine, spec = reshape.split(":", 1)

    if engine == "python":
        return ReshapeMapper(fn=_resolve_python_callable(spec), batch=False)

    if engine == "jq":
        return ReshapeMapper(fn=_create_jq_mapper(spec), batch=False)

    if engine == "polars":
        return ReshapeMapper(fn=_create_polars_mapper(spec), batch=True)

    raise ValueError(f"unknown reshape engine '{engine}' (expected python|jq|polars)")


def _resolve_python_callable(spec: str) -> "Callable[[dict], dict]":
    """Resolve a ``module.path:callable`` spec to a callable."""
    import importlib

    if ":" not in spec:
        raise ValueError(
            f"python reshape spec must be 'module.path:callable', got '{spec}'"
        )
    module_path, _, attr = spec.partition(":")
    module = importlib.import_module(module_path)
    fn = getattr(module, attr)
    if not callable(fn):
        raise ValueError(f"python reshape target '{spec}' is not callable")
    return fn


def _create_jq_mapper(program: str) -> "Callable[[dict], dict]":
    """Return a per-row mapper that runs the jq ``program`` over each document via Tikray."""
    try:
        from tikray import MokshaTransformation
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "the jq reshape engine needs Tikray; install the optional extra: "
            "pip install 'omniload[reshape]'"
        ) from exc

    transformation = MokshaTransformation().jq(program)

    def mapper(doc: dict) -> dict:
        # jq serializes its input as JSON, so BSON/Decimal types have to be coerced first.
        return transformation.apply(_to_jsonish(doc))

    return mapper


def _create_polars_mapper(spec: str) -> "Callable":
    """Return a batch mapper that reshapes an Arrow table via macropipe (compiled Polars).

    ``spec`` is a macropipe recipe list, one recipe per line (blank lines and ``#`` comments
    ignored). Each line is a ``<function>:<arg>:<arg>`` macro (see macropipe's expression
    language); ``omniload`` registers a couple of custom recipes (see
    ``_register_polars_recipes``) for the nested reshapes macropipe's builtins don't cover.

    The returned callable takes one pyarrow Table (an extraction batch) and yields the
    reshaped rows as dicts, so dlt's normalizer builds the child tables. It is wired via
    ``add_yield_map`` (one Arrow table in, N dicts out), not ``add_map``.

    Per-batch schema assumption: pymongoarrow infers the Arrow schema from each batch, so a
    field that is null in *some* rows is fine (the recipes are null-safe), but a field absent
    from *every* row in a batch produces no column, and a recipe referencing it then raises a
    Polars ``ColumnNotFoundError``. For a collection with sparse fields, pass a stable
    ``pymongoarrow_schema`` so absent fields still materialize as null columns. The harness
    fixture keeps all docs in one batch, so its sometimes-absent money/address fields stay
    present-but-null.
    """
    try:
        import polars as pl
        from macropipe.core import MacroPipe
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "the polars reshape engine needs macropipe; install the optional extra: "
            "pip install 'omniload[reshape]'"
        ) from exc

    _register_polars_recipes()

    recipes = [
        line.strip()
        for line in spec.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not recipes:
        raise ValueError(
            "polars reshape spec is empty; provide one macropipe recipe per line "
            "(e.g. 'select:_id,name')"
        )
    pipeline = MacroPipe.from_recipes(*recipes)

    def mapper(table):
        # table: one pyarrow Table/RecordBatch per extraction batch. from_arrow on a
        # tabular item always yields a DataFrame (the Series arm is for arrays); assert it
        # so a misrouted non-batch item fails loudly instead of mid-pipeline.
        frame = pl.from_arrow(table)
        assert isinstance(frame, pl.DataFrame), (
            f"polars reshape expected an Arrow table batch, got {type(frame).__name__}"
        )
        result = pipeline.apply(frame.lazy()).collect()
        yield from result.to_dicts()

    return mapper


def _register_polars_recipes() -> None:
    """Register omniload's custom macropipe recipes (idempotent).

    macropipe ``0.0.0`` ships only scalar-column builtins (cast/select/rename/...), and its
    ``cast`` is strict. The convoluted Mongo reshape needs two things its builtins lack, so
    we register them as ``@recipe`` functions (issue #112 finding: a real job has to extend
    macropipe with custom recipes for nested reshapes):

      - ``geojson_point_flatten:<struct_col>:<lng_alias>:<lat_alias>`` flattens a GeoJSON
        ``<struct_col>.location.coordinates`` ``[lng, lat]`` list into two typed columns,
        null-safe when the address/coordinates are absent.
      - ``cast_number:<col,col,...>`` casts columns to ``Float64`` non-strict, so the Mongo
        source's stringified money (absent values arrive as the literal ``"None"``) becomes a
        number or null instead of raising.
    """
    import polars as pl
    from macropipe.registry import Registry

    def geojson_point_flatten(lazy_frame, struct_col, lng_alias, lat_alias):
        coordinates = (
            pl.col(struct_col).struct.field("location").struct.field("coordinates")
        )
        return lazy_frame.with_columns(
            [
                coordinates.list.get(0, null_on_oob=True).alias(lng_alias),
                coordinates.list.get(1, null_on_oob=True).alias(lat_alias),
            ]
        )

    def cast_number(lazy_frame, column_names):
        columns = [name.strip() for name in column_names.split(",")]
        return lazy_frame.with_columns(
            [pl.col(name).cast(pl.Float64, strict=False) for name in columns]
        )

    # Register each recipe independently and only if absent: macropipe's Registry is a
    # process-global class var and Registry.register raises on a duplicate name, so a
    # blanket "register all" would break a re-run (and a one-name guard could leave a
    # partial set). Idempotent per name.
    for fn in (geojson_point_flatten, cast_number):
        if fn.__name__ not in Registry.r:
            Registry.register(fn)


def _to_jsonish(value):
    """Recursively coerce BSON / Decimal values into JSON-native ones jq can serialize.

    ``Decimal128``/``Decimal`` -> ``float`` (jq has no decimal type, so money loses its
    ``Decimal`` typing here; see the module docstring), ``ObjectId`` -> ``str``,
    ``datetime``/``date`` -> ISO string. Containers recurse; JSON-native scalars pass
    through. An unsupported type raises ``TypeError`` rather than being silently
    ``str()``-ed: a harness lane should fail loudly instead of corrupting a value (e.g.
    ``bytes``, ``bson.Binary``, regex/code BSON). Extend the coercion here if a real source
    needs another type.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: _to_jsonish(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonish(v) for v in value]

    from datetime import date, datetime
    from decimal import Decimal

    from bson import Decimal128, ObjectId

    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(
        f"jq reshape cannot serialize {type(value).__name__}; "
        "extend _to_jsonish in omniload/codec/reshape.py to coerce it"
    )
