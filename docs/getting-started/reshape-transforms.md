# Reshape transforms

A reshape restructures each source document on its way to the destination:
flatten nested objects into columns, coerce types, drop or rename fields, and
leave arrays as real lists rather than opaque JSON. On a source that allows a
level of nesting, those lists then normalize into child tables.

Reshapes exist for sources whose documents do not map onto a table without
help. A deeply nested MongoDB collection is the motivating case: GeoJSON
locations, arrays of sub-documents, `Decimal128` money, and fields present in
some documents and absent in others.

## Usage

```bash
omniload ingest \
  --source-uri "mongodb://localhost:27017" \
  --source-table "sample_airbnb.listingsAndReviews" \
  --dest-uri "duckdb:///listings.db" \
  --dest-table "public.listings" \
  --reshape "python:my_project.transforms:reshape_listing"
```

### Format

```
--reshape <engine>:<spec>
```

The value splits on the first colon only, so a jq program or a macropipe recipe
may itself contain colons.

| Engine | Spec | Execution model |
|--------|------|-----------------|
| `python` | `<module>:<callable>` resolving to a `(doc: dict) -> dict` mapper | per row |
| `jq` | a jq program, run per document through [Tikray] | per row |
| `polars` | [macropipe] recipes, one per line, compiled to Polars expressions | per Arrow batch |

The `jq` and `polars` engines need the optional extra:

```bash
pip install 'omniload[reshape]'
```

The `polars` engine consumes Apache Arrow batches, so it requires a source that
can deliver Arrow. Only the MongoDB source does today; a batch reshape over any
other source is rejected before the run starts.

The per-row engines work on every source. Where the source is already columnar,
which includes SQL on the default `pyarrow` backend, rows are materialized before
the mapper sees them. That is the cost of running per-document logic over a
columnar batch, and it is why `polars` is the engine that stays columnar
end to end.

A bad spec, a missing extra, or a batch engine over a source that cannot feed it
is reported by `--dry-run`, so a reshape can be checked without moving data.

Passing a `(doc: dict) -> dict` callable directly is also supported through the
[Python API](python-api.md).

## Arrays and child tables

Whether an array your reshape leaves in place becomes a child table or a JSON
column is decided by the source, not by the reshape. Two things have to line up:
the source has to allow a level of nesting (`max_table_nesting >= 1`), and
nothing may have hinted the array as JSON before the normalizer sees it.

**MongoDB** satisfies both once a reshape is active. It allows one level of
nesting, and omniload skips the type-hinting pass that would otherwise store
top-level arrays as JSON columns, because a reshape owns the schema instead. For
a document with `reviews` and `amenities` arrays loaded into `--dest-table
public.listings`, that yields three tables:

- `listings`, one row per document, with the flattened scalar columns
- `listings__reviews`, one row per review, keyed back to the parent
- `listings__amenities`, one row per amenity, with the scalar in a `value`
  column

**Filesystem sources** do not. Their readers register `max_table_nesting = 0`,
so a list a reshape produces there is stored as a JSON column whatever the
reshape does with it. Flattening and coercion still work as described; only the
array-to-child-table step is unavailable.

Other sources sit in between. Couchbase, Airtable and Notion allow one level and
Jira allows two, so a list survives as a child table there, but none of them
carries MongoDB's type-hinting pass, so nothing had to be skipped for it to.

## Engines and type fidelity

The three engines reach the same table shape but not the same column types.

**`python`** preserves Python types, including `Decimal`. Use it when exact
decimal money matters:

```python
def reshape_listing(doc: dict) -> dict:
    coordinates = ((doc.get("address") or {}).get("location") or {}).get(
        "coordinates"
    ) or []
    return {
        "_id": str(doc["_id"]),
        "name": doc.get("name"),
        "price": Decimal(doc["price"]) if doc.get("price") else None,
        "address_location_lng": coordinates[0] if len(coordinates) > 0 else None,
        "address_location_lat": coordinates[1] if len(coordinates) > 1 else None,
        "amenities": list(doc.get("amenities") or []),
    }
```

**`jq`** runs one program per document. jq operates on JSON, so BSON values are
coerced to JSON-native ones first and decimal money collapses to a plain number
(`120.00` becomes `120`):

```bash
--reshape 'jq:{
  _id: (._id | tostring),
  name: .name,
  price: (.price | if . == null then null else tonumber end),
  address_location_lng: (.address.location.coordinates[0]? // null),
  address_location_lat: (.address.location.coordinates[1]? // null),
  amenities: (.amenities // [])
}'
```

**`polars`** compiles a macropipe recipe list, one recipe per line, and runs it
over Arrow batches. Money is cast to `Float64`, so it loses `Decimal` typing the
same way jq does:

```bash
--reshape 'polars:
geojson_point_flatten:address:address_location_lng:address_location_lat
cast_number:price,weekly_price
select:_id,name,price,weekly_price,address_location_lng,address_location_lat,amenities
'
```

`geojson_point_flatten` and `cast_number` are recipes omniload registers on top
of macropipe's builtins, which cover scalar columns only and cast strictly.
`geojson_point_flatten` drills a GeoJSON `[lng, lat]` coordinate pair into two
typed columns; `cast_number` casts non-strictly, so an absent value becomes null
instead of raising.

:::{note}
The `polars` engine infers its Arrow schema per batch. A field that is null in
some rows is fine, but a field absent from *every* row in a batch produces no
column at all, and a recipe naming it raises `ColumnNotFoundError`. A stable
Arrow schema would avoid this, but no omniload entry point exposes one yet, so
for a collection with sparse fields prefer a per-row engine.
:::

## Pushing the transform into the source

For MongoDB, an aggregation pipeline is an alternative to a client-side reshape.
The source accepts a `collection:<pipeline-json>` table specifier, and the
server does the value work:

```bash
--source-table 'listingsAndReviews:[{"$addFields": {"lng": {"$arrayElemAt": ["$address.location.coordinates", 0]}}}]'
```

This reaches every value but not the table shape. The decision to split an array
into a child table happens client-side during normalization, so an aggregation
alone lands a flattened root table with its arrays still stored as JSON columns.

[Tikray]: https://tikray.readthedocs.io/
[macropipe]: https://macropipe.readthedocs.io/
