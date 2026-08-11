# ADR-002: Own filesystem run options at one boundary

**Status**: Proposed
**Date**: 2026-08-11

## Context

Every source's `dlt_source` receives the same fixed set of run-level keyword
arguments, assembled by `omniload.api` from CLI flags and `run_ingest` arguments.
Two of them describe the reader the filesystem package builds
(`filesystem_incremental`, `column_types`); the rest describe other stages of the
pipeline and mean nothing to a filesystem.

A filesystem connector therefore holds keyword arguments from two different
owners at once, and nothing in the code says which is which. Each connector
decided for itself, and connectors are written by copying a neighbour, so the
decision spread unevenly: some built an explicit constructor dictionary and read
the two reader options individually, while others merged the whole run into their
fsspec keyword arguments and passed the reader options nowhere.

The consequences are invisible from the outside, which is why they persisted.
An unknown keyword raises nothing on backends that accept `**kwargs`, but fsspec
keys its instance cache on the constructor arguments, so unrelated run parameters
mint distinct filesystem instances, and a backend that forwards unknown keywords
into a client library rejects them. A reader option that reaches the constructor
instead of the reader is accepted and silently ignored, while the run still
reports the feature as enabled.

Run options are not the only carrier. A source URI's query string becomes
constructor arguments too, so a query parameter can deliver the same names by a
different route.

The pipeline has a broader parameter-validation layer planned as a typed request
object across all sources. This decision is about the filesystem package's own
boundary, and is meant to be absorbed by that layer rather than to compete
with it.

## Decision

We name the ownership boundary once, in the filesystem package, and every
connector defers to it.

A single pinned set records the run-option keys omniload owns. One function
divides a run into the resource options this package consumes and the connector
keyword arguments that are none of its business; a second applies the same rule
to the URI query string. A connector passes the resource options to resource
construction and the remainder to its filesystem constructor.

The division is **subtractive**: it removes the pinned omniload keys and returns
everything else untouched. It is not a whitelist of recognized connector
arguments.

The pinned set is asserted against the keywords `omniload.api` actually passes,
read from that call site, so the two cannot drift apart silently.

Removing a name from the constructor arguments is not the same as consuming it.
`column_types` is readable from a URI query parameter on the connectors that
build their resource through the shared locator, and there the run value wins
with the URI value as its fallback. `filesystem_incremental` has no URI form and
is read from the run alone, so a query parameter naming it is dropped rather than
honoured.

Connectors that already built an explicit constructor dictionary and took nothing
from the run keep doing so; the boundary generalizes their shape rather than
replacing it. They read both resource options from the run only, so the URI
fallback for `column_types` does not reach them.

## Alternatives considered

- **Remove the known keys inside each connector**: rejected because it is a
  convention rather than a definition, repeated at every connector and enforced
  nowhere. This is the state that produced the uneven behaviour, and the next
  connector inherits whichever neighbour it was copied from.
- **Return only the recognized filesystem keyword arguments (a whitelist)**:
  rejected because a programmatic caller may legitimately pass connector keywords
  straight through, which the Google Cloud Storage source supports today by
  honouring a caller-supplied token. A whitelist drops them silently, trading a
  visible leak for an invisible loss.
- **Replace the source protocol's keyword arguments with a typed request object**:
  the correct long-term shape and the subject of its own planned work. Rejected
  *here* because it changes every source, not the filesystem family, and this
  boundary is behavioural and compatible with it. A typed request object should
  absorb this decision rather than sit beside it.
- **Give the resource options their own parameter on every `dlt_source`**:
  rejected because it changes the shared source protocol to fix one family's
  problem, and leaves the URI carrier unaddressed.
- **Document the ownership rule without enforcing it**: rejected because the
  original issue is a documentation question whose investigation found two
  measurable defects. A rule that is stated and not checked is the rule that was
  already in place.

## Consequences

- The reader options work on every scheme in the family and mean the same thing
  on each, so a transport's incremental behaviour no longer depends on which
  construction path its connector happens to use.
- Adding a run parameter to `omniload.api` without classifying it fails a test
  rather than reaching filesystem constructors.
- A new connector cannot leak run options by copying its neighbour, because the
  neighbour calls the boundary instead of restating it.
- fsspec instances stop being keyed on unrelated run parameters, so instance
  caching works as intended and a backend that validates its keyword arguments
  cannot be broken by an omniload-side parameter.
- A URI query parameter sharing a name with a run option no longer reaches the
  filesystem constructor. The run parameter already took precedence over it
  there, so no connection setting changes meaning. Whether it is read at all
  afterwards depends on the name: `column_types` is still consumed as the reader
  fallback on the locator path, and every other owned name is dropped.
- The URI fallback for `column_types` is uneven across the family, because only
  the locator path reads it. Making it uniform means first deciding whether a URI
  should carry it at all, which is a question for the parameter layer rather than
  for this boundary.
- The set is a maintained list, not a derived one. It is pinned to its call site
  by test, which converts drift into a failure but does not remove the list.
- Connectors that build their filesystem explicitly do not call the boundary.
  They forward nothing, so they cannot leak, but they also do not learn about a
  new resource option automatically.
