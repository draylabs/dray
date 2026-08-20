"""A Python record layer for Amazon Aurora DSQL."""

from importlib.metadata import PackageNotFoundError, version as _installed

# Read out of the installed package rather than written here, so the number
# lives in exactly one place — `pyproject.toml` — and a release cannot ship a
# wheel that disagrees with the constant inside it. The fallback is for a tree
# that has been checked out but never installed, where there is no metadata to
# read and no version to be right about.
try:
    __version__ = _installed("dray")
except PackageNotFoundError:  # pragma: no cover - only outside an install
    __version__ = "0+unknown"

# For the module itself rather than a name out of it. Every other submodule is
# bound here as a side effect of the names imported from it, and `schema` is the
# one nothing imports a name from — so without this line the README sends a
# reader to something `dir(dray)` does not admit exists.
from dray import schema
from dray.handlers import clock, describe, records_change
from dray.child import child
from dray.collection import Collection, cached_for, collection, jsonb
from dray.hooks import after_commit, before_delete, before_save, check
from dray.model import (
    Change,
    DrayError,
    Names,
    Record,
    Sql,
    ValidationError,
    Write,
    any_of,
    as_uuid,
    asc,
    desc,
    field,
    index,
    key_of,
    names_of,
    none_of,
    record,
)
from dray.store import (
    AfterCommitFailed,
    CommitRefused,
    ConcurrencyExhausted,
    ConnectionLost,
    DuplicateRecord,
    RecordHasChanged,
    Pool,
    RecordNotFound,
    Store,
    replaying,
    retrying,
)
from dray.watching import Span

# What dray hands out, which is narrower than what this module binds. `retrying`
# is the one deliberate gap: it is dray's own decorator for the three write paths
# that own their transaction end to end, and on a caller's function it silently
# replays nothing — `store.transaction()` has already turned the refusal it
# catches into a `CommitRefused`. It stays reachable because the manual sends a
# reader here to see that `dir(dray)` offers both it and `replaying`, and to be
# told which of the two is theirs.
__all__ = [
    "AfterCommitFailed",
    "Change",
    "Collection",
    "CommitRefused",
    "ConcurrencyExhausted",
    "ConnectionLost",
    "DrayError",
    "DuplicateRecord",
    "Names",
    "Pool",
    "Record",
    "RecordHasChanged",
    "RecordNotFound",
    "Span",
    "Sql",
    "Store",
    "ValidationError",
    "Write",
    "after_commit",
    "any_of",
    "as_uuid",
    "asc",
    "before_delete",
    "before_save",
    "cached_for",
    "check",
    "child",
    "clock",
    "collection",
    "desc",
    "describe",
    "field",
    "index",
    "jsonb",
    "key_of",
    "names_of",
    "none_of",
    "record",
    "records_change",
    "replaying",
    "schema",
]
