"""
Collections: everything you can ask or do about one kind of record.

This is the only place SQL lives. A record above it never sees a row, and a
caller above it never sees a cursor.
"""

import contextlib
import inspect
import json
import types
from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import partial, wraps
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from dray.caching import CacheInfo
from dray.hooks import (
    AFTER_COMMIT,
    BEFORE_DELETE,
    BEFORE_SAVE,
    CHECK,
    declares,
    run,
)
from dray.model import (
    _Class,
    BLOB,
    CACHE_MOST,
    AnyOf,
    DrayError,
    NoneOf,
    Sql,
    ValidationError,
    Write,
    _NOT_YOURS,
    _checked_most,
    _checked_ttl,
    _ordering,
    as_text,
    convert,
    fits,
    key_of,
    names_of,
    new_etag,
    normalised,
)
from dray.store import (
    DuplicateRecord,
    RecordHasChanged,
    RecordNotFound,
    batching,
    cursor,
    retrying,
)


def _as_param(value: Any) -> Any:
    """One value on its way into a statement against a column.

    A `list` or a `dict` has a `jsonb` column, and left alone psycopg sends a
    list as a Postgres array and cannot adapt a dict at all — so those two go
    over wrapped, exactly as the write wraps them. Everything else psycopg
    already knows."""
    return jsonb(value) if isinstance(value, (list, dict)) else value


def jsonb(value: Any) -> Jsonb:
    """A value on its way to the jsonb column, or to a filter against one.

    `as_text` handles what JSON cannot — a `date`, a `datetime`, a `Decimal` —
    and `restore` on the way back is what makes them round-trip rather than
    arriving as strings.
    """
    return Jsonb(value, dumps=lambda obj: json.dumps(obj, default=as_text))


# DSQL takes 3,000 rows and 10 MiB in a transaction. A bulk write costs a row
# per record, so what this caps is rows rather than records — short of the
# ceiling by enough that a wide row cannot carry the other limit past its own.
MAX_ROWS = 2_000

# Record class to the collection class written for it, filled in by
# `@collection`. A record with no entry gets the plain one.
COLLECTIONS: dict[type, type] = {}


class Collection:
    """
    The default collection. Every record has one whether or not anybody wrote
    a class for it, so `store.people.by_id(...)` works the moment `Person` is
    declared.
    """

    def __init__(self, store: Any, cls: type) -> None:
        self.store = store
        self.cls = cls
        # The seven names a statement is built out of are one object, held
        # here and delegated to, so `store.people.table` and
        # `dray.names_of(Person).table` are the same string by construction
        # rather than by two implementations going on agreeing.
        self._names = names_of(cls)

    #
    # What a statement is written against
    #

    @property
    def conn(self) -> psycopg.Connection:
        return self.store.conn

    def __deepcopy__(self, memo: dict) -> "Collection":
        """A collection is not a value, so a deep copy of something holding one
        stops here.

        Which is what makes an answer copyable at all. A record carries its way
        back to the store, so copying one would otherwise walk through here to
        a connection — and a connection is not a thing anybody can have two of.

        What comes out of the copy is therefore a record still pointing at the
        store the answer was computed on, which is the wrong store for every
        caller after the first. `_rebound` is what puts that right, and this is
        deliberately not the place to do it: a copy has no idea who asked for
        it.
        """
        return self

    @property
    def _cache(self) -> Any:
        """Where this collection's rows are kept, or nothing — a record that
        asked for no cache, a read inside a transaction, a block that asked to
        go without one. Off the store rather than held here, since all three
        of those can change between one read and the next."""
        return self.store._cache_for(self.cls)

    @property
    def _watch(self) -> Any:
        """Whatever is watching this store, or the thing that is not. Off the
        store rather than held here, because a collection is built per store and
        `store.watching()` can turn one on after this one exists."""
        return self.store._watch

    # The seven properties below, and `sql_for` after them, are `names_of`
    # under the names a collection has always published them under. `by_id`
    # keeps its spelling — it is dray's word for asking by the key, and the
    # question is the same whatever the column is called — so `self.id` here is
    # the column and not the question.

    @property
    def table(self) -> str:
        return self._names.table

    @property
    def blob(self) -> str:
        """The jsonb column, for a statement of your own that reaches inside
        it. Named here so nobody has to remember what it is called."""
        return self._names.blob

    @property
    def id(self) -> str:
        """The key column, for a statement of your own."""
        return self._names.id

    @property
    def etag(self) -> str:
        """The stale-write guard's column."""
        return self._names.etag

    @property
    def parent_type(self) -> str:
        """The column naming a child's parent's table. A collection whose
        record is not a child has no such column, and asking raises."""
        return self._names.parent_type

    @property
    def parent_id(self) -> str:
        """The column holding a child's parent's key. A collection whose
        record is not a child has no such column, and asking raises."""
        return self._names.parent_id

    @property
    def columns(self) -> str:
        """Every column, for a select. Built from the class rather than typed
        out, because a hand-copied list is a field that silently stops being
        read."""
        return self._names.columns

    def sql_for(self, name: str) -> str:
        """
        One declared field, as the SQL that reads it.

            self.sql_for("family_name")   # 'family_name'
            self.sql_for("suburb")        # "(data->>'suburb')::text"

        For a statement whose fields are named by data rather than typed out —
        a report grouping and summing by whatever this month asks about. A
        field with a column of its own is that column's name, so the statement
        is what you would have written by hand. A field in the blob is the
        expression that brings it back as the type the class declares, which
        is not always a cast: a `timedelta` is `make_interval` and a `bytes`
        is `decode`, because both are encoded on the way in.

        The same string either side of `schema.promote`, so a statement built
        with this survives the move that leaves a hand-written
        `data->>'suburb'` answering null.

        A `Decimal` comes back rounded to the size its column would have —
        `numeric(18,6)` unless the field said otherwise — where the document
        itself holds every digit. That is the cost of matching the column, and
        matching the column is what makes the promotion change nothing.

        A name the class never declared is refused.
        """
        return self._names.sql_for(name)

    #
    # Reading
    #

    def select_many(
        self, statement: str, params: Sequence[Any] = ()
    ) -> list[Any]:
        """
        Records from a statement you wrote.

        The escape hatch, and the normal way to ask anything `find` cannot:
        a range, a join. Rows come back as dicts and are hydrated by the class,
        which drops any column it no longer declares. An answer with no record
        to become — an aggregate, a count per group — wants `select_rows`
        instead.

        The statement has to select the whole record — `select {self.columns}`,
        which is what every example writes and what `_check_select` below
        insists on.

        A read of a cached record fills the cache with what it brought back,
        which is most of what a cache is worth and costs no invalidation that a
        write does not already do. It is not itself answered from one: a write
        to any record can change what a statement matches and dray cannot know
        which statements it touched.

        What it fills with is what the statement saw, which for a slow read is
        not what the table says by the time the rows arrive. A write that
        commits while they are in flight drops its keys and would then find
        them put back as they were before it, so a read notes what the cache
        has dropped before it asks and fills nothing where that has moved.
        """
        cache = self._cache
        # Before the statement rather than after it, which is the whole of the
        # ordering: an eviction is only visible as one to a read that knew the
        # number on the other side of its own fetch.
        dropped = 0 if cache is None else cache.evictions()
        with cursor(
            self.conn, rows=dict_row, watch=self._watch, cls=self.cls
        ) as cur:
            cur.execute(statement, list(params))
            _check_select(self.cls, cur.description)
            rows = cur.fetchall()
            self._seed(cache, rows, dropped)
            # Hydrating is dray's own time rather than the database's, and it is
            # the one number in the tree that is. A read that came back in forty
            # milliseconds and spent four seconds becoming records is a
            # completely different problem from a slow query, and without this
            # the two look identical from above.
            with self._watch.span("hydrate", cls=self.cls) as span:
                span.rowcount = len(rows)
                return [
                    self._attached(self.cls._dray_load(row)) for row in rows
                ]

    def _seed(self, cache: Any, rows: Sequence[dict], dropped: int) -> None:
        """Put what a read brought back where `by_id` will find it, unless the
        cache has dropped a key since `dropped` was read off it — in which case
        one of these rows may be the row that eviction was about, and none of
        them go in. The cache handed in rather than asked for again, since the
        number and the map it counts have to be the same one."""
        if cache is not None and rows:
            cache.seed([(row[self.id], row) for row in rows], since=dropped)

    def select_first(
        self, statement: str, params: Sequence[Any] = ()
    ) -> Any | None:
        """
        The first record from a statement you wrote, or None.

        `first` rather than `one`, because a statement matching four hundred
        rows answers this happily and hands back whichever the database
        offered — so the name says what you get rather than promising a
        uniqueness nothing here checks. Say `order by` in the statement if you
        care which one it is.
        """
        found = self.select_many(statement, params)
        return found[0] if found else None

    def select_rows(
        self, statement: str, params: Sequence[Any] = ()
    ) -> list[dict]:
        """
        Rows from a statement you wrote, for an answer that is not records.

        How many people in each status, the takings by section, a count per
        parent — there is no record for those to become, and `select_many`
        cannot hand them back because it hydrates one class per statement and
        refuses one that does not select the whole of it. Without this the only
        way to ask is `store.conn`, which takes the table name, the blob's name
        and the column order out of dray's knowledge and copies them by hand
        into a place that will not notice when the class changes.

        So this is the same escape hatch as `select_many` with the hydrating
        removed, and it exists for what stays rather than what goes:
        `{self.table}`, `{self.columns}` and `{self.blob}` are still in reach,
        and what comes back is keyed rather than positional, so a statement that
        grows a column does not silently move `row[0]`.

        The values are the driver's rather than the class's, which is the honest
        cost of nothing hydrating: a blob key read with `->>` is text even where
        the field declares a `date`. And two unaliased aggregates come back
        under one key, so `as` earns its keep here in a way it does not in a
        select of columns.

        A statement naming one column is the ordinary case here rather than the
        refused one, since there is no record to come back half-built. It is a
        read all the same: an `update` run through this reaches the database
        exactly as it would through `select_many`, minting no etag and calling
        no handler, and everything that follows from that is the caller's.
        """
        # No `_check_select`. That guard stops a record being built from half a
        # row and then saving its defaults over the rest, which cannot happen
        # where nothing is hydrated.
        with cursor(
            self.conn, rows=dict_row, watch=self._watch, cls=self.cls
        ) as cur:
            cur.execute(statement, list(params))
            return cur.fetchall()

    def by_id(self, record_id: Any) -> Any:
        """
        One record, by its id.

        An id is whatever the class made its key: a `UUID` where it declared
        none, and a `str`, an `int` or a `date` where it declared one. Text is
        taken for the `UUID`, because dray puts a converter on the key it adds
        itself — `by_id(request.args["id"])` is converted and then checked, the
        way an assignment is, so a caller arriving from a URL never has to know
        what that one is made of. A class that declares its own key decides
        that for itself: text reaches an `int` or a `date` key where the field
        said `converter=` and is refused where it did not. A value that is not
        an id at all is a `ValidationError` here rather than a question put to
        the database.

        Raises rather than returning None, because every caller of this either
        has an id it believes in or is about to write `if x is None: raise`
        itself — and the exception carries the class and the id, which is
        everything the caller knows at the only point it still knows it.

        That is the rule the reads here divide on: asking by identity raises,
        and searching returns None. `find_first(equals={"id": ...})` is the same
        question asked the other way for a caller who genuinely does not know.

        This is the only read a `cached_for=` record answers out of memory, and
        the reason is that it is the only one dray can invalidate exactly. What
        comes back is built fresh from the cached row every time, so two callers
        holding one record never hold one object.
        """
        wanted = _checked_id(self.cls, record_id)
        cache = self._cache
        if cache is None:
            _, found = self._row_for(wanted)
            if found is None:
                raise RecordNotFound(f"no {self.cls.__name__} {record_id!r}")
            return found

        # What the read built, if this call is the one that did the reading.
        # `_row_for` hands the record back as well as the row because the row is
        # what the cache keeps and the record is what the caller wanted, and
        # building it twice to keep the two apart would be work for nothing.
        read: list[Any] = []

        def reading() -> dict | None:
            row, found = self._row_for(wanted)
            read.append(found)
            return row

        watch = self._watch
        # Where the cache reports what answering out of memory cost — mostly
        # the wait behind another thread's round trip, which is the half of a
        # hit that only the map can see and the half that matters, since the
        # shape this exists for is several threads warming one key at once.
        # Nothing where nobody is watching, which is what keeps an unwatched
        # read from so much as reading a clock.
        took: list[int] | None = [] if watch else None
        row = cache.row(wanted, reading, took=took)
        if read:
            if read[0] is None:
                raise RecordNotFound(f"no {self.cls.__name__} {record_id!r}")
            return read[0]
        if row is None:
            raise RecordNotFound(f"no {self.cls.__name__} {record_id!r}")
        # The read that did not happen, drawn rather than left as a gap. A hit
        # emits no statement, so without this a page the cache answered and a
        # page that never asked are the same picture — and the question anybody
        # takes to a trace is whether the cache is earning its place. The
        # `hydrate` hangs under it because building the record is the rest of
        # what the hit cost, on top of the wait the span is backdated by.
        with watch.span(
            "cache", cls=self.cls, ago=took[0] if took else 0
        ) as hit:
            hit.rowcount = 1
            with watch.span("hydrate", cls=self.cls) as span:
                span.rowcount = 1
                return self._attached(self.cls._dray_load(row))

    def _row_for(self, wanted: Any) -> tuple[dict | None, Any]:
        """One row by key and the record built from it, or nothing twice over.

        The read behind `by_id`, and the one the cache is handed to fill itself
        with — hence the row as well as the record, since the row is what is
        kept and the record is what the caller asked for. It seeds nothing on
        its own account, which would be storing the same row twice on the way
        past.

        The record is built inside the cursor, where every other read builds
        its own: a `hydrate` belongs under the `statement` that fetched the row,
        so that keeping only the statements leaves each one's elapsed an honest
        total. A row that came out of memory has no statement to hang under and
        is hydrated by `by_id` itself.
        """
        with cursor(
            self.conn, rows=dict_row, watch=self._watch, cls=self.cls
        ) as cur:
            cur.execute(
                f"select {self.columns} from {self.table} "
                f"where {self.id} = %s",
                [wanted],
            )
            _check_select(self.cls, cur.description)
            rows = cur.fetchall()
            if not rows:
                return None, None
            with self._watch.span("hydrate", cls=self.cls) as span:
                span.rowcount = 1
                return rows[0], self._attached(self.cls._dray_load(rows[0]))

    def find(
        self,
        *,
        parent: Any = None,
        parent_type: type | None = None,
        equals: dict[str, Any] | None = None,
        order_by: Any = None,
        limit: int | None = None,
    ) -> list[Any]:
        """
        Every record matching these values exactly.

            store.people.find(equals={"status": "volunteer"}, limit=20)

        Equality and nothing else on the filtering, deliberately. A range or a
        join is `select_many` with SQL you wrote, and an aggregate is
        `select_rows`, which is clearer than a query language that can almost
        say what you mean.

        `parent` and `parent_type` are how a child's own collection is asked
        across parents, which is the commonest question it exists for:

            store.notes.find(parent=person)
            store.notes.find(parent_type=Person, equals={"kind": "call"})

        A record and a class rather than the two strings underneath, so the
        table name comes off the declaration and a renamed table follows it.
        Either one, not both — a record already says what kind it is. All four
        reads take them, so the same question narrowed the same way reads the
        same whether you want the records, the first of them, how many there
        are or a walk through them.

        `order_by` and `limit` are the two things this says past the matching,
        and they are here rather than left to SQL because of what a name is. A
        page that lets somebody choose the sort takes a column name from a query
        string, which is the one identifier an application genuinely has to
        accept from outside — and the alternative to accepting it here, where it
        is checked against the declaration, is an f-string in a collection
        method, which is precisely the case AWS's DSQL guidance names. `desc()`
        and a tuple of names both work, spelled as a `@child` spells them.

        Leaving it out takes the class's own order — `@record(order_by=...)`,
        or the key where the class declared none — so a read is never in
        whatever order the database felt like. The key goes on the end of
        whatever is named either way, so the read is total and the same rows
        arrive in the same order every time. Rows tied on everything named fall
        through to a key that is random unless the class said otherwise, which
        settles nothing anybody chose — the whole of what this promises is in
        *The order they come back in* on the page.

        Your field names are inside `equals` and dray's options are beside it,
        so neither can be read for the other: a record declaring `order_by`,
        `limit` or `parent` filters on it like any other field, and a misspelt
        option is a `TypeError` rather than a filter nobody asked for.
        """
        where, params = self._conditions_for(parent, parent_type, equals)
        statement = f"select {self.columns} from {self.table}"
        if where:
            statement += " where " + " and ".join(where)
        if order_by is None:
            statement += f" order by {self.cls.__dray_order__}"
        else:
            statement += f" order by {_ordering(self.cls, order_by)}, {self.id}"
        if limit is not None:
            if not isinstance(limit, int) or isinstance(limit, bool):
                raise TypeError(f"limit is a number of records, not {limit!r}")
            if limit < 1:
                raise ValueError(
                    f"a limit is at least one record, not {limit}. Asking for "
                    "none of them is not asking."
                )
            statement += f" limit {int(limit)}"
        return self.select_many(statement, params)

    def find_first(
        self,
        *,
        parent: Any = None,
        parent_type: type | None = None,
        equals: dict[str, Any] | None = None,
        order_by: Any = None,
    ) -> Any | None:
        """
        The first record matching these values exactly, or None.

        What `find` gives you when you want one of them — the newest note, the
        ticket somebody is asking about, whether anything matches at all. It
        goes to the database for one row rather than building every match and
        taking the head of the list, which is the difference that matters on a
        table where the answer is one of forty thousand.

        `order_by` decides which one, and leaving it out takes the class's own
        order as `find` does. So the row is the same row every time — but on a
        class that declared no order it is the first by key, which is a row
        nobody chose. Say what "first" means wherever it matters.

        None rather than an exception, because this is a search: nothing
        matching is an ordinary answer to a question about what exists. `by_id`
        raises for the opposite reason, and the two together are the whole rule.

        `parent` and `parent_type` narrow it as they narrow `find`, so the
        newest note about one person is asked for in one call and without SQL.

        There is no `limit` option here, because one record is what this is. A
        record that declares a field of that name filters on it like any other,
        inside `equals`.

        On a `cached_for=` record this is where a natural key is answered from
        memory: a filter covering every column of a unique index identifies one
        row, so it is remembered as a key pointing at an id and the row itself
        comes back through `by_id`. One copy under one key rather than two that
        can disagree. Nothing else here is cached, because nothing else here
        names one row.
        """
        natural = self._natural(parent, parent_type, equals, order_by)
        if natural is None:
            return self._first(parent, parent_type, equals, order_by)

        # Where the answer had to be read, it is the answer: going back through
        # `by_id` would build the same record a second time for nothing.
        cache, key = natural
        read: list[Any] = []

        def ask() -> Any:
            found = self._first(parent, parent_type, equals, order_by)
            read.append(found)
            return key_of(found) if found is not None else None

        watch = self._watch
        # What answering the key cost, as `by_id` asks for it above.
        took: list[int] | None = [] if watch else None
        record_id = cache.identity(key, ask, took=took)
        if read:
            return read[0]
        if record_id is None:
            return None
        # A key remembered a moment ago can still name the wrong row: the record
        # it named may have been written to since, or removed, and only the row
        # itself can say. Checked here rather than dropped on every write,
        # because the check is free and the alternative is a second index from
        # ids back to the keys naming them.
        #
        # The span is the key's own hit, and what hangs under it says how the
        # row arrived: a second `cache` where that was in memory too, a
        # `statement` where it had to be fetched. Which is the difference
        # between a question answered entirely from memory and one that was
        # only half of the way there.
        with watch.span("cache", cls=self.cls, ago=took[0] if took else 0):
            try:
                found = self.by_id(record_id)
            except RecordNotFound:
                found = None
            if found is not None and all(
                getattr(found, name) == value for name, value in key
            ):
                return found
        cache.forget_identity(key)
        return self._first(parent, parent_type, equals, order_by)

    def _first(
        self,
        parent: Any,
        parent_type: type | None,
        equals: dict[str, Any] | None,
        order_by: Any,
    ) -> Any | None:
        """The statement behind `find_first`, with nothing remembered."""
        found = self.find(
            parent=parent,
            parent_type=parent_type,
            equals=equals,
            order_by=order_by,
            limit=1,
        )
        return found[0] if found else None

    def _natural(
        self,
        parent: Any,
        parent_type: type | None,
        equals: dict[str, Any] | None,
        order_by: Any,
    ) -> tuple[Any, tuple] | None:
        """
        Whether this `find_first` asks about one row by a key that identifies
        it, and that key as something a cache can hold.

        Every column of some unique index has to be in the filter. A leading run
        of one is not enough — a unique index over `(depot, code)` says nothing
        about how many rows share a depot — and neither is a value that stands
        for several, since `any_of` asks about a set rather than a row. `None`
        is out for the same reason: a unique index takes as many nulls as it
        likes.

        More conditions than the index covers is fine and is not the same
        question, so they go into the key: at most one row can hold the indexed
        values, and the rest decide whether that row is the answer. A `parent`,
        a `parent_type` or an `order_by` gives up instead — the first two are
        conditions worth having in the key and are not worth the second spelling
        here, and an order that a caller named has to reach `find` to be checked.
        """
        cache = self._cache
        if cache is None or not equals or order_by is not None:
            return None
        if parent is not None or parent_type is not None:
            return None
        # Through the same converters the statement's own values go through, so
        # a key held in memory and a key sent to the database are one value.
        # This also refuses a name the class does not declare, which is what
        # keeps a misspelt filter raising on the call that is answered from
        # memory as loudly as on the one that is not.
        wanted = normalised(self.cls, equals)
        # Anywhere in the filter, not only over the indexed columns. A value
        # standing for several is not one this could check the answer against
        # afterwards, and a key that never verifies would be remembered and
        # then dropped on every call.
        if any(
            isinstance(value, (AnyOf, NoneOf)) for value in wanted.values()
        ):
            return None
        settled = {
            name for name, value in wanted.items() if value is not None
        }
        if not any(
            settled.issuperset(str(column) for column in one.columns)
            for one in self.cls.__dray_indexes__
            if one.unique
        ):
            return None
        key = tuple(sorted(wanted.items(), key=lambda pair: pair[0]))
        try:
            hash(key)
        except TypeError:
            # A list or a dict in the filter, which a jsonb field takes. It is
            # a value like any other to the statement and not one a map can be
            # keyed by, so this question goes to the database.
            return None
        return cache, key

    def in_batches(
        self,
        of: int = 500,
        *,
        parent: Any = None,
        parent_type: type | None = None,
        equals: dict[str, Any] | None = None,
    ) -> Iterator[list[Any]]:
        """
        The same records `find` would give you, a batch at a time.

            for batch in store.people.in_batches(
                of=500, equals={"status": "volunteer"}
            ):
                ...

        `find` and `select_many` build everything the statement matches, so a
        set too large to hold is a set you cannot read at all. The gap was never a limit
        — `find` takes one, and so does whatever SQL you write — it is the walk,
        and writing a keyset loop at every call site is how records come to be
        visited twice or skipped.

        It rides `key > %s` ordered by the key. Total and stable, and on DSQL
        the primary key *is* the table, so this needs no index of its own and no
        second lookup to fetch the row it found. Which also means the order is
        the key's and not yours: a walk is for visiting everything, and anything
        that cares what order it arrives in wants `find` with an `order_by`, or
        SQL of your own where the set is too large to hold at once.

        It pairs with the write chunking rather than repeating it. A
        read-modify-write over a large set has to batch both ends — 3,000 rows
        to a transaction going out, and whatever fits in memory coming in — so
        the batch this yields is the set to hand to `save_all`.

        Changing rows as you walk them is the case this exists for and it is
        safe in one direction only. The walk never goes back, so a record you
        have edited out of the filter is not seen twice; a record edited *into*
        it, behind where the walk has reached, is not seen at all.

        `of` is a batch size and sits beside the filter rather than in it, so a
        record declaring a field of that name filters on it like any other.
        Nothing else about `find` changes: every filter it takes, this takes,
        `any_of` and `parent`/`parent_type` included — which is what a walk over
        one kind of parent's children needs, since a set too large to hold is
        exactly the set somebody reaches for a script for.
        """
        if of < 1:
            raise ValueError(f"a batch is at least one record, not {of}")

        where, params = self._conditions_for(parent, parent_type, equals)
        after = None
        while True:
            clauses = (
                [*where, f"{self.id} > %s"] if after is not None else list(where)
            )
            statement = f"select {self.columns} from {self.table}"
            if clauses:
                statement += " where " + " and ".join(clauses)
            statement += f" order by {self.id} limit {int(of)}"

            walked = params if after is None else [*params, after]
            batch = self.select_many(statement, walked)
            if not batch:
                return
            yield batch
            # Short of a full batch means the table ran out, so the next round
            # trip could only ever come back empty.
            if len(batch) < of:
                return
            after = key_of(batch[-1])

    def count(
        self,
        *,
        parent: Any = None,
        parent_type: type | None = None,
        equals: dict[str, Any] | None = None,
    ) -> int:
        """How many match, asked of the database rather than measured with
        len() over everything. The filter and the scope are spelled as `find`
        spells them — `parent` and `parent_type` included — so the two answer
        the same question about the same records."""
        where, params = self._conditions_for(parent, parent_type, equals)
        statement = f"select count(*) from {self.table}"
        if where:
            statement += " where " + " and ".join(where)
        with cursor(self.conn, watch=self._watch, cls=self.cls) as cur:
            cur.execute(statement, params)
            return cur.fetchone()[0]

    def counts_for(
        self,
        parents: Sequence[Any],
        *,
        equals: dict[str, Any] | None = None,
    ) -> dict[Any, int]:
        """
        How many children each of these parents has, keyed by parent id.

            store.notes.counts_for(people)
            # {UUID('92e446d2…'): 4, UUID('d644131f…'): 2, UUID('7c0a1b3e…'): 0}

            store.notes.counts_for(people, equals={"whom": "rod"})

        One statement for a whole page of parents, where `person.notes.count()`
        is one per row. That is what a list showing a number about children it
        is not displaying asks for — the summary column, the badge, the
        collapsed section — once for every row on the page.

        **Every parent asked about is in the answer, zero included.** A `group
        by` has nothing to group for a parent with no children, so the dict
        built from one by hand is missing exactly the rows a template will index
        into. The keys here are the parents you passed, in the order you passed
        them, whatever the table holds. Passing none asks nothing and answers
        `{}`.

        Records rather than ids, so the parent's table name comes off the class
        the way `parent=` takes it — a caller writing `parent_type = 'person'`
        into a statement has copied out dray's bookkeeping, and it goes quietly
        wrong the day the parent is renamed.

        `equals` narrows what is counted, spelled as it is everywhere else. And
        queued children count, the way `ChildSet.count` counts them, so a number
        is the same number whichever door it was asked through — a list rendered
        inside a `store.transaction()` must not disagree with the objects on the
        same screen. The cost of that is a pass over what the records in hand
        are holding unsaved, on top of the one statement.
        """
        if not self.cls.__dray_parents__:
            raise TypeError(
                f"{self.cls.__name__} is not a child, so it has no parents to "
                "count for. Only a @child carries the two columns naming one."
            )

        # The keys are the parents that were passed rather than the rows that
        # come back, which is the whole of what a `group by` cannot do — so the
        # answer is laid out here first and starts from what each record is
        # holding unsaved. `tables` is the same set of keys with the parent each
        # belongs to, kept for matching the rows back against what was asked.
        counts: dict[Any, int] = {}
        tables: dict[Any, str] = {}
        seen: set[int] = set()
        for parent in parents:
            if not hasattr(type(parent), "__dray_table__"):
                raise TypeError(
                    "counts_for takes the records the children hang off, not "
                    f"{parent!r}. Records rather than ids, so the parent's "
                    "table comes off the class."
                )
            key, table = key_of(parent), type(parent).__dray_table__
            if tables.setdefault(key, table) != table:
                raise ValueError(
                    f"a {tables[key]} and a {table} were both asked about "
                    f"under the key {key!r}, and one dict cannot answer for "
                    "both. Ask about one kind of parent at a time."
                )
            counts.setdefault(key, 0)
            # By identity, so the same record handed over twice is one parent
            # rather than a queue counted twice.
            if id(parent) in seen:
                continue
            seen.add(id(parent))
            for items in (getattr(parent, "_dray_sets", None) or {}).values():
                if items.cls is self.cls:
                    counts[key] += len(items._queued(equals))

        if not counts:
            return counts

        # The two columns as two `= any(...)` clauses rather than a pair at a
        # time, because that is what an ordinary parameter can say and what the
        # `(parent_type, parent_id)` index every child table carries is for. It
        # over-asks where one list holds two kinds of parent — one person's id
        # against the events' table name — so what comes back is matched
        # against what was actually asked about rather than trusted.
        kind, whose = self.parent_type, self.parent_id
        kinds = tuple(dict.fromkeys(tables.values()))   # each table once
        where, params = self._conditions(
            {
                kind: AnyOf(kinds),
                whose: AnyOf(tuple(tables)),
                **(equals or {}),
            }
        )
        with cursor(self.conn, watch=self._watch, cls=self.cls) as cur:
            cur.execute(
                f"select {kind}, {whose}, count(*) from {self.table}"
                f" where {' and '.join(where)}"
                f" group by {kind}, {whose}",
                params,
            )
            for held_by, key, many in cur.fetchall():
                if tables.get(key) == held_by:
                    counts[key] += many
        return counts

    def _conditions_for(
        self,
        parent: Any,
        parent_type: type | None,
        equals: dict[str, Any] | None,
    ) -> tuple[list[str], list[Any]]:
        """The scope and the filter as one set of conditions.

        Every read that takes both goes through here, so there is one answer to
        what they mean together rather than one per method. A filter naming a
        parent column beside a scope that sets it wins, which is what
        `ChildSet._scope` has always done through a parent — the caller has
        written the column out longhand, and a scope silently overwriting it
        would be the more surprising of the two.
        """
        return self._conditions(
            {**self._scoped_to(parent, parent_type), **(equals or {})}
        )

    def _scoped_to(self, parent: Any, parent_type: type | None) -> dict[str, Any]:
        """
        One parent, or one kind of parent, as conditions on a child's own
        collection. Nothing named means every parent, which is what a read of a
        child's table through the store is for in the first place. `find`,
        `find_first`, `count` and `in_batches` all narrow through this, so the
        scope means one thing and refuses the same things on all four.

        Filtering on `parent_type` by hand was always possible, since a child's
        parent columns are ordinary fields. What the options add is that neither
        name is typed: the table comes off the class, so a record renamed to
        `individual` moves every read here with it, where
        `equals={"parent_type": "person"}` would go on finding nothing and
        saying nothing about it.
        """
        if parent is None and parent_type is None:
            return {}
        if parent is not None and parent_type is not None:
            raise TypeError(
                "a read takes parent or parent_type, not both. A record "
                "already says which kind it is."
            )
        if not self.cls.__dray_parents__:
            raise TypeError(
                f"{self.cls.__name__} is not a child, so it has no parent to "
                "be read by. Only a @child carries the two columns naming one."
            )

        if parent_type is not None:
            # A class, not an instance and not a table name. `Person` and a
            # `Person` both answer to `__dray_table__`, so the instance is
            # refused here rather than quietly meaning "any person's".
            if not isinstance(parent_type, type) or not hasattr(
                parent_type, "__dray_table__"
            ):
                raise TypeError(
                    f"parent_type is the parent's record class, not "
                    f"{parent_type!r}. Pass Person rather than 'person' or a "
                    "person, so the table name follows the declaration."
                )
            return {self.cls.__dray_parent_type__: parent_type.__dray_table__}

        return _naming(self.cls, parent)

    def _conditions(self, equals: dict[str, Any]) -> tuple[list[str], list[Any]]:
        """
        Turn field names into conditions, whichever side of the storage split
        they live on. A blob field is read out of jsonb, which scans — fine
        while the table is small, and the signal to promote it when it is
        not.

        A blob field is compared as jsonb against a jsonb parameter, not as
        text. `->>` extracts as text, which makes a filter on `reply_count` of 4
        into `text = smallint` and no such operator exists — so a blob field
        could only ever be filtered on as a string, an integer would raise, and
        a list would quietly match nothing. `->` hands back the value
        with its type intact and the parameter goes over as jsonb, which is the
        same thing the blob was written with.

        `None` asks for `is null`, because `= null` is never true and returning
        nothing at all is a wrong answer rather than an empty one. "Which
        tickets have no owner yet" is an ordinary question. That one keeps
        `->>`, deliberately: `->` on a key holding JSON null hands back a jsonb
        null rather than a SQL one, so `is null` would miss it.

        A `list` or a `dict` goes over as jsonb on the column side too, for the
        same reason the write does it: their column is `jsonb`, and left alone
        psycopg sends a list as a Postgres array and cannot adapt a dict at all
        — so the two types the schema declares were writable and then not
        findable, which is the write-path defect over again on the way back.

        It settles the scalar case with it. A list against a `text` column was
        rendered `{a,b}` and compared as text, so
        `find(equals={"status": ["open", "waiting"]})` matched nothing and said
        nothing. Against jsonb there is no operator and it raises — which is
        what leaves `any_of` a clear field: several values are asked for by
        saying so, and a bare list keeps its one meaning of *equal to that
        list*.

        `any_of` becomes `= any(%s)`, which describes one field the same way a
        bare value does and so is still something `find` is allowed to write. On
        a column the values go over as a list and psycopg makes the array;
        inside the blob each one is jsonb, the same as it was written and the
        same as a single value would be. An empty one matches nothing and raises
        no SQL error, which needs no special case here.

        `none_of` is its opposite and carries the null test with it, because
        `<> all(%s)` on its own means *has a value, and it is not one of these*
        — three-valued logic drops the rows that never answered, which is not
        what anybody writing "none of these" means. The blob side spells its
        half with `->>` for the same reason the `None` branch does, and then
        compares with `->` as jsonb like every other value here.

        One corner, on the blob side only: a key that was never written and a
        key holding null both read as null, so both match. That agrees with
        `_dray_load`, which gives an absent key the field's default — except
        where that default is not `None`, and then a record can match
        `find(equals={"x": None})` and hydrate holding something else. It takes
        a blob field with a non-null default and rows older than the field to
        arrange.
        """
        where, params = [], []
        # Checked and converted by `normalised`, which the queued half of a
        # child set calls too — the two have to agree about what a filter means
        # and differ only in comparing SQL against comparing Python.
        for name, value in normalised(self.cls, equals).items():
            # The name is a declared field, so interpolating it is safe; the
            # value never is and always goes as a parameter.
            inside = name in self.cls.__dray_blob__
            if isinstance(value, AnyOf):
                if inside:
                    where.append(f"{self.blob}->'{name}' = any(%s)")
                    params.append([jsonb(each) for each in value])
                else:
                    where.append(f"{name} = any(%s)")
                    params.append([_as_param(each) for each in value])
            elif isinstance(value, NoneOf):
                if inside:
                    where.append(
                        f"({self.blob}->>'{name}' is null"
                        f" or {self.blob}->'{name}' <> all(%s))"
                    )
                    params.append([jsonb(each) for each in value])
                else:
                    where.append(f"({name} is null or {name} <> all(%s))")
                    params.append([_as_param(each) for each in value])
            elif value is None:
                column = f"{self.blob}->>'{name}'" if inside else name
                where.append(f"{column} is null")
            elif inside:
                where.append(f"{self.blob}->'{name}' = %s")
                params.append(jsonb(value))
            else:
                where.append(f"{name} = %s")
                params.append(_as_param(value))
        return where, params

    #
    # What is remembered between reads
    #

    def forget(self, record_id: Any) -> None:
        """
        Drop one record from the cache, so the next read of it goes to the
        database.

            store.people.forget(person_id)

        dray drops its own keys after a write of its own, so this is for the row
        that moved some other way — a statement of your own through
        `store.conn`, a job in another language, a trigger. A record that is not
        cached, or an id that was never read, is not an error: this says *do not
        answer that from memory*, which is already true.
        """
        cache = self._caching()
        if cache is not None:
            cache.forget(_checked_id(self.cls, record_id))

    def forget_all(self) -> None:
        """Drop everything this collection remembers, for every store sharing
        it — the rows, and the answers any `@cached_for` method of yours kept.
        `store.pool.forget_all()` is the same act for every collection at
        once."""
        cache = self._caching()
        if cache is not None:
            cache.clear()
        for kept in self.store._caches.asked_of(type(self)):
            kept.clear()

    def cache_info(self) -> Any:
        """
        What this collection has been asked, and what it is holding.

            store.people.cache_info()
            # CacheInfo(hits=41, misses=3, size=3)

        Everything it remembers added together: the rows a `cached_for=` record
        keeps, and the answers of every `@cached_for` method on it. One number
        for one question — *is anything here being served from memory* — since
        two sets of counters would be two things to add up at every call site
        that asks it.

        `None` where the collection remembers nothing at all: a record that
        named no `cached_for` and a class with no `@cached_for` method on it.
        That is a different answer from a cache nobody has used yet, and telling
        them apart is most of what this is for in a test.

        A read that waited on another thread's round trip counts as a hit,
        because that is what it cost. Rows a `find` left behind are neither:
        nobody asked for those by key.
        """
        cache = self._caching()
        asked = self.store._caches.asked_of(type(self))
        if cache is None and not asked and not _keeps_answers(type(self)):
            return None
        maps = asked if cache is None else [cache, *asked]
        counted = [one.counts() for one in maps]
        return CacheInfo(
            sum(one[0] for one in counted),
            sum(one[1] for one in counted),
            sum(one[2] for one in counted),
        )

    def _caching(self) -> Any:
        """This collection's cache whatever the store is in the middle of.

        `_cache` is for a read and answers nothing inside a transaction or a
        `uncached()` block; forgetting and counting are about the cache itself
        and are true in there as much as anywhere."""
        return self.store._caches.of(self.cls)

    def _forget_written(self, records: Sequence[Any]) -> None:
        """Drop what a write dirtied, once its rows are durable.

        This is the half a caller could not write for itself: dray knows which
        keys a write touched, so a save drops exactly its own rather than
        leaving a lifetime in which some other store reads back the row as it
        was. After the commit and once per write — `_when_committed` waits for
        the outermost block and runs nothing at all if it rolls back, and it
        sits outside the replay `@retrying` does, so a write DSQL refuses four
        times still evicts once.
        """
        dirtied = [
            (type(one), key_of(one))
            for one in records
            if getattr(type(one), "__dray_cached_for__", None)
        ]
        if dirtied:
            self.store._when_committed(
                partial(self.store._caches.forget, dirtied)
            )

    def _forget_removed(
        self, keys: Sequence[Any] = (), *, whole: bool = False
    ) -> None:
        """Drop what a removal took, once it is durable.

        The rows named go one at a time; everything below them goes wholesale,
        and so does this collection's own where `whole` says the rows were never
        named. A cascade is one statement per generation and loads not a row of
        them, and so is a `clear` of a set — there are no keys to drop, and the
        cost of that shape is that the only honest thing to say about the cache
        underneath it is that all of it may be wrong.
        """
        classes = [
            chain[-1]
            for chain in _descendants(self.cls)
            if getattr(chain[-1], "__dray_cached_for__", None)
        ]
        if whole and self.cls.__dray_cached_for__:
            classes.append(self.cls)
        mine = (
            [(self.cls, key) for key in keys]
            if self.cls.__dray_cached_for__ and not whole
            else []
        )
        if not classes and not mine:
            return

        def forgetting() -> None:
            self.store._caches.forget(mine)
            for cls in classes:
                self.store._caches.forget_class(cls)

        self.store._when_committed(forgetting)

    #
    # Writing
    #

    def add(
        self,
        record: Any,
        *,
        parent: Any = None,
        given: dict[str, Any] | None = None,
    ) -> Any:
        """
        Write a new record, and whatever it has queued.

        `parent` writes a child on its own, without writing its parent:

            store.notes.add(Note(body="Called back."), parent=person)

        The other door is `person.notes.add(...)` and then `person.save()`,
        which queues the note and writes the person's row alongside it. That is
        the one to reach for where the note explains a change to the person,
        and the wrong one where nothing about the person has changed — several
        people adding to one shared list turns *everybody adds an item* into
        *everybody writes the list*, and on DSQL that is a conflict rather than
        a queue.

        A record rather than a table name, resolved the way every read of a
        child resolves it, so `parent_type` stays off the page and a renamed
        record takes its children's writes with it. A record already naming a
        different parent is refused rather than moved, and so is `parent` on a
        collection whose record is not a child at all.

        A child is written under somebody or it is not written: neither
        `parent` nor the two columns set by hand is refused here, since a row
        naming nobody is reached by no read through a parent and taken by no
        parent's delete.

        `given` is what the write tells the `on_add` and `on_save` handlers,
        over the store's own defaults.
        """
        self._under([record], parent)
        self._write_all([record], creating=True, assigned=given or {})
        return record

    def add_all(
        self,
        records: Sequence[Any],
        *,
        parent: Any = None,
        given: dict[str, Any] | None = None,
    ) -> list[Any]:
        """
        Write many at once, in transactions that fit.

        Validated up front, all of them, before anything is written — a bad
        record at position 4,000 must not leave the first 2,000 committed.

        `parent` names one parent for the whole set, as it does on `add`: forty
        notes about one person are a row each and nothing at all for the person.
        """
        self._under(records, parent)
        self._write_all(records, creating=True, assigned=given or {})
        return list(records)

    def save(
        self,
        record: Any,
        *,
        etag: str | None = None,
        given: dict[str, Any] | None = None,
    ) -> Any:
        """
        Write the changes to a record that already exists.

            store.people.save(person, given={"whom": "rod"}, etag=shown)

        Pass the `etag` a reader was shown and the write is refused if anybody
        got there first. DSQL's own concurrency control spans a transaction —
        microseconds — and a form sits open for minutes, so this is what carries
        across the gap.

        A refusal raises `RecordHasChanged`. It carries `ids` and `records` when
        the statement was what refused, and neither when the comparison below
        did — that one has asked the database nothing, and the record and its id
        are already in your hand.
        """
        # Before anything else touches the record. A guard read after the write
        # path has minted the new token would compare a value against itself and
        # pass every time, silently.
        #
        # It carries nothing, deliberately. Filling `records` here would mean a
        # round trip on the one refusal where the caller is already holding the
        # record and its id, so whether to spend one is left where it can be
        # decided cheaply.
        if etag is not None and getattr(record, self.etag) != etag:
            raise RecordHasChanged(
                f"this {self.cls.__name__} was changed by someone else"
            )
        self._write_all(
            records=[record], creating=False, assigned=given or {},
            guarded=etag is not None,
        )
        return record

    def save_all(
        self,
        records: Sequence[Any],
        *,
        guarded: bool = False,
        given: dict[str, Any] | None = None,
    ) -> list[Any]:
        """
        Many at once. Unguarded by default, since a deliberate overwrite is a
        legitimate thing a batch job does.

        `guarded=True` needs nothing from the caller, which is what separates it
        from `save(etag=...)`. That one carries the token a *reader* was shown,
        which only the caller knows; this one guards each record with the etag it
        was loaded with, so it catches anybody committing between this process's
        read and its write. That is exactly the window a bulk edit sits in, and
        skipping it means a forty-record edit overwrites somebody quietly where a
        one-record edit would have stopped.

        It raises rather than reporting, and carries what a recovery needs:
        `RecordHasChanged` with `ids` for the records somebody else got to first,
        `records` for each of those as the table now has it, and `written` for
        the ones that landed before the write stopped. An id in `ids` with no
        record among `records` is one that has gone, which is how *somebody
        wrote this* is told from *somebody removed this* without a second round
        trip. A returned report can be ignored, and a report nobody reads is a
        silent overwrite by another name.

        What rolls back is a transaction, not the call. A set above `MAX_ROWS` is
        several of them, so the chunk holding the conflict is undone and the
        chunks before it are not — which is what `written` is for.

        `guarded` is an option and sits beside `given` rather than in it, so a
        record declaring a field of that name is assigned it like any other.
        `etag` is spelled out on `save` for the same reason.
        """
        self._write_all(
            records, creating=False, assigned=given or {}, guarded=guarded
        )
        return list(records)

    def delete(self, record: Any) -> None:
        """Remove a record and everything hanging off it, in one transaction.

        DSQL has no foreign keys and so no cascade of its own; this is the only
        thing standing between the caller and orphaned rows.

        A `@before_delete` on the record runs inside that transaction, so what
        it writes lands with the removal and what it refuses leaves the row
        where it was. The records below are removed without being loaded, and
        theirs do not run.

        Raises `RecordNotFound` if the record's row is not there — a delete
        asks by identity, as `by_id` and a save do, so removing the same record
        twice is a broken assumption rather than a second success. The handler
        has already run and whatever it wrote goes back with the raise."""
        self._delete_batch(record)
        self._forget_removed([key_of(record)])

    #
    # The write path
    #

    def _under(self, records: Sequence[Any], parent: Any) -> None:
        """
        The two columns naming a parent, onto the records about to be written.

        Here rather than inside the write, because the refusals are about the
        call and not about the values: a collection whose record is no child
        has no such columns to fill, a parent of a kind the child does not hang
        off is a row nothing will reach, and a child that already belongs to
        somebody is being asked to belong to two. Said at the door, they name
        the line that is wrong.

        Around `__setattr__`, as `_prepare` sets the same two columns on a
        queued child. dray filling its own bookkeeping is not a change somebody
        made, so an `on_change` has no business firing for it — and every
        record is checked before any of them is touched, which is the promise
        `add_all` already makes about a bad value at position 4,000.
        """
        if parent is None:
            # Nothing to fill, but something to ask: a child is written under
            # somebody or it is not written. The two columns set by hand count,
            # which is what an importer holding raw ids rather than records has
            # to use — it is only the row naming nobody that is refused.
            for record in records:
                if self._orphaned(record):
                    raise ValueError(_no_parent(self.cls))
            return
        if not self.cls.__dray_parents__:
            raise TypeError(
                f"{self.cls.__name__} is not a child, so it has no parent to "
                "be written under. Only a @child carries the two columns "
                "naming one."
            )
        # The other door cannot get this wrong — `person.notes` exists because
        # `Note` named `Person` in `of=`, and there is no such attribute on a
        # record it did not name. This one takes any record at all, so the
        # check the attribute was doing has to be made here or the two doors
        # do not agree.
        if type(parent) not in self.cls.__dray_parents__:
            declared = " or ".join(
                dict.fromkeys(
                    kind.__name__ for kind in self.cls.__dray_parents__
                )
            )
            raise TypeError(
                f"{self.cls.__name__} hangs off {declared}, not "
                f"{type(parent).__name__}. `of=` on the declaration is which "
                "records a child belongs to, and `parent_id` takes its type "
                "from their keys — so a parent from outside it writes a row "
                "that record's own delete will never take away."
            )
        named = _naming(self.cls, parent)
        for record in records:
            for name, value in named.items():
                held = getattr(record, name)
                if held is not None and held != value:
                    raise ValueError(_two_parents(self.cls, record, parent))
        for record in records:
            for name, value in named.items():
                object.__setattr__(record, name, value)

    def _orphaned(self, record: Any) -> bool:
        """Whether this record is about to be written under nobody.

        Both columns or neither, because a read through a parent matches on
        both and a half-named child is as unreachable as an unnamed one. A
        record that is not a child is never orphaned — it has no parent to be
        missing.
        """
        if not self.cls.__dray_parents__:
            return False
        return (
            getattr(record, self.cls.__dray_parent_type__) is None
            or getattr(record, self.cls.__dray_parent_id__) is None
        )

    def _write_all(
        self,
        records: Sequence[Any],
        *,
        creating: bool,
        assigned: dict[str, Any],
        guarded: bool = False,
    ) -> None:
        """Split into transactions that fit, rather than refusing.

        How big a set is tends not to be knowable where the call is written, so
        a caller who has to ask ends up guessing, and guesses that were right
        once stop being."""
        from dray.child import _told

        if not records:
            return
        # Two names for what look like one thing, and the difference is worth
        # keeping: `assigned` is this write's half — the `given=` a caller
        # passed — and `given` is the whole of what the write was told, store
        # defaults included, which is what a handler reads off `Write.given`.
        given = {**self.store.defaults, **assigned}
        # Everything before the first statement, which on a bulk write is a
        # real share of it: the handlers that fill fields in, the rules every
        # record and every child has to pass, and working out how many
        # transactions this is. dray's own time again, and the number that
        # says whether a slow save is the database or the validation.
        with self._watch.span("prepare", cls=self.cls) as watched:
            watched.rowcount = len(records)
            _refuse_derived(self.cls, records, assigned)

            # What the write was told, onto the records as well as their
            # children. Both halves listen, so `defaults={"batch": ...}` stamps
            # the note a job writes and the person it creates alike — one
            # store, one write, one answer. What that takes is knowing which
            # fields the caller chose, and a record carries that the same way a
            # queued child does.
            for record in records:
                _told(record, self.cls, given)
                record._dray_validate()

            # Before the first transaction opens, and for the whole set rather
            # than a chunk of it. `add_all` and `save_all` check every record up
            # front so that a bad value cannot leave half a set written; a child
            # was only checked by the chunk that carried it, which is the same
            # failure the up-front pass exists to prevent.
            #
            # Field rules and no more. Those are about values somebody supplied,
            # so what the caller said and what the write was told is the whole
            # of what they need. A rule the record wrote about *itself* reads
            # the record, and the write has not finished filling it in yet.
            self._validate_children(records, given)

            size = max(1, MAX_ROWS // max(1, self._rows_per(records)))

            # A chunk of one is the floor, and one record can be over the
            # ceiling on its own — a thousand children queued against it is a
            # thousand and one rows with nothing to split it away from. The
            # arithmetic above cannot see that, since dividing by a fanout
            # larger than the ceiling gives nought and the floor makes it one,
            # so a set that cannot fit would go to the database looking sized.
            # Refused here, in the same words the block below uses, because it
            # is the same thing gone wrong.
            widest = max(records, key=lambda one: len(_queued(one)))
            if 1 + len(_queued(widest)) > MAX_ROWS:
                raise ValueError(
                    f"one {self.cls.__name__} carries "
                    f"{len(_queued(widest))} queued children, which is "
                    f"{1 + len(_queued(widest))} rows with its own, and one "
                    f"transaction holds {MAX_ROWS}. A record cannot be split "
                    f"from its own children, so save it with fewer and add "
                    f"the rest to it afterwards."
                )

            # Splitting is the one thing this cannot do inside somebody's block.
            # Every chunk would join their transaction rather than opening its
            # own, so the ceiling the split exists to stay under would be
            # crossed anyway — and found at the database, mid-write, with some
            # of it already sent. Said here instead, before anything is written.
            #
            # Counted rather than taken from `size`, which is the *worst*
            # record's fanout applied to all of them. Outside a block that is a
            # harmless over-estimate — it makes more chunks than it needs — and
            # inside one it would refuse a write that fits perfectly well,
            # because twenty records of which one carries ten children is thirty
            # rows and not two hundred.
            if self.store.in_transaction:
                rows = len(records) + sum(len(_queued(r)) for r in records)
                if rows > MAX_ROWS:
                    raise ValueError(
                        f"these {len(records)} {self.cls.__name__} records are "
                        f"{rows} rows with their children, and one transaction "
                        f"holds {MAX_ROWS}. A transaction you opened cannot be "
                        f"split, so write them in smaller sets inside the "
                        f"block — or take the write outside it and let dray "
                        f"chunk it."
                    )
            # Every chunk worked out before the first of them is sent, which is
            # what buys the pass below. A record is only whole once the write
            # has filled in what it fills, and filling a chunk at a time would
            # mean the third chunk's rule being judged with the first two
            # already durable — the failure the up-front pass exists to prevent,
            # arrived at the long way round. What it costs is that a set refused
            # by a later chunk has had every record's `on_add` run; a handler
            # fills a field, so that is a value nobody stored rather than work
            # nobody undid.
            batches = [
                self._prepare_batch(
                    records[start : start + size], creating=creating, given=given
                )
                for start in range(0, len(records), size)
            ]
            self._check_all(batches)

        # Whether a chunk committing means anything, asked once and before any
        # of them runs. Inside a block the caller owns the transaction and
        # every chunk joins it, so nothing is durable until their `with` exits
        # and a set that stopped partway landed none of itself. `written` there
        # would name records that rolled back, which is a worse answer than
        # naming none — and the block is what knows what it carried anyway.
        durable = not self.store.in_transaction

        written: list[Any] = []
        committed: list[Any] = []
        carried: list[Any] = []
        # What the chunks already sent are still costing, which is nothing at
        # all unless somebody is holding a transaction open. Outside a block a
        # chunk *is* a transaction and each one starts from nought; inside one
        # every chunk joins the same transaction, so children a rule queued in
        # the first are still in the transaction the last is being added to. The
        # refusal above counts what the caller queued and cannot count those,
        # since no rule has run when it is asked.
        spent = 0
        for prepared, children in batches:
            try:
                carried.extend(
                    self._write_batch(
                        prepared,
                        children,
                        creating=creating,
                        given=given,
                        guarded=guarded,
                        spent=spent,
                    )
                )
            except DrayError as failed:
                # The chunk that raised rolled back; the ones before it
                # committed and are not coming back. Saying which is the whole
                # difference between a caller who can re-read and one reading a
                # private attribute to work it out.
                #
                # Every one of them rather than the guard alone. A lost guard
                # was the first to need this and is not the only way a set
                # stops partway — a key clash, a row that has gone, a commit
                # DSQL refused five times over — and a caller left holding
                # thirty-five records and twenty rows is in the same position
                # whichever of them arrived.
                if durable:
                    failed.written = tuple(written)
                    # The rows the chunks before this one wrote are durable, so
                    # the cache is now wrong about every one of them — and the
                    # raise goes straight past the eviction at the foot of this
                    # function, which is the only place it would happen. A set
                    # that stopped partway is a case the caller is told to
                    # expect and recover from, and recovering starts with
                    # reading records the write left in the table.
                    self._forget_written([*committed, *carried])
                raise
            written.extend(key_of(record) for record, *_ in prepared)
            # The records themselves as well as their keys, because the
            # eviction above wants a class per record and the children ride
            # with them.
            committed.extend(record for record, *_ in prepared)
            # Counted after the chunk rather than before it, because what a rule
            # queued is only in `children` once the rule has run.
            if not durable:
                spent += len(prepared) + len(children)

        # Out here rather than anywhere inside the write, which is the whole of
        # what keeps an `@after_commit` to once per save. `_commit_batch` is
        # replayed when DSQL refuses the commit; this is outside the replay, the
        # same placement `_filled_by_write` gets and for the same reason — a
        # handler enqueuing a job from inside it would send one per attempt.
        #
        # After the last chunk rather than after each of them, so a set too big
        # for one transaction still gets one pass of handlers with every row
        # already durable. Per chunk, a handler that raised would take the
        # chunks behind it with it, and records nobody wrote is a worse thing to
        # go wrong than handlers that ran late.
        #
        # The children the write carried are records that landed too, and take
        # their own the same way a rule about the whole record does. Their
        # parents first, because that is the order they were written in.
        landed = (*records, *carried)
        # dray's own door rather than the queue beside it, and it runs before
        # the handlers do: what a caller wrote is counted and reported by an
        # `AfterCommitFailed`, and bookkeeping of dray's is neither a handler
        # somebody can read nor a failure anybody can act on.
        self.store._when_committed(
            partial(_forget_what_they_were, _as_this_write_leaves_them(landed))
        )
        # The other thing that waits for the commit and is nobody's handler: the
        # cached rows this write has just made wrong, its children's included.
        self._forget_written(landed)
        try:
            self.store._after_commit_all(
                [
                    partial(run, record, AFTER_COMMIT)
                    for record in landed
                    if declares(record, AFTER_COMMIT)
                ]
            )
        except DrayError as failed:
            # The one that means every record landed, so `written` is all of
            # them rather than a prefix. Left empty here it would say nothing
            # was written about a write that entirely was, which is the only
            # reading worse than saying nothing at all.
            #
            # The records handed over, as everywhere else — the children they
            # carried are rows too and are not in it, because a caller holds
            # parents and can do nothing with a key it never had.
            #
            # Inside a `store.transaction()` the handlers wait for the
            # block, so an `AfterCommitFailed` comes out of the `with` and
            # never through here — and there `written` is empty and honest,
            # because the block knows what it carried and this call does not.
            if durable:
                failed.written = tuple(written)
            raise

    def _rows_per(self, records: Sequence[Any]) -> int:
        """A record costs its own row plus one for each child queued against
        it. Measured rather than assumed, since that is what decides how many
        fit in a transaction."""
        most = max(len(_queued(record)) for record in records)
        return 1 + most

    def _prepare_batch(
        self, records: Sequence[Any], *, creating: bool, given: dict[str, Any]
    ) -> tuple[list[tuple], list[tuple]]:
        """
        One transaction's worth worked out, and nothing sent: what the write
        fills in, a fresh etag, which fields it touched, the `Write` itself, and
        the children riding with it. An insert has no use for the third — every
        column of a new row is new — and takes the whole class regardless.

        The `Write` is carried rather than rebuilt because a `@before_save` is
        handed it, and that runs inside the transaction, where making an object
        per record per attempt would be paid for on every replay.

        Apart from the commit because the commit is replayed and this is not. A
        field that named a handler is filled exactly once per save, so a handler
        may derive its value from what the record currently holds —
        `on_save=lambda w: w.record.touched + 1` counts saves rather than
        attempts, where filling inside the replay would have it read back what
        the last refused attempt already put there and count again.
        """
        prepared = []
        for record in records:
            # Read before anything is filled in: this is what we believe is
            # stored, and it is what the update has to match.
            stored = getattr(record, self.etag)
            filled, write = _filled_by_write(self.cls, record, creating, given)
            filled[self.etag] = new_etag()
            touched = _touched(self.cls, record, filled, given)
            prepared.append((record, stored, filled, touched, write))
        return prepared, self._prepare_children(records, given)

    def _check_all(self, batches: Sequence[tuple]) -> None:
        """
        The rule each record wrote about itself, for the whole write, with
        nothing sent yet.

        Nowhere earlier in the write because a record is not whole until the
        write has filled in what it fills: a rule reading a field the store's
        `defaults` carry, or one an `on_add` supplies, has nothing to read at
        any earlier moment and would refuse a record that is not wrong. `parse`
        runs the same rules on its own door, against what the caller supplied,
        which is why one reading a filled-in field has to guard for its absence.

        Here and nowhere later because a set is refused whole or not at all.
        Run inside the chunk carrying the record, a rule broken at position
        4,000 would leave the first 2,000 committed.

        The records first and their children after, the order the field rules
        already run in. A field an `on_add` fills with `Sql` is the one thing a
        rule cannot see: the value is an expression for the database to work out
        and there is nothing to put on the object until the row comes back.
        """
        for prepared, _ in batches:
            for record, *_ in prepared:
                run(record, CHECK)
        for _, children in batches:
            for _, item, *_ in children:
                run(item, CHECK)

    def _before_saving(
        self, prepared: Sequence[tuple], children: Sequence[tuple]
    ) -> None:
        """
        The rule each record wrote about the write itself, with the
        transaction open and nothing sent.

        Here rather than beside `_check_all` because that is the whole of the
        difference between the two: a rule about the values is judged before
        any transaction exists, so a bad value cannot leave half a set written,
        and a rule about the write has to be inside the transaction or what it
        writes outlives a refusal. Which also puts it inside the replay —
        deliberately, and the opposite of where `on_add`, `on_save` and
        `after_commit` sit. Those must not happen twice; this is work a
        rollback destroyed and has to redo.

        The records first and the children riding with them after, the order
        `_check_all` and the `after_commit` pass already use.

        The write is handed over as well as the record, and it is the one this
        write already built for the field handlers — so a rule can read what the
        call was told, and a record that marked nothing has still had no object
        made for it that was not made anyway.

        Attached before it runs, which is earlier than the write attaches
        anything else. `self.store` is the whole of how a handler reaches a
        record that is not this one, and an `add` that left the record
        unattached until after the commit would have given the insert side a
        hook that can refuse and cannot write. What it costs is an object that
        believes it came from a store it may have no row in, and that is a
        position `_undone` already argues dray should take: a rolled-back `add`
        leaves one too, because detaching would take `person.notes` away from a
        caller who is about to try again.

        The declaration is asked before the attaching and before the call, so a
        bulk write of records that marked nothing pays one dictionary lookup
        each and touches none of them.
        """
        for record, *_, write in prepared:
            if declares(record, BEFORE_SAVE):
                run(self._attached(record), BEFORE_SAVE, write)
        for cls, item, _, write in children:
            if declares(item, BEFORE_SAVE):
                where = _collection_for(self.store, cls)
                run(where._attached(item), BEFORE_SAVE, write)

    def _before_saving_all(
        self,
        prepared: Sequence[tuple],
        children: list[tuple],
        given: dict[str, Any],
        spent: int = 0,
    ) -> None:
        """
        The rules, and the children they queue while they run.

        `self.history.add(...)` inside a `@before_save` is the spelling anybody
        who has used a child anywhere else reaches for, and a child queued that
        late is past the walk that gathered the rest: the tree is read before
        the transaction opens. So it is picked up here instead —
        which is the whole of why a rule may queue, and why dray's own
        `records_change` keeps its line when a rule moves the field.

        Round by round rather than in one pass, because a child a rule queued
        has a rule of its own to run and that rule may queue as well. Each
        round judges only what the round before it queued: the field rules, the
        `@check`, then the rule about the write. A round that queues nothing
        ends it, and the ceiling below is what ends one that never would.

        **What a late child gets is judged inside the transaction**, which the
        rest of the write's checking deliberately is not. There is no earlier
        moment available — the child did not exist when the up-front pass ran —
        so the promise that a bad value leaves nothing written narrows for this
        one case from the whole set to the chunk carrying it. Everything the
        caller queued is still judged before the first row is sent.

        The row ceiling is the other thing the up-front pass cannot do. How
        many rows a transaction is was worked out before any rule ran, so a
        rule that queues is adding rows to arithmetic that has already been
        done — refused here, in dray, rather than sent to the cluster as a
        transaction dray's own sizing said would fit.

        `spent` is what the chunks before this one are still costing, and it is
        the difference between a check that holds and one that only looks like
        it. Outside a block a chunk is a transaction and this starts at nought;
        inside one every chunk joins the same transaction, so a rule adding a
        row per record overshoots by a chunk's worth at a time and no single
        chunk ever looks close to the ceiling.
        """
        records = [record for record, *_ in prepared]
        # By identity, and holding everything the write already prepared, so
        # the next round can tell a child a rule queued from one it has already
        # filled in. Filling one twice runs its `on_add` handlers twice, which
        # is a value nobody asked for on a save nobody repeated.
        seen = {id(item) for _, item, *_ in children}

        self._before_saving(prepared, children)
        while True:
            self._validate_children(records, given, seen)
            late = self._prepare_children(records, given, seen)
            if not late:
                return
            for _, item, *_ in late:
                run(item, CHECK)
            children.extend(late)
            rows = spent + len(prepared) + len(children)
            if rows > MAX_ROWS:
                # Two ways out, and which one is open depends on who owns the
                # transaction — the same split the refusal before the write
                # makes, and for the same reason: a block cannot be chunked.
                over = (
                    "A transaction you opened cannot be split, so write "
                    "smaller sets inside the block — or take the write "
                    "outside it, where a chunk is a transaction of its own "
                    "and a rule only has to leave room in that."
                    if self.store.in_transaction
                    else "Queue fewer in the rule, or hand the write fewer "
                    "records at a time."
                )
                raise ValueError(
                    f"a @before_save queued children that take this "
                    f"transaction to {rows} rows, and one transaction holds "
                    f"{MAX_ROWS}. How many rows a write is was worked out "
                    f"before any rule ran, so a rule that queues has to leave "
                    f"room. {over}"
                )
            self._before_saving((), late)

    def _write_batch(
        self,
        prepared: Sequence[tuple],
        children: list[tuple],
        *,
        creating: bool,
        given: dict[str, Any],
        guarded: bool = False,
        spent: int = 0,
    ) -> list[Any]:
        """
        One transaction's worth, already worked out: commit it, and leave the
        records usable. Hands the children it wrote back, because the whole
        write is what the caller is holding and a chunk of it is all this can
        see.
        """
        # What the tidying below is about to destroy, in case it has to go back,
        # and only when somebody is holding a transaction open — outside one the
        # commit has already happened by the time `_commit_batch` returns, so
        # there is nothing to undo and nothing to remember.
        #
        # Before the write rather than after it, which matters for one thing
        # only: a `@before_save` may queue a child while it runs, and this is
        # the queue as the *caller* left it. Put back after a rollback, a
        # rule's child would be queued against a record whose next save runs
        # the rule again — two of it for one save, which is the same doubling
        # the replay inside `_commit_batch` is careful about, arrived at
        # through the caller's block instead.
        undo = (
            [(record, getattr(record, self.etag), _queued_below(record))
             for record, *_ in prepared]
            if self.store.in_transaction
            else []
        )

        self._commit_batch(
            prepared,
            children,
            creating=creating,
            given=given,
            guarded=guarded,
            spent=spent,
        )

        # Now rather than at the commit, because a record has to be usable for
        # the rest of the block it was written in: `add` then `save` wants the
        # collection this attaches, and the second save wants the etag this
        # applies or it fails its own guard against the row the first one wrote.
        for record, _, filled, *_ in prepared:
            object.__setattr__(record, self.etag, filled[self.etag])
            self._attached(record)
            _settle(record)
        self._attach_children(children)

        if undo:
            self.store._undo_on_rollback(lambda: _undone(undo))

        return [item for _, item, *_ in children]

    @retrying
    def _commit_batch(
        self,
        prepared: Sequence[tuple],
        children: list[tuple],
        *,
        creating: bool,
        given: dict[str, Any],
        guarded: bool = False,
        spent: int = 0,
    ) -> None:
        """The statements, and the rules that have to sit beside them.
        Replayed whole if DSQL rejects the commit, which is safe because a
        rejected commit made nothing durable and everything in hand was worked
        out before the first attempt — a `@before_save` excepted, and that one
        is here precisely so a replay runs it again.

        Which is what `_rewinding` is wrapped around it for. A rule that queues
        a child adds to `children`, and `children` was built once outside the
        replay and is handed to every attempt — so an attempt that fails has to
        leave the queue and that list exactly as it found them, or the second
        attempt writes the first attempt's rows on top of its own. Outside
        `_transacting` in the same `with`, so it runs after the rollback rather
        than before it."""
        moved = []
        with (
            _rewinding(prepared, children) as rules,
            self.store._transacting() as conn,
        ):
            # Skipped whole when nothing in the chunk marked a method, which is
            # the question `_rewinding` had to answer anyway on its way to the
            # snapshot — so a bulk write of records that declare no rule pays
            # that one lookup each and nothing here at all.
            if rules:
                self._before_saving_all(prepared, children, given, spent)
            with batching(conn, watch=self._watch, cls=self.cls) as batch:
                # Queued, all of it, before a single result is read back. That
                # is the whole of what makes a set of a hundred one wait rather
                # than a hundred.
                sent = [
                    self._insert(batch, record, filled)
                    if creating
                    else self._update(
                        batch,
                        record,
                        filled,
                        touched,
                        stored if guarded else None,
                    )
                    for record, stored, filled, touched, _ in prepared
                ]
                # The children go out with them, because a parent and what
                # rides with it were always one transaction and are now one
                # trip as well.
                #
                # Under a guard they wait, and that is the one place the trip
                # is not shared. A lost guard is answered by reading the rows
                # back in this same transaction, and a child that clashed would
                # have killed the transaction before that read could be made —
                # leaving a refusal that carries `ids` and no `records`, which
                # says the row was deleted about a row that is sitting there.
                # So a guarded write spends a second round trip rather than a
                # wrong sentence.
                queued = (
                    [] if guarded else self._insert_children(batch, children)
                )

                # Read back in the order they were sent, which is the only
                # thing tying a failure to the row that caused it. A
                # record's own statement comes before the children riding with
                # it, so a clash and a lost guard in the same write are
                # reported in that order.
                for (record, *_), each in zip(prepared, sent):
                    # An insert has no guard to lose and answers nothing; an
                    # update says whether the row was still there to write.
                    if not each.landed() and not creating:
                        moved.append(key_of(record))
                # Inside the block, so the raise is what rolls it back. Every
                # record is tried before anything is said, because "one of
                # these forty" is a poor thing to hear forty times, and a
                # caller re-reading wants the whole list rather than the first
                # name in it.
                if moved:
                    # One statement for the whole batch, on a path that was
                    # already raising: a save that goes through sends nothing
                    # extra, and a save that does not was going to be re-read
                    # anyway. Here rather than in the caller's hands because
                    # only here is it still the transaction that found out.
                    try:
                        found = self._as_stored(moved)
                    except psycopg.Error as failed:
                        # A failure inside a failure, and what this raises
                        # matters more than what it carries. `retrying` replays
                        # on `SerializationFailure`, so letting a broken lookup
                        # out raw would spend five attempts and then report a
                        # commit DSQL never refused — sending a caller who has
                        # lost a write off to run the whole thing again.
                        raise RecordHasChanged(
                            _conflict(
                                self.cls, len(moved), len(prepared), gone=0
                            )
                            + ", and what is there now could not be read",
                            ids=moved,
                        ) from failed
                    raise RecordHasChanged(
                        _conflict(
                            self.cls,
                            len(moved),
                            len(prepared),
                            gone=len(moved) - len(found),
                        ),
                        ids=moved,
                        records=found,
                    )
                if guarded:
                    queued = self._insert_children(batch, children)
                for each in queued:
                    each.landed()

    def _as_stored(self, ids: Sequence[Any]) -> list[Any]:
        """The rows behind these ids as the table now has them, in the order the
        ids were given, and short by one for every row that has gone.

        Records rather than rows, because a caller catching the refusal asks the
        object questions — `ticket.status`, `ticket.closed_by` — and a dict
        of columns would make them do the hydrating dray is for. It is a failure
        path, so building them costs a write that was already lost.
        """
        found = self.select_many(
            f"select {self.columns} from {self.table} where {self.id} = any(%s)",
            [list(ids)],
        )
        by_key = {key_of(record): record for record in found}
        return [by_key[each] for each in ids if each in by_key]

    def _insert(self, batch: Any, record: Any, filled: dict[str, Any]) -> Any:
        names, holders, params = _written_for(self.cls, record, filled)
        computed = [n for n, v in filled.items() if isinstance(v, Sql)]
        returning = f" returning {', '.join(computed)}" if computed else ""

        def landed(cur: psycopg.Cursor) -> None:
            # Anything the database worked out for itself comes straight back,
            # so the object in hand agrees with the row rather than holding the
            # None it was carrying before the write.
            if computed:
                with self._watch.span("returning", cls=self.cls):
                    row = cur.fetchone()
                for name, value in zip(computed, row):
                    object.__setattr__(record, name, value)

        return batch.send(
            f"insert into {self.table} ({', '.join(names)})"
            f" values ({', '.join(holders)}){returning}",
            params,
            landed=landed,
            clash=(self.cls, record),
        )

    def _update(
        self,
        batch: Any,
        record: Any,
        filled: dict[str, Any],
        touched: set[str],
        stored: str | None = None,
    ) -> Any:
        """
        Write the fields in `touched`, refusing if `stored` no longer matches
        the row.

        The comparison in `save` catches a form that went stale before it was
        submitted. This one closes the narrower gap between reading the record
        in this process and updating it, which no amount of checking beforehand
        can cover.

        Whether the row was there to write comes back from `landed()` on the
        statement this queues, rather than as a raise on a guard that failed.
        One record or four hundred, a lost guard means the same thing and the
        batch is what knows how many of them there were — so the answer comes
        back here and the raising happens once, naming all of them.

        Guarded, a row that is simply gone comes back the same `False`, because
        a row nobody can find is the furthest a row can have changed. Which of
        the two it was is not knowable from a rowcount and is not asked here:
        the batch reads the ids back and tells them apart by what comes. With no
        guard there is nothing to have lost and nothing further to collect, so a
        missing row raises `RecordNotFound` here and now.
        """
        names, holders, params = _written_for(
            self.cls, record, filled, updating=True, touched=touched
        )
        assignments = ", ".join(
            f"{name} = {holder}" for name, holder in zip(names, holders)
        )
        where = f" where {self.id} = %s" + (
            f" and {self.etag} = %s" if stored else ""
        )
        guard = [stored] if stored else []
        # The same `returning` an insert writes, for the same reason. A field
        # whose `on_save` handed back `Sql` has no Python value to put on the
        # object, so without this the record goes on reading whatever the add
        # left there while the row moves underneath it — and the field that
        # does this is the one saying when the record was last written.
        #
        # Everything filled is in `touched`, which is what keeps this honest
        # now that the statement is narrow: a name here is always a name the
        # `set` above just wrote, rather than a column asking the row what it
        # already held.
        computed = [n for n, v in filled.items() if isinstance(v, Sql)]
        returning = f" returning {', '.join(computed)}" if computed else ""

        def landed(cur: psycopg.Cursor) -> bool:
            if cur.rowcount == 0:
                if stored:
                    return False
                raise RecordNotFound(
                    f"no {self.cls.__name__} {key_of(record)!r} to save"
                )
            if computed:
                with self._watch.span("returning", cls=self.cls):
                    row = cur.fetchone()
                for name, value in zip(computed, row):
                    object.__setattr__(record, name, value)
            return True

        return batch.send(
            f"update {self.table} set {assignments}{where}{returning}",
            [*params, key_of(record), *guard],
            landed=landed,
        )

    @retrying
    def _delete_batch(self, record: Any) -> None:
        """
        The record and everything below it, in one transaction.

        Deepest first, so nothing is orphaned partway: an attachment is reached
        through the note it hangs off, and deleting the note before it would
        leave nothing able to say the attachment was ever anybody's.

        Every row counts against DSQL's 3,000, and a tree multiplies — fifty
        notes with twenty attachments each is a thousand rows for one person.
        Nothing here splits, so a deep enough record is a delete the database
        will refuse.

        A record whose own row is not there raises `RecordNotFound`, the same
        as a save of one does. Nothing below it is consulted: a record with no
        children has none to remove and is not missing.
        """
        with self.store._transacting() as conn:
            # Inside the transaction and before any of it, which is the whole of
            # what `before_delete` promises: a rule that refuses leaves the row
            # untouched, and a handler writing what the record said writes it
            # where the removal can still take it back. It is deliberately not
            # lifted out the way `on_add` and `on_save` are — those are values a
            # replay must not recompute, this is work a replay must redo,
            # because the rollback took the first attempt's rows with it.
            #
            # Only this record's, never a descendant's. The cascade below is one
            # statement per generation and loads not a row of them, so running
            # theirs would mean reading the whole tree first — which is the cost
            # this shape exists to avoid, and the tree is the part of a delete
            # that multiplies.
            run(record, BEFORE_DELETE)
            with batching(conn, watch=self._watch, cls=self.cls) as batch:
                # A generation apiece and the record's own row last, and one
                # trip for the lot. The order is the order they are sent in, so
                # nothing is orphaned by the batching either.
                for chain in _descendants(self.cls):
                    batch.send(_cascade(chain), [key_of(record)])
                removed = batch.send(
                    f"delete from {self.table} where {self.id} = %s",
                    [key_of(record)],
                )
                batch.settle()
                # This statement's count and no other's. The cascade above
                # deletes nothing for a record that never had children, and one
                # statement per generation could not tell that from a record
                # that was never there without loading the tree this shape of
                # delete exists to avoid loading.
                #
                # Which puts the raise after the handler rather than in front
                # of it, and that is the honest order rather than the cheap
                # one: the rowcount is the only thing that knows, and a read
                # beforehand would be a round trip that still raced the delete
                # it was guarding. What the handler wrote goes back with the
                # rollback this causes, so a second delete leaves nothing
                # behind claiming the record went twice.
                if removed.rowcount == 0:
                    raise RecordNotFound(
                        f"no {self.cls.__name__} {key_of(record)!r} to delete"
                    )

    @retrying
    def _clear_batch(self, parent: Any) -> int:
        """
        One parent's children of this kind, and everything below them, in one
        transaction — and how many of the children went.

        The statements are `_delete_batch`'s, one generation shorter. The
        chains are the ones rooted at the parent's class that pass through this
        one, so the parent's own row is in none of them, and deepest first for
        the reason it is there in a delete: an attachment is reached through
        the note it hangs off, and taking the note first would leave nothing
        able to find it.

        A `@before_delete` on the child class is the only thing that makes this
        read a row. It runs on every child, inside the transaction and in front
        of every statement, so a rule that refuses leaves the whole generation
        where it was — and the read is inside the retried part along with it,
        because a replay is a fresh transaction and what the first attempt read
        went back with the rollback. A class declaring none is asked nothing at
        all, which is what keeps the ordinary case one round trip whatever the
        size of the set.

        No `RecordNotFound` for a set that was already empty. A delete by id
        is a belief about one row and the raise is what says the belief was
        wrong; a set carries no such belief, so removing nothing out of one is
        a set operation that worked.
        """
        chains = _chains_through(type(parent), self.cls)
        with self.store._transacting() as conn:
            if declares(self.cls, BEFORE_DELETE):
                for child in self.find(parent=parent):
                    run(child, BEFORE_DELETE)
            with batching(conn, watch=self._watch, cls=self.cls) as batch:
                # The children's own delete is the shortest chain and so the
                # last of them, which is also where it has to go: everything
                # below them is reached through the rows it is about to remove.
                for chain in chains[:-1]:
                    batch.send(_cascade(chain), [key_of(parent)])
                removed = batch.send(_cascade(chains[-1]), [key_of(parent)])
                batch.settle()
                # Read here, with the batch still open: a cursor outside it has
                # been given back and answers -1 to everything.
                went = removed.rowcount
        # Whole, because a set removal names a parent rather than the rows it
        # took: there are no keys to drop one at a time.
        self._forget_removed(whole=True)
        return went

    @retrying
    def _thin_batch(self, parent: Any, at_a_time: int) -> int:
        """
        One pass: up to `at_a_time` rows of one generation under this parent, in
        one transaction, and how many went.

        The chains are `_clear_batch`'s, deepest first, and the statement for
        each is `_cascade`'s with that generation's own ids taken by a bounded
        subselect. The first chain that removes anything is the pass, so a pass
        never touches a generation while there is a deeper one with rows in it —
        which is what makes stopping half way a shortened tree rather than a
        broken one.

        **Nothing is carried from one pass to the next, and that is the
        correctness of the loop rather than a detail of it.** Every pass starts
        again at the deepest generation, so a child inserted under a level a
        previous pass emptied is found by the next one. Draining a generation
        and then moving up as a phase would walk past exactly that row and leave
        it unreachable.

        What it costs is a statement per empty generation on the way down,
        because the next one depends on the last one's rowcount and no pipeline
        can save a round trip it has to read first. Bounded by the depth of the
        tree, which is two for most of them.

        A `@before_delete` on the child class is the only thing that makes this
        read a row, and only on the pass that reaches the children themselves —
        the generations below them go the way a cascade goes, unread. That pass
        reads the rows it is about to take and removes those rows by id rather
        than sending the bounded delete a second time, which is what keeps the
        rule and the delete about the same rows: an unordered `limit` is under
        no obligation to answer twice with the same ones. Across a replay it is
        the read that holds it rather than the delete — `find` orders it and a
        class's order is total, so a fresh attempt over the same rows takes the
        same ones again.
        """
        chains = _chains_through(type(parent), self.cls)
        went = 0
        with self.store._transacting() as conn:
            for chain in chains:
                # The children's own generation is the shortest chain and so the
                # last of them, and the only one whose rules are this call's to
                # run — the same division `_clear_batch` makes, one pass at a
                # time.
                if chain is chains[-1] and declares(self.cls, BEFORE_DELETE):
                    took = self._thin_read(parent, at_a_time)
                else:
                    with cursor(conn, watch=self._watch, cls=self.cls) as cur:
                        cur.execute(
                            _thinning(chain, at_a_time), [key_of(parent)]
                        )
                        took = cur.rowcount
                if took:
                    went = took
                    break
        # Outside the transaction, and reached only by the attempt that got
        # through it — a refused pass raises out of `_transacting` above and
        # never arrives here — so a pass replayed four times still forgets
        # once, even though this line is inside the replay. Whole, for the
        # reason `_clear_batch` forgets whole: a bounded delete names no keys.
        if went:
            self._forget_removed(whole=True)
        return went

    def _thin_read(self, parent: Any, at_a_time: int) -> int:
        """One pass over the children of a class that declared a rule: the rows
        this pass would take, the rule on each of them, everything under those
        rows, and then the rows themselves. Inside the pass's transaction and
        inside its replay, because a replay is a fresh transaction and what the
        last attempt read went back with the rollback."""
        taking = self.find(parent=parent, limit=at_a_time)
        if not taking:
            return 0
        for child in taking:
            run(child, BEFORE_DELETE)
        ids = [key_of(child) for child in taking]
        with batching(self.conn, watch=self._watch, cls=self.cls) as batch:
            # Down through the ids first, which is `delete`'s order for one
            # record and is here for the same reason it is there. This pass
            # reached the children only because every generation below them
            # answered empty, and then it ran a rule — so anything a rule wrote
            # under a child it was losing arrived *after* the statement that
            # would have taken it, and the delete below does not cascade. One
            # more statement per generation, in the same round trip, and the
            # rows it finds are only ever the ones the rules just wrote.
            for chain in _descendants(self.cls):
                batch.send(_cascade(chain, keys="= any(%s)"), [ids])
            removed = batch.send(
                f"delete from {self.table} where {self.id} = any(%s)", [ids]
            )
            batch.settle()
            # Read with the batch still open, as `_clear_batch` reads its own:
            # a cursor given back answers -1 to everything.
            return removed.rowcount

    #
    # Children are flushed by whatever writes their parent
    #

    def _prepare_children(
        self,
        records: Sequence[Any],
        given: dict[str, Any],
        seen: set[int] | None = None,
    ) -> list[tuple]:
        from dray.child import _prepare

        return _prepare(records, given, self.table, seen)

    def _insert_children(
        self, batch: Any, prepared: Sequence[tuple]
    ) -> list[Any]:
        from dray.child import _insert_all

        return _insert_all(batch, prepared)

    def _attach_children(self, prepared: Sequence[tuple]) -> None:
        from dray.child import _attach_all

        _attach_all(self.store, prepared)

    def _validate_children(
        self,
        records: Sequence[Any],
        given: dict[str, Any],
        seen: set[int] | None = None,
    ) -> None:
        from dray.child import _validate_queued

        _validate_queued(records, given, seen)

    def _attached(self, record: Any) -> Any:
        """Give the record its way back, so `person.save()` and `person.notes`
        work without threading a store through every caller."""
        object.__setattr__(record, "_dray_collection", self)
        return record


_UNSET = object()


def _checked_id(cls: type, record_id: Any) -> Any:
    """
    Whatever the record says an id is, and nothing else — a router handing over
    what happened to be in the URL is the usual way to get this wrong.

    Refused here rather than sent to the database, which answers
    `operator does not exist: text = smallint` and tells whoever passed the
    integer nothing about where it came from.

    Read from the field rather than assumed, because the key is a name a record
    may declare for itself and a column a record may move — and undisturbed it
    is a `UUID`, which a URL hands over as text. Converted first and then
    checked, exactly as an assignment would be, so `by_id(request.args["id"])`
    works without the caller knowing what an id is made of.
    """
    key = cls.__dray_key__
    rules = cls.__dray_fields__.get(key, {})
    record_id = convert(key, record_id, rules)
    allowed = rules.get("accepts") or (str,)
    if not fits(record_id, allowed):
        wanted = " or ".join(dict.fromkeys(kind.__name__ for kind in allowed))
        raise ValidationError(
            f"a {cls.__name__} id is {wanted}, not "
            f"{type(record_id).__name__}: {record_id!r}"
        )
    return record_id


def _naming(cls: type, parent: Any) -> dict[str, Any]:
    """
    One parent as the two columns that point at it.

    Read and write share it because they have to agree. A note written under a
    person is found again by asking for that person's, so the day the two doors
    worked the parent's table out differently is the day a child is stored where
    nothing goes looking for it.
    """
    if not hasattr(type(parent), "__dray_table__"):
        raise TypeError(
            f"parent is the record the children hang off, not {parent!r}."
        )
    return {
        cls.__dray_parent_type__: type(parent).__dray_table__,
        cls.__dray_parent_id__: key_of(parent),
    }


def _no_parent(cls: type) -> str:
    """
    What a write says when a child names no parent at all.

    Worth spelling the recovery out, because the row this refuses is one
    nothing complains about afterwards. It is written by an ordinary call and
    then reached by no read through a parent, taken by no parent's delete, and
    not askable for either — `find(parent_type=None)` means *unset* and so
    filters on nothing, which is the one place the ordinary reading of a filter
    works against somebody.
    """
    return (
        f"a {cls.__name__} is written under a parent: pass parent=, or set "
        f"{cls.__dray_parent_type__} and {cls.__dray_parent_id__} on the "
        "record yourself. A child naming nobody is reached by no read through "
        "a parent and removed by no parent's delete, which is the one state "
        "@child says cannot exist."
    )


def _two_parents(cls: type, record: Any, parent: Any) -> str:
    """
    What a write says when `parent=` and the record disagree about whose it is.

    Both are named, because the one the caller did not mean was written
    somewhere else — a constructor, a parsed row, a form — and a sentence
    naming only the argument sends them looking at the line that is right.
    """
    # Whichever of the two the record is actually carrying. They are ordinary
    # fields and a half-filled one is a real thing to arrive with, so printing
    # both regardless said "belongs to None <id>" — which reads as a fault in
    # the sentence rather than a fault in the record it is about.
    held_type = getattr(record, cls.__dray_parent_type__)
    held_id = getattr(record, cls.__dray_parent_id__)
    if held_type is None:
        belongs = f"whatever has the id {held_id!r}"
    elif held_id is None:
        belongs = f"some {held_type}"
    else:
        belongs = f"{held_type} {held_id!r}"
    return (
        f"this {cls.__name__} already belongs to {belongs}, and parent= names "
        f"{type(parent).__dray_table__} {key_of(parent)!r}. Which parent a "
        "child belongs to is settled when it is written and nothing moves it "
        "afterwards, so say it on the record or say it here — not both, "
        "differently."
    )


def _check_select(cls: type, described: Sequence[Any] | None) -> None:
    """
    Refuse a statement that asked for part of a record.

    `_dray_load` gives a missing key the field's default, which is right for a
    row written before the field existed and wrong for a column nobody asked
    for: `select id, family_name from person` hydrates a Person reading
    `status='enquiry'` where the row says `'volunteer'`, and the next save
    writes that default over what was there. Without the key it is worse — the
    record is minted a fresh one and belongs to no row at all, so its save
    either finds nothing or inserts a stranger.

    Which is why the check is here rather than in `_dray_load`, and the
    distinction is the whole of it. `_dray_load` stays as lenient as it was: a
    key the class no longer declares is dropped so a field can be retired
    without a backfill, and nothing is validated so tightening a rule cannot
    make a stored record unreadable. An unrecognised column coming back is
    fine. A declared one not coming back is a mistake, and only the read path
    can tell, because only it knows what was selected.

    Read off the cursor's description rather than the rows, so a partial select
    that happens to match nothing today is refused as loudly as one that matched
    — the statement is what is wrong, and it will be run again tomorrow against
    a table that has rows in it.

    Every column and the blob, which together are exactly what `self.columns`
    names. A blob field is not a column and never comes back under its own name,
    so it is not looked for — but the jsonb it lives in is, because a select
    without it defaults every field inside it in one go.

    A statement that produced no result set at all has no description and is
    left alone. That is `select_many` handed an update or a call rather than a
    query, which psycopg already complains about in its own terms.

    A plain `ValueError` and not a name of its own, because the names dray
    exports are for what a caller can do something about while it runs — a
    value that arrived, a row somebody else moved, a key already taken. This is
    none of those. It is the same on every run and is fixed by editing the
    statement, so an `except` for it would never be written. It keeps company
    with the batch of nought rather than with `RecordNotFound`. Nor does it
    collide with what a wrong statement raises: psycopg's errors descend from
    `psycopg.Error`, so a syntax error was never a `ValueError` either.
    """
    if described is None:
        return
    selected = {column.name for column in described}
    missing = [
        name
        for name in (*cls.__dray_columns__, cls.__dray_blob_column__)
        if name not in selected
    ]
    if not missing:
        return
    rootless = (
        " Without its key it belongs to no row at all, so its next save would "
        "either find nothing or insert a stranger."
        if cls.__dray_key__ in missing
        else ""
    )
    raise ValueError(
        f"this statement selects part of a {cls.__name__}: {', '.join(missing)} "
        f"did not come back. What it built would hold the class defaults for "
        f"those, and saving it would write them over the row.{rootless} Select "
        f"{{self.columns}}, which is the whole record and comes off the class, "
        f"so it cannot fall behind it."
    )


def _conflict(cls: type, moved: int, of: int, *, gone: int) -> str:
    """
    What a guarded write says when it is refused.

    *Changed* includes *gone*, so the sentence has to as well. A guard is lost
    either way and the exception is the same one, but "was changed by someone
    else" about a ticket the reporter withdrew sends whoever reads it looking
    for an edit nobody made — and code reacting to the refusal has to write
    its way around a sentence that is not true of the row.

    Three answers rather than one hedge that covers them all. Only a batch can
    hold both at once; a single record — the common case — knows exactly which
    of the two happened to it and should say so.
    """
    what = (
        "changed" if not gone
        else "removed" if gone == moved
        else "changed or removed"
    )
    if of > 1:
        return (
            f"{moved} of {of} {cls.__name__} records were {what} by someone else"
        )
    return f"this {cls.__name__} was {what} by someone else"


def _write_once(cls: type, name: str) -> bool:
    """A field filled on add and never on save. `created_at` is the obvious one:
    an update must leave it alone rather than write whatever is in memory, which
    for a value the database computed is nothing at all."""
    rules = cls.__dray_fields__.get(name, {})
    return rules.get("on_add") is not None and rules.get("on_save") is None


def _touched(
    cls: type, record: Any, filled: dict[str, Any], given: dict[str, Any]
) -> set[str]:
    """
    The fields a save has to write, which is every way a value can have got
    onto the record since it was read.

    Three of them, and missing any one loses a write silently. `_dray_said` is
    what somebody assigned, seeded at construction and added to by every
    assignment after. `given` is what the write was told — the store's
    `defaults` under a caller's — which `_told` puts on the record through
    `object.__setattr__` and so never enters the said-set. And `filled` is what
    a handler produced, plus the fresh etag, which is why an update always has
    at least one column to write.

    Wider than what moved since the *last* save, because nothing empties the
    said-set: a field assigned before the first save is sent again at the
    second. That is a wider statement rather than a wrong one, and emptying it
    is not available — `_told` asks the same set which fields a caller chose
    for itself, so clearing it would let the next write's `given` overwrite a
    value somebody had explicitly assigned.
    """
    said = getattr(record, "_dray_said", None) or ()
    told = (name for name in given if name in cls.__dray_fields__)
    return {*said, *told, *filled}


def _written_for(
    cls: type,
    record: Any,
    filled: dict[str, Any],
    *,
    updating: bool = False,
    touched: set[str] | None = None,
) -> tuple[list[str], list[str], list[Any]]:
    """
    The columns, their placeholders and the parameters to go with them.

    Placeholders rather than a bare list of values, because a field filled by
    the write may have handed back `Sql` — a clock the database has to evaluate
    for itself — and that belongs in the statement rather than the parameters.

    An insert writes every column because every one of them is new. An update
    writes `touched` and nothing else: a column left out keeps whatever the row
    holds, which is the point — the value this record read minutes ago is not
    news, and sending it back reverts anybody who wrote that column in between.

    A `list` or a `dict` is the one thing psycopg would get wrong on its own. Its
    column is `jsonb`, since those are the two entries in `SQL_TYPES` that map
    there, but left alone psycopg sends a list as a Postgres array and cannot
    adapt a dict at all — so the value is wrapped here the same way the blob
    is, and a column the schema will declare is one a write can fill.
    """
    names, holders, params = [], [], []
    for name in cls.__dray_columns__:
        if updating and (_write_once(cls, name) or name not in touched):
            continue
        value = filled.get(name, _UNSET)
        if value is _UNSET:
            value = getattr(record, name)
        names.append(name)
        if isinstance(value, Sql):
            holders.append(str(value))
        else:
            holders.append("%s")
            params.append(_as_param(value))
    # The document goes whole or not at all. It is one jsonb parameter, so
    # there is no narrowing inside it: any blob field moving sends the lot, and
    # a save touching only columns stops sending it at all — which is most of
    # what this buys, since the blob is where the long text lives. Merging with
    # `data = data || %s` would narrow within the document and has to answer
    # what removing a key means, a field back to `None` being left out rather
    # than written as a null; nobody has asked for it.
    if not updating or not touched.isdisjoint(cls.__dray_blob__):
        names.append(cls.__dray_blob_column__)
        holders.append("%s")
        params.append(jsonb(record._dray_blob()))
    return names, holders, params


def _refuse_derived(
    cls: type, records: Sequence[Any], assigned: dict[str, Any]
) -> None:
    """
    A derived field named in the `given=` this call was passed. Raises to
    refuse, the way the constructor does about the same field.

    `assigned` and not the merged bag, and that distinction is the whole of why
    this can be said at all. A caller writing `save(given={"search_name": ...})`
    has typed the name of this class's field, on this write, deliberately — the
    same mistake as assigning to it, and it hears the same sentence. The store's
    `defaults` are the other half and stay silent: their entire purpose is to be
    applied by name to whatever happens to declare a field of it, so a job whose
    default collides with somebody else's derived field must keep working. The
    value lands there and the handler works the field out anyway, which is what
    it does for a field the caller never named.

    Every class the write touches, because a child carries `given` too and the
    two halves of a write cannot answer this differently — a name refused on the
    person and taken on the note is the drift `normalised` exists to prevent one
    layer down. The queued tree is walked whole, so a grandchild's declaration
    counts as much as its parent's.
    """
    if not assigned:
        return
    kinds = {cls}
    for record in records:
        kinds.update(type(item) for item in _queued(record))
    for kind in kinds:
        for name in assigned:
            rules = kind.__dray_fields__.get(name)
            if rules is not None and rules.get("derived") is not None:
                raise ValidationError(
                    _NOT_YOURS.format(cls=kind.__name__, name=name)
                )


def _filled_by_write(
    cls: type, record: Any, adding: bool, given: dict[str, Any]
) -> tuple[dict[str, Any], Write]:
    """
    Run the `on_add` or `on_save` handler of every field that named one.

    Only one of the two fires — `on_add` when the record is first written,
    `on_save` when it is saved — so a field wanting both says both. A handler
    returning `None` has no opinion and the field keeps what it already had.

    A field the caller named is not asked at all. `on_add` is what the first
    write knows and whoever built the record does not, and an import carrying
    its own timestamps knows better — so what somebody chose
    stands and the handler fills in the rest. A record read back off the table
    has named nothing, which is what keeps `on_save` filling on the ordinary
    save. A derived field cannot reach this: it is refused at every door a
    caller could set it through, so it is never in the said-set and the rule is
    silent about it.

    The values are kept here rather than set on the record, because a handler
    may return `Sql` for the database to evaluate and there is no sensible
    Python value to put on the object until the row comes back.

    The `Write` comes back with them because a `@before_save` is handed it too,
    and it is built here: one object per record per save, made where the field
    handlers need it and carried on to the rule rather than made twice.

    What a handler returns goes through the field's converter, the same as a
    value arriving any other way — so a handler decides *which* value and the
    field decides what shape it takes. `Sql` is exempt and has to be: it is text
    for the statement rather than a value, and `converter=int` would make
    nonsense of `clock_timestamp()`.

    Which is also why `Sql` needs a column and is refused without one. The blob
    is written as a single parameter, so a field inside it has no place in the
    statement for an expression to sit and nothing to return it from. Said here,
    because what a handler hands back is only known when it is called — and said
    at all, because the alternative was `column "seen_at" does not exist` from a
    `returning` naming a field that was never a column, which points at the
    schema, where nothing is wrong.
    """
    write = Write(
        record=record, adding=adding, given=given, was=_was(record, adding)
    )
    # The same set `_told` reads to decide what the write may assign, asked here
    # for the same reason: `whom="System"` passed in and `whom` left alone hold
    # the same value, so the record remembering which of the two happened is the
    # only thing that can tell a chosen value from a default.
    said = getattr(record, "_dray_said", None) or ()
    filled = {}
    for name, rules in cls.__dray_fields__.items():
        handler = rules.get("on_add") if adding else rules.get("on_save")
        if handler is None or name in said:
            continue
        value = handler(write)
        if value is not None:
            if isinstance(value, Sql):
                if rules.get("derived") is not None:
                    raise TypeError(
                        f"{cls.__name__}.{name} is derived and its handler "
                        "returned SQL for the database to work out. A derived "
                        "field is computed in Python from other fields of the "
                        "record, and a value the statement works out never "
                        "lands back on the object — so this field would read "
                        "empty in Python while its row held the value, which is "
                        "the one thing a field kept true about other fields "
                        "cannot be. Return a value from Python, or name on_add "
                        "and on_save instead, which is where an expression for "
                        "the database belongs."
                    )
                if rules.get("stored_in") == BLOB:
                    raise TypeError(
                        f"{cls.__name__}.{name} is stored in the blob and its "
                        f"{'on_add' if adding else 'on_save'} handler returned "
                        "SQL for the database to work out. The blob goes over "
                        "as one parameter, so there is no place in the "
                        "statement for an expression to be evaluated and "
                        "nothing to return it from. Give the field a column by "
                        'dropping stored_in="blob", or return a value from '
                        "Python rather than Sql."
                    )
            else:
                value = convert(name, value, rules)
                object.__setattr__(record, name, value)
            filled[name] = value
    return filled, write


def _was(record: Any, adding: bool) -> Mapping[str, Any]:
    """
    What this record held before this write, as a rule reading `write.was` gets
    it: the values `__setattr__` kept as each field first moved.

    A copy of the accumulator, so a rule cannot reach the record's own through
    it, and read-only, so it cannot reach this one either. One `Write` is built
    per record per save and handed to every attempt of a commit DSQL refuses —
    the object deliberately outlives the replay, since building one per attempt
    is a cost paid on a path that is already losing — so a mapping a rule could
    edit is a mapping the second attempt would be judged against. Refusing the
    assignment says so where it happens; `given` needs no such thing, because
    dray has finished reading that one before the first statement goes.

    **Empty on the write that creates the record.** `__setattr__` remembers an
    assignment whether or not there is a row behind it, so a record built and
    then edited before its `add` has an accumulator like any other — and handing
    those over as prior state would judge a new record against a value that was
    never stored anywhere. A creating write has no *before*, which is what
    `adding` says.
    """
    remembered = None if adding else getattr(record, "_dray_was", None)
    return types.MappingProxyType(dict(remembered) if remembered else {})


def _as_this_write_leaves_them(
    records: Sequence[Any],
) -> list[tuple[Any, dict[str, Any]]]:
    """
    What each of these records now says about the fields it was remembering —
    which, once the statements have gone, is what the row says about them.

    Read here rather than at the commit because that is a moment later, and what
    happens in between is the caller's: a `with` block they own, with their own
    lines in it. Read there, an assignment made after the save and before the
    block ended would be taken for the value that was written.

    Only the fields already being remembered, because those are the only ones a
    later assignment can move without the accumulator noticing — anything else
    starts remembering the moment it is assigned, and what it remembers is this
    value.
    """
    holding = []
    for record in records:
        remembered = getattr(record, "_dray_was", None)
        if remembered:
            holding.append(
                (record, {name: getattr(record, name) for name in remembered})
            )
    return holding


def _forget_what_they_were(
    stored: Sequence[tuple[Any, dict[str, Any]]],
) -> None:
    """
    The prior values dropped, now that the rows are durable, and what the write
    stored put in their place where the record has moved on since.

    At the moment `@after_commit` runs, and for the reason the page gives for
    the transient this replaces. Anywhere earlier and the replay of a refused
    commit would judge the second attempt against a record that had already
    forgotten what it said; anywhere later and the next save of the same object
    would be judged against the state before the last one, so the owner who
    wrote it first would go on being the only one who could.

    Which also leaves them standing when a block rolls back, and that is right:
    the work is going to be run again and it is still the same write.

    The half that is not dropping anything is what keeps *before this write*
    meaning the row rather than the object. Inside a block, the commit is as far
    from the write as the caller's remaining lines, and a field assigned across
    that gap holds a value no row ever took — so what goes back under its name
    is what the write did store, and the next save is judged against that.
    """
    for record, written in stored:
        remembered = getattr(record, "_dray_was", None)
        if not remembered:
            continue
        for name, value in written.items():
            if getattr(record, name) == value:
                remembered.pop(name, None)
            else:
                remembered[name] = value


def _queued(record: Any) -> list[Any]:
    """Everything waiting on this record, however far down. A note costs a row
    and so does the attachment queued on it, and what decides how many records
    fit in a transaction is rows."""
    found = []
    for items in (getattr(record, "_dray_sets", None) or {}).values():
        for item in items._pending():
            found.append(item)
            found.extend(_queued(item))
    return found


def _settle(record: Any) -> None:
    """The queue emptied, all the way down. Missing a level leaves a grandchild
    queued against a note that has been written, so the next save of that note
    writes it a second time."""
    for items in (getattr(record, "_dray_sets", None) or {}).values():
        for item in items._pending():
            _settle(item)
        items._settled()


def _queued_below(record: Any) -> list[tuple]:
    """
    Every child set below this record, with what is waiting in each.

    Taken before a write empties them, so that a transaction which rolls back
    can put them back. Nothing else can: a queued child has no row to be read
    again from, so `_settle` on a write that never committed is the one loss
    here that re-reading cannot undo.
    """
    saved = []
    for items in (getattr(record, "_dray_sets", None) or {}).values():
        pending = items._pending()
        saved.append((items, pending))
        for item in pending:
            saved.extend(_queued_below(item))
    return saved


def _requeue(saved: Sequence[tuple]) -> None:
    """`_queued_below` put back, after a rollback."""
    for items, pending in saved:
        items._requeue(pending)


@contextlib.contextmanager
def _rewinding(
    prepared: Sequence[tuple], children: list[tuple]
) -> Iterator[bool]:
    """
    One attempt's worth of what a `@before_save` queued, taken back off when
    the attempt fails.

    Yields whether any rule can run at all, which is the same question
    `_before_saving` asks and is asked here because the answer is needed before
    the rules rather than during them. Nothing declared means nothing to
    snapshot and nothing to put back, so a bulk write of records that marked no
    method pays the lookups it already paid and no walk.

    A rule runs once per attempt, and everything it queues has to go with the
    attempt that ran it. Three things accumulate otherwise: the queue on the
    record, the queue on any child below it, and `children` itself, which was
    built once outside the replay and is handed to every attempt. All three are
    put back to what they were when this attempt opened — the sets by
    assignment rather than by merging, because a set the rule reached for the
    first time was made while it ran and does not appear in the snapshot at
    all, and clearing it is exactly right.

    Not `_requeue`, which merges and is the tool for the other direction: there
    the queue is being restored after being emptied and anything queued since
    has to survive, where here the queue is being trimmed back and what was
    added is what has to go.
    """
    rules = any(
        declares(record, BEFORE_SAVE) for record, *_ in prepared
    ) or any(declares(item, BEFORE_SAVE) for _, item, *_ in children)
    if not rules:
        yield False
        return

    records = [record for record, *_ in prepared]
    baseline = len(children)
    held = {
        id(items): list(pending)
        for record in records
        for items, pending in _queued_below(record)
    }

    def rewind(record: Any) -> None:
        for items in (getattr(record, "_dray_sets", None) or {}).values():
            items._adding[:] = held.get(id(items), ())
            for item in items._adding:
                rewind(item)

    try:
        yield True
    except BaseException:
        del children[baseline:]
        for record in records:
            rewind(record)
        raise


def _undone(saved: Sequence[tuple]) -> None:
    """
    A write's bookkeeping put back, because the block it was in rolled back.

    The etag and the queued children. Those two because they are what makes a
    record unusable afterwards: an etag no row carries refuses its own next
    save, and a queued child that was thrown away is gone for good, having never
    been written anywhere to be read back from.

    **Everything else the record holds is left as it stands, and it is now
    ahead of its row.** Every field the caller set is still set — a
    `ticket.status = "closed"` before a block that rolled back is a record
    still reading `"closed"` against a row that says `"open"`. So is anything
    an `on_add` or `on_save` handler filled in, and anything the database
    computed and handed back through `returning`.

    That is deliberate for the caller's own values, which dray never knew the
    prior state of and which the work being run again wants as intended rather
    than as stored. For the handler-filled ones it is a consequence rather than
    a decision, and the reason the page says to read the records again rather
    than reuse the ones in hand: putting two of them back does not make the
    object true, it makes it usable.

    What is deliberately not put back is the collection a record was attached
    to. A rolled-back `add` leaves an object that believes it came from a store
    it has no row in — which reads badly and is the lesser of the two, because
    detaching it would take `person.notes` away from a caller who is holding it
    and about to try again.
    """
    for record, stored, children in saved:
        object.__setattr__(record, record.__dray_etag__, stored)
        _requeue(children)


def _descendants(cls: type) -> list[tuple[type, ...]]:
    """
    Every kind of record below this one, deepest first, each as the chain of
    classes from the record down to it.

    The chain rather than the class alone, because there is no foreign key and
    nothing in an attachment's row says which person it belongs to — only which
    note. Reaching it from the person means going through the note, and the
    chain is what says how.

    Classes rather than the table names the delete is written out of, because a
    statement crossing three tables needs each one's own answer about what its
    key and its parent columns are called, and only the class has those.

    A class that is somehow its own descendant is walked once. Nothing in
    `@child` can build that, since the class being declared is new and has no
    children yet, but a walk that would not end is a poor thing to leave to
    trust.
    """
    from dray.child import CHILDREN

    found: list[tuple[type, ...]] = []

    def walk(parent: type, chain: tuple[type, ...], seen: frozenset) -> None:
        for kind in CHILDREN.get(parent, ()):
            if kind in seen:
                continue
            below = (*chain, kind)
            found.append(below)
            walk(kind, below, seen | {kind})

    walk(cls, (cls,), frozenset({cls}))
    # Deepest first, so an attachment goes before the note it hangs off and
    # nothing is orphaned by the row that would have found it being gone.
    return sorted(found, key=len, reverse=True)


def _chains_through(parent: type, cls: type) -> list[tuple[type, ...]]:
    """
    Every generation a set removal has to walk, deepest first: the chains
    hanging off this parent class that pass through this kind of child, and none
    of the parent's other children.

    Rooted at the parent rather than at the child, because `_cascade` writes one
    `in (select ...)` per level down from whatever the key it is given belongs
    to — and the key a set removal has is the parent's, which is the whole of
    the scoping. So every chain starts a level below the parent and the parent's
    own row is in none of the statements they become, and the shortest of them
    is always the children themselves.
    """
    return [
        chain
        for chain in _descendants(parent)
        if len(chain) > 1 and chain[1] is cls
    ]


def _under(chain: Sequence[type], keys: str = "in (%s)") -> str:
    """
    What places the last generation of a chain under the record at its head:
    everything after the `where`, with that record's key as the only parameter.

    `(Person, Note, Attachment)` becomes

        parent_type = 'note' and parent_id in (
          select id from note where parent_type = 'person' and parent_id in (%s))

    One `in (select ...)` per level, so the depth of the chain is the depth of
    the statement and nothing else about it changes. Every name in it comes off
    a declaration, the same as any statement here interpolates.

    `keys` is how the record at the head of the chain is matched, and the only
    part of this that ever varies. One key is `in (%s)`, which is what a delete
    and a clear both have. A pass that has already read the rows it is taking
    has a list instead and asks for `= any(%s)`, which is the same predicate
    over as many heads as it read.
    """
    ids = keys
    for depth in range(1, len(chain) - 1):
        step = chain[depth]
        ids = (
            f"in (select {step.__dray_key__} from {step.__dray_table__}"
            f" where {step.__dray_parent_type__}"
            f" = '{chain[depth - 1].__dray_table__}'"
            f" and {step.__dray_parent_id__} {ids})"
        )
    last = chain[-1]
    return (
        f"{last.__dray_parent_type__} = '{chain[-2].__dray_table__}'"
        f" and {last.__dray_parent_id__} {ids}"
    )


def _cascade(chain: Sequence[type], keys: str = "in (%s)") -> str:
    """
    The delete for one kind of record below another, given the chain down to it.

        delete from attachment
         where parent_type = 'note' and parent_id in (
           select id from note where parent_type = 'person' and parent_id in (%s))

    Every row it reaches goes, however many that is — which is what makes a
    delete one statement per generation and what makes it unsized.

    `keys` is `_under`'s, and says whether the chain hangs off one key or off a
    list of them.
    """
    return (
        f"delete from {chain[-1].__dray_table__}"
        f" where {_under(chain, keys)}"
    )


def _thinning(chain: Sequence[type], at_a_time: int) -> str:
    """
    The same delete, bounded to a number of rows.

        delete from attachment where id in (
          select id from attachment
           where parent_type = 'note' and parent_id in (
             select id from note
              where parent_type = 'person' and parent_id in (%s))
           limit 500)

    The bound goes on a subselect over the generation's own table rather than on
    the delete, which has no `limit` to take. Reads are not capped by DSQL, so
    scanning four thousand rows to take five hundred of them costs the pass
    nothing against the ceiling — only the delete's own rows count.

    In no particular order, because there is none to want: a pass says how many
    rows it takes and the loop around it says the rest, and an `order by` here
    would sort a set that is going away entirely. Which rows a pass takes is
    the one thing this deliberately does not promise.
    """
    last = chain[-1]
    return (
        f"delete from {last.__dray_table__}"
        f" where {last.__dray_key__} in ("
        f"select {last.__dray_key__} from {last.__dray_table__}"
        f" where {_under(chain)}"
        f" limit {int(at_a_time)})"
    )


def collection(*, of: type) -> Callable[[_Class], _Class]:
    """
    Write down the vocabulary a record has of its own.

        @collection(of=Event)
        class Events:
            def upcoming(self) -> list[Event]: ...

    Declared after the record rather than named inside it, because a collection
    needs the class it is for and the class would then need the collection —
    the same reason `@child` names its parent rather than the other way round.
    """

    def wrap(cls: _Class) -> _Class:
        # Rebuilt on `Collection` rather than made to inherit it, so the class
        # you write stays a plain one with no import and no base to remember.
        # The rebuild is dray's doing, so what the author wrote has to survive
        # it: the bases come across in the order they were written and
        # `Collection` goes behind them, which puts a base class's own `find`
        # in front of dray's the way Python's method order says it should be.
        #
        # Two traps. `type()` refuses a base named twice, so the documented
        # `class Events(Collection)` needs the `issubclass` — and `issubclass`
        # rather than `is`, for a collection built on another collection. And
        # `__bases__` rather than `__orig_bases__`, which holds `Mixin[int]`
        # for a generic base and is not something `type()` will resolve.
        # Mutating in place is not the way out either: assigning `__bases__`
        # on a class whose only base is `object` is a deallocator mismatch.
        members = {
            name: value
            for name, value in cls.__dict__.items()
            if name not in ("__dict__", "__weakref__")
        }
        bases = tuple(base for base in cls.__bases__ if base is not object)
        if not any(issubclass(base, Collection) for base in bases):
            bases = (*bases, Collection)
        built = type(cls.__name__, bases, members)
        COLLECTIONS[of] = built
        return built

    return wrap


def cached_for(
    seconds: float, *, cache_most: int = CACHE_MOST
) -> Callable[[Callable], Callable]:
    """
    Keep what this method answers, for this many seconds.

        @collection(of=Lookup)
        class Lookups:
            @cached_for(1800)
            def by_name(self, name: str) -> Lookup | None:
                return self.find_first(equals={"name": name})

    A question cache, kept under the arguments the method was called with, so
    the same question asked again inside the lifetime is answered without
    reaching the database at all. The method is yours and dray never looks
    inside it: whatever it returns is what is kept and what later callers get.

    **Nothing evicts this.** dray drops the keys its own writes touched because
    a key is a thing a write can be matched back to, and a question is not — a
    write to any record can change what a method of yours answers and dray has
    no way to know which. So the lifetime is the whole of what makes it safe,
    and it is yours to choose because you are the one who knows what staleness
    this answer tolerates: a summary that costs four seconds to build is very
    often fine a minute old, and a balance is not fine ten seconds old.

    Every caller is handed its own copy of the answer, for the reason `by_id`
    hands out its own record: a shared object is one that a caller can edit
    underneath the next. So what a method returns has to be copyable, which
    everything a collection method sensibly answers with is — records, dicts,
    lists, numbers.

    **The arguments are the key, so they have to be hashable.** A call with a
    list or a set in it is refused where it is written rather than quietly
    going to the database every time: a method with a lifetime on it is one
    somebody found expensive, and a call that silently opts out of the cache is
    the kind of thing nobody finds for a year. Pass a tuple or a frozenset, or
    take the decorator off.

    On a collection's methods and nowhere else. A method on a record is about
    one row, which is the thing `by_id` already keeps and the one thing a write
    *can* be matched back to — so the answer there is `cached_for=` on the
    record rather than a second cache under a different key.

    Where it is kept is what dray adds over reaching for a cache library
    directly: it goes with the rest on the pool, so every store shares it, one
    `pool.forget_all()` empties it along with everything else, and a test gets
    a clean one rather than a module-level map that outlives the process's
    stores. A hit is also a `cache` span, the same one a row answered out of
    memory opens, so a question the cache answered is a node in a trace rather
    than an absence from one.

    `cache_most` is how many answers are kept, and is a backstop against a
    method taking an argument nobody bounded rather than a number anybody
    tunes.
    """
    if callable(seconds):
        raise TypeError(
            "cached_for takes a number of seconds a method's answer may be "
            "kept for, so it is `@cached_for(1800)` rather than "
            "`@cached_for`. There is no sensible default for it: the number "
            "is the whole of what makes a question cache safe."
        )

    def wrap(method: Callable) -> Callable:
        whose = getattr(method, "__qualname__", getattr(method, "__name__", "?"))
        ttl = _checked_ttl(whose, seconds)
        most = _checked_most(whose, cache_most)
        # The parameter names, read once at the declaration so that a refusal
        # can name the argument rather than its position. Never read at the
        # call, where it would be a signature lookup on a path that exists to
        # be fast.
        names = tuple(inspect.signature(method).parameters)[1:]

        @wraps(method)
        def asking(self: Any, *args: Any, **kwargs: Any) -> Any:
            if not isinstance(self, Collection):
                raise TypeError(
                    f"@cached_for is on {whose}, which is not a method of a "
                    "collection. A method on a record is about one row — "
                    "which is the thing `cached_for=` on the record already "
                    "keeps, and the one thing a write can be matched back to, "
                    "so a second cache in front of it would be two answers "
                    "about one row with two lifetimes."
                )
            # Before the store is asked anything, so a call this could not
            # remember is refused the same way inside a transaction as outside
            # one. An argument that is a key here and not there would be a
            # refusal nobody could reproduce.
            key = _asked_under(whose, names, args, kwargs)
            kept = self.store._asking(type(self), method, ttl, most)
            if kept is None:
                return method(self, *args, **kwargs)
            # Whether this call is the one that ran the method, which is the
            # only thing that separates a hit from a miss here: dray never
            # looks inside a method of yours, so there is nothing else to read
            # the outcome off.
            ran: list[Any] = []

            def ask() -> Any:
                ran.append(True)
                return method(self, *args, **kwargs)

            watch = self._watch
            # What answering out of memory cost, as `by_id` asks for it: the
            # wait behind another thread's call is the half of it a method of
            # yours cannot be timed for from out here.
            took: list[int] | None = [] if watch else None
            answer = kept.get(key, ask, copies=True, took=took)
            if ran:
                return _rebound(self.store, answer)
            # A hit is a hit whichever cache answered — the same span a row
            # answered by `by_id` opens, for the reason `cache_info` adds the
            # two together: *is anything here being served from memory* is one
            # question, and two names for it would be two things to add up.
            # The label is what separates them once they are drawn, since `cls`
            # here is the record class and says the same thing on both.
            # `_rebound` is inside because it is the rest of what the hit cost.
            with watch.span(
                "cache",
                label=whose,
                cls=self.cls,
                ago=took[0] if took else 0,
            ):
                return _rebound(self.store, answer)

        # What `cache_info` reads to tell a collection that keeps answers and
        # has not been asked one yet from a collection that keeps nothing. A
        # map is not made until the first call, so nothing else knows.
        asking.__dray_cached_for__ = ttl
        return asking

    return wrap


def _asked_under(
    whose: str, names: Sequence[str], args: tuple, kwargs: dict
) -> tuple:
    """What a call to a `@cached_for` method is remembered under.

    Keyword arguments are sorted, so the same call spelled two ways is one
    entry. Which is as far as this goes: a positional argument and the same
    value passed by name are two keys and two round trips, and matching them up
    would mean binding the signature on every call to save a duplicate nobody
    writes."""
    key = (args, tuple(sorted(kwargs.items())))
    try:
        hash(key)
    except TypeError:
        raise TypeError(_not_a_key(whose, names, args, kwargs)) from None
    return key


def _not_a_key(
    whose: str, names: Sequence[str], args: tuple, kwargs: dict
) -> str:
    """Which argument cannot be a key, said by name and by type.

    The type rather than the value, for the reason `_clash` leaves values out
    of what it raises: an argument is as likely to be somebody's data as
    anything else dray refuses, and a message is a thing that gets logged.
    """
    given = [
        (names[at] if at < len(names) else f"argument {at + 1}", value)
        for at, value in enumerate(args)
    ]
    given += sorted(kwargs.items())
    called, was = "an argument", None
    for called, was in given:
        try:
            hash(was)
        except TypeError:
            break
    return (
        f"{whose} is @cached_for, so what it is called with is what its answer "
        f"is kept under — and {called} was given a {type(was).__name__}, which "
        "cannot be a key. Pass something hashable in its place: a tuple rather "
        "than a list, a frozenset rather than a set. It is refused rather than "
        "passed through, because a call that quietly went to the database every "
        "time is the kind of thing nobody finds for a year."
    )


def _rebound(store: Any, answer: Any) -> Any:
    """
    Point every record in a kept answer back at the store that asked for it.

    A record carries the collection it was read through, and a collection
    carries a connection. An answer computed on one store and handed to a
    caller on another would arrive holding the first store's connection — which
    by then has gone back to the pool and may be in another thread's hands, so
    `served.save()` or `served.notes` would put statements down a connection
    somebody else is using. That is two threads on one connection in exactly the
    shape this feature exists for: warm on a fan-out, read straight afterwards.

    `by_id` never has this problem, because what it keeps is a row and every
    hydrate binds the record it builds to the store doing the building. This is
    the same binding, applied to whatever a method of yours happened to return.

    Containers are walked, since an answer is as likely to be a list of records
    or a mapping of them as one, and anything that is neither is left exactly as
    it is. The walk is over an answer somebody was about to be handed anyway, so
    it costs a pass over something already in memory against a round trip.
    """
    seen: set[int] = set()
    reached: dict[type, Collection] = {}

    def walk(value: Any) -> None:
        if id(value) in seen:
            return
        seen.add(id(value))
        cls = type(value)
        if hasattr(cls, "__dray_table__"):
            if cls not in reached:
                reached[cls] = _reachable(store, cls)
            object.__setattr__(value, "_dray_collection", reached[cls])
        elif isinstance(value, Mapping):
            for one in value.values():
                walk(one)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for one in value:
                walk(one)

    walk(answer)
    return answer


def _reachable(store: Any, cls: type) -> "Collection":
    """The collection this store reads that class through.

    The store's own where the class named a collection and the store resolves
    that name to this class, so a rebound record ends up holding the same
    object a `by_id` on that store would have given it. A `@child` declared
    without `collection=` has no such name and gets one built for it, which is
    what reading it through its parent does anyway.
    """
    name = cls.__dray_collection__
    if name:
        found = getattr(store, name, None)
        if found is not None and found.cls is cls:
            return found
    return _collection_for(store, cls)


def _keeps_answers(cls: type) -> bool:
    """Whether this collection class has any `@cached_for` method at all, asked
    of the class rather than of what has been called — so `cache_info` can tell
    a collection that keeps answers and has been asked none from one that keeps
    nothing."""
    return any(
        getattr(member, "__dray_cached_for__", None)
        for base in cls.__mro__
        for member in vars(base).values()
    )


def _collection_for(store: Any, cls: type) -> Collection:
    return COLLECTIONS.get(cls, Collection)(store, cls)
