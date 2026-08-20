"""
Declaration: what a record is, before anything touches a database.

Nothing in this module knows about SQL, connections or transactions. A record is
a dataclass that has been told which of its fields earn columns, what they will
accept, and what to call when one of them moves.
"""

import copy
import dataclasses
import sys
import types
import typing
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import MISSING
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeVar, dataclass_transform

from uuid import UUID, uuid4

from dray.hooks import CHECK, declared_on, run

BLOB = "blob"
COLUMN = "column"

# The plain words dray's own columns are called when nobody says otherwise.
# Every one of them is a role with an option naming the column that fills it —
# `@record(key="ref", blob="payload")` — so these are defaults and never
# answers. What a given class calls each is on the class, in `__dray_key__` and
# its neighbours, because that is the only place that can be right for two
# records that made different choices.
#
# Plain words rather than `dray_id` and `dray_etag` because a table is read by
# people who never use dray: an analyst at a psql prompt, a reporting job, the
# service next door. None of them should have to read the machinery to find the
# data, so the machinery is what wears a prefix on the rare class that needs one.
KEY = "id"
BLOB_COLUMN = "data"

# How a child names its parent. Every child table carries both, whatever it
# hangs off, so `of=` mostly decides which records get an accessor rather than
# what the table can hold — attaching a child to one more record keyed the way
# these are is a change to a declaration rather than a migration. `parent_id`
# holds the key it points at and is typed as that key, which is the one thing
# `of=` cannot paper over: records keyed differently want a child table each.
PARENT_TYPE = "parent_type"
PARENT_ID = "parent_id"

# Collection name to record class, filled in by `@record`. A store resolves
# `store.people` through here, so a record is reachable from the moment its
# module is imported and nothing has to be registered by hand.
RECORDS: dict[str, type] = {}

# The one thing a field may not be called, and it is a prefix rather than a list
# of words. Everything dray keeps on a record lives under it: the second spelling
# of each member a caller is meant to use, the three it builds and stores a
# record with, the backref a save reaches storage through, and `_dray_stored`,
# which is a keyword rather than a member — `_dray_load` passes it to the
# constructor to say the values came out of the table and must not be converted
# again.
#
# A field under the prefix collides with one of those without a word being said,
# and `_setattr` would not catch it either: a name under the prefix goes
# straight to the object, unconverted and unchecked. No plain word is refused
# outright. `save`, `children` and the rest are the domain's for the taking, and
# the columns dray owns are the class's answer rather than a fixed word, so
# `_declare` asks the decorator what it was told about those instead.
RESERVED_PREFIX = "_dray_"

# The column carrying the guard against a stale write, when nobody moves it.
# dray owns the column wherever it stands: minted fresh on every write and never
# set by anyone. `save(etag=...)` is safe to spell out on a record carrying an
# `etag` field of its own, since the values a write assigns are inside `given`
# and nothing beside it can be read for one.
ETAG = "etag"

# How many of a cached record's rows are kept, where the class says nothing. A
# backstop against a key space nobody bounded rather than a policy anybody
# tunes: the TTL is what decides how stale a row may be, and this only decides
# which rows are given up when a class has more of them in play than anyone
# expected. `cache_most=` moves it for the record that needs it moved.
CACHE_MOST = 1_000

# What each of dray's own columns is for, in the words a refusal uses. The key
# is absent because it is never refused: it is the one dray hands over outright.
FILLS: dict[str, str] = {
    "etag": "the guard against a stale write",
    "blob": "the jsonb column holding every field without one of its own",
    "parent_type": "the table a child's parent lives in",
    "parent_id": "the key of a child's parent",
}


class DrayError(Exception):
    """
    Anything dray raises on purpose.

    One name to catch when what you want is "dray said no" rather than one
    particular no — a request handler turning any of them into a 400, a job
    logging and moving on. Without it every `except` has to list the lot, and
    every list written today misses whatever the next version adds.

    It is a second base rather than a replacement, and every exception below
    keeps the builtin it already had. `ValidationError` is still a
    `ValueError`, `RecordNotFound` is still a `LookupError`, and
    `ConnectionLost` is still a `psycopg.OperationalError` — because code that
    handles a bad value or a dead connection generally is right to catch this
    one too, and should not have to know dray is underneath. The dray name is
    for the caller who does know.

    Lives here rather than beside the rest in `store.py` because a validation
    error is raised before anything has touched a database, and this module is
    the one every other module already imports.

    `written` is on every one of them, and holds the keys of the records that
    had already landed when the write stopped. A set above the row ceiling is
    several transactions, so a failure partway leaves the chunks before it
    committed and not coming back — and *a failure partway leaves the earlier
    ones committed* is a true sentence a caller can do nothing with unless dray
    says which. It is empty everywhere nothing landed, which is most places,
    and empty is the honest answer there rather than a missing attribute.

    Here rather than a keyword on the ones a bulk write can raise,
    because the alternative is a caller having to know which of them carries it
    before writing `except`. An attribute that is sometimes absent is worse
    than one that is usually empty.
    """

    # A class attribute rather than something every `__init__` has to take:
    # what fills it is the loop that knows how far it got, and no raise site
    # does. Assigned per instance there, so this is only ever the default.
    written: tuple = ()


class ValidationError(DrayError, ValueError):
    """A value a field will not accept."""


def new_etag() -> str:
    """
    A fresh token for the stale-write guard.

    A string, and deliberately nothing more. dray reasons about nothing except
    whether two of them are equal, so what one is made of is free to change — a
    uuid today, a clock reading or a digest tomorrow — without anything that
    holds one having to care. It goes out to a browser and comes back as text,
    and it is never an identity.
    """
    return str(uuid4())


def as_uuid(value: Any) -> UUID:
    """
    A `UUID`, from one or from the string a URL hands you.

    Everything downstream of a request is text, so a record whose id is a `UUID`
    still has to be findable from `request.args["id"]`. Handed one already, it
    passes straight through — `UUID(a_uuid)` raises rather than being a no-op.

    Anything else is refused here rather than left to `UUID` to complain about,
    because it would say `'int' object has no attribute 'replace'`, which tells
    whoever passed the integer nothing at all.
    """
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        return UUID(value)
    raise TypeError(f"a UUID or its text, not {type(value).__name__}")


def key_of(record: Any) -> Any:
    """
    The key of a record, whatever this class calls the column.

        dray.key_of(person)

    For code that works across record types and still needs the key — an admin
    screen, a serialiser, an audit log. On a class that said `key="ref"`,
    `person.id` is somebody's employee number and this is dray's key.

    Domain code should keep saying `person.id`, because it knows what it is
    holding. A function rather than a member so that it costs a record no name.
    """
    return getattr(record, record.__dray_key__)


class Names:
    """
    The names dray owns on one record class, for a statement you wrote.

    The seven a collection publishes — `table`, `columns`, `blob`, `id`,
    `etag`, `parent_type` and `parent_id` — read off the class instead. Every
    one of them is the same string the collection hands back, and follows a
    rename the same way. `sql_for` is the eighth thing a statement is built
    out of and the only one that is about a field of yours rather than a name
    dray owns.

    `dray.names_of` is the door; this is what comes back, for annotating.
    """

    __slots__ = ("_cls",)

    def __init__(self, cls: Any) -> None:
        # A record answers as its class does, because the names are a fact
        # about the class either way. Which is why an instance is taken here
        # where `find(parent_type=...)` refuses one: there an instance would
        # have quietly meant every walker's rather than this one's, and here
        # the two readings are the same string.
        held = cls if isinstance(cls, type) else type(cls)
        if not hasattr(held, "__dray_table__"):
            raise TypeError(
                f"names_of takes a record class, or a record, not {cls!r}. "
                "The names come off a class @record or @child has been run "
                "over, which is what writes them onto it — pass Person, or a "
                "person, rather than a class dray has never met."
            )
        self._cls = held

    # Every property below reads its dunder and works nothing out. `_declare`
    # settles each of these names once, out of what the decorator was told, and
    # a second reading of the same question is a second answer waiting to
    # disagree with the statements dray writes for itself.

    @property
    def table(self) -> str:
        """The table name, for a statement of your own. Bare, never qualified
        by a namespace: a store's schema is a `search_path` set once on the
        connection, and dray writes an unqualified name into every statement
        it makes."""
        return self._cls.__dray_table__

    @property
    def blob(self) -> str:
        """The jsonb column, for a statement of your own that reaches inside
        it. Named here so nobody has to remember what it is called."""
        return self._cls.__dray_blob_column__

    # The four below are the same offer as `table` and `blob`: a name dray owns
    # rather than a field, so a statement somebody wrote follows a rename where
    # the word typed into the f-string would not.

    @property
    def id(self) -> str:
        """The key column, for a statement of your own."""
        return self._cls.__dray_key__

    @property
    def etag(self) -> str:
        """The stale-write guard's column."""
        return self._cls.__dray_etag__

    @property
    def parent_type(self) -> str:
        """The column naming a child's parent's table. A record that is not a
        child has no such column, and asking raises."""
        _check_parented(self._cls, "parent_type")
        return self._cls.__dray_parent_type__

    @property
    def parent_id(self) -> str:
        """The column holding a child's parent's key. A record that is not a
        child has no such column, and asking raises."""
        _check_parented(self._cls, "parent_id")
        return self._cls.__dray_parent_id__

    @property
    def columns(self) -> str:
        """Every column, for a select. Built from the class rather than typed
        out, because a hand-copied list is a field that silently stops being
        read."""
        return ", ".join([*self._cls.__dray_columns__, self.blob])

    def sql_for(self, name: str) -> str:
        """
        One declared field, as the SQL that reads it.

            names.sql_for("family_name")   # 'family_name'
            names.sql_for("suburb")        # "(data->>'suburb')::text"

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
        cls = self._cls
        if name not in cls.__dray_fields__:
            raise ValueError(
                f"{cls.__name__} has no field {name!r} to read. The SQL is "
                "built from what the class declares, so a name it has never "
                "heard of has neither a column nor a blob key to name."
            )
        if name not in cls.__dray_blob__:
            return name
        # Imported here rather than at the top, because `schema` imports this
        # module: the types and the two encodings live where the `create table`
        # is built, and this is the same table read a second way rather than a
        # copy of it.
        from dray.schema import _field_sql

        return _field_sql(cls, name)[1]


def _check_parented(cls: type, column: str) -> None:
    """
    Refuse a question about a parent column on a record that has no parent.

    `_declare` puts both names on every class, child or not, because it builds
    the two the same way and the names are what the decorator was told rather
    than what the table has. So without this `store.things.parent_type` would
    answer with the word `parent_type`, and the idiom the manual teaches —
    `f"where {c.parent_type} = %s"` — would go to the database and come back
    with *column does not exist*, about a column nobody asked for. The class
    knows the answer here, so it is said here.
    """
    if not cls.__dray_parents__:
        raise TypeError(
            f"{cls.__name__} is not a child, so its table has no {column} "
            "column to name. Only a @child carries the two columns naming a "
            "parent."
        )


def names_of(cls: Any) -> Names:
    """
    The names dray owns on a record class, for a statement you wrote.

        dray.names_of(Person).table      # 'person'
        dray.names_of(Person).columns    # every column, for a select

    The seven a collection publishes — `table`, `columns`, `blob`, `id`,
    `etag`, `parent_type` and `parent_id` — for code holding classes rather
    than collections: a report assembled across record types, a test emptying
    every table it made. A `@child` declared without `collection=` has no
    collection to be asked at all, and most children are declared that way.

    Takes a record as happily as its class, because the answer is a fact about
    the class either way. A function rather than a member so that it costs the
    record no name — `Person.table` is the domain's if the class declared a
    field called `table`, and `person.id` is already the key's *value*, where
    this hands back the key column's *name*.
    """
    return Names(cls)


class Sql(str):
    """
    A value the database works out for itself.

    A handler returns this when the value cannot come from Python — a clock that
    has to advance within a transaction, a sequence, anything the row's own
    insert should compute. dray puts the text into the statement rather than
    passing it as a parameter.
    """

    __slots__ = ()


class Ordered(str):
    """
    A field name with something said about how it is read.

    A value rather than syntax, for the same reason a filter takes one: the
    name still has to be checked against what the class declared, and
    `"written_at desc nulls last"` would have to be pulled apart with string
    work before anything could check it — which is the first step of a query
    language this is not.
    """

    # The one marker in this file without `__slots__`, because a str subclass
    # will not take a non-empty one and the placement has to live somewhere. A
    # dict per term named on a call is cheaper than the alternative, which is a
    # class per direction and placement, or a name that has stopped being a str
    # and has to be unwrapped everywhere it is checked.

    def __new__(cls, name: str, *, nulls: str | None = None) -> "Ordered":
        if nulls not in (None, "first", "last"):
            raise ValueError(
                f"nulls={nulls!r} is not a placement: it is 'first' or 'last'. "
                "Leaving it off takes the database's own, which is nulls last "
                "on the way up and nulls first on the way down."
            )
        term = super().__new__(cls, name)
        term.nulls = nulls
        return term


class Ascending(Ordered):
    """A field name read forwards, saying where its empty values go."""


class Descending(Ordered):
    """A field name to read backwards, and where its empty values go."""


def _nulls(name: Any) -> str:
    """
    The ` nulls first` a term carries, or nothing where it carries none.

    Shared with the index DDL rather than written twice, because placement is
    the one thing a term says in both places — an index key may say it and may
    not say a direction, which is the whole of the difference between them.
    """
    where = getattr(name, "nulls", None)
    return f" nulls {where}" if where else ""


def asc(name: str, *, nulls: str | None = None) -> Ascending:
    """
    Forwards, and where the empty ones go.

        find(order_by=asc("due_on", nulls="first"))     # undated at the top
        index("area_id", asc("due_on", nulls="first"))  # and an index for it

    Forwards is what a bare name already means, so this is worth saying for
    `nulls=` and for nothing else. Without it both PostgreSQL and DSQL put an
    empty value last on the way up, which is what a bare name has always given.
    """
    return Ascending(name, nulls=nulls)


def desc(name: str, *, nulls: str | None = None) -> Descending:
    """
    Newest first, and where the empty ones go.

        @child(of=Person, name="notes", table="note", order_by=desc("written_at"))
        find(order_by=desc("due_on", nulls="last"))     # undated at the bottom

    Wraps one field name, so directions can be mixed where several are given.
    Without `nulls=` both databases put an empty value first on the way down.

    A `desc` term is for `order_by`. An index key may not carry a direction on
    DSQL, so `index(desc("due_on"))` is refused where the class is written.
    """
    return Descending(name, nulls=nulls)


def _ordering(built: type, order_by: Any) -> str:
    """
    The `order by` terms a read carries, from names the class declares.

    One spelling for the two places an order is asked for. A `@child` says it
    once at declaration, because its statement is built for every read of every
    child of every parent and a name the class does not have would fail on all
    of them rather than on the line that got it wrong. `find` says it per call,
    because that is where a sort column chosen by whoever is looking at the page
    arrives — and a name arriving from outside is exactly what wants checking
    against the declaration before it reaches a statement.

    A blob field is refused either way: it has no column to sort on, so it
    cannot be ordered by however much the caller would like to.

    A term that says nothing about nulls emits nothing about them, rather than
    writing the database's own default out loud. The two are the same ordering,
    and the shorter statement is the one every existing caller was already
    getting — saying it out loud would change every plan being read today for
    no answer anybody asked a different question about.

    The key that makes a read total is appended by each caller rather than
    here: `_declare` puts it on the end of what a class declared and keeps the
    whole of it, `find` puts it on the end of what one call named. The key
    belongs to the statement rather than to what the class said about itself,
    and this is only ever asked for the names.
    """
    named = order_by if isinstance(order_by, (tuple, list)) else (order_by,)
    if not named:
        raise TypeError(
            f"{built.__name__} is ordered by nothing. Name a field, or leave "
            "order_by off and take the default."
        )

    terms = []
    for field_name in named:
        if field_name not in built.__dray_fields__:
            raise TypeError(
                f"{built.__name__} is ordered by {str(field_name)!r}, which it "
                "does not declare. Add the field, or order on something it has."
            )
        if field_name in built.__dray_blob__:
            raise TypeError(
                f"{built.__name__} is ordered by {str(field_name)!r}, which is "
                "stored in the blob and has no column to sort on. Give it a "
                'column by dropping stored_in="blob", or order on something else.'
            )
        direction = " desc" if isinstance(field_name, Descending) else ""
        terms.append(f"{field_name}{direction}{_nulls(field_name)}")
    return ", ".join(terms)


class AnyOf(tuple):
    """
    Several values where a filter takes one.

    A value rather than syntax, for the same reason `Descending` is one. A bare
    list cannot mean this: `find(equals={"tags": ["a", "b"]})` already means
    *tags equals that list*, on a blob field and on a `jsonb` column alike, so
    a bare list would mean "equals" for a list-valued field and "one of" for a
    scalar one — the same call site with opposite meanings, chosen by an
    annotation the reader cannot see.
    """

    __slots__ = ()


class NoneOf(tuple):
    """
    Several values a field must match none of.

    A value rather than syntax for the same reason `AnyOf` is one, and read the
    same way in both the places a filter is answered.
    """

    __slots__ = ()


def _members(values: tuple[Any, ...]) -> tuple[Any, ...]:
    """
    What a filter helper was handed: several values loose, or one iterable of
    them.

    Shared because the two helpers make the same promise about it and a reader
    comparing them should not have to check that they still agree. A field whose
    own values are lists is the only ambiguous case, and there the loose form
    says it without room to doubt.
    """
    if len(values) == 1 and isinstance(values[0], (list, tuple, set, frozenset)):
        return tuple(values[0])
    return values


def any_of(*values: Any) -> AnyOf:
    """
    Equal to any of these.

        store.people.find(equals={"id": any_of(ids)})
        store.people.find(equals={"status": any_of("candidate", "volunteer")})

    Takes them loose or in one iterable, because both read well and the
    ambiguity only arises for a field whose values are themselves lists — where
    `any_of(a, b)` says it without room to doubt.

    Empty matches nothing, which is what `= any('{}')` does and the right
    reading of "in an empty set". A caller filtering by a list that turned out
    to have nothing in it wants no rows, not every row and not an exception.
    """
    return AnyOf(_members(values))


def none_of(*values: Any) -> NoneOf:
    """
    Equal to none of these, and a field holding nothing counts as one.

        store.tickets.find(equals={"status": none_of("closed", "merged")})

    Loose or in one iterable, the same as `any_of`.

    A record whose field was never set matches. `equals` describes a row rather
    than naming an SQL operator, and `None` in a filter already means *unset*,
    so a ticket nobody has decided about is a ticket whose status is none of
    these. Written by hand it would not be: `status <> all(...)` drops those
    rows, because `null <> 'closed'` is unknown rather than true.

    That leaves one question this cannot ask — *has a status, and it is not one
    of these* — and that one is `select_many` with SQL you wrote.

    Empty matches everything, the mirror of `any_of()` matching nothing. Both
    are the right reading, and together they mean a list that came back empty
    asks for no rows one way round and every row the other.
    """
    members = _members(values)
    if any(each is None for each in members):
        raise ValidationError(
            "none_of does not take None: a field holding nothing already "
            "matches, so a None among the values asks for the opposite of the "
            "rest of the call. Ask for the rows that hold something with SQL "
            "of your own, handed to select_many."
        )
    return NoneOf(members)


@dataclasses.dataclass(frozen=True)
class Index:
    """
    One index a class asks its table to carry.

    A value rather than a bare tuple of names, for the same reason `Descending`
    is one: what makes an index unique travels with the columns it is about, and
    a second list beside the first would be the same sentence said twice with
    one word changed.
    """

    columns: tuple[str, ...]
    unique: bool = False


def index(*columns: str, unique: bool = False) -> Index:
    """
    An index over one column or several.

        @record(table="shift", collection="shifts",
                indexes=[index("on_date", "ward"),
                         index("on_date", "slot", unique=True)])

    The order of the columns is yours and dray keeps it. Only a leading run of
    an index's columns can be searched, so `(on_date, ward)` answers a question
    about a date and one about both, and nothing about a ward on its own. That
    order is chosen against the questions the table gets, which is something
    dray cannot see and does not guess at.

    Which is also why one whose columns are a leading run of another's is
    refused where the class is written: the wider index already answers every
    read the narrower one could, so the narrower one is a slot out of the 23 a
    table has and a write on every insert, bought for nothing. A unique index
    is never the redundant one — it enforces something no wider index does.

    `unique=True` says the columns are unique together, which is a constraint as
    well as an index. Where the DDL for it goes is dray's to decide and depends
    on whether the table exists yet — `schema.create_table` and
    `schema.create_indexes` each say it the way that is valid there.

    A column may say where its empty values go and may not say a direction:

        index("area_id", asc("due_on", nulls="first"))

    which is DSQL's rule rather than dray's. A key takes `nulls first` and
    `nulls last`; `desc` on one comes back `specifying sort order not supported
    for index keys`, so `index(desc("due_on"))` is refused where the class is
    written. Nothing is lost by that — a btree is scanned backwards as readily
    as forwards, so the index a bare name gives already serves the descending
    read. A partial index is refused by the cluster outright and is not
    expressible here or in a migration written by hand.

    Placement and `unique=True` do not go together, and that is dray's own
    limit. The unique kind arrives as a constraint inside a `create table`,
    where `unique (due_on nulls first)` is not a grammar SQL has — so the two
    tables dray writes for would be indexed differently. Drop the placement:
    what a bare name gives is the index the constraint brings anyway, and it
    serves the default ordering forwards and backwards alike.

    The name is dray's — the table and then the columns — and it is cut to the
    63 bytes an identifier holds, which is what the database would have done to
    it anyway. Two on one table that come out of that cut as the same name are
    refused where the class is written, because `create index if not exists`
    would find the first and build the second nowhere.
    """
    if not columns:
        raise ValueError(
            "index() takes the columns it covers, and an index over none of "
            "them is not one. Name at least one field."
        )
    return Index(tuple(columns), unique)


@dataclasses.dataclass(frozen=True)
class Write:
    """
    What is handed to an `on_add`, `on_save` or `derived` function, and to a
    `@before_save`.

    `given` is what the write was told: the store's defaults, then the save, then
    anything said on the record itself. dray never reads it — a field decides
    what it wants out of it, which is why `whom` is not a word dray knows. It is
    a plain dict and the same one for every record in the write; dray has
    finished reading it before the first statement is sent, so a handler writing
    into it changes nothing.

    `record` is what the write is about, which is the half a derived field
    reads: it hands back a value worked out from the record's other fields,
    where a handler like `whom`'s hands back something out of `given`.

    `adding` is true on the write that creates the record and false on a save of
    one that exists. A rule has no other honest way to ask: the etag is minted
    at construction, so a record carries one before its first row.

    `was` is what the record held before this write, for the fields that have
    moved since the row was last written and for no others — `{"owner": "rod"}`
    on a save that gives the record to somebody else, and no key at all for a
    field the caller left alone. `record` is the record as it *will be*, every
    assignment already on it, so a rule about what the row said a moment ago
    reads `write.was.get("owner", record.owner)`: the mapping answers where the
    field moved, and the default answers where it did not, since a field that
    did not move still holds what it held. It is empty on the write that creates
    the record, which has no prior anything, and it is read-only, unlike
    `given` — one `Write` is handed to every attempt of a commit DSQL refuses,
    so a mapping a rule could edit would have the second attempt judged against
    what the first one wrote into it.

    Four things it does not see, and they are on the manual page under *Before a
    record is written*: a blob container edited in place, whatever the write
    itself fills in, the creating write, and a value handed to the constructor.
    """

    record: Any
    adding: bool
    given: dict[str, Any]
    was: Mapping[str, Any]


@dataclasses.dataclass(frozen=True)
class Change:
    """
    What is handed to an `on_change` function.

    An object rather than four arguments so a handler can take what it needs and
    ignore the rest.
    """

    record: Any
    field_name: str
    old: Any
    new: Any


def handlers(owner: str, name: str, kind: str, given: Any) -> tuple:
    """
    One or several, kept as several.

    `validator` and `on_change` both raise or return nothing, so a list of them
    has an obvious meaning: each in turn, in the order written. `on_add` and
    `on_save` hand back a value the write uses, where two of them would be two
    answers and no rule for choosing, so they take one and only one.

    Checked here, when the class is built, because the alternative is a
    `'list' object is not callable` on some unrelated line months later.
    """
    if given is None:
        return ()
    listed = given if isinstance(given, (list, tuple)) else (given,)
    for item in listed:
        if not callable(item):
            raise TypeError(
                f"{owner}.{name} gives {kind} something that cannot be called: "
                f"{item!r}. It takes a function, or a list of them."
            )
    return tuple(listed)


def handler(owner: str, name: str, kind: str, given: Any) -> Any:
    """One, or none. The counterpart to `handlers` for a hook that returns a
    value, where two of them would be two answers and no rule for choosing."""
    if given is None:
        return None
    if isinstance(given, (list, tuple)):
        raise TypeError(
            f"{owner}.{name} gives {kind} a list. It takes one function — two "
            "would each hand back a value, with no rule for which to keep."
        )
    if not callable(given):
        raise TypeError(
            f"{owner}.{name} gives {kind} something that cannot be called: "
            f"{given!r}."
        )
    return given


def normalised(cls: type, equals: Mapping[str, Any]) -> dict[str, Any]:
    """
    A filter, checked against the class and put through its converters.

    One function because there are two places that answer a filter and they
    have to agree: `Collection._conditions` builds SQL from it, and
    `ChildSet._queued` matches it against children held in memory. Those two
    differ in how they compare — a statement against Python — and in nothing
    else, so everything before the comparing belongs here.

    That is not tidiness. Split in two they drift, and every way they drift is
    the same defect: a raw value compared against a converted column, an
    `any_of` converted as though a tuple of values were a value, a name
    silently dropped rather than refused so that a typo finds everything
    instead of nothing. Each is a filter that answers differently depending on
    whether anybody has saved yet, which is the one thing a child set promises
    not to do.

    A name the class does not declare is refused rather than ignored, because
    the alternative is a condition that quietly is not applied. `any_of` and
    `none_of` are checked and converted a member at a time, since they hold
    values rather than being one. `None` is left alone by `convert` itself and
    so arrives here unchanged, which is what keeps `find(equals={"x": None})`
    meaning *is null*.
    """
    wanted = {}
    for name, value in equals.items():
        rules = cls.__dray_fields__.get(name)
        if rules is None:
            raise ValidationError(
                f"{cls.__name__} has no field {name!r} to filter on"
            )
        # Whichever container arrived is the one rebuilt, because which of them
        # it is *is* the condition — converting members out of one and back into
        # the other would answer the opposite question and say nothing about it.
        wanted[name] = (
            type(value)(comparable(name, each, rules) for each in value)
            if isinstance(value, (AnyOf, NoneOf))
            else comparable(name, value, rules)
        )
    return wanted


def comparable(name: str, value: Any, rules: Mapping[str, Any]) -> Any:
    """
    One value from a filter: converted, and then held to the field's type and
    to nothing else the field says.

    The type is the one rule that has to run here. A column holds what the
    annotation allows and nothing else, so a filter of another type is not a
    narrow question but one the table cannot answer — and asked anyway it
    reaches the driver as `operator does not exist: text = smallint`, which
    names neither dray nor the field nor which of the two types the class
    declared. Worse, it is asymmetric: a `str` against a `uuid` column is
    inferred to the column's type and quietly works, so which mistake is fatal
    turns on a column type nobody is thinking about while writing a filter.

    `choices` and the validators deliberately do not run. Loading a row does
    not validate it, so a record written under a rule that has since been
    tightened stays readable — and a filter is how somebody goes and finds
    those rows. `choices` running here would make the rows holding a retired
    status unaskable-for on the day it was retired, and the migration meant to
    fix them could not select them; `find(equals={"family_name": ""})` is
    precisely the query somebody runs after adding `not_blank`.

    The corner where the type is not quite that absolute is a blob field whose
    annotation has changed, since old rows there really can hold the previous
    type and are now out of `find`'s reach. `select_many` with SQL of your own
    is the way out, and it is the one the rest of the equality-only design
    already relies on.
    """
    value = convert(name, value, rules)
    allowed = rules.get("accepts")
    if value is None or not allowed or fits(value, allowed):
        return value
    wanted = " or ".join(dict.fromkeys(kind.__name__ for kind in allowed))
    raise ValidationError(
        f"{name}: a filter is {wanted}, not {type(value).__name__}: {value!r}"
    )


def convert(name: str, value: Any, rules: Mapping[str, Any]) -> Any:
    """
    What a field makes of a value arriving from outside, before anything looks
    at it.

    `None` is exempt, as it is everywhere else — a nullable field with
    `converter=int` should not be asked for `int(None)`.

    What a converter raises about the value comes back as a `ValidationError`
    naming the field, because a converter handed "four" is reporting on the data
    rather than failing. That means the four ways a short converter complains
    about what it was given: `int("four")` raises `ValueError`, `strptime(7, ...)`
    raises `TypeError`, `value.strip()` on an integer raises `AttributeError`,
    and a lookup of an unknown code raises `KeyError`.

    Anything else is left alone. A `NameError` or a misspelled attribute inside
    the converter is a bug in it, and wrapping that up as a rejected value would
    hide the fault behind a message about somebody's data.
    """
    converter = rules.get("converter")
    if converter is None or value is None:
        return value
    try:
        return converter(value)
    except ValidationError:
        raise
    except (ValueError, TypeError, AttributeError, KeyError) as error:
        raise ValidationError(f"{name}: {error}") from error


# What DSQL will take for a `numeric` column, from its own table of supported
# types. Local PostgreSQL takes a precision of up to 1,000, so a column
# declared past this is one more thing local PostgreSQL builds and a cluster
# refuses — except that here the cluster refuses the `create table` rather than
# the value, which is a deployment stopping on a statement dray wrote.
#
# The ceiling is here because refusing a declaration is this module's work. The
# default size a column gets when nobody declares one belongs to
# `schema.SQL_TYPES`, because it is written into a statement and nothing here
# knows about SQL.
MAX_PRECISION = 38
MAX_SCALE = 37


def check_size(precision: int | None, scale: int | None) -> None:
    """
    Whether a size is one a `numeric` column can have. Raises to refuse, like
    everything else `field` decides for itself.

    Answerable here, unlike `check_indexable`, because nothing in it depends on
    the annotation — these are the numbers DSQL takes for a column of that type,
    and whether the field has a column of that type is `check_numeric`'s half.
    """
    if (precision is None) != (scale is None):
        raise ValueError(
            "precision and scale are said together or not at all. A column "
            "given only a precision is `numeric(12)`, which means a scale of "
            "zero and rounds every digit after the point away — which is never "
            "what somebody sizing a column for a rate is asking for."
        )
    if precision is None or scale is None:
        return
    if not 1 <= precision <= MAX_PRECISION:
        raise ValueError(
            f"precision {precision} is outside what DSQL holds in a numeric "
            f"column, which is 1 to {MAX_PRECISION} digits. Refused here rather "
            "than by the cluster, which would refuse the create table and stop "
            "a deployment on a statement dray wrote."
        )
    if not 0 <= scale <= MAX_SCALE:
        raise ValueError(
            f"scale {scale} is outside what DSQL holds in a numeric column, "
            f"which is 0 to {MAX_SCALE} digits after the point."
        )
    if scale > precision:
        raise ValueError(
            f"scale {scale} is more than precision {precision}, and the scale "
            "is part of the precision rather than beside it — a column cannot "
            "keep more digits after the point than it holds altogether."
        )


# What a derived field says at every door a value could arrive through — the
# constructor, `parse` and assignment — because the refusal is one fact and
# hearing it in two wordings reads as two rules. Nothing about the field's
# handler is in it: the caller does not have to know that a column is kept true
# by a function to be told that this one is not theirs.
_NOT_YOURS = (
    "{cls}.{name} is derived: it is worked out from other fields of the record "
    "on every write, so it is not a value to set. One set here would be a claim "
    "about the fields it is computed from that nothing keeps true — set those, "
    "and the write works this out from them."
)


def field(
    *,
    default: Any = None,
    default_factory: Any = MISSING,
    stored_in: str = COLUMN,
    precision: int | None = None,
    scale: int | None = None,
    choices: Collection | Callable[[], Collection] | None = None,
    converter: Callable[[Any], Any] | None = None,
    validator: Callable[[Any], None] | list | tuple | None = None,
    on_change: Callable[[Change], None] | list | tuple | None = None,
    on_add: Callable[[Write], Any] | None = None,
    on_save: Callable[[Write], Any] | None = None,
    derived: Callable[[Write], Any] | None = None,
) -> Any:
    """
    A field that needs saying more than an annotation can.

        suburb: str | None = field(default=None, stored_in="blob")
        status: str = field(default="enquiry", choices=STATUSES)

    One construct for both kinds of storage. `stored_in="blob"` is the opt-out
    of having a column; deleting it is the whole of a promotion, and the rules
    travel with the field because they were never tied to where it lives.

    `choices` is the whole vocabulary a field will take. A collection is fixed
    where it is declared — dray keeps the values rather than the name, so
    appending to `STATUSES` afterwards changes nothing about what is accepted —
    and a vocabulary that moves says so by being a function instead:

        status: str = field(default="enquiry", choices=current_statuses)

    which is asked every time a value is checked. It is handed nothing, so what
    it reads is yours to keep in reach — and it has to be already in memory,
    since one that queries the database is a round trip per assignment. An
    `Enum` class is a collection here rather than a function, and is never
    called.

    `converter` runs first and hands back the value to keep, which is how a
    field takes `"4"` for an `int` — dray never guesses a conversion, but a
    field may say exactly what one is. It runs wherever a value reaches the
    field: `parse`, the constructor, assignment, the value a read filters on,
    what a write is told, and what a handler hands back. The one door it skips
    is `_dray_load`, because a stored row came through a converter already and
    re-running one could stop it loading under a rule tightened since. Which
    means a converter is handed its own output constantly, so it has to accept
    what it returns.

    `on_add` and `on_save` are called once per write, not once per attempt. A
    write DSQL refuses is replayed, and the handlers are not called again, so a
    field may derive its value from what the record currently holds:

        touched: int = field(default=0, on_save=lambda w: w.record.touched + 1)

    counts saves. `on_change` fires on assignment and is further out still,
    which is why a value rejected by a validator never reaches it.

    A field you set is not filled in by either of them. `on_add` says what a
    record's first write knows and the caller does not, and an import carrying
    its own timestamps knows better — so what somebody named at the constructor,
    assigned afterwards or handed `parse` in the data stands, and the handler
    fills in the rest.

    `derived` is a different sentence about a field rather than a third place
    for a handler to live: a value worked out from other fields of the record on
    every write, and nobody's to set:

        search_name: str = field(default="", derived=folded)

    A caller assigning one is refused the way a record's key is, and so is a
    value handed to the constructor, found by `parse`, or named in a write's
    own `given=` — a search name set by hand is a claim about the name it is
    folded from that nothing keeps true, which is the whole of what a derived
    column is for. A store's `defaults` are the exception and stay silent, since
    a bag applied by name to whatever declares a field of it is not somebody
    naming this one.

    It runs on the first write and on every save, so `on_add` and `on_save` say
    nothing beside it, and it may not hand back `Sql`: an expression the
    database works out never lands back on the object, and a field the record is
    wrong about is the one thing this cannot be.

    Indexes are not here. They are said on the decorator with `indexes=`,
    because an index is a fact about the table rather than about one field —
    `index("on_date", "slot")` names two of them, and what a table has spent
    of its index budget is only countable where the whole list is in one place.

    `precision` and `scale` are how big a `Decimal`'s column is, and they are
    the words DSQL's documentation, `information_schema` and the `create table`
    all use:

        rate: Decimal | None = field(default=None, precision=12, scale=8)

    Said or not, that column has a size — DSQL's default is `numeric(18,6)`,
    which is comfortable for money and rounds a rate carried to eight places
    without raising anything. So this is what a field says when six places after
    the point is not what it means. Both together or neither: `numeric(12)` on
    its own means a scale of zero, which rounds away every fractional digit and
    is the opposite of what somebody declaring a size for a rate is asking for.

    Everything is keyword-only. A bare `field("planned")` would leave a reader
    guessing which of six things the string was.
    """
    if stored_in not in (COLUMN, BLOB):
        raise ValueError(f"stored_in must be {COLUMN!r} or {BLOB!r}, not {stored_in!r}")
    check_size(precision, scale)

    # Frozen at the declaration so what a field accepts cannot move underneath
    # it. Mutating the collection that was handed in used to change the
    # vocabulary and rebinding the name did not, the two read identically at a
    # call site, and nothing anywhere said which one had been written — so the
    # capability was real and the only route to it was a trap. A vocabulary
    # that moves is a function instead, which says so out loud. A callable is
    # left as it is, and that covers an `Enum` class: a tuple of the members
    # would stop accepting the values those members hold.
    # A bare string is refused rather than frozen. It is iterable, so freezing
    # it would quietly turn "abc" into three one-character values — and it read
    # as a substring test before that, which was never a vocabulary either.
    # Whichever of the two somebody meant, they meant a collection.
    if isinstance(choices, str):
        raise TypeError(
            f"choices is the values a field accepts, not {choices!r}. A string "
            "is a run of characters here, not one value. Wrap it in a tuple."
        )

    if choices is not None and not callable(choices):
        try:
            choices = tuple(choices)
        except TypeError as error:
            raise TypeError(
                "choices takes the values a field accepts, or a function "
                f"handing them back, not {choices!r}."
            ) from error

    # Compiled to the pair rather than carried as a third mechanism: everything
    # downstream already knows how to run a handler on add and on save, and a
    # derived field is that pair with one function in both. What `derived` adds
    # is the fact kept beside them — that the value is nobody's to set — which
    # is what `_setattr` and the constructor read it for.
    if derived is not None:
        if not callable(derived):
            raise TypeError(
                "derived takes a function that works the value out from the "
                f"record, not {derived!r}."
            )
        if on_add is not None or on_save is not None:
            raise ValueError(
                "derived and on_add/on_save are two answers to when a field is "
                "the caller's. A derived field is worked out from other fields "
                "on every write and cannot be set at all; a field naming on_add "
                "or on_save is filled in where the caller said nothing. Say one "
                "of them."
            )
        on_add = on_save = derived

    metadata = {
        "dray": True,
        "stored_in": stored_in,
        "precision": precision,
        "scale": scale,
        "choices": choices,
        "converter": converter,
        "validator": validator,
        "on_change": on_change,
        "on_add": on_add,
        "on_save": on_save,
        "derived": derived,
    }
    if default_factory is not MISSING:
        return dataclasses.field(default_factory=default_factory, metadata=metadata)
    return dataclasses.field(default=default, metadata=metadata)


def base_type(annotation: Any) -> Any:
    """`str | None` is a `str` as far as anything downstream is concerned."""
    if typing.get_origin(annotation) in (types.UnionType, typing.Union):
        rest = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(rest) == 1:
            return rest[0]
    return annotation


# The annotations whose column DSQL will not index, and the column each becomes.
# An arbitrary-looking list until it is read off DSQL's own table of supported
# types, which carries an index support column: `interval`, `bytea` and `jsonb`
# are the three entries with none. Local PostgreSQL builds an index on all three
# without a word, which is the whole problem — the suite is green and the
# cluster is where the declaration would otherwise be found to be wrong.
#
# The column type is written here rather than read out of `schema.SQL_TYPES`,
# because nothing in this module knows about SQL and importing the schema to ask
# would turn that around for four entries. It is also what the refusal has to
# say out loud, since `bytes` on its own explains nothing. The two have to
# agree, and a test holds them to it.
UNINDEXABLE: dict[Any, str] = {
    timedelta: "interval",
    bytes: "bytea",
    dict: "jsonb",
    list: "jsonb",
}


def check_indexable(owner: str, name: str, annotation: Any) -> None:
    """
    Whether a column a declared index covers is one DSQL can index. Raises to
    refuse, like `check`, so there is no return convention to remember.

    Checked when the class is built rather than inside `index`, which is the one
    place it can be: `index` is handed column names on the decorator and never
    sees the annotations that decide what those columns are.

    Read through `base_type`, so `bytes | None` is refused along with the rest,
    and so this agrees with `_sql_type` about what the column will be rather than
    reasoning separately. `list[str]` is unrecognised by both and becomes a
    `text` column, which indexes; refusing it here would be refusing an index
    that works.
    """
    column = UNINDEXABLE.get(base_type(annotation))
    if column is None:
        return

    raise ValueError(
        f"{owner}.{name} is stored as {column}, and DSQL has no index support "
        f"for {column} at all, unique or otherwise — so this is an index the "
        "cluster refuses. Local PostgreSQL builds it without complaint, "
        "which is why this is said here rather than left to the deployment. "
        "What can be indexed is a column beside it holding what the reads "
        "actually match on: a digest as text, a duration in seconds, the one "
        "key lifted out of the document."
    )


def check_key(owner: str, name: str, annotation: Any) -> None:
    """
    Whether a declared id is a type DSQL will have in a key. Raises to refuse,
    like `check_indexable`, whose list it shares.

    The cluster's refusal is about keys rather than about indexes — it answers
    a unique constraint, a unique index and a plain index alike with `datatype
    bytea is not supported in a key`. A primary key is a key too, and `id` is
    the one column dray makes one, so `id: bytes` implies `id bytea primary
    key`: a table local PostgreSQL creates and a cluster will not.

    Its own function rather than a third branch of `check_indexable`, because
    what a record can do about it is different. An index is something a field
    asked for and can stop asking for; a primary key is what an id *is*, so the
    only way out is an id of another type — which is what this has to say
    instead. Nothing in `field` could catch it either way, since a record
    declaring an id says nothing about indexing at all.
    """
    column = UNINDEXABLE.get(base_type(annotation))
    if column is None:
        return

    raise ValueError(
        f"{owner}.{name} is stored as {column}, and DSQL will not have {column} "
        "in a key — which a primary key is. Local PostgreSQL takes "
        f"`id {column} primary key` without complaint, which is why this is "
        "said here rather than left to the create table. An id wants a type a "
        "key can hold: leave it undeclared for the uuid a record gets by "
        "itself, or declare str or int. A digest that has to be the identity "
        "goes in as its hex."
    )


def check_numeric(
    owner: str, name: str, annotation: Any, rules: Mapping[str, Any]
) -> None:
    """
    Whether the field a size was declared on has a `numeric` column to put it
    on. Raises to refuse, like `check_indexable`, and here for the same reason:
    the annotation deciding the column is not visible from inside `field`.

    Two ways it does not. A `Decimal` in the blob is written as its own text and
    comes back whole, so a size there is a promise about rounding that nothing
    ever keeps — the same shape as an index over a blob field, and answered the
    same way, by giving the field a column. And every other annotation
    becomes a column with no size to declare at all.
    """
    if base_type(annotation) is not Decimal:
        raise ValueError(
            f"{owner}.{name} is not a Decimal, so precision and scale have "
            "nothing to say about it. They are the size of a numeric column, "
            "and no other column dray writes has a size to declare."
        )
    if rules.get("stored_in") == BLOB:
        raise ValueError(
            f"{owner}.{name} is stored in the {BLOB!r}, which has no column to "
            "size. A Decimal goes into the document as its own text and comes "
            "back whole, so nothing would round where this says it does. Give "
            'it a column by dropping stored_in="blob", which is where a size '
            "means something."
        )


# The types dray knows that JSON does not. A column gets these for nothing —
# psycopg turns a `timestamptz` back into a `datetime` on the way out — and the
# blob gets no such adapter, so this is it. `Decimal` goes through `str` and not
# `float`, or 4.99 comes back as 4.990000000000000213162820728030055761337280273438.
FROM_TEXT: dict[Any, Callable[[str], Any]] = {
    datetime: datetime.fromisoformat,
    date: date.fromisoformat,
    time: time.fromisoformat,
    Decimal: Decimal,
    UUID: UUID,
    bytes: bytes.fromhex,
    timedelta: lambda text: timedelta(seconds=float(text)),
}


def as_text(value: Any) -> str:
    """Encode a value JSON cannot hold. Handed to `json.dumps` as its
    `default`, so it is only ever called for what the encoder gave up on."""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        # Seconds rather than "1:30:00", because Python parses one and not the
        # other. A string rather than a number so `restore` recognises it.
        return str(value.total_seconds())
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(f"{type(value).__name__} cannot be stored in the blob")


def restorer(annotation: Any) -> Callable[[str], Any] | None:
    """What brings a blob value back, or `None` for a field that needs nothing
    doing. Driven by the annotation rather than by looking at the string, so a
    `str` field holding "2026-03-14" stays a string."""
    return FROM_TEXT.get(base_type(annotation))


def restore(value: Any, rules: Mapping[str, Any]) -> Any:
    """
    A blob value on its way onto a record.

    Hydration rather than validation — the same job psycopg does for a column,
    which is why it belongs on the lenient path. A stored value that will not
    parse is handed back as it is, because `_dray_load` does not raise: a row
    written before the field was a date has to keep loading.
    """
    parse = rules.get("restore")
    if parse is None or not isinstance(value, str):
        return value
    try:
        return parse(value)
    except (ValueError, TypeError):
        return value


def accepts(annotation: Any) -> tuple[type, ...] | None:
    """
    The Python types a field will take, or `None` where we cannot tell and had
    better not guess — an annotation left as a string by
    `from __future__ import annotations`, an `Any`, a generic with no class
    behind it. A field we cannot read is simply not type-checked, which is the
    safe way to be wrong.
    """
    if annotation is None or isinstance(annotation, str):
        return None

    if typing.get_origin(annotation) in (types.UnionType, typing.Union):
        parts = [a for a in typing.get_args(annotation) if a is not type(None)]
    else:
        parts = [annotation]

    allowed: list[type] = []
    for part in parts:
        # `list[str]` is a `list`; the argument is jsonb's problem, not ours.
        part = typing.get_origin(part) or part
        if not isinstance(part, type):
            return None
        allowed.append(part)
        # An int is a perfectly good float, and a numeric column takes one.
        if part in (float, Decimal):
            allowed.append(int)
    return tuple(allowed)


def fits(value: Any, allowed: tuple[type, ...]) -> bool:
    """`isinstance`, with the one correction it needs: `bool` is a subclass of
    `int`, so `True` would otherwise pass for a count of one."""
    if isinstance(value, bool) and bool not in allowed:
        return False
    return isinstance(value, allowed)


def check(name: str, value: Any, rules: Mapping[str, Any]) -> None:
    """
    Whether a field will take this value. Raises to reject; saying nothing is
    acceptance, so there is no return convention to remember.

    `None` is exempt. A field that must have a value says so with a validator
    that rejects it, rather than every optional field having to opt out.

    The type goes first, and deliberately: a validator handed a string where it
    expected a number raises `TypeError` from inside somebody's own comparison,
    which is a poor way to hear that a form posted what forms post. Anything
    else a validator raises is left alone, because a validator that breaks is a
    bug rather than a value being refused.
    """
    if value is None:
        return

    allowed = rules.get("accepts")
    if allowed and not fits(value, allowed):
        wanted = " or ".join(dict.fromkeys(kind.__name__ for kind in allowed))
        raise ValidationError(
            f"{name}: expected {wanted}, got {type(value).__name__} {value!r}"
        )

    given = rules.get("choices")
    if given is not None:
        # A function is asked here rather than kept from the declaration, which
        # is the whole of what one buys: a vocabulary the application reloads
        # is checked against what it holds now. Not a type, though — an `Enum`
        # class is a legal `choices` and is callable, where calling it looks a
        # member up by value rather than handing back the members. Calling one
        # raises at the first check, a long way from the declaration.
        choices = given
        if callable(given) and not isinstance(given, type):
            choices = given()
        try:
            outside = value not in choices
        except TypeError as error:
            raise TypeError(
                f"{name}: choices {given!r} handed back {choices!r}, which "
                "nothing can look a value up in. It has to hand back the "
                "values the field accepts."
            ) from error
        if outside:
            allowed = ", ".join(str(choice) for choice in choices)
            raise ValidationError(f"{name}: {value!r} is not one of {allowed}")

    for validator in rules.get("validator") or ():
        try:
            validator(value)
        except ValidationError:
            raise
        except ValueError as error:
            raise ValidationError(f"{name}: {error}") from error


def resolved(cls: type, annotations: dict[str, Any]) -> dict[str, Any]:
    """
    The annotations as types rather than as the text of types.

    `from __future__ import annotations` turns every annotation on a module into
    a string, and a string tells `accepts` and `restorer` nothing — so a field
    would take any value at all and a blob field would never come back as what
    it declared, quietly, for every record in that file. The schema reads the
    same annotations correctly, which is what makes it so hard to see: the
    column is right and the record is not.

    One at a time, and failure is per field rather than per class.
    `typing.get_type_hints` is all-or-nothing and raises on the first name it
    cannot see, which would take out every field on a record declared inside a
    function — something the tests do constantly. Here a name that will not
    resolve keeps its string, and only that field falls back to not being
    checked, which is the safe way to be wrong.
    """
    scope = getattr(sys.modules.get(cls.__module__, None), "__dict__", {})
    read = {}
    for name, annotation in annotations.items():
        if isinstance(annotation, str):
            try:
                # A string annotation is resolved the only way Python
                # resolves one, in the scope the class was written in.
                annotation = eval(annotation, scope)
            except Exception:
                pass
        read[name] = annotation
    return read


# A class in and the same class out. `@record` mutates the class it is handed
# and gives it back, so this is the truth rather than a convenience — and it
# is the difference between an editor knowing what a `Person` is and believing
# it is the builtin `type`.
_Class = TypeVar("_Class", bound=type)


# What lets a checker read the constructor. `field` is named as the specifier
# so `family_name: str = field()` is understood as a field rather than as a
# class attribute holding a `Field` object. A field with no `default=` reads as
# required, which is what dray's own declarations now say out loud.
@dataclass_transform(field_specifiers=(field,))
def record(
    *,
    table: str,
    collection: str,
    order_by: str | tuple | list | None = None,
    indexes: Index | Sequence[Index] | None = None,
    cached_for: float | None = None,
    cache_most: int = CACHE_MOST,
    key: str = KEY,
    etag: str = ETAG,
    blob: str = BLOB_COLUMN,
) -> Callable[[_Class], _Class]:
    """
    Turn a plain class into a record.

        @record(table="person", collection="people")
        class Person:
            family_name: str

    Applied rather than inherited, so the machinery is visible on the line above
    the class instead of hidden in a base. `table` is where rows live and
    `collection` is what the store calls them, both said out loud because
    deriving either one from the class name is a rule you have to remember.

    `key`, `etag` and `blob` name the three columns dray puts on every record,
    and default to the plain words `id`, `etag` and `data`. Say one where your
    domain wants the word for itself:

        @record(table="person", collection="people",
                key="ref", etag="dray_etag", blob="payload")
        class Person:
            id: str                              # the employee number
            etag: str = field(default=None)      # the upstream API's ETag
            data: dict = field(default=None)     # the old system's column

    The key is the one dray hands over outright, because it only ever needed the
    name — declare `id: str` and an employee number is the primary key, with no
    option said at all. The other two carry values dray mints and reads on every
    write, so a class declaring either without moving dray's is refused.

    `order_by` is the order a read gets when nobody asks for one, in the words
    a `@child` declares its own order in. It defaults to the key, which is
    total and stable and means nothing — a key is random unless the class said
    otherwise — so a class whose domain has an order says it once here rather
    than at every call site:

        @record(table="person", collection="people", order_by="family_name")
        class Person:
            family_name: str

    `find(order_by=...)` overrides it for the one call, and the key goes on the
    end whichever of them decided, so a read is always total. Any order but
    the key is a sort, and an index on the column does not spare you one —
    which is the cost the same declaration on a `@child` has always carried,
    and is there to be read in `explain`.

    `indexes` is what the table is indexed for, said in one list because a table
    has a budget of them rather than a field having one each:

        @record(table="shift", collection="shifts",
                indexes=[index("on_date", "ward"),
                         index("on_date", "slot", unique=True)])

    Each is one `index(...)` over the columns it covers, in the order it covers
    them, and `unique=True` where the columns are unique together. `schema` hands
    over the statements and a caller runs them; nothing here builds an index.

    `cached_for` is how many seconds a row of this class may be answered out of
    memory rather than read again:

        @record(table="ward", collection="wards", cached_for=1800)
        class Ward:
            name: str

    Off unless it is said, because a cache nobody asked for is a staleness
    nobody agreed to. It is here rather than on `@collection` for two reasons:
    how often a row is re-read is a storage fact, like the table and the key
    beside it, and a record with no collection class of its own still has
    `by_id` to serve. What it turns on, and what it does not, is *Not asking
    twice* on the page.

    `cache_most` is how many rows of this class are kept — a backstop against a
    key space nobody bounded rather than a number anybody tunes, since the
    lifetime is what decides how stale a row may be.
    """

    def wrap(cls: _Class) -> _Class:
        return _declare(
            cls,
            table=table,
            collection=collection,
            order_by=order_by,
            indexes=indexes,
            cached_for=cached_for,
            cache_most=cache_most,
            parent_types=(),
            key=key,
            etag=etag,
            blob=blob,
        )

    return wrap


def _parent_key(owner: str, parents: tuple) -> tuple[Any, Any]:
    """
    What a child's `parent_id` is made of: the annotation and the converter of
    the key it will hold. Raises to refuse, like the checks above.

    A child table belongs to one `@child` declaration, so a timelog hanging off
    a day keyed by date and a note hanging off a person keyed by uuid are two
    tables and neither has to give anything up. What cannot be represented is
    one table serving both, because the column holds one type — refused here,
    where the declaration is, rather than by the first row that tries to point
    somewhere the column cannot reach.

    Parents whose keys agree is the ordinary case and stays untouched: `of=` is
    the list of records that get an accessor, and `parent_type` is what tells
    their rows apart. Agreeing means the converter as well as the type, because
    the field takes one of each — every write fills `parent_id` from the
    parent's own key, already converted, so a converter here is only ever
    reached by a value handed in from outside, and which parent's rule that
    value is held to is a question with as many answers as there are parents.
    Taking the first one's is the kind of rule that reads as an accident later.
    """

    def spelt(annotation: Any) -> str:
        kind = base_type(annotation)
        return getattr(kind, "__name__", str(kind))

    def spelling(converter: Any) -> str:
        if converter is None:
            return "none"
        return getattr(converter, "__name__", repr(converter))

    def named(pairs: list, describe: Any) -> str:
        return ", ".join(
            f"{parent.__name__} ({describe(what)})" for parent, what in pairs
        )

    keys = [
        (parent, parent.__dray_annotations__.get(parent.__dray_key__, str))
        for parent in parents
    ]
    if len({base_type(annotation) for _, annotation in keys}) > 1:
        raise TypeError(
            f"{owner} hangs off records whose keys are not all of one type: "
            f"{named(keys, spelt)}. A child names its parent in a column "
            "holding that parent's key, and one column holds one type, so "
            "these cannot share a table. Declare a child per key type — each "
            "with its own table, and each parent keeping the accessor `name` "
            "gives it."
        )

    converters = [
        (parent, parent.__dray_fields__[parent.__dray_key__].get("converter"))
        for parent in parents
    ]
    # Compared with `!=` rather than gathered into a set, because a converter
    # is only ever promised to be callable and a callable object need not be
    # hashable.
    if any(converter != converters[0][1] for _, converter in converters):
        raise TypeError(
            f"{owner} hangs off records whose keys are of one type but do not "
            f"convert the same way: {named(converters, spelling)}. The column "
            "naming a parent takes one converter, so a key handed to it from "
            "outside would be normalised by one of these and not the rest, "
            "with nothing but the order of `of=` to say which. Give the keys "
            "the same converter, or declare a child per converter — each with "
            "its own table, and each parent keeping the accessor `name` gives "
            "it."
        )

    _, annotation = keys[0]
    _, converter = converters[0]
    return annotation, converter


def _as_written(one: Index) -> str:
    """One index said back the way the decorator said it, for a refusal that
    has to name two of them against the lines that declared them."""
    said = ", ".join(repr(str(name)) for name in one.columns)
    return f"index({said}, unique=True)" if one.unique else f"index({said})"


def _declared_indexes(built: type, given: Any) -> tuple:
    """
    The indexes a decorator was handed, held to the class that has to carry
    them. Raises to refuse, like `_ordering`, and for the same reason: a name
    the class does not have is a `create index` that fails during somebody's
    deployment rather than on the line that got it wrong.

    Two of them refuse something that would not fail at all. An index whose
    columns are a leading run of another's answers no read the wider one was not
    already answering, so it is a slot and a write per insert bought for
    nothing. And two declarations dray would call the same thing are one index
    and a statement that quietly does nothing, so the class asks for two, the
    table carries one, and the first anybody hears of it is a read nobody
    indexed. Neither shows up anywhere but the list both declarations are in,
    which is why they are caught here.

    A child's own two columns are fields like any other, so an index may name
    them — `index("parent_type", "parent_id", "starts_at")` is how a child says
    it wants its parent's reads served by more than the one dray builds anyway.

    Two more refusals are about what an index key may say. A direction is
    DSQL's refusal said early, and it has to be said early because local
    PostgreSQL takes `(due_on desc)` without a word — so the suite that would
    catch it is the one nobody runs until the deployment. Placement on a unique
    index is dray's own, and the reason is a `create table`, which is the only
    place either of them could otherwise diverge unseen.
    """
    if given is None:
        return ()

    declared = tuple(given) if isinstance(given, (tuple, list)) else (given,)
    for one in declared:
        if not isinstance(one, Index):
            raise TypeError(
                f"{built.__name__} declares {one!r} as an index. indexes= takes "
                "index(...), which is what carries the columns and whether they "
                "are unique together."
            )
        for name in one.columns:
            if name not in built.__dray_fields__:
                raise TypeError(
                    f"{built.__name__} is indexed on {str(name)!r}, which it "
                    "does not declare. Add the field, or index something it has."
                )
            if name in built.__dray_blob__:
                raise TypeError(
                    f"{built.__name__} is indexed on {str(name)!r}, which is "
                    "stored in the blob and has no column to index. Give it a "
                    'column by dropping stored_in="blob" — which is what '
                    "indexing a field means here, since jsonb has no index "
                    "support on DSQL at all."
                )
            if isinstance(name, Descending):
                raise ValueError(
                    f"{built.__name__} is indexed on desc({str(name)!r}), and "
                    "DSQL will not have a direction on an index key: "
                    "`create index ... (col desc)` comes back `specifying sort "
                    "order not supported for index keys`, where local "
                    "PostgreSQL builds it without complaint — which is why "
                    "this is said here rather than left to the deployment. A "
                    "btree is scanned backwards as readily as forwards, so the "
                    "bare name is the index that serves the descending read. "
                    "desc() belongs in order_by; asc(name, nulls=...) is what "
                    "an index key may say."
                )
            if one.unique and _nulls(name):
                raise ValueError(
                    f"{built.__name__} indexes {str(name)!r} with a null "
                    "placement and unique=True together, which dray cannot "
                    "write. A unique index goes into a `create table` as a "
                    "constraint, and `unique (col nulls first)` is not a "
                    "grammar SQL has — so a table being created and a table "
                    "already there would end up indexed differently, with "
                    "nothing to say so. Drop the placement: the index the "
                    "constraint brings serves the default ordering forwards "
                    "and backwards alike."
                )
            check_indexable(built.__name__, name, built.__dray_annotations__.get(name))

    # An index is skipped against itself by position rather than by value,
    # because two declarations can be equal — `index("email")` said twice is
    # this same mistake — and skipping by value would take that pair with it.
    for here, narrow in enumerate(declared):
        # A unique index is never the redundant one. It enforces something no
        # wider index does, whatever their columns share.
        if narrow.unique:
            continue
        for there, wider in enumerate(declared):
            if here == there or len(wider.columns) < len(narrow.columns):
                continue
            if wider.columns[: len(narrow.columns)] != narrow.columns:
                continue
            raise ValueError(
                f"{built.__name__} declares {_as_written(narrow)}, whose "
                f"columns are a leading run of {_as_written(wider)}'s. A btree "
                "is searched by any leading run of its columns, so the wider "
                "index already answers every read the narrower one could — "
                "which leaves the narrower one costing a slot out of the 23 a "
                "table has and a write on every insert, for nothing. Drop it — "
                "or, where both reads matter, order the wider one's columns so "
                "that it does not lead with the narrower one's."
            )
    # Asked of the one function that names an index, rather than of a second
    # copy of the rule here. The whole failure below is a name generated one way
    # and stored another, and two places building it is how that starts again.
    from dray.schema import _index_name

    named: dict[str, Index] = {}
    for one in declared:
        called = _index_name(built, one)
        if called in named:
            first = ", ".join(str(column) for column in named[called].columns)
            second = ", ".join(str(column) for column in one.columns)
            raise ValueError(
                f"{built.__name__} declares two indexes that dray calls the "
                f"same thing: ({first}) and ({second}) are both "
                f"{called!r}. An index is named for its table and its columns, "
                "cut to the 63 bytes an identifier holds, and the statement "
                "for one is `create index if not exists` — so the second finds "
                "the first and succeeds having built nothing, leaving the "
                "class asking for two indexes and the table carrying one. Drop "
                "the one you do not need, or, where the cut is what brought "
                "them together, shorten the table name so that the two names "
                "stay apart."
            )
        named[called] = one
    return declared


def _claimed(cls: type, name: str, lent: Any) -> bool:
    """
    Whether the class has said something about one of the words dray lends.

    Asked of the whole hierarchy rather than of one `__dict__`, because a
    `save` on a base class is the class using the name every bit as much as a
    `save` in its own body is — and it is how a rule gets shared by several
    record types, which is what `@check` collecting through the bases is for.

    The awkward case is that a record may subclass a record, and the parent
    carries dray's own method under the plain word. That is dray's binding
    rather than anybody's rule, so the first holder Python would reach is
    compared against what is being lent: finding dray's own leaves the word
    free, and the subclass has it bound again where it would have inherited it.
    Read out of `vars` rather than through `getattr`, since `parse` is a
    `classmethod` and `children` and `store` are `property` objects — only the
    raw attribute is the object that was lent.
    """
    for base in cls.__mro__:
        held = vars(base)
        if name in held:
            return held[name] is not lent
    return False


def _checked_ttl(whose: str, cached_for: Any) -> float | None:
    """How long an answer may be kept, or nothing.

    Refused where the declaration is written rather than where the first read
    is served, because both wrong answers are quiet ones: nought reads as
    *cache it forever* to anybody skimming and would mean *cache it not at
    all*, and a negative number is an entry that has expired before it was
    stored.

    Named rather than handed the class, because the same two checks are what
    `@cached_for` on a collection method has to make and a method is not one.
    """
    if cached_for is None:
        return None
    if isinstance(cached_for, bool) or not isinstance(cached_for, (int, float)):
        raise TypeError(
            f"{whose} says cached_for={cached_for!r}, and that is a number of "
            "seconds an answer may be kept for. Leave it off for a record that "
            "is not cached."
        )
    if cached_for <= 0:
        raise ValueError(
            f"{whose} says cached_for={cached_for!r}. A lifetime of nought is "
            "not a cache that is off — it is one that stores everything and "
            "answers from none of it. Leave cached_for off instead."
        )
    return float(cached_for)


def _checked_most(whose: str, cache_most: Any) -> int:
    """How many answers are kept. A backstop rather than a policy, so the only
    thing worth refusing is a number that cannot hold one."""
    if isinstance(cache_most, bool) or not isinstance(cache_most, int):
        raise TypeError(
            f"{whose} says cache_most={cache_most!r}, and that is a number of "
            "answers to keep."
        )
    if cache_most < 1:
        raise ValueError(
            f"{whose} says cache_most={cache_most!r}. A cache holding nothing "
            "is one that is off, which is what leaving cached_for off says."
        )
    return cache_most


def _declare(
    cls: _Class,
    *,
    table: str,
    collection: str | None,
    order_by: str | tuple | list | None,
    indexes: Index | Sequence[Index] | None,
    parent_types: tuple,
    key: str,
    etag: str,
    blob: str,
    cached_for: float | None = None,
    cache_most: int = CACHE_MOST,
    parent_type: str = PARENT_TYPE,
    parent_id: str = PARENT_ID,
) -> _Class:
    """The shared body of `@record` and `@child` — a child is a record that
    knows its parent, and everything else about the two is identical."""
    # Read through the descriptor rather than out of `cls.__dict__`. Since 3.14
    # annotations are computed lazily from `__annotate__`, so the dict is not
    # there until something asks for it — and assigning over the top of an empty
    # one silently discards every field the class declared.
    annotations = dict(getattr(cls, "__annotations__", None) or {})

    # What the class itself declared, before dray adds its own. The first of
    # them is a child's content — the body of a note, the message of a log —
    # which is what `notes.add("...")` fills in and what a change handler writes
    # to without having to be told the field's name.
    declared = tuple(annotations)

    taken = sorted(n for n in declared if n.startswith(RESERVED_PREFIX))
    if taken:
        raise TypeError(
            f"{cls.__name__} declares {', '.join(taken)}. Every name starting "
            f"{RESERVED_PREFIX!r} is dray's own — the second spelling of a "
            "member, or a keyword the constructor reads — and a field there "
            "collides with one silently. Rename the field; the plain words "
            "are yours, and a column dray still fills is moved on the "
            "decorator rather than reserved."
        )

    # The columns dray fills on this class. A question asked of the class rather
    # than a list of fixed words, because what is taken depends on what the
    # decorator was told: a record that said `blob="payload"` has given `data`
    # back to the domain, and one that said nothing has not.
    owned = [(etag, "etag"), (blob, "blob")]
    if parent_types:
        owned += [(parent_type, "parent_type"), (parent_id, "parent_id")]

    landed: dict[str, str] = {}
    for name, option in [(key, "key"), *owned]:
        if name in landed:
            raise TypeError(
                f"{cls.__name__} puts dray's {option} and its {landed[name]} "
                f"both in {name!r}. Each of them holds a different value on "
                "every row, so each wants a column of its own."
            )
        landed[name] = option

    # The key is not in this half, and that is the whole of what makes it
    # different. It is the one dray hands over outright, because it only ever
    # needed the name: a class declaring it keeps its own type, default and
    # converter. The rest carry values dray mints and reads on every write, so
    # it cannot give them up — it can only stand somewhere else.
    for name, option in owned:
        if name in declared:
            raise TypeError(
                f"{cls.__name__} declares {name!r}, which is where dray keeps "
                f"{FILLS[option]}. dray writes that column on every save, so "
                f'it cannot hand the name over — {option}="dray_{name}" on the '
                "decorator moves dray's and leaves the field yours."
            )

    # A child names its parent in two columns rather than a foreign key, which
    # DSQL does not have. They are ordinary fields so that hydrating, writing
    # and the schema all treat them like anything else.
    if parent_types:
        # `parent_type` is a table name and `parent_id` is a key, so it follows
        # the key it points at rather than assuming the `UUID` a record gets
        # when it declares nothing: a parent whose id is an employee number
        # gives its children a `text` column, and one keyed by date a `date`.
        # Which is also why `check_key` has nothing to say about `parent_id` —
        # the type came off a key that has already been through it.
        annotations[parent_type] = str
        setattr(cls, parent_type, dataclasses.field(default=None))
        held, converter = _parent_key(cls.__name__, parent_types)
        annotations[parent_id] = held
        setattr(cls, parent_id, field(default=None, converter=converter))

    # The key is added rather than declared, because a record without one cannot
    # be found again and there is nothing to decide. Timestamps are not: a record
    # that wants to know when it was made says so with `on_add`, which is also
    # how it says who made it and anything else a write should fill in.
    if key not in annotations:
        annotations[key] = UUID
        setattr(
            cls,
            key,
            field(default_factory=uuid4, converter=as_uuid),
        )

    # The stale-write guard, on every record whether or not anybody uses it. A
    # record that could forget it would be silently unguarded, which is the
    # failure this whole design keeps deleting.
    annotations[etag] = str
    setattr(cls, etag, dataclasses.field(default_factory=new_etag))
    cls.__annotations__ = annotations

    # dray's own copy of each of the six, always, under a spelling no field can
    # take. An application wanting a note written and never edited says so by
    # defining `save`, and dray leaves the plain name alone — but the method it
    # wrote is a rule to stand in front of dray rather than instead of it, so
    # `self._dray_save(**kw)` has to be there to call when the rule has passed.
    #
    # The plain name is bound on top only where the class has said nothing —
    # `_claimed` asks the hierarchy that question — and the annotations have to
    # be read as well. They stay this class's own: a field declared bare —
    # `save: str`, with no `field(...)` beside it — is in the annotations and
    # not in `__dict__`, so binding a method over it hands `dataclasses` a
    # function as that field's default and a required field quietly becomes
    # optional. A bare annotation on a base class is not a field of the
    # subclass at all, so widening this half would refuse a word nothing had
    # spent.
    for name, method in _RECORD_LENT.items():
        setattr(cls, RESERVED_PREFIX + name, method)
        if not _claimed(cls, name, method) and name not in annotations:
            setattr(cls, name, method)

    # The three a record is built and stored with, under names of dray's own
    # rather than a second spelling, because nobody calls them. Bound outright:
    # they sit under the reserved prefix, which every field was refused above,
    # so there is nothing already on the class for them to land on top of.
    for name, method in _RECORD_MEMBERS.items():
        setattr(cls, name, method)

    # The dunders are the corner the prefix cannot reach, since Python looks
    # each of them up by its own spelling — so they get the second spelling
    # too, and for the same reason as the six above. Each carries the whole of
    # a behaviour: `__setattr__` is where converting, validating and `on_change`
    # happen, and `__eq__` and `__hash__` are identity by key, which is what a
    # set and a dict key ask. A class taking one of these names had no way to
    # call dray's, so defining one was only ever a way to lose the behaviour
    # rather than to stand in front of it.
    #
    # A class defining `__eq__` is left unhashable, which is Python's doing
    # rather than dray's: the class body gets `__hash__ = None` written into it,
    # and that is the class having said something about the name. Such a class
    # gets its hash back by defining one that calls `self._dray_hash()`.
    for name, method in _RECORD_HOOKS.items():
        setattr(cls, RESERVED_PREFIX + name, method)
        hook = f"__{name}__"
        if hook not in cls.__dict__ and hook not in annotations:
            setattr(cls, hook, method)

    built = dataclasses.dataclass(cls)

    # Worked out once, here, and kept on the class. Everything downstream reads
    # these rather than the raw annotations or its own `get_type_hints` — the
    # schema included, so the column a field gets and the values it accepts can
    # never be decided from two different readings of the same line.
    read = resolved(cls, annotations)

    specs = {}
    for spec in dataclasses.fields(built):
        rules = dict(spec.metadata) if spec.metadata.get("dray") else {}
        rules.setdefault("stored_in", COLUMN)
        rules["accepts"] = accepts(read.get(spec.name))
        for kind in ("validator", "on_change"):
            rules[kind] = handlers(cls.__name__, spec.name, kind, rules.get(kind))
        rules["converter"] = handler(
            cls.__name__, spec.name, "converter", rules.get("converter")
        )
        rules["restore"] = restorer(read.get(spec.name))
        # The key is the one column with an index nobody asked for, so what it
        # may be typed as is checked whether or not the class said anything.
        if spec.name == key:
            check_key(cls.__name__, spec.name, read.get(spec.name))
        if rules.get("precision") is not None:
            check_numeric(cls.__name__, spec.name, read.get(spec.name), rules)
        specs[spec.name] = rules

    # Bookkeeping lives in dunder names so it cannot collide with a field. The
    # five naming dray's own columns are here rather than on the store because
    # `@record` runs at import and bakes the class's columns in there and then,
    # where a store is connected long afterwards — often after every record
    # module has been imported, which would leave each class built against a
    # name it was told about too late.
    built.__dray_table__ = table
    built.__dray_collection__ = collection
    built.__dray_parents__ = parent_types
    built.__dray_key__ = key
    built.__dray_etag__ = etag
    # `__dray_blob__` is already the *fields* that live in the blob, so the
    # column they live in needs a name of its own rather than a near-collision.
    built.__dray_blob_column__ = blob
    built.__dray_parent_type__ = parent_type
    built.__dray_parent_id__ = parent_id
    # How long a row of this class may be answered from memory, and how many of
    # them are kept. A storage fact like the ones above it — how often a row is
    # re-read belongs with the table and the key rather than with a domain's
    # vocabulary — and `None` is the default because a cache nobody asked for is
    # a staleness nobody agreed to.
    built.__dray_cached_for__ = _checked_ttl(built.__name__, cached_for)
    built.__dray_cache_most__ = _checked_most(built.__name__, cache_most)
    built.__dray_fields__ = specs
    # What the class asked dray to call, worked out here with everything else a
    # class settles once. Read off the markers the decorators left rather than
    # off the methods' names, so no plain word is reserved by dray having
    # started calling it.
    built.__dray_hooks__ = declared_on(built)
    built.__dray_annotations__ = read
    built.__dray_declared__ = declared
    built.__dray_content__ = declared[0] if declared else None
    columns = tuple(
        name for name, rules in specs.items() if rules["stored_in"] == COLUMN
    )
    # The key leads, wherever the class happened to declare it. A table is read
    # by people who never use dray and the first column is where they look to
    # find out what a row *is*; in declaration order a moved key sits somewhere
    # in the middle, and whatever came first reads as though it were the key.
    built.__dray_columns__ = (
        (key, *(name for name in columns if name != key))
        if key in columns
        else columns
    )
    built.__dray_blob__ = tuple(
        name for name, rules in specs.items() if rules["stored_in"] == BLOB
    )
    # After the fields, because every one of these names has to be a field the
    # class ended up with — dray's own parent columns included, which are added
    # in this function rather than declared.
    built.__dray_indexes__ = _declared_indexes(built, indexes)
    # The whole `order by` a read gets when the call names none, worked out here
    # so a record and a child answer that question the same way. The key on the
    # end of whatever was declared is what makes it total; the key on its own is
    # what a class that never mentioned an order gets, which is total and stable
    # without the word `id` having to be written into it.
    built.__dray_order__ = (
        key if order_by is None else f"{_ordering(built, order_by)}, {key}"
    )
    # A blob field holding `None` is written as an absent key rather than a
    # stored null, so that a document says only what somebody put in it — which
    # is what makes `data ? 'x'` mean the same as `x is not null` does on a
    # column, rather than accidentally reporting whether the row has been saved
    # since the field was declared.
    #
    # Only where the field's default is `None`, and that exception is the whole
    # of the care this needs: `_dray_load` gives an absent key the declared
    # default, so omitting the key round-trips as `None` for those fields and
    # would come back as something else for a field defaulting to anything but.
    # Such a field keeps its stored null, and stays exactly as it was.
    built.__dray_blob_omitted__ = frozenset(
        spec.name
        for spec in dataclasses.fields(built)
        if spec.name in built.__dray_blob__
        and spec.default is None
        and spec.default_factory is dataclasses.MISSING
    )

    original_init = built.__init__
    # In `__init__` order, so a positional argument can be named after the fact.
    positional = tuple(spec.name for spec in dataclasses.fields(built) if spec.init)
    # The fields nobody may give a value to, gathered once so the constructor
    # asks a set rather than walking the rules on every record it builds.
    derived_fields = frozenset(
        name for name, rules in specs.items() if rules.get("derived") is not None
    )

    def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
        # Construction is exempt from validation and from `on_change`. Hydrating
        # a row must not validate it — a record written under a rule that has
        # since been tightened would become unloadable — and it must not fire a
        # handler, because nothing changed, the value was always there.
        #
        # It is not exempt from the converter. A field's rules are meant to hold
        # wherever a value comes from, and a service layer builds its records
        # here, so leaving this door out meant the normalising happened for a
        # form and an import and not for the application's own code.
        stored = kwargs.pop("_dray_stored", False)
        if not stored:
            fields = built.__dray_fields__
            # A derived field is refused here as well as at assignment, and
            # refused rather than ignored. It is the door `parse` builds
            # through, so a spreadsheet with a column of folded names is told
            # about it where somebody can still fix the spreadsheet — and a
            # value quietly dropped by the write that recomputes it is the
            # defect this whole shape exists to remove. A row on its way back
            # out of the table is exempt with everything else `stored` exempts:
            # those values *are* what the last write worked out.
            if derived_fields:
                for name in (*positional[: len(args)], *kwargs):
                    if name in derived_fields:
                        raise ValidationError(
                            _NOT_YOURS.format(cls=built.__name__, name=name)
                        )
            # Zipped rather than indexed, so a surplus argument is carried
            # through to the dataclass rather than running off the end of
            # `positional` with dray's own `IndexError`.
            #
            # What that reaches is narrower than it sounds: `positional`
            # holds `id` and `etag` too, so the third positional a record takes
            # is `id`, and `as_uuid` refuses a stray one there before any count
            # is reached. The message a caller gets is then about the id rather
            # than about how many arguments they passed, which is the trade —
            # a `str` left in `id` for a write to trip over later would be the
            # worse of the two.
            args = tuple(
                convert(name, value, fields[name])
                for name, value in zip(positional, args)
            ) + tuple(args[len(positional):])
            kwargs = {
                name: convert(name, value, fields[name]) if name in fields
                else value
                for name, value in kwargs.items()
            }
        object.__setattr__(self, "_dray_ready", False)
        # One `ChildSet` per kind, made when first asked for and kept, so what
        # is queued on it is still there at save. `_dray_children` is the other
        # question — every kind the class declared, reached for or not.
        object.__setattr__(self, "_dray_sets", {})
        # Which fields somebody chose, as opposed to which took the default they
        # were declared with. A dataclass cannot tell the two apart afterwards —
        # `whom="System"` passed in and `whom` left alone hold the same value —
        # and that is exactly the fact a write needs to know what it may fill in.
        object.__setattr__(self, "_dray_said", {*positional[: len(args)], *kwargs})
        original_init(self, *args, **kwargs)
        object.__setattr__(self, "_dray_ready", True)

    built.__init__ = __init__

    if collection:
        RECORDS[collection] = built
    return built


#
# The members every record gets. Written as plain functions and attached in
# `_declare`, so a record inherits from nothing and `Person.__mro__` says what
# you would expect.
#


def _setattr(self: Any, name: str, value: Any) -> None:
    """
    The single point where a field being set is noticed.

    Dataclasses have no per-field hook, so everything — rejecting a value,
    rejecting a name, calling `on_change` — happens here or not at all.
    """
    # dray's own bookkeeping, and every assignment the constructor makes before
    # the record is whole. Both are dray writing to itself, so neither is a
    # field being set and neither is anybody's to notice.
    ready = getattr(self, "_dray_ready", False)
    if name.startswith(RESERVED_PREFIX) or not ready:
        object.__setattr__(self, name, value)
        return

    rules = self.__dray_fields__.get(name)
    if rules is None:
        # A leading underscore says *mine, not yours* in Python generally, and
        # hanging a transient on an object in hand is an ordinary thing to do.
        # It is only a field dray was never told about that gets that latitude:
        # a declared one is a column whichever way it is spelled, and reaches
        # everything below.
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        raise AttributeError(
            f"{type(self).__name__} has no field {name!r}. "
            "Declare it on the class; nothing is stored that was not declared."
        )

    if name == self.__dray_key__:
        # Which row a save writes to is whatever the object currently says, so
        # moving a key moves the write: assigning somebody else's and saving
        # overwrote their row with these values and left this one behind,
        # silently. And the key is what a record hashes on, so one that moved
        # while the record sat in a set is a key the set can no longer find.
        raise AttributeError(
            f"{name!r} is a {type(self).__name__}'s key, and a key cannot be "
            "changed once the record exists. Give it one when you build it — "
            "the constructor and `parse` both take it — or build the record it "
            "should be."
        )

    if rules.get("derived") is not None:
        raise AttributeError(
            _NOT_YOURS.format(cls=type(self).__name__, name=name)
        )

    value = convert(name, value, rules)
    check(name, value, rules)

    was = getattr(self, name, None)
    object.__setattr__(self, name, value)

    # Assigned, so it is a value somebody chose and no longer one a write may
    # fill in. The same set `__init__` starts, extended by everything after it.
    said = getattr(self, "_dray_said", None)
    if said is not None:
        said.add(name)

    if was == value:
        return

    # What the record held before this write, which is what `Write.was` hands a
    # rule. The first value per field and only for a field that actually moved:
    # a second assignment is this same write moving it again, and the question
    # is what the row said before any of them.
    #
    # Here rather than in a snapshot taken when the row loads, because this is
    # the one moment the old value is already in hand — it was read a few lines
    # up to decide whether `on_change` fires. So a record nobody assigns to
    # accumulates nothing at all, and a read of ten thousand rows pays for this
    # exactly nothing. `_dray_said` cannot double as it: that one holds names
    # rather than values, holds a name whether or not the value moved, and is
    # what `_touched` reads to decide which columns a save sends.
    remembered = getattr(self, "_dray_was", None)
    if remembered is None:
        remembered = {}
        object.__setattr__(self, "_dray_was", remembered)
    remembered.setdefault(name, was)

    for handler in rules.get("on_change") or ():
        handler(Change(record=self, field_name=name, old=was, new=value))


def _validate(self: Any) -> None:
    """Every rule on every field at once. What `parse` runs and hydrating a row
    does not — a row the table already holds has been through this once and has
    to keep loading whatever has been tightened since."""
    problems = []
    for name, rules in self.__dray_fields__.items():
        try:
            check(name, getattr(self, name), rules)
        except ValidationError as error:
            problems.append(str(error))
    if problems:
        raise ValidationError("; ".join(problems))


def _blob(self: Any) -> dict[str, Any]:
    """
    The fields with no column of their own, as they go into jsonb.

    A field holding `None` is left out rather than written as a null, so that
    the document holds what somebody put there and nothing else. That keeps the
    two sides of the split saying the same thing: `data ? 'x'` on the blob means
    what `x is not null` means on a column, and neither says anything about when
    the field was declared or when the row was last written.

    `__dray_blob_omitted__` is which fields that applies to — the ones whose
    default is `None`, since `_dray_load` hands an absent key the declared
    default and only those come back as `None`.
    """
    omitted = self.__dray_blob_omitted__
    return {
        name: value
        for name in self.__dray_blob__
        if not ((value := getattr(self, name)) is None and name in omitted)
    }


def _as_dict(self: Any) -> dict[str, Any]:
    """
    Every field this record declares, by name.

        person.as_dict()
        # {"family_name": "Hemingway", "suburb": "Katoomba",
        #  "id": UUID("d4e6...ac1"), "etag": "6c6942b5-9d36-4428-a84d-01f7…"}

        Person.parse(person.as_dict())      # the same record again

    Columns and blob fields alike, and the ones dray fills among them — `id`
    and `etag` are in it, and a child's `parent_type` and `parent_id` too — so
    what a form is handed carries the token a guarded save wants.

    It is a snapshot: changing what you get back does not change the record.

    The one asterisk is a field the class derives. It is handed out here with
    everything else, because it is part of what the record says and a method of
    this name quietly missing a field the record has is a surprise nobody can
    see. What it costs is the round trip: `parse` refuses a derived field
    wherever it arrives from, since the write works it out again, so a dict
    from a record that derives anything comes back with an error naming the
    field rather than going quietly through.
    """
    # `__dray_fields__` rather than a list anybody typed, for the reason
    # `_blob` reads it: a hand-written enumeration stops being right the day
    # the class gains a field, and silently, since nothing downstream is
    # asking.
    #
    # Copied deeply and over the whole mapping rather than value by value, so
    # that two fields holding one object still hold one object afterwards. The
    # cost is bounded by what these values are: a blob field is jsonb-bound,
    # which is plain data, and a column is a scalar. Shallow is the tempting
    # half-measure and is wrong: a `list` handed out by reference can be edited
    # in place, which reaches the row on the next save while `__setattr__`
    # never runs, so a field with `records_change` logs nothing and the change
    # log is silently short an edit.
    return copy.deepcopy(
        {name: getattr(self, name) for name in self.__dray_fields__}
    )


def _parse(cls: type, data: Mapping[str, Any]) -> Any:
    """
    Build from data that came from outside — a form, a spreadsheet, an API.

    Strict on purpose. An unknown key is a misspelled column header and a bad
    value is bad now rather than at the database, which is the opposite of what
    loading a row wants.

    Every rule on every field runs, and then every rule the record wrote about
    itself, so what this hands back is a record the write will not refuse for
    anything that was visible here. A rule sees what you supplied and no more —
    the write has filled in nothing yet — so one reading a field the write fills
    should say nothing when it is absent rather than refuse it.
    """
    unknown = sorted(set(data) - set(cls.__dray_fields__))
    if unknown:
        raise ValidationError(
            f"{cls.__name__} has no field {', '.join(repr(k) for k in unknown)}"
        )
    # The converting is the constructor's now, so this is a plain build. What
    # `parse` still adds over `Person(**data)` is the pair either side of it: an
    # unknown key refused above, and every rule on every field checked below in
    # one go rather than at the first failure.
    built = cls(**data)
    built._dray_validate()
    # Called from here rather than from `_dray_validate`, which is the field
    # pass the write runs too. The write has a pass of its own for these, after
    # the filling, and a rule reached through the field pass would run twice per
    # save — the first time with nothing an `on_add` supplies on the record to
    # read, which is the defect that moved it to the write in the first place.
    # This is the other door, and the only one where a rule can be judged
    # against what a form posted at the moment the form is read.
    run(built, CHECK)
    return built


def _load(cls: type, row: Mapping[str, Any]) -> Any:
    """
    Build from a row this table already holds.

    Lenient, and deliberately the mirror of `parse`. Keys the class no longer
    declares are dropped so a field can be retired without a backfill, and
    nothing is validated, so tightening a rule never makes existing records
    unreadable.
    """
    known = {k: v for k, v in row.items() if k in cls.__dray_fields__}
    for k, v in (row.get(cls.__dray_blob_column__) or {}).items():
        if k in cls.__dray_fields__:
            known[k] = restore(v, cls.__dray_fields__[k])
    # The one door that does not convert, and the reason the constructor takes
    # a flag to say so. A stored value has already been through the converter
    # on its way in; running it again would be at best a waste and at worst a
    # row that stops loading because the rule it was written under has since
    # been tightened.
    built = cls(**known, _dray_stored=True)
    # Nothing here was said by anybody. These values came out of the table, and
    # a row carrying an author from the write that made it two years ago has not
    # thereby named one for the write about to happen — so a save may fill them
    # in exactly as it would on a record nobody had touched.
    object.__setattr__(built, "_dray_said", set())
    return built


def _collection(self: Any) -> Any:
    """
    The collection this record came from.

    A record knows it is persisted, and reaches storage through the collection
    that loaded it. That backref is what lets `person.save()` and `person.notes`
    work without threading a store through every caller, and severing it would
    cost more than it tidied.
    """
    found = getattr(self, "_dray_collection", None)
    if found is None:
        raise RuntimeError(
            f"this {type(self).__name__} did not come from a store, so it has "
            "nowhere to save to. Add it through a collection first."
        )
    return found


def _children(self: Any) -> Any:
    """
    Every kind of child declared for this record, by the name its parent calls
    it — `{"notes": ..., "logs": ...}`.

    Driven by the declaration rather than by what happens to be there, so
    `"notes" in record.children` answers whether this kind of record is
    noteable at all. That is a different question from whether it has any
    notes, and a handler shared across record types is asking the first one.

    A mapping rather than a set of names, so a page can walk everything hanging
    off a record without being told in advance what that is.

    Spelled `_dray_children` as well, and that is the spelling to write in code
    shared across record types: a household declaring `children: int = 3` has
    taken the plain word for its own, and dray's reading is still here.
    """
    from dray.child import CHILDREN

    return {
        kind.__dray_name__: getattr(self, kind.__dray_name__)
        for kind in CHILDREN.get(type(self), ())
    }


def _store(self: Any) -> Any:
    """
    The store this record came from, so a method on the class can read the rest
    of the database.

        @after_commit
        def tell_the_next_one(self):
            person = self.store.people.by_id(self.parent_id)
            enqueue("person-updated", person.id)

    Which is the only way a marked method gets one: it is called with `self` and
    nothing else, and the class was written long before any store existed.
    Everything the store can do it can do here — the store is open while a
    handler runs, so a `@before_delete` writing through this writes into the
    delete's own transaction and an `@after_commit` reading through it sees the
    rows that just landed, exactly as one holding a store in a closure did.

    **It is the store, not a copy, and it lasts as long as that store does.**
    Read it where you are rather than putting it somewhere: on a pool the
    connection goes back when the `with pool.store()` ends, and a record kept
    past that point is holding a store that is somebody else's next request.
    Which is already true of `person.save()`, and no worse here.

    A record that has never been in a store has none to give, and says so the
    way it says it has nowhere to save to. Which is what a `@check` on a record
    built in memory sees: the write attaches the record after the rules have
    run, so a rule needing a store has one on a save and not on the first `add`.

    Spelled `_dray_store` as well, for a class whose domain wanted the word or
    which stands its own `store` in front of dray's.
    """
    return _collection(self).store


def _save(
    self: Any, *, etag: str | None = None, given: dict[str, Any] | None = None
) -> Any:
    """
    Write this record and everything queued against it, in one transaction.

        person.save(given={"whom": "rod"}, etag=posted["etag"])

    Pass the `etag` the reader was shown to refuse a write over somebody else's
    work. It is spelled out rather than put in `given` because it is a
    precondition on the write and not a value being assigned — and with the
    assignments inside `given` it can only mean the one thing, whatever fields
    the record declares.
    """
    return _collection(self).save(self, etag=etag, given=given)


def _delete(self: Any) -> None:
    """Remove this record and everything hanging off it, raising
    `RecordNotFound` if its row has already gone."""
    _collection(self).delete(self)


def _eq(self: Any, other: Any) -> Any:
    """
    Whether these are the same record, which is not whether they are alike.

    A record has a primary key from the moment it is made, so its identity is
    settled and nothing about its contents has a say in it. Two objects for the
    same row are the same record however far they have drifted — one read hours
    ago and one read just now, one carrying edits nobody has saved. That is the
    question anybody comparing records is actually asking, and the one a set or
    a dict key has to be able to answer.

    A dataclass compares every field instead, which made a record stop equalling
    itself the moment somebody wrote it: the token in the row moved and the copy
    in hand did not. Reading it twice either side of a save gave two objects that
    were not equal, so `in`, `index` and `remove` all answered on how stale
    something was.

    Different kinds are never equal, whatever their ids. Two tables can hold the
    same id and mean nothing by it.
    """
    if type(self) is not type(other):
        return NotImplemented
    return key_of(self) == key_of(other)


def _hash(self: Any) -> int:
    """
    The key, and so fixed for the life of the object — which is what `_setattr`
    refusing to reassign one is protecting. A key that moves while it is in a
    set is a key the set can no longer find.
    """
    return hash(key_of(self))


# The six a caller is meant to reach for. Each is attached twice — under the
# plain word where the class has not claimed it, and always under `_dray_` —
# so no domain word is spent and a class that claims one can still call dray's.
_RECORD_LENT: dict[str, Any] = {
    "save": _save,
    "delete": _delete,
    "parse": classmethod(_parse),
    "as_dict": _as_dict,
    "children": property(_children),
    "store": property(_store),
}

# How a record is built and stored, rather than anything a caller reaches for.
# One name apiece and all of them under `RESERVED_PREFIX`, so that everything
# dray puts on a record stands somewhere no field can — the prefix is the whole
# of the answer to "what may a field not be called", and a second list of words
# would be a second answer to keep true. They are named in the manual only so
# that finding one in a traceback is not a mystery.
_RECORD_MEMBERS: dict[str, Any] = {
    "_dray_validate": _validate,
    "_dray_blob": _blob,
    "_dray_load": classmethod(_load),
}

# The three Python looks up by its own spelling, keyed here by the word in the
# middle: `__setattr__` where the class has not claimed it, `_dray_setattr`
# always. Nobody calls these by name in the ordinary way — they are what `=`,
# `==` and `hash()` reach — but each is the whole of a behaviour dray promises,
# so a class standing a rule in front of one still needs dray's underneath.
_RECORD_HOOKS: dict[str, Any] = {
    "setattr": _setattr,
    "eq": _eq,
    "hash": _hash,
}

# The base a record inherits to be legible, and nothing else. `@record` is
# applied rather than inherited, and this changes none of that: it carries no
# behaviour, is empty when the program runs, and a record that leaves it off
# behaves identically. What it buys is that `person.save()` and
# `self._dray_save()` are calls an editor can see rather than attributes on a
# class it has given up resolving.
#
# Declared under `TYPE_CHECKING` and empty everywhere else, which is the whole
# of the care here rather than a tidiness. `_claimed` asks the *hierarchy*
# whether a word has been spoken for, because a `save` on a base class is the
# domain using that word every bit as much as one in the body is — so a base
# carrying real methods would have dray step politely aside from all six of
# them, and `person.as_dict()` would quietly hand back `None`. A base that is
# not there at runtime cannot claim anything.
#
# Each member says the first line of what the real one says, because that is
# what a hover shows and a caller reading it should not get a second, shorter
# story. `test_the_base_says_what_the_real_member_says` is what keeps the two
# from drifting.
if TYPE_CHECKING:

    class Record:
        """
        Inherit this and an editor can see what dray attaches.

            @record(table="person", collection="people")
            class Person(Record):
                family_name: str = field()

            person.save()      # a call, rather than an unknown attribute

        Optional, and it changes nothing about how a record behaves. The
        members below are declared and never defined: dray binds the real ones
        when the decorator runs.
        """

        id: Any
        etag: Any

        def save(
            self,
            *,
            etag: str | None = None,
            given: dict[str, Any] | None = None,
        ) -> Any:
            """
            Write this record and everything queued against it, in one
            transaction.
            """
            ...

        def delete(self) -> None:
            """
            Remove this record and everything hanging off it, raising
            `RecordNotFound` if its row has already gone.
            """
            ...

        def as_dict(self) -> dict[str, Any]:
            """Every field this record declares, by name."""
            ...

        @classmethod
        def parse(cls, data: Mapping[str, Any]) -> Any:
            """
            Build from data that came from outside — a form, a spreadsheet, an
            API.
            """
            ...

        @property
        def children(self) -> Any:
            """
            Every kind of child declared for this record, by the name its
            parent calls it — `{"notes": ..., "logs": ...}`.
            """
            ...

        @property
        def store(self) -> Any:
            """
            The store this record came from, so a method on the class can read
            the rest of the database.
            """
            ...

        def _dray_save(
            self,
            *,
            etag: str | None = None,
            given: dict[str, Any] | None = None,
        ) -> Any:
            """
            Write this record and everything queued against it, in one
            transaction.
            """
            ...

        def _dray_delete(self) -> None:
            """
            Remove this record and everything hanging off it, raising
            `RecordNotFound` if its row has already gone.
            """
            ...

        def _dray_as_dict(self) -> dict[str, Any]:
            """Every field this record declares, by name."""
            ...

        @classmethod
        def _dray_parse(cls, data: Mapping[str, Any]) -> Any:
            """
            Build from data that came from outside — a form, a spreadsheet, an
            API.
            """
            ...

        def _dray_validate(self) -> None:
            """
            Every rule on every field at once. What `parse` runs and hydrating
            a row does not — a row the table already holds has been through
            this once and has to keep loading whatever has been tightened
            since.
            """
            ...

        def _dray_blob(self) -> dict[str, Any]:
            """
            The fields with no column of their own, as they go into
            jsonb.
            """
            ...

        def _dray_hash(self) -> int:
            """
            The key, and so fixed for the life of the object — which is what
            `_setattr` refusing to reassign one is protecting. A key that moves
            while it is in a set is a key the set can no longer find.
            """
            ...

else:

    class Record:
        pass
