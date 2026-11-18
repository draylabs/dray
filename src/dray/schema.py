"""
The table a record implies.

dray does not run migrations. It works out what a record's table should look
like, hands you the statements, and can tell you when the table and the class
have drifted apart — which is the failure a hand-written schema cannot see,
because a missing column looks exactly like a field nobody has set yet.
"""

import hashlib
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID
from typing import Any

from dray.model import (
    Index,
    Sql,
    _nulls,
    base_type,
    indexed_text,
    referenced,
)
from dray.store import cursor

# What a `numeric` column is when the field says nothing: DSQL's own default,
# written out rather than left to it. DSQL fills these in whether or not they
# are asked for and applies them on `INSERT` and `UPDATE` — so a bare `numeric`
# is unbounded on local PostgreSQL and eighteen digits with six after the point
# on a cluster, and a rate stored whole by the test suite is rounded in
# production with nothing raised at either end. Said out loud, both databases
# round in the same place and a green suite means something.
DECIMAL_PRECISION = 18
DECIMAL_SCALE = 6

# The Python types the database already has a type for, and no more. This is
# not dray growing a type system — every entry is a column type PostgreSQL and
# DSQL both hold natively, so the value goes down as itself and comes back as
# itself without anyone converting anything.
#
# Anything absent falls through to `text` below, which is a guess and a quiet
# one: a type missing from this table is stored as a string that looks exactly
# like the value it should have been, and nothing at either end says so.
SQL_TYPES: dict[Any, str] = {
    str: "text",
    int: "bigint",
    bool: "boolean",
    float: "double precision",
    Decimal: f"numeric({DECIMAL_PRECISION},{DECIMAL_SCALE})",
    datetime: "timestamptz",
    date: "date",
    time: "time",
    timedelta: "interval",
    UUID: "uuid",
    bytes: "bytea",
    dict: "jsonb",
    list: "jsonb",
}


def _sql_type(annotation: Any, rules: Mapping[str, Any] | None = None) -> str:
    """
    The column an annotation becomes, and how big it is where a field said.

    The rules are optional because only `numeric` has anything to say beyond
    its name, and only when `field(precision=..., scale=...)` was given. A
    field saying that when its annotation is not a `Decimal` never reaches
    here — `model.check_numeric` refuses the class — so a precision arriving at
    all is enough to know what to write.
    """
    kind = SQL_TYPES.get(base_type(annotation), "text")
    precision = (rules or {}).get("precision")
    if precision is None:
        return kind
    return f"numeric({precision},{rules['scale']})"


def _columns(cls: type) -> list[tuple[str, str]]:
    """
    Each column and its type, in the order the table is built — the key
    first, then what the class declared, then the guard.

    Read off the class rather than resolved again here. `get_type_hints` is
    all-or-nothing and raises on the first name it cannot see, so a record
    declared inside a function using a local type had no schema at all — and
    two readings of the same annotations is how a field comes to accept one
    type and be stored as another.
    """
    hints = cls.__dray_annotations__
    return [
        (name, _sql_type(hints.get(name, str), cls.__dray_fields__.get(name)))
        for name in cls.__dray_columns__
    ]


# What either database will hold of an identifier. Anything longer is cut to
# this and nothing is said about it, so a name dray generates past the limit is
# not the name the table ends up carrying unless dray cuts it first.
NAME_LIMIT = 63


def _shortened(name: str, limit: int = NAME_LIMIT) -> str:
    """An identifier as the database will hold it: itself, or its first 63
    bytes where it is longer than that. `limit` is for a name that has
    something still to go on the end of it."""
    if len(name.encode()) <= limit:
        return name
    # On a character boundary rather than at a byte. `name[:63]` agrees with the
    # database for as long as every identifier is ASCII and disagrees the moment
    # one is not — a split multi-byte character is a name no table has, which is
    # this same bug arriving by another road. Everything before the cut is valid
    # UTF-8 by construction, so `errors="ignore"` drops the partial sequence at
    # the end and nothing else.
    return name.encode()[:limit].decode(errors="ignore")


def _literal(value: Any) -> str:
    """
    One value of a predicate as the text a statement holds, since a `create
    index` has nowhere to pass a parameter.

    Written here rather than asked of psycopg, which would quote it as well and
    no worse. The text is what an index is named from, so how a date or a
    quote mark is spelled has to be the same for as long as there are tables
    carrying that name — and a spelling that belonged to a driver would be one
    a driver's release could move.

    `model.PREDICATE_VALUES` is the list this answers for and a class is held
    to it before it gets here. `bool` before `int`, which it is a kind of.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        # By its value and not by how it was typed. `1.5` and `1.50` are the
        # same rows, and `str` would make them two names for one index.
        return format(value.normalize(), "f")
    if isinstance(value, int):
        return str(value)
    # A quote doubled, which is the whole of quoting while a backslash means
    # nothing — and it means nothing on DSQL and on any PostgreSQL left at its
    # default, `standard_conforming_strings`.
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _key(cls: type, name: Any) -> str:
    """
    One key of an index as its statement writes it: the column, or for a field
    in the blob its value read out of the document as text.

    As text and never cast to what the field declares. A cast is the better
    index for as long as every row holds what the class says, and the blob is
    where nothing makes that so — one row with something else under the key
    fails the build, and then every read that goes through it. `->>` fails on
    nothing, and `model.BLOB_INDEXABLE` is the fields it is good enough for.
    """
    if str(name) not in cls.__dray_blob__:
        return f"{name}{_nulls(name)}"
    return f"({cls.__dray_blob_column__}->>'{name}'){_nulls(name)}"


def _predicate(cls: type, declared: Index) -> str | None:
    """
    Which rows an index covers, as the `where` of its statement — or `None` for
    one that covers them all.

    `Sql` goes through as it was written, spacing and all. Tidying it here
    would mean knowing which spaces are inside a quoted value and which line
    ends a comment, and that is the database's reading to do. A mapping is one
    equality per field, in name order, which `model._covered` has already put
    them in.
    """
    where = declared.where
    if where is None:
        return None
    if isinstance(where, Sql):
        return where.strip()
    terms = []
    for name, value in where:
        inside = name in cls.__dray_blob__
        reads = f"{cls.__dray_blob_column__}->>'{name}'" if inside else name
        if value is None:
            terms.append(f"{reads} is null")
        else:
            # Text against text for a field in the blob, whatever it holds:
            # `->>` hands a number back as a string, and `= 7` would compare
            # that with an integer.
            against = indexed_text(value) if inside else value
            terms.append(f"{reads} = {_literal(against)}")
    return " and ".join(terms)


def _index_name(cls: type, declared: Index) -> str:
    """
    What dray calls an index it asked for: the table, then the columns it
    covers, joined with underscores, cut to what an identifier can hold — and
    for one covering only some of the rows, six characters after that which
    say which.

    `drift` matches an index by this name and by nothing else, which makes the
    name a promise rather than a detail — change how it is built and every table
    already carrying one reports an index it has as missing. Which is also why
    the columns are joined the way one column always was: `person_family_name`
    is the name it had before there was any other kind of index.

    And why the cut is the database's own rather than anything cleverer. A hash
    suffix would keep every long name distinct and would rename every over-long
    index already in a live table, so `drift` would report each of them missing
    for ever. Cutting the way the database cuts renames nothing: the string this
    starts returning is the string those tables were already carrying.
    """
    named = "_".join((cls.__dray_table__, *declared.columns))
    covers = _predicate(cls, declared)
    over = [_key(cls, name) for name in declared.columns]
    # An index over the blob is marked the same way and for the same reason:
    # the field's name does not say the index is over the document. It matters
    # on the day the field is given a column — the class then asks for the
    # unmarked name, which no index has, where otherwise it would find this
    # one under it and build nothing over the column.
    if any(str(name) in cls.__dray_blob__ for name in declared.columns):
        covers = f"({', '.join(over)})" + (f" where {covers}" if covers else "")
    if covers is None:
        return _shortened(named)
    # The columns no longer say which index this is, since the same ones over
    # other rows are another index. So the predicate is in the name, as six
    # characters of a digest of it: short enough to leave the columns
    # readable, and worked out rather than chosen so that a predicate cannot
    # be edited under a name that stays put. The objection to a digest above
    # is about names tables already carry, and no table carries one of these.
    #
    # Of the predicate with its spacing closed up, which is the one place that
    # is done. A statement reflowed onto two lines is the same index and has to
    # keep its name; the cost is that two differing only by spaces inside a
    # quoted value share one, and are refused where the class is written like
    # any other pair that do.
    #
    # Cut before it goes on rather than after, or two long names differing
    # only in their predicates would be cut back into one.
    mark = hashlib.sha256(" ".join(covers.split()).encode()).hexdigest()[:6]
    return f"{_shortened(named, NAME_LIMIT - 7)}_{mark}"


def _reference_name(cls: type, name: str) -> str:
    """What dray calls the key a `references=` field declares: the table, the
    field, and `_fk`, cut so that the last of those survives. `drift` looks
    for it by this name and a violation is traced back by it."""
    return f"{_shortened(f'{cls.__dray_table__}_{name}', NAME_LIMIT - 3)}_fk"


def _reference(cls: type, name: str) -> str:
    """The constraint itself, as a `create table` or an `alter table` holds it.

    With nothing said about what a delete of the other record does, which
    leaves the database's own answer: it is refused. Clearing the field or
    taking the row as well would be the database writing rows dray did not —
    past the etag, the read cache and every hook."""
    target = referenced(cls, name)
    return (
        f"constraint {_reference_name(cls, name)} foreign key ({name})"
        f" references {target.__dray_table__} ({target.__dray_key__})"
    )


def references(cls: type, *, asynchronous: bool = True) -> list[str]:
    """
    The statements that give a table already there the keys its class
    declares, two for each.

        schema.references(Event)

    The first adds the key `not valid`, which holds every write from then on
    to it and looks at no row already in the table — the only form DSQL takes
    on a table that exists. The second checks the rows that were there, as a
    background job on a cluster, and fails on the first one naming a record
    that has gone. So what points at nobody has to be put right before it
    will pass.

    `asynchronous=False` writes the second the way PostgreSQL takes it.
    """
    table = cls.__dray_table__
    validate = "alter table async" if asynchronous else "alter table"
    made = []
    for name in cls.__dray_references__:
        made.append(f"alter table {table} add {_reference(cls, name)} not valid")
        made.append(
            f"{validate} {table} validate constraint {_reference_name(cls, name)}"
        )
    return made


def create_table(cls: type, *, without: Any = ()) -> str:
    """
    The `create table` for a record.

    A field declared `references=` is a key here, to a table that has to
    exist when this runs. `without` names fields whose keys to leave out, for
    two records that name each other: make one without, make the other, and
    add what was left out with `references`.

    `if not exists` because DSQL cannot put DDL and DML in one transaction, so
    the statement and the row recording that it ran cannot commit together —
    which means every migration has to survive being run twice.

    The blob column is always there, even for a class that declares no blob
    fields. It is what makes adding an attribute a write rather than a
    migration, and a table that lacks it would need one on the day you want it.

    An index declared `unique=True` over every row is a constraint here rather
    than a statement of its own, which is dray deciding where the DDL goes
    rather than the caller. One with a `where=`, or over a field in the blob,
    is not here at all, since a constraint has no way to say either.

    A constraint brings its own backing index, built with the table and
    enforcing the moment the table exists — where `create unique index async` is
    a background job that lets a duplicate through until it finishes.
    """
    # Named rather than left to the database, which would call it
    # `shift_on_date_slot_key`. The backing index takes the constraint's
    # name, and that name is what `drift` goes looking for.
    constraints = [
        f"    constraint {_index_name(cls, declared)}"
        f" unique ({', '.join(declared.columns)})"
        for declared in cls.__dray_indexes__
        if _is_constraint(cls, declared)
    ]
    # A key to another record, which that record's table has to be there for.
    # `without` is the ones to leave out, for two tables that name each other:
    # neither can go first carrying its key, so one is made bare and given it
    # afterwards.
    constraints += [
        f"    {_reference(cls, name)}"
        for name in cls.__dray_references__
        if name not in without
    ]
    key = cls.__dray_key__
    lines = [
        f"    {name} {kind}" + (" primary key" if name == key else "")
        for name, kind in _columns(cls)
    ]
    lines.append(
        f"    {cls.__dray_blob_column__} jsonb not null default '{{}}'::jsonb"
    )
    body = ",\n".join(lines + constraints)
    return f"create table if not exists {cls.__dray_table__} (\n{body}\n)"


def _is_constraint(cls: type, declared: Index) -> bool:
    """Whether an index is one a `create table` can hold: unique, over every
    row, and over columns. A constraint has no `where` and takes no
    expression, so a unique index over some of the rows or over a field in the
    blob is a statement of its own whichever table it is for."""
    return (
        declared.unique
        and declared.where is None
        and not any(str(name) in cls.__dray_blob__ for name in declared.columns)
    )


def _leads_with_the_parent(cls: type) -> bool:
    """Whether the class has declared an index that already serves the reads
    the implicit one is there for.

    Only the two columns in that order. `(parent_type, parent_id, walked_at)`
    covers the pair; `(parent_id, parent_type)` is left standing beside dray's
    own, and would in fact serve a read matching both columns — but everything
    here is said about a leading run rather than about a set of columns, and the
    strict reading costs an index nobody needed where the loose one would cost a
    read nobody indexed. Uniqueness is beside the point: a unique btree serves a
    leading run like any other.
    """
    pair = (cls.__dray_parent_type__, cls.__dray_parent_id__)
    # Over every row, or it serves the reads through a parent for some of a
    # parent's children and leaves the rest — and the delete that takes them
    # all — to a scan.
    return any(
        one.columns[:2] == pair and one.where is None
        for one in cls.__dray_indexes__
    )


def _index_statements(cls: type, asynchronous: bool) -> list[tuple[bool, str]]:
    """Every index dray asks for, each paired with whether it is one the
    `create table` holds as a constraint — which is what decides whether
    `statements` writes it out here or leaves it there."""
    table = cls.__dray_table__
    plain = "create index async" if asynchronous else "create index"
    unique = "create unique index async" if asynchronous else "create unique index"

    made = []
    if cls.__dray_parents__ and not _leads_with_the_parent(cls):
        made.append(
            (
                False,
                f"{plain} if not exists {table}_parent"
                f" on {table} ({cls.__dray_parent_type__},"
                f" {cls.__dray_parent_id__})",
            )
        )
    for declared in cls.__dray_indexes__:
        kind = unique if declared.unique else plain
        # The key carries a null placement where one was declared and nothing
        # else. A direction is refused on the class, and the unique kind is
        # refused a placement — so what is written here is always a shape both
        # this statement and the constraint in `create_table` can hold.
        over = ", ".join(_key(cls, name) for name in declared.columns)
        covers = _predicate(cls, declared)
        made.append(
            (
                _is_constraint(cls, declared),
                f"{kind} if not exists {_index_name(cls, declared)}"
                f" on {table} ({over})"
                + (f" where {covers}" if covers else ""),
            )
        )
    return made


def create_indexes(cls: type, *, asynchronous: bool = True) -> list[str]:
    """
    The indexes a record declares, and the one a child's shape implies.

    These are the statements for a table that is already there — a migration
    adding an index to a live table. `statements` is the other half, for a table
    being created, and a unique index over every row and over columns
    appears there as a constraint in the `create table` instead of here.

    Every read of a child is "the children of this parent", so an index on
    `(parent_type, parent_id)` is on every child table that has not asked for
    one itself: `by_id`, `find`, `count`, the ordered read and the cascading
    delete all filter on those two columns. Everything the class declares is
    added beside it, over exactly the columns it named.

    A child that declares an index leading with those two columns gets that one
    and nothing else. It serves every read the implicit index was there for, so
    building both would spend a second slot on a question already answered.

    A unique index is `create unique index async`, and that is where the window
    is. DSQL builds it as a background job and it enforces nothing until the
    build finishes, so a duplicate written in the meantime is taken — which is
    the migration's problem to think about: create it before the data, or hold
    the check in code until `sys.jobs` reports the build complete.

    `async` because DSQL builds a secondary index as a background job rather
    than blocking writes on a table that already has rows, and it is the only
    form DSQL takes. It is also not PostgreSQL, which is the one place dray
    stops writing statements local PostgreSQL will accept —
    `asynchronous=False` is what `store.create` asks for, and a migration takes
    these as they are.
    """
    return [statement for _, statement in _index_statements(cls, asynchronous)]


def statements(
    cls: type, *, asynchronous: bool = True, without: Any = ()
) -> list[str]:
    """Everything a record's table needs, one statement per entry.

    One per entry because DSQL takes a single DDL statement per transaction, so
    a caller that ran these as one script would fail on the second.

    `asynchronous=False` gives the same schema in statements PostgreSQL
    accepts, which is what `store.create` runs against local PostgreSQL. A
    migration wants these as they are: `create index async` is the form DSQL
    takes.

    A unique index is in the `create table` rather than among the statements
    after it, so it is enforcing from the moment the table is there. It is the
    same declaration `create_indexes` writes as `create unique index async`,
    said the way that is valid for a table nobody has written to yet. One with
    a `where=`, or over a field in the blob, is the exception, since a
    constraint can carry neither: it is among the statements after, and on a
    cluster it enforces once its build has finished."""
    return [
        create_table(cls, without=without),
        *(
            statement
            for constraint, statement in _index_statements(cls, asynchronous)
            if not constraint
        ),
    ]


# How a value comes back out of the blob and into a column of its own. Almost
# always a cast, because jsonb holds a string wherever JSON has no type of its
# own — but two are encoded rather than written out, so the SQL that reverses
# each is the mirror of `model.FROM_TEXT` and has to stay that way.
OUT_OF_BLOB: dict[str, str] = {
    "jsonb": "{blob}->'{name}'",
    "bytea": "decode({blob}->>'{name}', 'hex')",
    "interval": "make_interval(secs => ({blob}->>'{name}')::double precision)",
}


def _field_sql(cls: type, name: str) -> tuple[str, str]:
    """
    One field's column type, and the SQL that reads it back out of the blob.

    Both answers come from here because two callers ask the same question from
    opposite ends: `promote` is moving the value into a column, and
    `Names.sql_for` is reading it where it still is. Each holding its own copy
    of the table above is a mirror that cracks the first time a type is added
    to `SQL_TYPES` — a promotion that works beside a report that does not,
    about the same field in the same table.
    """
    kind = _sql_type(
        cls.__dray_annotations__.get(name, str), cls.__dray_fields__.get(name)
    )
    shape = OUT_OF_BLOB.get(kind, "({blob}->>'{name}')::" + kind)
    return kind, shape.format(blob=cls.__dray_blob_column__, name=name)


def promote(cls: type, *names: str) -> list[str]:
    """
    The statements that move a field out of the blob into a column of its own.

        schema.promote(Person, "suburb")

    Deleting `stored_in="blob"` is the first of four steps. These are the other
    three: add the column, copy across what the blob is holding, and drop the
    key.

    The last one is the step nobody thinks of and the one that bites.
    `_dray_load` lets the blob override the column, so a row still carrying the
    key hydrates with the old value while `find` — reading the column now,
    correctly — cannot see it. The record says `'Leura'` and nothing can find
    `'Leura'`. Hand-written SQL reaching in with `data->>'suburb'` has the
    mirror of the same problem, seeing only the rows written before the change
    until it sees none.

    Refused for a field still declared `stored_in="blob"`, because changing the
    class is the step these statements come after and doing them in the other
    order leaves a column nothing writes to.

    Written to survive being run twice, like everything else here: the column is
    added `if not exists` and both updates skip the rows that no longer carry
    the key. Which also makes them safe to stop halfway and resume.

    What they do not do is fit. DSQL takes 3,000 rows to a transaction, so each
    update is one transaction and a table past that has to be walked in batches
    instead. dray is not a migration runner — these are statements to read, and
    to put in a migration that knows how big the table is.
    """
    table = cls.__dray_table__
    blob = cls.__dray_blob_column__
    written = []

    for name in names:
        if name not in cls.__dray_fields__:
            raise ValueError(
                f"{cls.__name__} has no field {name!r} to promote. Promotion "
                "moves a declared field from the blob to a column; it does not "
                "add one."
            )
        if name in cls.__dray_blob__:
            raise ValueError(
                f'{cls.__name__}.{name} is still declared stored_in="blob". '
                "Delete that first — these statements are for a field the class "
                "already says has a column, and running them before the class "
                "changes leaves a column nothing ever writes to."
            )

        kind, held = _field_sql(cls, name)

        written += [
            f"alter table {table} add column if not exists {name} {kind}",
            f"update {table} set {name} = {held} where {blob} ? '{name}'",
            f"update {table} set {blob} = {blob} - '{name}'"
            f" where {blob} ? '{name}'",
        ]
    return written


def create_namespace(name: str) -> str:
    """
    The schema a `Store(namespace=...)` expects to find.

        schema.create_namespace("orders")
        # create schema if not exists orders

    A statement rather than something `store.create` does behind you, because
    making a schema is an admin operation and this is not a migration runner.
    It belongs in a migration beside the `create table`s that will land in it.

    On DSQL this is what a deployment that would rather not run as cluster admin
    has to do: permissions are managed with schema-level grants, the admin role
    owns `public`, and a non-admin role creates its objects in a schema made for
    it. The grants themselves are not here — without them a namespace is a
    naming convention with no teeth, and they are DSQL-side work.
    """
    if not name.isidentifier():
        raise ValueError(
            f"{name!r} is not a name a schema can have. Letters, digits and "
            "underscores, not starting with a digit."
        )
    return f"create schema if not exists {name}"


def drift(conn: Any, cls: type) -> list[str]:
    """
    What the class declares that the table does not have.

    This is the whole reason dray knows about DDL at all. The blob has no
    constraints, so a field that quietly stopped being saved looks exactly like
    a field nobody has filled in yet — and the only place that can be noticed is
    here, by asking the database what it actually has.

    `current_schema()` and not the table name alone. A cluster holding two
    services, each with a `person`, answers for both — so a table genuinely
    missing a column comes back complete because the other one has it, which is
    the failure this exists to catch, reported as all clear. It is also the
    schema every statement dray writes already resolves through, since none of
    them qualify a table name.

    Indexes as well as columns, and that is the point of them being declarable
    at all. Drift is the argument for dray knowing about DDL, and indexes are
    the dimension that decides whether the thing is usable: a table with every
    column and none of its indexes reads exactly like a table that is right,
    until somebody times a query on it. dray only asks about the indexes
    it names itself, because those are the ones it can be sure of — anything
    else on the table is somebody's deliberate work and none of its business.

    Which reaches a unique index either way it was made. The constraint in a
    `create table` and the `create unique index async` for a table that was
    already there carry the same name, so one question answers both.

    What it still cannot see is a column of the wrong *type*, which is the same
    blind spot one notch along: an annotation dray does not know becomes `text`,
    and a `text` column that should be a `date` is present and named correctly.
    """
    table = cls.__dray_table__
    with cursor(conn) as cur:
        cur.execute(
            "select column_name from information_schema.columns"
            " where table_name = %s and table_schema = current_schema()",
            [table],
        )
        present = {row[0] for row in cur.fetchall()}

    if not present:
        return [f"table {table!r} does not exist"]

    expected = {name for name, _ in _columns(cls)} | {cls.__dray_blob_column__}
    missing = [
        f"{table}.{name} is declared but not in the table"
        for name in sorted(expected - present)
    ]

    with cursor(conn) as cur:
        cur.execute(
            "select indexname from pg_indexes"
            " where tablename = %s and schemaname = current_schema()",
            [table],
        )
        built = {row[0] for row in cur.fetchall()}

    # Named rather than matched on their columns, which is what makes this
    # answerable at all: dray decides the name, so an index it asked for either
    # carries that name or was never made.
    wanted = {
        statement.split(" if not exists ")[1].split(" on ")[0]
        for statement in create_indexes(cls)
    }
    keyed = _keys(conn, table)
    keys = {_reference_name(cls, name) for name in cls.__dray_references__}
    return (
        missing
        + [
            f"{table} has no index {name!r}, which the class asks for"
            for name in sorted(wanted - built)
        ]
        + [
            f"{table} has no key {name!r}, which the class asks for"
            for name in sorted(keys - set(keyed))
        ]
        # Added and never checked against the rows that were there, which is
        # the first of `references`' two statements run without the second.
        # Every write since is held to it and the rows before it may not be.
        + [
            f"{table} has the key {name!r} and has not checked the rows that "
            "were already there against it"
            for name in sorted(keys & set(keyed))
            if not keyed[name]
        ]
    )


def _keys(conn: Any, table: str) -> dict[str, bool]:
    """The foreign keys a table carries, each with whether it has been checked
    against every row. In the schema statements resolve through, for the
    reason `drift` asks about columns that way."""
    with cursor(conn) as cur:
        cur.execute(
            "select c.conname, c.convalidated from pg_constraint c"
            " join pg_class r on r.oid = c.conrelid"
            " join pg_namespace n on n.oid = r.relnamespace"
            " where c.contype = 'f' and r.relname = %s"
            " and n.nspname = current_schema()",
            [table],
        )
        return {row[0]: bool(row[1]) for row in cur.fetchall()}
