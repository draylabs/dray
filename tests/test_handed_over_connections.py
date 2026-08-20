"""
A connection an application built, rather than one dray made for itself.

`Store(psycopg.connect(...))` is the documented way to run against a local
PostgreSQL, and a connection that already exists has already been configured. A
row factory is the usual one — `dict_row` so that everything else in the
application reads rows by name — and dray was inheriting it on every cursor but
`select_many`, then reading those rows by position.

`count()` raised `KeyError: 0`, which is at least loud. The insert did not:
`zip(computed, cur.fetchone())` walks a dict's *keys*, so a field filled by the
write took its own column name as a string — `created_at` holding
`'created_at'`, assigned through `object.__setattr__` and so past the type the
field declared.
"""

from datetime import datetime

import pytest
from psycopg.rows import dict_row

from dray import Store, child, clock, field, record, schema


@pytest.fixture
def dict_store(postgresql):
    """A store on a connection that hands rows back as dicts."""
    postgresql.row_factory = dict_row
    return Store(postgresql)


@record(table="visitor", collection="visitors")
class Visitor:
    family_name: str = field()
    created_at: datetime | None = field(default=None, on_add=clock)


@child(of=Visitor, name="notes", table="visitor_note")
class VisitorNote:
    body: str = field()
    written_at: datetime | None = field(default=None, on_add=clock)


@pytest.fixture
def visitors(dict_store):
    dict_store.create(Visitor, VisitorNote)
    return dict_store.visitors


def test_counting_reads_the_number_and_not_a_key(visitors):
    visitors.add(Visitor(family_name="Hemingway"))
    assert visitors.count() == 1
    assert visitors.count(equals={"family_name": "Hemingway"}) == 1


def test_a_record_comes_back_holding_the_time_and_not_its_own_column_name(
    visitors, dict_store
):
    visitor = visitors.add(Visitor(family_name="Hemingway"))

    assert isinstance(visitor.created_at, datetime)
    row = dict_store.conn.execute("select created_at from visitor").fetchone()
    assert visitor.created_at == row["created_at"]


def test_a_child_comes_back_holding_the_time_too(visitors):
    # The same `zip` over the same cursor, one module along.
    visitor = visitors.add(Visitor(family_name="Hemingway"))
    note = visitor.notes.add("As written.")
    visitor.save()

    assert isinstance(note.written_at, datetime)


def test_drift_reads_the_column_names(visitors, dict_store):
    assert schema.drift(dict_store.conn, Visitor) == []


def test_the_connection_keeps_the_row_factory_it_was_given(dict_store):
    """dray says what it wants on its own cursors and switches nothing on the
    connection. `store.conn` is still the caller's to query — autocommit is
    changed because dray genuinely needs it and says so, and this does not."""
    assert dict_store.conn.row_factory is dict_row
    row = dict_store.conn.execute("select 1 as n").fetchone()
    assert row == {"n": 1}


def test_the_default_connection_is_unaffected(postgresql):
    """The other half: a connection with no row factory of its own still reads
    as tuples, which is what every other test in the suite assumes."""
    store = Store(postgresql)
    store.create(Visitor)
    assert store.visitors.count() == 0
    assert isinstance(postgresql.execute("select 1").fetchone(), tuple)
