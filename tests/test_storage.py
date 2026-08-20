"""
Records against a real database: the schema they imply, and the round trip.
"""

import dataclasses
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pytest

from dray import (
    AfterCommitFailed,
    ConcurrencyExhausted,
    ConnectionLost,
    DrayError,
    DuplicateRecord,
    RecordHasChanged,
    RecordNotFound,
    Sql,
    Store,
    ValidationError,
    Write,
    after_commit,
    any_of,
    as_uuid,
    asc,
    before_delete,
    before_save,
    check,
    child,
    clock,
    desc,
    field,
    index,
    key_of,
    names_of,
    none_of,
    record,
    records_change,
)
from dray import schema

STATUSES = ("enquiry", "candidate", "volunteer", "lapsed")


def whoever(write: Write) -> str | None:
    """Ours, not dray's — `whom` is a word this file chose and dray has never
    heard."""
    whom = write.given.get("whom")
    return str(whom) if whom is not None else None


@record(table="walker", collection="walkers")
class Walker:
    family_name: str = field()
    given_names: str = field(default="")
    status: str = field(default="enquiry", choices=STATUSES)
    suburb: str | None = field(default=None, stored_in="blob")
    postcode: str | None = field(default=None, stored_in="blob")

    created_at: datetime | None = field(default=None, on_add=clock)
    updated_at: datetime | None = field(
        default=None, on_add=clock, on_save=clock
    )


@pytest.fixture
def walkers(store):
    store.create(Walker)
    return store.walkers


#
# The table a record implies
#


def test_only_column_fields_become_columns():
    statement = schema.create_table(Walker)
    assert "family_name text" in statement
    assert "suburb" not in statement


def test_the_blob_column_is_always_there():
    # Even a record declaring no blob fields gets one, so adding an attribute
    # later is a write rather than a migration.
    @record(table="plain", collection="plains")
    class Plain:
        name: str = field()

    assert "data jsonb" in schema.create_table(Plain)


def test_a_decimal_column_says_the_size_it_was_going_to_get_anyway(store):
    """A bare `numeric` is unbounded on local PostgreSQL and `numeric(18,6)` on
    a cluster, which fills its own default in and applies it when a row is
    written rather than when the table is made. So a rate carried to eight
    places was stored whole by this suite and rounded in production, with
    nothing raised at either end — the value simply differed by where it ran.
    Written out, both databases round in the same place."""

    @record(table="tariff", collection="tariffs")
    class Tariff:
        rate: Decimal | None = field(default=None)

    assert "rate numeric(18,6)" in schema.create_table(Tariff)

    store.create(Tariff)
    written = store.tariffs.add(Tariff(rate=Decimal("0.00012345")))
    assert store.tariffs.by_id(written.id).rate == Decimal("0.000123")


def test_a_field_can_say_how_precise_its_column_is(store):
    """Which is the answer for the rate above: eighteen digits with six after
    the point is comfortable for money and wrong for an FX conversion, and the
    size is a fact about the field rather than about the database. `drift` is
    still silent, because a sized column is the column the class asked for."""

    @record(table="conversion", collection="conversions")
    class Conversion:
        rate: Decimal | None = field(default=None, precision=12, scale=8)

    assert "rate numeric(12,8)" in schema.create_table(Conversion)

    store.create(Conversion)
    written = store.conversions.add(Conversion(rate=Decimal("0.00012345")))
    assert store.conversions.by_id(written.id).rate == Decimal("0.00012345")
    assert schema.drift(store.conn, Conversion) == []


def test_a_promoted_column_is_built_at_the_size_the_field_declared():
    """Promotion builds the column the class now asks for, and for a `Decimal`
    that includes how big it is. Built at the default instead, a field the class
    says keeps eight places would start rounding at six on the day it moved out
    of the blob — where nothing is watching, because the whole point of the four
    statements is that the values come across unchanged."""

    @record(table="fee", collection="fees")
    class Fee:
        rate: Decimal | None = field(default=None, precision=12, scale=8)

    assert (
        "alter table fee add column if not exists rate numeric(12,8)"
        in schema.promote(Fee, "rate")
    )


def test_create_table_can_be_run_twice(store):
    store.create(Walker)
    store.create(Walker)


def test_drift_reports_a_column_the_table_does_not_have(store):
    store.create(Walker)

    @record(table="walker", collection="walkers_later")
    class WalkerLater:
        family_name: str = field()
        wwcc_number: str | None = field(default=None)

    assert schema.drift(store.conn, WalkerLater) == [
        "walker.wwcc_number is declared but not in the table"
    ]


def test_drift_is_silent_when_they_agree(store):
    store.create(Walker)
    assert schema.drift(store.conn, Walker) == []


def test_drift_answers_for_the_schema_in_use_and_not_the_cluster(store):
    """A cluster can hold two services and a `person` each. Asked by table name
    alone, `information_schema` answers for both — so the one genuinely missing
    a column reads as complete because the other has it, which is this function
    reporting all clear on the exact failure it exists to catch."""
    store.create(Walker)

    with store.conn.cursor() as cur:
        cur.execute("create schema other")
        cur.execute("create table other.walker (id uuid primary key, data jsonb)")
        cur.execute("set search_path to other")

    assert schema.drift(store.conn, Walker) == [
        "walker.created_at is declared but not in the table",
        "walker.etag is declared but not in the table",
        "walker.family_name is declared but not in the table",
        "walker.given_names is declared but not in the table",
        "walker.status is declared but not in the table",
        "walker.updated_at is declared but not in the table",
    ]

    # And back where the table is right, it is still silent.
    with store.conn.cursor() as cur:
        cur.execute("set search_path to public")
    assert schema.drift(store.conn, Walker) == []


def test_promoting_a_field_out_of_the_blob(store):
    """The manual said promotion was deleting `stored_in="blob"` and nothing
    else. Two things broke silently: `find` reads the column and stops seeing
    every row written before the change, and `load` lets the blob override the
    column, so `by_id` on one of those rows still reports the old value. The
    record says 'Leura' and nothing can find 'Leura'."""

    @record(table="dweller", collection="dwellers_before")
    class Before:
        family_name: str = field()
        suburb: str | None = field(default=None, stored_in="blob")

    store.create(Before)
    old = store.dwellers_before.add(Before(family_name="Hemingway", suburb="Leura"))

    # The class changes. Same table, `suburb` now wants a column.
    @record(table="dweller", collection="dwellers_after")
    class After:
        family_name: str = field()
        suburb: str | None = field(default=None)

    # Which on its own is the trap.
    assert schema.drift(store.conn, After) == [
        "dweller.suburb is declared but not in the table"
    ]

    for statement in schema.promote(After, "suburb"):
        with store.conn.cursor() as cur:
            cur.execute(statement)

    assert schema.drift(store.conn, After) == []
    assert [
        d.family_name
        for d in store.dwellers_after.find(equals={"suburb": "Leura"})
    ] == ["Hemingway"]
    assert store.dwellers_after.by_id(old.id).suburb == "Leura"

    # And the key is gone, so nothing shadows the column from now on.
    with store.conn.cursor() as cur:
        cur.execute("select data from dweller")
        assert cur.fetchone()[0] == {}


def test_promotion_statements_survive_being_run_twice(store):
    """Every migration has to, because DSQL cannot commit the statement and the
    row recording that it ran in one transaction."""

    @record(table="lodger", collection="lodgers_before")
    class Before:
        name: str = field()
        town: str | None = field(default=None, stored_in="blob")

    store.create(Before)
    store.lodgers_before.add(Before(name="Shelley", town="Katoomba"))

    @record(table="lodger", collection="lodgers_after")
    class After:
        name: str = field()
        town: str | None = field(default=None)

    for _ in range(2):
        for statement in schema.promote(After, "town"):
            with store.conn.cursor() as cur:
                cur.execute(statement)

    assert store.lodgers_after.by_id(
        store.lodgers_after.find(equals={"town": "Katoomba"})[0].id
    ).town == "Katoomba"


def test_promotion_carries_every_type_the_blob_had_to_write_down(store):
    """jsonb holds a string wherever JSON has no type of its own, so the
    backfill is the mirror of what put the value there — a cast for most of
    them, and something more for the two that are encoded."""
    from uuid import UUID, uuid4

    @record(table="parcel3", collection="parcels3_before")
    class Before:
        weight: int | None = field(default=None, stored_in="blob")
        due_on: date | None = field(default=None, stored_in="blob")
        cost: Decimal | None = field(default=None, stored_in="blob")
        seal: UUID | None = field(default=None, stored_in="blob")
        tags: list | None = field(default=None, stored_in="blob")
        stamp: bytes | None = field(default=None, stored_in="blob")
        took: timedelta | None = field(default=None, stored_in="blob")

    store.create(Before)
    seal = uuid4()
    written = store.parcels3_before.add(
        Before(
            weight=4,
            due_on=date(2026, 3, 14),
            cost=Decimal("4.99"),
            seal=seal,
            tags=["fragile", "heavy"],
            stamp=b"\xde\xad",
            took=timedelta(hours=1, minutes=30),
        )
    )

    @record(table="parcel3", collection="parcels3_after")
    class After:
        weight: int | None = field(default=None)
        due_on: date | None = field(default=None)
        cost: Decimal | None = field(default=None)
        seal: UUID | None = field(default=None)
        tags: list | None = field(default=None)
        stamp: bytes | None = field(default=None)
        took: timedelta | None = field(default=None)

    for statement in schema.promote(
        After, "weight", "due_on", "cost", "seal", "tags", "stamp", "took"
    ):
        with store.conn.cursor() as cur:
            cur.execute(statement)

    back = store.parcels3_after.by_id(written.id)
    assert back.weight == 4
    assert back.due_on == date(2026, 3, 14)
    assert back.cost == Decimal("4.99")
    assert back.seal == seal
    assert back.tags == ["fragile", "heavy"]
    assert back.stamp == b"\xde\xad"
    assert back.took == timedelta(hours=1, minutes=30)

    # Every one of them findable, which is the point of having a column.
    assert store.parcels3_after.count(equals={"weight": 4}) == 1
    assert store.parcels3_after.count(equals={"cost": Decimal("4.99")}) == 1


def test_promoting_a_field_still_declared_in_the_blob_is_refused():
    """The class is the first of the four steps. These statements come after
    it, and running them before leaves a column nothing writes to."""

    @record(table="tenant", collection="tenants")
    class Tenant:
        name: str = field()
        town: str | None = field(default=None, stored_in="blob")

    with pytest.raises(ValueError, match='still declared stored_in="blob"'):
        schema.promote(Tenant, "town")

    with pytest.raises(ValueError, match="has no field 'twon'"):
        schema.promote(Tenant, "twon")


#
# The round trip
#


def test_a_record_comes_back_as_it_went_in(walkers):
    walkers.add(Walker(family_name="Hemingway", given_names="Ernest", suburb="Leura"))
    [found] = walkers.find(equals={"family_name": "Hemingway"})
    assert found.given_names == "Ernest"
    assert found.suburb == "Leura"


def test_a_blob_field_and_a_column_read_the_same(walkers):
    walkers.add(Walker(family_name="Hemingway", status="volunteer", suburb="Leura"))
    assert walkers.find(equals={"status": "volunteer", "suburb": "Leura"})
    assert (
        walkers.find(equals={"status": "volunteer", "suburb": "Katoomba"}) == []
    )


def test_by_id_raises_rather_than_returning_none(walkers):
    from uuid import uuid4

    with pytest.raises(RecordNotFound):
        walkers.by_id(uuid4())


def test_saving_writes_both_kinds_of_field(walkers):
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    walker.status = "volunteer"
    walker.suburb = "Katoomba"
    walkers.save(walker)

    again = walkers.by_id(walker.id)
    assert (again.status, again.suburb) == ("volunteer", "Katoomba")


def test_a_record_saves_itself(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    walker.status = "volunteer"
    walker.save()
    assert walkers.by_id(walker.id).status == "volunteer"


def test_a_declared_timestamp_is_filled_by_the_write(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    stored = walkers.by_id(walker.id)
    assert stored.created_at is not None
    # The same moment, not the same value — `clock_timestamp()` advances inside
    # a transaction, which is the whole reason `clock` returns it.
    assert stored.updated_at - stored.created_at < timedelta(milliseconds=1)

    walker.status = "volunteer"
    walker.save()

    again = walkers.by_id(walker.id)
    assert again.updated_at > again.created_at
    assert again.created_at == stored.created_at


def test_a_save_brings_back_the_timestamp_it_wrote(walkers):
    """An add asked the database for what it had worked out and a save did not,
    so the record went on reading the moment it was created while its row said
    the moment it was last written. Silently, and about the one field whose job
    is to say when that was — anything reading it after the save, an
    `@after_commit` handler or a response body, reported the previous write."""
    walker = walkers.add(Walker(family_name="Hemingway"))
    added_at = walker.updated_at

    walker.status = "volunteer"
    walker.save()

    assert walker.updated_at > added_at
    assert walker.updated_at == walkers.by_id(walker.id).updated_at


def test_a_save_asks_for_nothing_back_that_python_already_knows(
    store, monkeypatch
):
    """`returning` is for values only the database has. A handler returning an
    ordinary value has already put it on the record, and naming it anyway would
    be a wider statement for an answer nobody reads — while an empty clause is
    a syntax error. So the columns come from what was filled with `Sql`, which
    on this record is none of them."""

    @record(table="tally", collection="tallies")
    class Tally:
        name: str = field(default="")
        touched: int = field(default=0, on_save=lambda w: w.record.touched + 1)

    store.create(Tally)
    tally = store.tallies.add(Tally(name="one"))

    # The statement is the only observable. What the record holds afterwards is
    # the same either way, because the handler set it before the write went out.
    ran = []
    real = psycopg.Cursor.execute

    def watching(self, statement, params=None, **rest):
        ran.append(statement)
        return real(self, statement, params, **rest)

    monkeypatch.setattr(psycopg.Cursor, "execute", watching)
    tally.save()

    [update] = [statement for statement in ran if statement.startswith("update")]
    assert "returning" not in update
    assert tally.touched == 1


def test_a_refused_value_never_reaches_the_database(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    with pytest.raises(ValidationError):
        walker.status = "voluntear"
    assert walkers.by_id(walker.id).status == "enquiry"


def test_a_record_its_own_rule_refuses_never_reaches_the_database(store):
    """A rule spanning two fields runs before the write for the same reason a
    validator does: the alternative is a service function remembering to call
    it, and the day it forgets the bad row is durable."""

    @record(table="stay", collection="stays")
    class Stay:
        arrives_on: date | None = field(default=None)
        leaves_on: date | None = field(default=None)

        @check
        def leaves_after_it_arrives(self):
            if self.leaves_on < self.arrives_on:
                raise ValueError("a stay cannot end before it starts")

    store.create(Stay)
    stay = Stay(arrives_on=date(2026, 3, 14), leaves_on=date(2026, 3, 1))

    with pytest.raises(ValueError, match="cannot end before it starts"):
        store.stays.add(stay)
    assert store.conn.execute("select count(*) from stay").fetchone()[0] == 0

    stay.leaves_on = date(2026, 3, 21)
    store.stays.add(stay)
    assert store.stays.by_id(stay.id).leaves_on == date(2026, 3, 21)

    # And again on the way to an update, where the record already exists and the
    # write is the only door left between an edit and the row.
    stay.arrives_on = date(2026, 4, 1)
    with pytest.raises(ValueError, match="cannot end before it starts"):
        stay.save()
    assert store.stays.by_id(stay.id).arrives_on == date(2026, 3, 14)


def test_deleting_removes_the_row(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    walker.delete()
    with pytest.raises(RecordNotFound):
        walkers.by_id(walker.id)


def test_counting_is_asked_of_the_database(walkers):
    walkers.add_all(
        [
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="volunteer"),
            Walker(family_name="Frankenstein", status="enquiry"),
        ]
    )
    assert walkers.count() == 3
    assert walkers.count(equals={"status": "volunteer"}) == 2


def test_find_refuses_a_field_that_does_not_exist(walkers):
    with pytest.raises(ValidationError):
        walkers.find(equals={"surbub": "Leura"})


def test_a_filter_describing_nothing_matches_every_row(walkers):
    """A search form where nobody typed anything hands over an empty dict, and
    the page now says what that asks for: everybody. Worth pinning because the
    two spellings have to agree — a caller building the filter up entry by
    entry reaches `{}` by a different road from the one that names no filter at
    all, and a difference between them would show up only on the day the form
    came back blank."""
    walkers.add_all(
        [
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="enquiry"),
        ]
    )

    assert len(walkers.find()) == 2
    assert len(walkers.find(equals={})) == 2
    assert walkers.count(equals={}) == 2


#
# A rule about the whole record
#
# `@check` is handed the record rather than one value, and runs at both doors a
# record comes in by. This is the write's: after it has filled in what it fills,
# and before the first transaction opens. `parse` needs no database and is in
# `test_model.py`; what is here is the pair of them holding together.
#


def test_a_rule_spanning_two_fields_runs_on_the_way_to_storage(store):
    """A validator is handed one value and nothing else, so ends-after-starts
    has nowhere on the class to live unless a rule can see the whole record.
    Written in service code and called by hand instead, it is a rule somebody
    can forget rather than one the record carries."""

    @record(table="booking", collection="bookings")
    class Booking:
        starts_on: date | None = field(default=None)
        ends_on: date | None = field(default=None)
        seats: int = field(default=1)

        @check
        def ends_after_it_starts(self):
            if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
                raise ValueError("a booking cannot end before it starts")

    store.create(Booking)

    with pytest.raises(ValueError, match="cannot end before it starts"):
        store.bookings.add(
            Booking(starts_on=date(2026, 3, 14), ends_on=date(2026, 3, 1))
        )
    assert store.bookings.count() == 0

    store.bookings.add(Booking(starts_on=date(2026, 3, 14), ends_on=date(2026, 3, 15)))
    assert store.bookings.count() == 1


def test_a_rule_reads_a_field_the_store_defaults_fill_in(postgresql):
    """A rule about a field the write supplies used to be a record that could
    never be written. `add` validated the caller's values, the write applied the
    store's defaults afterwards, and a rule wanting an author was refused on
    every pass — including the one that would have handed it one."""
    from dray import Store

    store = Store(postgresql, defaults={"author": "rod"})

    @record(table="note", collection="notes")
    class Note:
        body: str = field()
        author: str | None = field(default=None)

        @check
        def says_who_wrote_it(self):
            if not self.author:
                raise ValueError("a note says who wrote it")

    store.create(Note)
    note = store.notes.add(Note(body="Cleared to start."))

    assert store.notes.by_id(note.id).author == "rod"


def test_a_rule_reads_a_field_the_write_fills_in(store):
    """The worse half of the same defect, and the one no reordering could have
    fixed: `on_add` fires inside the chunk being written, which was after both
    validating passes, so a rule reading what it filled saw nothing on either of
    them and the record could not be written at all."""

    @record(table="memo", collection="memos")
    class Memo:
        body: str = field()
        filed_by: str | None = field(
            default=None, on_add=lambda write: "the clerk"
        )

        @check
        def says_who_filed_it(self):
            if not self.filed_by:
                raise ValueError("a memo says who filed it")

    store.create(Memo)
    memo = store.memos.add(Memo(body="Cleared to start."))

    assert store.memos.by_id(memo.id).filed_by == "the clerk"


def test_a_rule_reads_the_children_queued_against_the_record(store):
    """A rule over a record and the rows written with it belongs on the class
    rather than in whatever service function remembers to call it, and the page
    has to say so plainly enough that nobody writes the other one: children
    queued with `add` are there to be read at the write. They are not there at
    `parse`, for the same reason a field the write fills in is not, so the rule
    wants the same guard — and this
    is the one shape where forgetting it fails every `parse` rather than one
    write."""
    seen = []

    @record(table="parcel", collection="parcels")
    class Parcel:
        grams: int = field(default=0)

        @check
        def the_items_account_for_it(self):
            all_items = list(self.items)
            seen.append(len(all_items))
            if not all_items:
                return
            short = self.grams - sum(item.grams for item in all_items)
            if short:
                raise ValueError(f"the contents are out by {short}g")

    @child(of=Parcel, name="items", table="item", collection="items")
    class Item:
        grams: int = field(default=0)

    store.create(Parcel, Item)

    Parcel.parse({"grams": 1000})
    assert seen == [0]

    parcel = Parcel(grams=1000)
    parcel.items.add(grams=334)
    parcel.items.add(grams=666)
    store.parcels.add(parcel)
    assert seen == [0, 2]

    out = Parcel(grams=1000)
    out.items.add(grams=334)
    out.items.add(grams=600)
    with pytest.raises(ValueError, match="out by 66g"):
        store.parcels.add(out)


def test_a_rule_runs_once_for_a_write(store):
    """It ran twice, because `add` validated the record and then the write
    validated it again. A `check` is handed `self` and `self` reaches its own
    children and its collection, so twice is a thing its author can notice —
    where a validator handed one value mostly cannot."""
    ran = []

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def the_kitchen_is_open(self):
            ran.append(self.at)

    store.create(Sitting)
    sitting = store.sittings.add(Sitting())
    assert ran == ["19:00"]

    sitting.at = "20:00"
    sitting.save()
    assert ran == ["19:00", "20:00"]


def test_a_rule_reaches_a_store_only_once_the_record_has_been_in_one(store):
    """The one hook where the store on a record is not simply there, and worth
    writing down rather than leaving to be found at somebody's first `add`: a
    rule runs before the write attaches anything, so a record built in memory
    has no store to read the rest of the database through, and the same rule on
    the save of a record that came out of the store has one."""
    seen = []

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def the_kitchen_is_open(self):
            try:
                seen.append(self.store.sittings.count())
            except RuntimeError as unstored:
                seen.append(str(unstored))

    store.create(Sitting)
    sitting = store.sittings.add(Sitting())
    assert "did not come from a store" in seen[0]

    sitting.at = "20:00"
    sitting.save()
    assert seen[1] == 1


def test_a_write_refused_and_replayed_runs_a_rule_once(store, monkeypatch):
    """A rule sits outside the replayed part of a write, the same side as the
    handlers that fill a field. DSQL refuses a commit that raced another writer
    and dray replays the transaction; a rule that ran with it would run twice
    for one save, and a rule counting anything would count the attempts."""
    from dray import Collection

    ran = []

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def the_kitchen_is_open(self):
            ran.append(self.at)

    store.create(Sitting)

    # Refused once, at the last statement in the transaction, so the whole of it
    # rolls back and is replayed.
    refused = iter([True])
    real = Collection._insert_children

    def refusing_once(self, batch, prepared):
        sent = real(self, batch, prepared)
        if next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")
        return sent

    monkeypatch.setattr(Collection, "_insert_children", refusing_once)
    store.sittings.add(Sitting())

    assert ran == ["19:00"]
    assert store.sittings.count() == 1


def test_a_rule_broken_in_a_later_chunk_leaves_the_earlier_ones_unwritten(
    store, monkeypatch
):
    """The promise the up-front pass exists for, and the one the fix could most
    easily have lost. A set above the row ceiling is several transactions, so a
    rule run inside the chunk carrying the record would refuse the third one
    with the first two already durable and nothing to put them back."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def the_kitchen_is_open(self):
            if self.at > "21:00":
                raise ValueError("the kitchen has closed")

    store.create(Sitting)
    wanted = [Sitting(at=at) for at in ("18:00", "19:00", "20:00", "22:00")]

    with pytest.raises(ValueError, match="the kitchen has closed"):
        store.sittings.add_all(wanted)
    assert store.sittings.count() == 0


def test_the_fields_are_checked_before_the_record_is(store):
    """A rule spanning two fields is written for values that are the right type
    and each acceptable on its own, so it must not be what reports a string
    where a date belongs — it would do it from inside somebody's comparison, and
    a caller would hear about the wrong thing."""

    @record(table="booking", collection="bookings")
    class Booking:
        starts_on: date | None = field(default=None)
        seats: int = field(default=1)

        @check
        def the_room_is_big_enough(self):
            raise AssertionError("the fields, and not this")

    store.create(Booking)
    # Construction converts and does not validate, which is how a bad value gets
    # this far — the write is where every field is judged at once.
    booking = Booking(starts_on=date(2026, 3, 14), seats="four")

    with pytest.raises(ValidationError) as raised:
        store.bookings.add(booking)
    assert "seats" in str(raised.value)


def test_what_a_rule_raises_is_what_the_caller_catches(store):
    """dray wraps what a validator raises so the message can name the field. A
    rule about the record has already said what it is about, and wrapping it
    would put dray's name on an exception the domain chose."""

    class TooLate(Exception):
        pass

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def before_the_kitchen_closes(self):
            if self.at > "21:00":
                raise TooLate(self.at)

    store.create(Sitting)
    with pytest.raises(TooLate):
        store.sittings.add(Sitting(at="22:30"))


def test_a_rule_about_the_record_does_not_run_on_assignment(store):
    """Half a record is not something to judge. Moving a booking a week on means
    writing two dates, and a rule that fired on each of them would pass or fail
    on which one the caller happened to write first — here, on a booking that is
    perfectly good before the pair of assignments and after them."""

    @record(table="booking", collection="bookings")
    class Booking:
        starts_on: date | None = field(default=None)
        ends_on: date | None = field(default=None)

        @check
        def ends_after_it_starts(self):
            if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
                raise ValueError("a booking cannot end before it starts")

    store.create(Booking)
    booking = Booking.parse(
        {"starts_on": date(2026, 3, 14), "ends_on": date(2026, 3, 15)}
    )
    # The first of these leaves the booking ending a week before it starts, and
    # the second puts it right. Neither is where a rule about the pair belongs.
    booking.starts_on = date(2026, 3, 21)
    booking.ends_on = date(2026, 3, 22)

    store.bookings.add(booking)
    assert store.bookings.count() == 1


def test_a_record_parse_accepted_is_not_then_refused_by_the_write(store):
    """The contract the two doors keep between them: what `parse` hands back
    goes straight into `add`. Judged only at the write, a booking the form could
    have been told about was accepted by the handler and refused a call later,
    with whatever the handler had done in between already done."""
    ran = []

    @record(table="booking", collection="bookings")
    class Booking:
        starts_on: date | None = field(default=None)
        ends_on: date | None = field(default=None)

        @check
        def ends_after_it_starts(self):
            ran.append(self.ends_on)
            if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
                raise ValueError("a booking cannot end before it starts")

    store.create(Booking)
    booking = Booking.parse(
        {"starts_on": date(2026, 3, 14), "ends_on": date(2026, 3, 15)}
    )
    store.bookings.add(booking)

    assert store.bookings.count() == 1
    # Once at each door and not twice at either. `parse` judged what the form
    # supplied; the write judged the record it had finished filling in.
    assert ran == [date(2026, 3, 15), date(2026, 3, 15)]


def test_a_guarded_rule_reading_a_filled_field_is_judged_by_the_write(store):
    """What a rule can be told differs by door, so one about a field the write
    supplies has to say nothing while it is absent — and then be judged where
    the value is. Refusing at `parse` would refuse every form post, which is the
    defect the write-side pass was moved to fix, arrived at from the other
    end."""
    ran = []

    @record(table="memo", collection="memos")
    class Memo:
        body: str = field()
        filed_by: str | None = field(
            default=None, on_add=lambda write: write.given["clerk"]
        )

        @check
        def says_who_filed_it(self):
            ran.append(self.filed_by)
            if self.filed_by is not None and not self.filed_by.strip():
                raise ValueError("a memo says who filed it")

    store.create(Memo)
    unsigned = Memo.parse({"body": "Cleared to start."})
    signed = Memo.parse({"body": "Cleared to start."})
    assert ran == [None, None]

    with pytest.raises(ValueError, match="says who filed it"):
        store.memos.add(unsigned, given={"clerk": "  "})
    assert store.memos.count() == 0

    store.memos.add(signed, given={"clerk": "rod"})
    assert store.memos.by_id(signed.id).filed_by == "rod"


def test_a_method_called_check_without_the_marker_is_never_called(store):
    """The reason a hook is found by a decorator at all. `check` is an ordinary
    domain word — a booking's is checking a party in — and dray reaching for
    that spelling would start calling it before every write, on a class written
    before dray had ever heard of the idea."""
    called = []

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        def check(self):
            called.append("the party")

    store.create(Sitting)
    store.sittings.add(Sitting())

    assert called == []
    assert Sitting.__dray_hooks__ == {}


def test_a_subclass_overriding_a_rule_runs_its_own(store):
    """A rule is found by name at the moment it runs, so it is overridden the way
    any other method is — and an override does not have to remember the decorator
    to stay a rule."""
    ran = []

    class Sittable:
        @check
        def the_kitchen_is_open(self):
            ran.append("the kitchen")

    @record(table="sitting", collection="sittings")
    class LateSitting(Sittable):
        at: str = field(default="22:00")

        def the_kitchen_is_open(self):
            ran.append("the late kitchen")

    store.create(LateSitting)
    store.sittings.add(LateSitting())

    assert ran == ["the late kitchen"]


def test_every_rule_runs_in_the_order_it_was_written(store):
    """The bases first and then the class, which is the order they were
    collected in — and the only order somebody reading the class could predict.
    Declaration order is a promise as soon as one rule can leave the record in a
    state the next one reads."""
    ran = []

    class Sittable:
        @check
        def the_kitchen_is_open(self):
            ran.append("the kitchen")

    @record(table="sitting", collection="sittings")
    class Sitting(Sittable):
        at: str = field(default="19:00")

        @check
        def a_table_is_free(self):
            ran.append("a table")

    store.create(Sitting)
    store.sittings.add(Sitting())

    assert ran == ["the kitchen", "a table"]


#
# Before a record is written
#
# `@check` reaches every write door already. What it cannot do is run where the
# write's transaction is open, so a rule that has to write, or to read and then
# refuse on what it read, had nowhere to sit on this side.
#


def test_a_rule_before_a_write_runs_at_every_door(store):
    """A rule kept in an overridden `save` covers `booking.save()` and is walked
    past by `save_all`, `add_all` and anything going through the collection —
    which is how a domain rule holds for eleven call sites and not the twelfth.
    Marked, it is the write that reaches it rather than the call."""
    ran = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def say_so(self, write):
            ran.append(self.family_name)

    store.create(Member)
    one = store.members.add(Member(family_name="Hemingway"))
    store.members.add_all([Member(family_name="Shelley")])
    one.family_name = "Stein"
    one.save()
    store.members.save(one)
    store.members.save_all([one])

    assert ran == ["Hemingway", "Shelley", "Stein", "Stein", "Stein"]


def test_a_rule_before_a_write_runs_on_the_write_that_creates_the_record(store):
    """The question the delete side never had to answer, because a record is
    deleted once and written many times. *Write a line whenever this record is
    written* wants the first one, and an insert is a write."""
    ran = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def say_so(self, write):
            ran.append(self.family_name)

    store.create(Member)
    store.members.add(Member(family_name="Hemingway"))

    assert ran == ["Hemingway"]


def test_a_rule_before_a_write_reaches_the_store_on_the_record_it_creates(store):
    """A handler is handed nothing but `self`, so `self.store` is the whole of
    how it reaches another record — and the write used to attach a record only
    once its row had committed, which would have left the insert side a hook
    that can refuse and cannot write. The record is attached before the rule
    runs instead, which is what makes this the same hook at both doors."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_says(self, write):
            self.store.traces.add(Trace(message=f"wrote {self.family_name}"))

    store.create(Trace, Member)
    store.members.add(Member(family_name="Hemingway"))

    assert [trace.message for trace in store.traces.find()] == ["wrote Hemingway"]


def test_what_a_rule_before_a_write_wrote_lands_with_the_row(store):
    """The whole of what separates this from a `@check`, which runs before the
    write's transaction is open and so writes into one of its own. Here the line
    and the row are one transaction, and a reader never sees one without the
    other."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        status: str = field(default="enquiry")

        @before_save
        def keep_what_it_said(self, write):
            self.store.traces.add(
                Trace(message=f"{self.family_name} is {self.status}")
            )

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))
    member.status = "volunteer"
    member.save()

    # Sorted, because `Trace` declares no order and a bare `find` falls back
    # to the key, which is random — what this is about is that both lines are
    # there, not which came back first.
    assert sorted(trace.message for trace in store.traces.find()) == [
        "Hemingway is enquiry",
        "Hemingway is volunteer",
    ]


def test_what_a_rule_before_a_write_wrote_goes_back_when_the_next_refuses(store):
    """The same shape under a `@check` leaves the line standing, because a check
    has already committed its own transaction by the time it raises — a history
    saying the record was written and no write. Inside the write's transaction
    the refusal takes the line with it."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.store.traces.add(Trace(message=f"wrote {self.family_name}"))

        @before_save
        def but_not_this_one(self, write):
            raise ValueError("this one is not written")

    store.create(Trace, Member)

    with pytest.raises(ValueError, match="not written"):
        store.members.add(Member(family_name="Hemingway"))

    assert store.traces.count() == 0
    assert store.members.count() == 0


def test_a_rule_refusing_one_record_stops_the_set_it_was_written_with(store):
    """Every rule in a chunk runs before any of its statements, so a refusal at
    position four hundred leaves the first three hundred and ninety-nine
    unwritten rather than durable — the promise `add_all` already makes about a
    bad value, kept by a rule about the write as well."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def not_the_last_one(self, write):
            if self.family_name == "Stein":
                raise ValueError("not this one")

    store.create(Member)

    with pytest.raises(ValueError, match="not this one"):
        store.members.add_all(
            [Member(family_name=name) for name in ("Hemingway", "Shelley", "Stein")]
        )

    assert store.members.count() == 0


def test_what_a_check_wrote_goes_back_only_inside_a_block(store):
    """The page says a `@check` that writes leaves its writes behind, and that
    is true of the transaction dray opens for a write and false of one the
    caller opened — there the check runs inside the block like everything else,
    so the rollback takes what it wrote. The same handler litters or does not
    depending on where it was called from, which is worth pinning because
    nothing about the handler says so."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field(default="")

    @record(table="member", collection="members")
    class Member:
        family_name: str = field(default="")

        @check
        def writes_then_refuses(self):
            if self.family_name == "refused" and self.store:
                self.store.traces.add(Trace(message=f"saw {self.family_name}"))
                raise ValueError("the rule says no")

    store.create(Trace, Member)

    one = store.members.add(Member(family_name="fine"))
    one.family_name = "refused"
    with pytest.raises(ValueError, match="the rule says no"):
        one.save()
    # dray's own transaction had not opened yet, so the trace is its own and
    # nothing takes it back.
    assert [each.message for each in store.traces.find()] == ["saw refused"]

    two = store.members.add(Member(family_name="fine too"))
    two.family_name = "refused"
    with pytest.raises(ValueError, match="the rule says no"):
        with store.transaction():
            two.save()
    # The block was already open, so this one went back with it.
    assert [each.message for each in store.traces.find()] == ["saw refused"]


def test_a_rule_before_a_write_does_not_see_the_records_beside_it(store):
    """The limit worth knowing before a cap is written against it. The rules for
    a chunk all run before a single row of it is sent — which is what makes a
    refusal leave nothing written — so a rule that counts rows counts what was
    already committed and not the set it is riding in. Four rows on a
    three-place event, and neither hook was wrong about what it could see."""
    from uuid import UUID

    @record(table="event", collection="events")
    class Event:
        places: int = field(default=3)

    @record(table="signup", collection="signups")
    class Signup:
        event_id: UUID | None = field(default=None)

        @before_save
        def there_is_a_place(self, write):
            event = self.store.events.by_id(self.event_id)
            taken = self.store.signups.count(equals={"event_id": self.event_id})
            if taken >= event.places:
                raise ValueError("that event is full")

    store.create(Event, Signup)
    event = store.events.add(Event(places=3))
    store.signups.add_all([Signup(event_id=event.id) for _ in range(2)])

    store.signups.add_all([Signup(event_id=event.id) for _ in range(2)])
    assert store.signups.count() == 4

    # One at a time it holds, because each write is a transaction of its own and
    # the row before it is committed by the time the next rule reads.
    with pytest.raises(ValueError, match="is full"):
        store.signups.add(Signup(event_id=event.id))


def test_a_rule_before_a_write_runs_once_per_attempt(store, monkeypatch):
    """Inside the transaction means inside the replay, which is the thing about
    it a caller cannot see coming. A refused commit takes the handler's rows with
    it, so the handler runs again to put them back — right for what it writes and
    wrong for anything a rollback cannot reach, which is why a side effect
    belongs in `@after_commit` instead."""
    from dray.collection import Collection
    from dray.store import retrying

    ran = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            ran.append(self.family_name)
            self.store.traces.add(Trace(message=f"wrote {self.family_name}"))

    store.create(Trace, Member)

    # Refused once, on the write the handler itself makes — which is inside the
    # member's transaction and enlisted in it, so the refusal travels up to the
    # write that owns the commit and the whole of it is replayed.
    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    store.members.add(Member(family_name="Hemingway"))

    assert ran == ["Hemingway", "Hemingway"]
    # Twice run, once landed: the first attempt's row went with the rollback.
    assert store.traces.count() == 1
    assert store.members.count() == 1


def test_a_queued_child_runs_its_own_rule_when_its_parent_carries_it(store):
    """The opposite of the delete side's answer about a cascade, and for a reason
    that does not carry over: a cascade loads no rows and has nothing to run a
    hook on, where a queued child is in memory and whole. A note written by its
    parent's save is a note that was written."""
    ran = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

    @child(of=Member, name="notes", table="member_note")
    class MemberNote:
        body: str = field()

        @before_save
        def say_so(self, write):
            ran.append(self.body)

    store.create(Member, MemberNote)
    member = store.members.add(Member(family_name="Hemingway"))
    member.notes.add(MemberNote(body="Called back."))
    member.save()

    assert ran == ["Called back."]


def test_a_child_a_rule_queued_is_written_by_the_write_that_ran_it(store):
    """`self.notes.add(...)` inside a `@before_save` wrote nothing at all. The
    tree of queued children was walked before the transaction opened, so a child
    queued while the rule ran was not in what the write was about to send, and
    the settling afterwards emptied the queue regardless — the record's own
    change landed, the note did not, and nothing was raised. It is the spelling
    anybody who has used a child anywhere else in dray reaches for, and it reads
    exactly like `self.store.notes.add(...)`, which always worked."""

    @record(table="member", collection="members")
    class Member:
        status: str = field(default="enquiry")

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=self.status))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field(default="")

    store.create(Member, MemberNote)
    member = Member(status="enquiry")
    store.members.add(member)
    member.status = "volunteer"
    member.save()

    again = store.members.by_id(member.id)
    assert again.status == "volunteer"
    assert sorted(note.body for note in again.notes.find()) == [
        "enquiry",
        "volunteer",
    ]
    # And on the object in hand, which was the other half of the surprise: the
    # record the caller is holding had not got them either.
    assert sorted(note.body for note in member.notes.find()) == [
        "enquiry",
        "volunteer",
    ]


def test_a_rule_that_moves_a_field_keeps_the_line_its_handler_queues(store):
    """The route that bites without anybody writing `add`, and dray supplies
    both halves of it. `records_change` is an `on_change` handler whose whole
    job is to queue a line, so a rule that moved a field carrying one wrote the
    row and lost the line — where the same field moved from outside a rule
    wrote both."""

    @record(table="member", collection="members")
    class Member:
        status: str = field(
            default="enquiry", on_change=records_change(into="notes")
        )

        @before_save
        def they_are_a_volunteer_now(self, write):
            self.status = "volunteer"

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field(default="")

    store.create(Member, MemberNote)
    member = store.members.add(Member())

    assert [note.body for note in member.notes.find()] == [
        "status changed from 'enquiry' to 'volunteer'."
    ]


def test_a_grandchild_a_queued_childs_rule_queued_is_written_too(store):
    """The children pass has the same hole as the records pass. A note queued
    before the save runs its own `@before_save`, and an attachment queued inside
    *that* was dropped identically — one note written and nothing hanging off
    it. Both loops, since a caller cannot see which one their rule is in."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.files.add(NoteFile(name=f"{self.body}.txt"))

    @child(of=MemberNote, name="files", table="note_file", collection="files")
    class NoteFile:
        name: str = field()

    store.create(Member, MemberNote, NoteFile)
    member = Member(family_name="Hemingway")
    member.notes.add(MemberNote(body="Called back"))
    store.members.add(member)

    assert store.notes.count() == 1
    assert [each.name for each in store.files.find()] == ["Called back.txt"]


def test_a_child_a_rule_queued_runs_its_own_rule_before_it_is_written(store):
    """A queued child gets a `@before_save` of its own, and a child that arrived
    late is a queued child. Skipping it would break that promise for the one
    child that could not be seen coming — and the rule it runs may queue again,
    which is why this is worked round by round rather than in one pass."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=f"wrote {self.family_name}"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.files.add(NoteFile(name=f"{self.body}.txt"))

    @child(of=MemberNote, name="files", table="note_file", collection="files")
    class NoteFile:
        name: str = field()

    store.create(Member, MemberNote, NoteFile)
    store.members.add(Member(family_name="Hemingway"))

    assert [each.name for each in store.files.find()] == [
        "wrote Hemingway.txt"
    ]


def test_a_child_a_rule_queued_is_judged_by_the_rules_it_declared(store):
    """A child arriving after the up-front pass must not be the one door into
    the table that checks nothing. Its field rules and its own `@check` run,
    late and inside the transaction because there is no earlier moment left —
    so what they refuse takes the chunk carrying it rather than leaving the set
    unwritten, which is the one thing this narrows."""

    def not_blank(value: str) -> None:
        if not value.strip():
            raise ValueError("a note cannot be blank")

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=""))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field(validator=not_blank)
        kind: str = field(default="call")

        @check
        def a_call_says_who(self):
            if self.kind == "call" and not self.body:
                raise ValueError("say who called")

    store.create(Member, MemberNote)

    with pytest.raises(ValidationError, match="cannot be blank"):
        store.members.add(Member(family_name="Hemingway"))
    assert store.members.count() == 0
    assert store.notes.count() == 0


def test_a_late_childs_own_check_is_run_as_well_as_its_field_rules(store):
    """The rule a child wrote about *itself*, which is the half a field
    validator cannot see: `kind` and `body` are each fine and the pair is not.
    It runs where `_check_all` would have run it had the child existed then."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body="Called back", kind="visit"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()
        kind: str = field(default="call")

        @check
        def a_visit_says_where(self):
            if self.kind == "visit" and "at" not in self.body:
                raise ValueError("say where the visit was")

    store.create(Member, MemberNote)

    with pytest.raises(ValueError, match="say where the visit was"):
        store.members.add(Member(family_name="Hemingway"))
    assert store.members.count() == 0
    assert store.notes.count() == 0


def test_a_child_a_rule_queued_is_written_once_when_the_commit_is_replayed(
    store, monkeypatch
):
    """The failure this was most likely to ship with. A `@before_save` runs once
    per attempt deliberately, the settling that empties the queue is outside the
    replay, and the list of children the write is sending was built once and
    handed to every attempt — so a rule that queues grows all three and the
    second attempt writes the first attempt's note as well as its own. Rewound
    per attempt instead, the record's queue and that list both."""
    from dray.collection import Collection
    from dray.store import retrying

    ran = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            ran.append(self.family_name)
            self.notes.add(MemberNote(body=f"wrote {self.family_name}"))
            self.store.traces.add(Trace(message="attempted"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

    store.create(Trace, Member, MemberNote)

    # Refused on the write the rule itself makes, which is enlisted in the
    # member's transaction — so the refusal travels up to the write that owns
    # the commit and the whole of it is replayed, rather than being raised
    # after a commit local PostgreSQL has already made durable.
    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    member = store.members.add(Member(family_name="Hemingway"))

    assert ran == ["Hemingway", "Hemingway"]
    assert [note.body for note in member.notes.find()] == ["wrote Hemingway"]


def test_an_on_add_on_a_child_queued_before_a_rule_ran_fires_once(store):
    """What a caller's children are filled with is worked out outside the
    replay, so a field naming an `on_add` is filled once per save rather than
    once per attempt. Picking up what a rule queued means walking the tree a
    second time, and a second walk that did not know which children it had
    already filled would run those handlers again — a save that counts itself
    twice for one row. Which is a promise about the caller's children only, and
    the test below is the other half of it."""
    counted = []

    def counting(write: Write) -> int:
        counted.append(write.record.body)
        return len(counted)

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body="and a rule wrote this"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()
        ordinal: int = field(default=0, on_add=counting)

    store.create(Member, MemberNote)
    member = Member(family_name="Hemingway")
    member.notes.add(MemberNote(body="a caller wrote this"))
    store.members.add(member)

    assert counted == ["a caller wrote this", "and a rule wrote this"]
    assert sorted(note.ordinal for note in store.notes.find()) == [1, 2]


def test_an_on_add_on_a_child_a_rule_queued_fires_once_per_attempt(
    store, monkeypatch
):
    """The narrowing, pinned so it is not discovered. A rule runs once per
    attempt by design and builds a new child on each one, so nothing of a
    refused attempt survives for a handler to have been run once against — the
    child that lands carries the attempt count. A handler deriving its value
    from what the child holds is unharmed; one counting its own calls is
    counting attempts, and belongs in an `@after_commit`."""
    from dray.collection import Collection
    from dray.store import retrying

    counted = []

    def counting(write: Write) -> int:
        counted.append(write.record.body)
        return len(counted)

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body="a rule wrote this"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()
        ordinal: int = field(default=0, on_add=counting)

        # Refused from the late child's own rule, which runs after it has been
        # filled in — so the attempt that is thrown away is one where the
        # handler had already answered.
        @before_save
        def keep_what_it_said(self, write):
            self.store.traces.add(Trace(message="attempted"))

    store.create(Trace, Member, MemberNote)

    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    store.members.add(Member(family_name="Hemingway"))

    # One row, and it says two, because the handler ran on the child of each
    # attempt and only the second attempt's child was written.
    assert counted == ["a rule wrote this", "a rule wrote this"]
    assert [note.ordinal for note in store.notes.find()] == [2]


def test_a_rule_reading_write_adding_writes_an_opening_entry(store):
    """The shape a record with an opening entry wants and had no door for.
    `on_add` runs once at creation and cannot queue a child; `on_change` can
    queue and is correctly skipped at `add`, so `records_change` writes nothing
    about the value a record started at. A rule reading `write.adding` covers
    that one door, and reaches every way a record can be created rather than
    sitting beside one constructor call."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        status: str = field(
            default="enquiry", on_change=records_change(into="notes")
        )

        @before_save
        def opening_entry(self, write):
            if write.adding:
                self.notes.add(MemberNote(body=f"opened as {self.status}"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

    store.create(Member, MemberNote)
    member = store.members.add(Member(family_name="Hemingway"))
    member.status = "volunteer"
    member.save()
    member.family_name = "Stein"
    member.save()

    # One opening line however many saves follow, and the handler's line for the
    # move — with nothing added by the save that touched neither. Sorted,
    # because `MemberNote` declares no order and a bare `find` falls back to the
    # key, which is random.
    assert sorted(note.body for note in member.notes.find()) == [
        "opened as enquiry",
        "status changed from 'enquiry' to 'volunteer'.",
    ]


def test_a_rule_queuing_past_the_row_ceiling_is_refused_by_dray(
    store, monkeypatch
):
    """How many rows a transaction is was worked out before any rule ran, so a
    rule that queues is adding to arithmetic that is already finished. Refused
    here rather than sent — local PostgreSQL does not care how many rows are in
    a transaction and the cluster does, so the write that would have failed is
    the one nobody could reproduce in development."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 4)

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=f"wrote {self.family_name}"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

    store.create(Member, MemberNote)

    # Four records with nothing queued are sized as one chunk of four rows,
    # and the rules make it eight.
    with pytest.raises(ValueError, match="take this transaction to 8 rows"):
        store.members.add_all(
            [
                Member(family_name=name)
                for name in ("Hemingway", "Shelley", "Stein", "Woolf")
            ]
        )
    assert store.members.count() == 0


def test_a_rule_queuing_is_counted_across_the_chunks_of_one_block(
    store, monkeypatch
):
    """Outside a block a chunk is a transaction and the ceiling is a question
    about one chunk. Inside a block every chunk joins the transaction the caller
    opened, so a rule adding a row per record overshoots by a chunk's worth at a
    time and no single chunk ever looks close — the cluster kills a transaction
    whose every chunk was comfortably within the limit. Counted across them
    instead, which is the only place the count is true."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 8)

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=f"wrote {self.family_name}"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

    store.create(Member, MemberNote)

    def a_set():
        # Five records and three children queued on the first is eight rows,
        # exactly what the refusal before the write allows. The widest record
        # makes the chunks two records each, so no chunk is above five rows and
        # the rules take the transaction to thirteen.
        members = [Member(family_name=str(n)) for n in range(5)]
        for n in range(3):
            members[0].notes.add(MemberNote(body=f"a caller wrote {n}"))
        return members

    with pytest.raises(ValueError, match="take this transaction to 11 rows"):
        with store.transaction():
            store.members.add_all(a_set())
    assert store.members.count() == 0

    # And the same set outside a block writes, because there each chunk is a
    # transaction of its own and a rule only has to leave room in that one.
    store.members.add_all(a_set())
    assert store.members.count() == 5
    assert store.notes.count() == 8


def test_a_child_a_rule_queued_is_not_put_back_by_a_block_that_rolled_back(
    store,
):
    """A rollback puts a write's queued children back, because a queued child
    has no row to be read again from and losing it loses it for good. What a
    rule queued is not one of those: the rule runs again when the work does, and
    putting its child back as well would write two of it for one save."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.notes.add(MemberNote(body=f"wrote {self.family_name}"))

    @child(of=Member, name="notes", table="member_note", collection="notes")
    class MemberNote:
        body: str = field()

    store.create(Member, MemberNote)
    member = Member(family_name="Hemingway")

    with pytest.raises(ValueError, match="not this one"):
        with store.transaction():
            store.members.add(member)
            raise ValueError("not this one")

    # The work run again, which is what a refused block asks of its caller.
    store.members.add(member)
    assert [note.body for note in member.notes.find()] == ["wrote Hemingway"]


def test_a_rule_before_a_write_does_not_run_on_a_delete(store):
    """Two marked methods, two moments. A save must not pay for a rule about
    removal, and a delete must not run one about the write — nothing was written,
    and a record whose row is about to go has nothing to say about it."""
    ran = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def say_written(self, write):
            ran.append("written")

        @before_delete
        def say_gone(self):
            ran.append("gone")

    store.create(Member)
    member = store.members.add(Member(family_name="Hemingway"))
    assert ran == ["written"]

    member.delete()
    assert ran == ["written", "gone"]


def test_a_write_inside_a_block_runs_its_rule_in_that_transaction(store):
    """A write enlists where a caller already has a block open, so this runs
    inside theirs rather than one of its own — which means a block that rolls
    back takes what the handler wrote as well as the row, and neither is replayed
    here."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def keep_what_it_said(self, write):
            self.store.traces.add(Trace(message=f"wrote {self.family_name}"))

    store.create(Trace, Member)

    with pytest.raises(RuntimeError, match="thought better of it"):
        with store.transaction():
            store.members.add(Member(family_name="Hemingway"))
            raise RuntimeError("thought better of it")

    assert store.traces.count() == 0
    assert store.members.count() == 0


def test_a_bulk_write_of_records_that_marked_nothing_runs_no_rule(store):
    """What this costs a `save_all` is the question the hook had to answer before
    it could exist. A record marking nothing is asked once whether it did — a
    dictionary lookup — and is never touched, so the set of four hundred that is
    what `save_all` exists for pays for a feature it is not using in lookups
    rather than in calls."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

    store.create(Member)
    assert Member.__dray_hooks__ == {}

    members = store.members.add_all(
        [Member(family_name=f"Walker {i}") for i in range(50)]
    )
    for one in members:
        assert getattr(one, "_dray_collection", None) is store.members
    assert store.members.count() == 50


def test_a_rule_before_a_write_is_told_what_the_write_was_told(store):
    """The commonest rule a web application has — *this one is yours* — sat at
    three call sites, because the hook that exists to take it off them was
    handed nothing but the record and a record does not know who is asking. The
    `given=` of the call is on the write, over the store's defaults, so the rule
    reads it whichever door the write came in by."""

    class NotYours(Exception):
        pass

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        owner: str = field(default="")

        @before_save
        def only_the_owner_may_write(self, write):
            if write.given.get("whom") != self.owner:
                raise NotYours(str(write.given.get("whom")))

    store.create(Member)
    store.defaults["whom"] = "rod"
    one = store.members.add(Member(family_name="Hemingway", owner="rod"))

    one.family_name = "Stein"
    with pytest.raises(NotYours, match="jo"):
        one.save(given={"whom": "jo"})
    # The save is the narrower of the two and wins over the store's default,
    # which is the order `given` is built in everywhere else.
    one.save(given={"whom": "rod"})
    assert store.members.by_id(one.id).family_name == "Stein"


def test_a_rule_before_a_write_is_told_whether_it_creates_the_record(store):
    """Nothing on the record answers this. The etag is minted at construction,
    so a record carries one before its first row and a rule reading it cannot
    tell an insert from a save — which left a rule that only applies to the
    creating write gated on a flag somebody had to remember to set."""
    seen = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_save
        def say_which(self, write):
            seen.append((write.adding, write.record is self))

    store.create(Member)
    one = store.members.add(Member(family_name="Hemingway"))
    one.family_name = "Stein"
    one.save()

    assert seen == [(True, True), (False, True)]


def test_a_queued_child_is_told_the_write_carrying_it_is_an_insert(store):
    """A child written by its parent's save is a row that did not exist, however
    deep it hangs — and it hears what that write was told, the same as the
    record riding above it."""
    seen = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

    @child(of=Member, name="notes", table="member_note")
    class MemberNote:
        body: str = field()

        @before_save
        def say_which(self, write):
            seen.append(
                (write.adding, write.given.get("whom"), write.record is self)
            )

    store.create(Member, MemberNote)
    member = store.members.add(Member(family_name="Hemingway"))
    member.notes.add(MemberNote(body="Called back."))
    member.save(given={"whom": "rod"})

    assert seen == [(True, "rod", True)]


def test_the_write_a_rule_is_handed_is_the_one_the_field_handlers_saw(store):
    """`save_all` is the call whose whole purpose is not paying a cost per
    record, so a hook that sees the write may not mean an object per record. The
    one handed over is the one already built to run the fields' own handlers,
    carried to the rule rather than made a second time."""
    handlers = []
    rules = []

    def counting(write):
        handlers.append(write)
        return write.record.saves + 1

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        saves: int = field(default=0, on_save=counting)

        @before_save
        def say_so(self, write):
            rules.append(write)

    store.create(Member)
    one = store.members.add(Member(family_name="Hemingway"))
    one.save()

    assert rules[-1] is handlers[-1]


def test_what_a_rule_writes_into_the_given_of_a_write_is_read_by_nothing(store):
    """`given` is a plain dict and the same one for every record in the write,
    so a handler can write into it. Nothing dray does reads it afterwards —
    every chunk is prepared, so every field is filled, before the first
    statement is sent — and the record behind this one in the same `add_all`
    holds what the caller said rather than what the handler put there."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        author: str | None = field(default=None, on_add=whoever)

        @before_save
        def rewrite_what_it_was_told(self, write):
            write.given["whom"] = "nobody"

    store.create(Member)
    written = store.members.add_all(
        [Member(family_name="Hemingway"), Member(family_name="Shelley")],
        given={"whom": "rod"},
    )

    assert [one.author for one in written] == ["rod", "rod"]
    assert sorted(one.author for one in store.members.find()) == ["rod", "rod"]


def test_a_rule_that_cannot_take_the_write_is_refused_at_the_class(store):
    """Where the class is written and not at somebody's first save, which is
    where a wrong signature would otherwise be heard — a `TypeError` from inside
    a transaction that had already opened, naming dray's call rather than the
    line that is wrong. Both ways of getting it wrong, since a handler taking a
    third thing is as uncallable as one taking none."""

    with pytest.raises(TypeError, match=r"say_so\(self\) cannot be called"):

        @record(table="member", collection="members")
        class TooFew:
            family_name: str = field()

            @before_save
            def say_so(self):
                ...

    with pytest.raises(TypeError, match=r"say_so\(self, write, why\)"):

        @record(table="member", collection="members")
        class TooMany:
            family_name: str = field()

            @before_save
            def say_so(self, write, why):
                ...


def test_an_override_that_drops_the_write_is_refused_too(store):
    """An override need not repeat the decorator — ordinary Python dispatch is
    what somebody reading the subclass expects — and that is exactly how a
    signature gets left behind. The method dray would actually call is the one
    checked, so the class that narrowed it hears about it rather than the one
    that marked it."""

    class Owned:
        @before_save
        def only_the_owner_may_write(self, write):
            ...

    with pytest.raises(TypeError, match=r"only_the_owner_may_write\(self\)"):

        @record(table="member", collection="members")
        class Member(Owned):
            family_name: str = field()

            def only_the_owner_may_write(self):
                ...


def test_the_hooks_about_the_record_alone_are_handed_only_self(store):
    """The asymmetry is the point rather than an oddity to smooth over. A
    `@check` runs at `parse`, where no write exists; `delete()` takes no
    arguments, so a rule about a removal was told nothing; and an
    `@after_commit` is about rows that have already landed. A method reaching
    for a write at any of the three is asking for something its moment does not
    have, and hears so where it is written."""

    with pytest.raises(TypeError, match="handed nothing but the record"):

        @record(table="member", collection="members")
        class Checking:
            family_name: str = field()

            @check
            def wants_a_write(self, write):
                ...

    with pytest.raises(TypeError, match="handed nothing but the record"):

        @record(table="member", collection="members")
        class Removing:
            family_name: str = field()

            @before_delete
            def wants_a_write(self, write):
                ...

    with pytest.raises(TypeError, match="handed nothing but the record"):

        @record(table="member", collection="members")
        class Landed:
            family_name: str = field()

            @after_commit
            def wants_a_write(self, write):
                ...


#
# What the record said a moment ago
#
# A rule runs on the record as it *will be*, so the field it judges is one the
# write it is judging may already have moved. `write.was` is the other half:
# what the record held before this write, for the fields that moved.
#


def test_a_rule_cannot_be_defeated_by_assigning_the_field_it_judges(store):
    """The manual printed this rule against `self.owner` and it was defeated by
    one assignment, at every door and from any caller: set the field the rule
    reads and it compares the caller against the caller and passes, and the row
    ends up owned by whoever took it. Reproduced against a cluster before it
    was fixed."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")
        subject: str = field(default="")

        @before_save
        def only_the_owner_may_write(self, write):
            if write.given.get("whom") != write.was.get("owner", self.owner):
                raise ValueError("not yours")

    store.create(Ticket)
    one = store.tickets.add(
        Ticket(owner="rod", subject="a"), given={"whom": "rod"}
    )

    held = store.tickets.by_id(one.id)
    held.owner = "jo"
    held.subject = "b"
    with pytest.raises(ValueError, match="not yours"):
        held.save(given={"whom": "jo"})

    assert store.tickets.by_id(one.id).owner == "rod"


def test_what_a_record_was_holds_the_fields_that_moved_and_no_others(store):
    """A whole before-image would be a snapshot of every record at every read,
    and this is the diff instead: a key for a field the write is moving and
    nothing for one it is leaving alone. Which is what makes the default in
    `write.was.get(name, self.name)` the answer rather than a fallback."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")
        subject: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = store.tickets.add(Ticket(owner="rod", subject="a"))

    held = store.tickets.by_id(one.id)
    held.subject = "b"
    held.save()

    assert seen == [{}, {"subject": "a"}]


def test_what_a_record_was_keeps_the_first_of_two_moves_in_one_write(store):
    """Assigned twice before a save, the question is still what the row said:
    the value in between was never anywhere and nobody is judging against it.
    Which is also what makes a field moved and put back harmless — the mapping
    holds a key for it, and the value under it is what the record now says."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")
        subject: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = store.tickets.add(Ticket(owner="rod", subject="a"))

    held = store.tickets.by_id(one.id)
    held.owner = "jo"
    held.owner = "al"
    held.save()

    assert seen[-1] == {"owner": "rod"}


def test_what_a_record_was_is_empty_on_the_write_that_creates_it(store):
    """A record built and then edited before its `add` has a prior value in
    memory, and it was never stored anywhere — so handing it over would judge a
    new record against a value no row ever held. There is no *before* on the
    write that makes the row, which is what `adding` says."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = Ticket(owner="rod")
    one.owner = "jo"
    store.tickets.add(one)

    assert seen == [{}]


def test_what_a_record_was_is_forgotten_once_the_rows_are_durable(store):
    """The trap the same recipe hit as a transient on the record: nothing
    emptying it leaves the next save, for any reason at all, judged against the
    state before the last one — so the original owner would go on being the
    only one who could write, forever."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")
        subject: str = field(default="")

        @before_save
        def only_the_owner_may_write(self, write):
            if write.given.get("whom") != write.was.get("owner", self.owner):
                raise ValueError("not yours")

    store.create(Ticket)
    one = store.tickets.add(
        Ticket(owner="rod", subject="a"), given={"whom": "rod"}
    )
    one.owner = "jo"
    one.save(given={"whom": "rod"})

    one.subject = "b"
    one.save(given={"whom": "jo"})

    assert store.tickets.by_id(one.id).subject == "b"
    with pytest.raises(ValueError, match="not yours"):
        one.save(given={"whom": "rod"})


def test_what_a_record_was_stands_after_a_block_rolls_back(store):
    """The same reason the page gives for the transient it replaces. A block
    that rolled back wrote nothing, so the row still says what it said and the
    work about to be run again is still the same write — forgetting at the
    commit rather than at the write is what keeps the replay judging against
    the prior state rather than against its own first attempt."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = store.tickets.add(Ticket(owner="rod"))

    held = store.tickets.by_id(one.id)
    with pytest.raises(ValueError, match="nothing doing"):
        with store.transaction():
            held.owner = "jo"
            held.save()
            raise ValueError("nothing doing")

    held.save()

    assert seen == [{}, {"owner": "rod"}, {"owner": "rod"}]


def test_what_a_record_was_is_the_same_when_the_commit_is_replayed(
    store, monkeypatch
):
    """DSQL refuses a commit that raced another writer and dray replays the
    whole transaction, this rule included. The second attempt has to judge the
    same write against the same prior state: read off the record instead, it
    would find the values the first attempt left on it and let through what the
    first one refused."""
    from dray.collection import Collection
    from dray.store import retrying

    seen = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))
            self.store.traces.add(Trace(message="attempted"))

    store.create(Trace, Ticket)
    one = store.tickets.add(Ticket(owner="rod"))

    # Refused on the write the rule itself makes, which is enlisted in the
    # ticket's transaction, so the refusal reaches the write that owns the
    # commit and the whole of it is replayed.
    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    held = store.tickets.by_id(one.id)
    held.owner = "jo"
    held.save()

    assert seen == [{}, {"owner": "rod"}, {"owner": "rod"}]


def test_a_record_nobody_assigns_to_remembers_nothing_at_all(store):
    """What makes this affordable. The old value is read at assignment anyway,
    to decide whether `on_change` fires, so keeping the first of them costs a
    `setdefault` — and a read, or a `find` of ten thousand rows, never reaches
    that line and pays nothing. A snapshot taken when the row loads would be a
    tax on every read for a fact almost nobody asks for."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

    store.create(Ticket)
    store.tickets.add(Ticket(owner="rod"))

    found = store.tickets.find()

    assert [getattr(one, "_dray_was", None) for one in found] == [None]


def test_a_rule_cannot_write_into_what_a_record_was(store, monkeypatch):
    """Not the promise `given` makes, and the difference is that dray has
    finished reading that one. One `Write` is built per record per save and
    handed to every attempt of a commit DSQL refuses, so a mapping a rule could
    edit is a mapping the second attempt is judged against — the rule would let
    through what it had just refused and nothing would say so. The replay is
    forced here because it is the case that makes it worth refusing."""
    from dray.collection import Collection
    from dray.store import retrying

    seen = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))
            with pytest.raises(TypeError, match="does not support item"):
                write.was["owner"] = "somebody else"
            self.store.traces.add(Trace(message="attempted"))

    store.create(Trace, Ticket)
    one = store.tickets.add(Ticket(owner="rod"))

    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    held = store.tickets.by_id(one.id)
    held.owner = "jo"
    held.save()

    assert seen == [{}, {"owner": "rod"}, {"owner": "rod"}]


def test_what_a_record_was_is_what_the_row_took_and_not_what_it_holds(store):
    """The prior values go when the rows are durable, and inside a block that
    moment is as far from the write as the caller's remaining lines. A field
    assigned across that gap is holding a value the row never took — so what
    goes back under its name is what the write stored, or the next save is
    judged against an owner who never owned it."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = store.tickets.add(Ticket(owner="rod"))

    held = store.tickets.by_id(one.id)
    with store.transaction():
        held.owner = "jo"
        held.save()
        held.owner = "eve"
    held.owner = "mallory"
    held.save()

    assert store.tickets.by_id(one.id).owner == "mallory"
    assert seen[-1] == {"owner": "jo"}


def test_what_a_record_was_after_an_add_in_a_block_is_what_the_row_took(store):
    """The same gap from the other end. The write that made the row is the
    first thing in the block and the assignment after it never reached one, so
    the prior value the assignment kept is still what the row says — and
    dropping it at the commit would leave the next save anchored on a value
    only the object ever held."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    with store.transaction():
        one = store.tickets.add(Ticket(owner="rod"))
        one.owner = "jo"
    one.owner = "al"
    one.save()

    assert seen[-1] == {"owner": "rod"}


def test_what_a_record_was_before_its_add_is_gone_by_the_save_after(store):
    """The other side of the same bookkeeping. A record edited before its `add`
    remembers a value no row ever held, and the write that makes the row stored
    what the record says instead — so by the next save there is nothing left to
    judge against but the row."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = Ticket(owner="rod")
    one.owner = "jo"
    store.tickets.add(one)

    one.owner = "al"
    one.save()

    assert seen[-1] == {"owner": "jo"}


def test_what_a_record_was_says_nothing_about_a_blob_edited_in_place(store):
    """The one blind spot where `was` agreeing with the record is the failure
    this exists to fix rather than the truth. `tags.append(...)` never reaches
    `__setattr__`, so nothing is remembered — and the value that would have
    been remembered is the same list, which now reads as edited too."""
    seen = []

    @record(table="ticket", collection="tickets")
    class Ticket:
        owner: str = field(default="")
        tags: list = field(default_factory=list, stored_in="blob")

        @before_save
        def say_so(self, write):
            seen.append(dict(write.was))

    store.create(Ticket)
    one = store.tickets.add(Ticket(owner="rod", tags=["urgent"]))

    held = store.tickets.by_id(one.id)
    held.tags.append("mine")
    held.save()

    assert held.tags == ["urgent", "mine"]
    assert seen[-1] == {}


#
# Before a record goes
#
# `delete` opens its own transaction, so nothing above it can wrap one, and a
# policy that has to be atomic with the removal has nowhere else to sit.
#


def test_a_record_that_refuses_its_own_removal_keeps_its_row(store):
    """A domain saying a volunteer is lapsed and never deleted had to write that
    check at each of the places that call `delete`, and the day one of them was
    written without it the row was gone for good. The refusal runs inside the
    delete's own transaction, so the record is left exactly as it was."""

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        status: str = field(default="volunteer")

        @before_delete
        def a_volunteer_is_lapsed_rather_than_removed(self):
            if self.status == "volunteer":
                raise ValueError("lapse a volunteer before deleting them")

    store.create(Member)
    member = store.members.add(Member(family_name="Hemingway"))

    with pytest.raises(ValueError, match="lapse a volunteer"):
        member.delete()
    # And through the collection, which a `delete` defined on the class would
    # not have stood in front of: that one is a rule about the call.
    with pytest.raises(ValueError, match="lapse a volunteer"):
        store.members.delete(member)
    assert store.members.by_id(member.id).family_name == "Hemingway"

    member.status = "lapsed"
    member.save()
    member.delete()
    with pytest.raises(RecordNotFound):
        store.members.by_id(member.id)


def test_a_rule_about_removal_does_not_run_on_a_write(store):
    """The two things a record can mark are separate moments, and a save must
    not pay for a rule about deletion — nor a delete for one about the values,
    which have already been checked by every write that put them there."""
    ran = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def say_so(self):
            ran.append("gone")

    store.create(Member)
    member = store.members.add(Member(family_name="Hemingway"))
    member.family_name = "Shelley"
    member.save()

    assert ran == []
    member.delete()
    assert ran == ["gone"]


def test_what_one_rule_wrote_goes_back_when_the_next_one_refuses(store):
    """The whole reason this runs inside the delete's transaction rather than in
    front of it. A handler that had already written its line would otherwise
    leave the line standing without the removal — a history saying the record
    was deleted, and the record still there."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said(self):
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))

        @before_delete
        def but_not_this_one(self):
            raise ValueError("this one stays")

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))

    with pytest.raises(ValueError, match="this one stays"):
        member.delete()

    assert store.traces.count() == 0
    assert store.members.by_id(member.id).family_name == "Hemingway"


def test_a_rule_that_writes_lands_with_the_removal(store):
    """The other direction, and the case the hook exists for: what it writes is
    part of the same transaction as the delete, so a reader never sees one
    without the other."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said(self):
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))
    member.delete()

    assert [trace.message for trace in store.traces.find()] == [
        "member removed: Hemingway"
    ]
    assert store.members.count() == 0


def test_what_a_rule_wrote_goes_back_when_the_record_is_already_gone(store):
    """The hook runs before any of the statements, so a delete of a record that
    has already gone runs the handler, lets it write, and only then finds no
    row to remove — the rowcount is the thing that knows. So the raise arrives
    after a handler has written its line, and has to take that line with it: a
    second removal leaving a second entry in the history would be a record
    claiming to have gone twice."""
    ran = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said(self):
            ran.append(self.family_name)
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))
    member.delete()

    with pytest.raises(RecordNotFound, match="to delete"):
        member.delete()

    assert ran == ["Hemingway", "Hemingway"]
    assert [trace.message for trace in store.traces.find()] == [
        "member removed: Hemingway"
    ]


def test_a_delete_inside_a_block_runs_its_rule_in_that_transaction(store):
    """A delete enlists where a caller already has a block open, so this runs
    inside theirs rather than one of its own — which means a block that rolls
    back takes what the handler wrote as well as the removal, and neither is
    replayed here."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said(self):
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))

    with pytest.raises(RuntimeError, match="thought better of it"):
        with store.transaction():
            member.delete()
            raise RuntimeError("thought better of it")

    assert store.traces.count() == 0
    assert store.members.by_id(member.id).family_name == "Hemingway"


def test_a_rule_before_a_removal_runs_once_per_attempt(store, monkeypatch):
    """Inside the transaction means inside the replay, and this is the thing
    about it a caller cannot see coming. A refused commit takes the handler's
    rows with it, so the handler has to run again to put them back — which is
    right for what it writes and wrong for anything a rollback cannot reach,
    which is why a side effect belongs in `after_commit` instead."""
    from dray.collection import Collection
    from dray.store import retrying

    ran = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said(self):
            ran.append(self.family_name)
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))

    # Refused once, on the write the handler itself makes — which is inside the
    # delete's transaction and enlisted in it, so the refusal travels up to the
    # delete that owns the commit and the whole of it is replayed.
    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    member.delete()

    assert ran == ["Hemingway", "Hemingway"]
    # Twice run, once landed: the first attempt's row went with the rollback.
    assert store.traces.count() == 1
    assert store.members.count() == 0


def test_what_a_rule_hands_to_after_commit_runs_once_however_many_attempts(
    store, monkeypatch
):
    """The other half of once-per-attempt, and the promise the manual leans on
    when it sends an email or a call to another service out of the rule and
    into `store.after_commit`. The rule runs per attempt, so it registers the
    work per attempt too — the coordinator is told once only because a refused
    attempt's queue is dropped with its rollback, and nothing was exercising
    that the two halves meet."""
    from dray.collection import Collection
    from dray.store import retrying

    told = []

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @before_delete
        def keep_what_it_said_and_tell_the_coordinator(self):
            store.traces.add(Trace(message=f"member removed: {self.family_name}"))
            store.after_commit(lambda: told.append(self.family_name))

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))

    # Refused once, on the write the handler makes inside the delete's
    # transaction, so the whole delete — handler included — is replayed.
    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if self.cls is Trace and next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    member.delete()

    assert told == ["Hemingway"]
    assert store.traces.count() == 1
    assert store.members.count() == 0


def test_a_record_is_not_told_when_it_is_the_one_that_went(store):
    """A delete commits too, and the name suggests it would fire — which is
    exactly why this is worth a test rather than a paragraph. A hook is called
    with nothing, so a handler could not tell the two apart, and it would be
    handed a record whose row is gone and which it cannot read again. Work that
    has to wait for a delete to be durable goes in `store.after_commit` from a
    `before_delete`, which is the same moment reached from the other side and
    which runs once however many times the delete is replayed."""
    told = []

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()

        @after_commit
        def say_it_landed(self):
            told.append(("written", self.family_name))

        @before_delete
        def and_say_when_it_goes(self):
            store.after_commit(lambda: told.append(("gone", self.family_name)))

    store.create(Member)
    member = store.members.add(Member(family_name="Hemingway"))
    assert told == [("written", "Hemingway")]

    member.delete()
    assert told == [("written", "Hemingway"), ("gone", "Hemingway")]


def test_a_rule_before_a_removal_reaches_the_store_off_the_record(store):
    """Every test above closed over the fixture's store and every example in
    the manual reached a module global, so the hook had no way to reach a store
    from the class at all — and a service handing one out per request has none
    to close over. What it reaches is the store the delete is going through, so
    the line it writes is inside the delete's own transaction and goes back with
    the removal when a later rule refuses."""

    @record(table="trace", collection="traces")
    class Trace:
        message: str = field()

    @record(table="member", collection="members")
    class Member:
        family_name: str = field()
        refuse: bool = field(default=False)

        @before_delete
        def keep_what_it_said(self):
            self.store.traces.add(
                Trace(message=f"member removed: {self.family_name}")
            )

        @before_delete
        def unless_the_domain_says_otherwise(self):
            if self.refuse:
                raise ValueError("this one stays")

    store.create(Trace, Member)
    member = store.members.add(Member(family_name="Hemingway"))
    member.delete()

    assert [trace.message for trace in store.traces.find()] == [
        "member removed: Hemingway"
    ]

    kept = store.members.add(Member(family_name="Shelley", refuse=True))
    with pytest.raises(ValueError, match="this one stays"):
        kept.delete()

    assert store.traces.count() == 1
    assert store.members.by_id(kept.id).family_name == "Shelley"


#
# The order, and how many
#


def test_find_returns_them_in_the_order_named(walkers):
    walkers.add_all(
        [
            Walker(family_name="Shelley"),
            Walker(family_name="Hemingway"),
            Walker(family_name="Woolf"),
        ]
    )

    found = walkers.find(order_by="family_name")

    assert [w.family_name for w in found] == ["Hemingway", "Shelley", "Woolf"]


def test_find_reads_a_named_field_backwards(walkers):
    walkers.add_all(
        [
            Walker(family_name="Shelley"),
            Walker(family_name="Hemingway"),
            Walker(family_name="Woolf"),
        ]
    )

    found = walkers.find(order_by=desc("family_name"))

    assert [w.family_name for w in found] == ["Woolf", "Shelley", "Hemingway"]


def test_find_sorts_the_way_the_cluster_does_and_not_the_way_a_machine_does(
    walkers,
):
    """A capital sorts before a lowercase letter, which is what `C` does and
    what a language collation does not.

    The only test here whose answer depends on the collation of the database
    underneath it, and it exists for that reason. DSQL collates as `C`; `initdb`
    collates as whatever the machine says, so a suite that took the default
    would put `de Beauvoir` in the middle here and agree with nothing the
    deployment does. `conftest.py` makes each test's database `C` to close that,
    and `scripts/against_dsql.py` pins the cluster's half — this is the line
    that fails if either stops being true.

    A name with a lowercase particle because that is where it bites in practice:
    every other ordering test in this file uses values the two collations agree
    about, so none of them would notice.
    """
    walkers.add_all(
        [
            Walker(family_name="de Beauvoir"),
            Walker(family_name="Angelou"),
            Walker(family_name="Woolf"),
        ]
    )

    found = walkers.find(order_by="family_name")

    assert [w.family_name for w in found] == ["Angelou", "Woolf", "de Beauvoir"]


def test_find_settles_a_tie_on_the_next_field_named(walkers):
    """Several fields with directions mixed among them, spelled exactly as a
    `@child` spells it — which is the whole reason the two share the code that
    reads it."""
    walkers.add_all(
        [
            Walker(family_name="Woolf", status="enquiry"),
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="volunteer"),
        ]
    )

    found = walkers.find(order_by=("status", desc("family_name")))

    assert [(w.status, w.family_name) for w in found] == [
        ("enquiry", "Woolf"),
        ("volunteer", "Shelley"),
        ("volunteer", "Hemingway"),
    ]


def test_an_order_leaves_the_empty_ones_where_both_databases_put_them(store):
    """*Soonest first, undated at the bottom* is a reason to leave `find` for
    SQL of your own only if dray puts nulls wherever the database feels like
    putting them. It does not: a bare name gives the same order on both
    engines, and the page promises it — so it is pinned rather than left as a
    thing the default happened to do."""

    @record(table="chore", collection="chores")
    class Chore:
        label: str = field(default="")
        due_on: date | None = field(default=None)

    store.create(Chore)
    store.chores.add_all(
        [
            Chore(label="paint", due_on=date(2026, 3, 14)),
            Chore(label="sweep"),
            Chore(label="mend", due_on=date(2026, 3, 1)),
        ]
    )

    up = store.chores.find(order_by="due_on")
    down = store.chores.find(order_by=desc("due_on"))

    assert [chore.label for chore in up] == ["mend", "paint", "sweep"]
    assert [chore.label for chore in down] == ["sweep", "paint", "mend"]


def test_an_order_puts_the_empty_ones_where_it_is_told(store):
    """The two orders the default cannot give: up with the undated ones first,
    and down with them last. Before `nulls=` neither could be asked for at all
    — `order_by="due_on nulls first"` is refused as a field the class does not
    declare, which is the check earning its keep rather than failing."""

    @record(table="errand", collection="errands")
    class Errand:
        label: str = field(default="")
        due_on: date | None = field(default=None)

    store.create(Errand)
    store.errands.add_all(
        [
            Errand(label="paint", due_on=date(2026, 3, 14)),
            Errand(label="sweep"),
            Errand(label="mend", due_on=date(2026, 3, 1)),
        ]
    )

    up = store.errands.find(order_by=asc("due_on", nulls="first"))
    down = store.errands.find(order_by=desc("due_on", nulls="last"))

    assert [errand.label for errand in up] == ["sweep", "mend", "paint"]
    assert [errand.label for errand in down] == ["paint", "mend", "sweep"]


def test_a_term_saying_nothing_about_nulls_writes_nothing_about_them(postgresql):
    """`asc("family_name")` is the bare name and nothing more, so the statement
    every caller was already getting is the statement they still get. Saying the
    database's own default out loud instead would have rewritten every read in
    every application for an answer none of them would come back differently
    with — and a plan somebody had already read."""
    seen = []
    store = Store(postgresql, records=[Walker], observer=seen.append)
    store.create(Walker)
    seen.clear()

    store.walkers.find(order_by=asc("family_name"))
    store.walkers.find(order_by=("family_name", desc("given_names")))

    ordering = [
        span.sql.split(" order by ")[1]
        for span in seen
        if span.phase == "close" and span.kind == "statement"
    ]
    assert ordering == ["family_name, id", "family_name, given_names desc, id"]


def test_a_null_placement_that_is_not_a_placement_is_refused():
    """`nulls="firsts"` reaching a statement is a syntax error from the driver
    at the first read, which is a long way from the line that got it wrong —
    and the message names the two words rather than leaving them to be guessed
    at."""
    with pytest.raises(ValueError, match="'first' or 'last'"):
        asc("family_name", nulls="firsts")

    with pytest.raises(ValueError, match="'first' or 'last'"):
        desc("family_name", nulls="FIRST")


def test_an_order_is_total_so_the_same_rows_arrive_the_same_way(walkers):
    """`id` goes on the end of whatever was named. Rows tied on every named
    field would otherwise arrive in whatever order the table felt like that
    afternoon, which is a list reshuffling under somebody refreshing it."""
    walkers.add_all([Walker(family_name="Same") for _ in range(20)])

    ids = [w.id for w in walkers.find(order_by="family_name")]

    # Sorted rather than merely repeatable: two reads of a small table agree
    # with no `order by` at all, so agreeing proves nothing about totality.
    assert ids == sorted(ids)


def test_a_bare_find_comes_back_in_the_order_the_class_declared(store):
    """`find()` naming no order sent no `order by` at all, so the same list read
    twice could arrive two ways — while a `@child` asking the identical question
    had declared its order once and was given it every time. Same question,
    *nobody said*, and one library answering it two ways."""

    @record(table="ranger", collection="rangers", order_by="family_name")
    class Ranger:
        family_name: str = field()

    store.create(Ranger)
    store.rangers.add_all(
        [
            Ranger(family_name="Shelley"),
            Ranger(family_name="Hemingway"),
            Ranger(family_name="Woolf"),
        ]
    )

    found = store.rangers.find()

    assert [r.family_name for r in found] == ["Hemingway", "Shelley", "Woolf"]


def test_a_class_declaring_no_order_falls_back_to_its_key(postgresql):
    """The key is total and stable and means nothing, which is what a `@child`
    has always fallen back to and is now what a record does. Nothing before it
    was a read whose rows arrived in whatever order the table felt like, which
    is the one answer a caller cannot plan around."""
    seen = []
    store = Store(postgresql, records=[Walker], observer=seen.append)
    store.create(Walker)
    seen.clear()

    store.walkers.find()

    (statement,) = [
        span.sql
        for span in seen
        if span.phase == "close" and span.kind == "statement"
    ]
    assert statement.endswith(" order by id")


def test_the_order_a_call_names_beats_the_one_the_class_declared(store):
    """A class says how its records usually read, and a page that lets somebody
    sort by something else still has to win. Both are `order_by` and the nearer
    one takes it."""

    @record(table="scout", collection="scouts", order_by="family_name")
    class Scout:
        family_name: str = field()
        status: str = field(default="enquiry")

    store.create(Scout)
    store.scouts.add_all(
        [
            Scout(family_name="Hemingway", status="volunteer"),
            Scout(family_name="Shelley", status="enquiry"),
        ]
    )

    found = store.scouts.find(order_by="status")

    assert [s.family_name for s in found] == ["Shelley", "Hemingway"]


def test_find_first_takes_the_class_s_order_where_the_call_names_none(store):
    """A first with nothing ordering it was a row nobody chose and not the same
    row twice, which made `find_first` the one read a class could not settle on
    behalf of its callers."""

    @record(table="pilot", collection="pilots", order_by=desc("family_name"))
    class Pilot:
        family_name: str = field()

    store.create(Pilot)
    store.pilots.add_all(
        [Pilot(family_name="Hemingway"), Pilot(family_name="Woolf")]
    )

    assert store.pilots.find_first().family_name == "Woolf"


def test_a_record_ordered_by_a_field_it_does_not_declare_is_refused():
    """Checked where the class is written rather than at the first read, which
    is what a `@child` has always had: an order declared once is wrong on every
    read of every record, so the line that got it wrong is the only useful place
    to say so."""
    with pytest.raises(TypeError) as raised:
        @record(table="drover", collection="drovers", order_by="wwcc_number")
        class Drover:
            family_name: str = field()

    assert "wwcc_number" in str(raised.value)


def test_find_ordered_by_a_field_the_class_does_not_declare_is_refused(walkers):
    """The same refusal a `@child` gets, and the point of `order_by` existing
    at all: a sort column chosen by whoever is looking at the page is checked
    against the declaration before it can reach a statement."""
    with pytest.raises(TypeError) as raised:
        walkers.find(order_by="wwcc_number")

    assert "wwcc_number" in str(raised.value)


def test_find_ordered_by_a_blob_field_is_refused(walkers):
    """A key inside a shared jsonb document has no column to sort on, which is
    true here for the same reason it is true of a child."""
    with pytest.raises(TypeError) as raised:
        walkers.find(order_by="suburb")

    assert "blob" in str(raised.value)


def test_find_takes_only_as_many_as_it_was_asked_for(walkers):
    walkers.add_all([Walker(family_name=f"Walker{n:02}") for n in range(10)])

    found = walkers.find(order_by="family_name", limit=3)

    assert [w.family_name for w in found] == ["Walker00", "Walker01", "Walker02"]


def test_a_limit_that_is_not_a_number_is_refused(walkers):
    """The page sells `find` as where a value chosen by whoever is looking at
    the page belongs, and a query string hands over `"20"` rather than `20`.
    Saying so beats letting `limit '20'` reach the statement. `True` is an
    `int` to Python and a mistake here, so it is refused with the rest."""
    with pytest.raises(TypeError):
        walkers.find(limit="20")

    with pytest.raises(TypeError):
        walkers.find(limit=True)


def test_a_limit_of_nothing_is_refused(walkers):
    """`in_batches(of=0)` is refused for the same reason. Asking for none of
    them is not asking, and handing back an empty list would answer a question
    nobody meant to put."""
    with pytest.raises(ValueError, match="at least one record"):
        walkers.find(limit=0)


def test_the_order_and_the_limit_are_options_rather_than_filters(walkers):
    """Neither reaches `_conditions` to be looked up as a field, because they
    sit beside the filter rather than in it — which is what happened before
    they existed, where `find(order_by=...)` refused the class having no such
    field to filter on."""
    walkers.add_all(
        [
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="volunteer"),
            Walker(family_name="Woolf", status="enquiry"),
        ]
    )

    found = walkers.find(
        equals={"status": "volunteer"}, order_by="family_name", limit=1
    )

    assert [w.family_name for w in found] == ["Hemingway"]


def test_find_first_takes_the_head_of_what_find_would_give(walkers):
    """The same filters, the same ordering, and one of them back rather than
    all of them."""
    walkers.add_all(
        [
            Walker(family_name="Woolf", status="volunteer"),
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="enquiry"),
        ]
    )

    found = walkers.find_first(
        equals={"status": "volunteer"}, order_by="family_name"
    )

    assert found.family_name == "Hemingway"


def test_find_first_matching_nothing_is_none_rather_than_an_exception(walkers):
    """A search is a question about what exists, so nothing matching is an
    ordinary answer. `by_id` raises because an id is something the caller
    already believes in — the two together are the whole rule."""
    walkers.add(Walker(family_name="Hemingway", status="volunteer"))

    assert walkers.find_first(equals={"status": "nobody"}) is None


def test_find_first_asks_for_one_row_rather_than_building_every_match(
    walkers, monkeypatch
):
    """The point of it over `find(...)[0]`: on a table where the answer is one
    of forty thousand, the difference is the whole read."""
    from dray import Collection

    walkers.add_all([Walker(family_name=f"Walker{n:02}") for n in range(10)])

    ran = []
    real = Collection.select_many

    # The statement is the only observable. Whether ten rows were built or one
    # cannot be seen from what comes back, which is a record either way.
    def watching(self, statement, params=()):
        ran.append(statement)
        return real(self, statement, params)

    monkeypatch.setattr(Collection, "select_many", watching)

    found = walkers.find_first(order_by="family_name")

    assert found.family_name == "Walker00"
    assert ran[-1].endswith("limit 1")


def test_find_first_takes_no_limit_of_its_own(walkers):
    """`limit` is what it does, so there is no option of that name and Python
    says so, naming the call the caller actually made. It used to be a
    hand-written refusal here, because `limit` arrived in the same bag as the
    filter and forwarding it would have collided with `find`'s own — which
    answered somebody who never mentioned `find` with an error about it."""
    with pytest.raises(TypeError, match="find_first"):
        walkers.find_first(limit=2)


#
# Reading a set too large to hold
#


def test_walking_a_set_a_batch_at_a_time(walkers):
    walkers.add_all([Walker(family_name=f"Walker{n:03}") for n in range(250)])

    batches = list(walkers.in_batches(of=100))
    assert [len(batch) for batch in batches] == [100, 100, 50]

    # Everything, once each. Getting a keyset walk subtly wrong is how records
    # come to be visited twice or skipped, which is the reason this exists.
    seen = [w.id for batch in batches for w in batch]
    assert len(seen) == 250
    assert len(set(seen)) == 250


def test_a_walk_takes_the_filters_find_takes(walkers):
    walkers.add_all(
        [
            Walker(family_name=f"Walker{n:03}", status="volunteer" if n % 2 else "lapsed")
            for n in range(40)
        ]
    )

    volunteers = [
        w
        for batch in walkers.in_batches(of=7, equals={"status": "volunteer"})
        for w in batch
    ]
    assert len(volunteers) == 20
    assert {w.status for w in volunteers} == {"volunteer"}

    # Including the ones that are more than equality.
    live = [
        w
        for batch in walkers.in_batches(
            of=7, equals={"status": any_of("volunteer", "lapsed")}
        )
        for w in batch
    ]
    assert len(live) == 40


def test_a_walk_over_nothing_yields_nothing(walkers):
    assert list(walkers.in_batches()) == []
    walkers.add(Walker(family_name="Hemingway", status="volunteer"))
    assert list(walkers.in_batches(equals={"status": "lapsed"})) == []


def test_a_walk_stops_without_a_round_trip_it_does_not_need(walkers):
    """Short of a full batch means the table ran out, so asking again could
    only ever come back empty."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(3)])
    assert [len(batch) for batch in walkers.in_batches(of=10)] == [3]

    # An exact multiple cannot know that, so it asks once more — and the empty
    # answer ends the walk rather than being yielded as a batch of nothing.
    assert [len(batch) for batch in walkers.in_batches(of=3)] == [3]


def test_reading_and_writing_a_set_batches_both_ends(walkers):
    """The case this exists for: 3,000 rows to a transaction going out, and
    whatever fits in memory coming in, so the batch it yields is the set to
    hand to `save_all`."""
    walkers.add_all(
        [Walker(family_name=f"Walker{n:03}", status="candidate") for n in range(120)]
    )

    for batch in walkers.in_batches(of=50, equals={"status": "candidate"}):
        for walker in batch:
            walker.status = "volunteer"
        walkers.save_all(batch)

    assert walkers.count(equals={"status": "volunteer"}) == 120
    assert walkers.count(equals={"status": "candidate"}) == 0


def test_a_batch_of_nothing_is_refused(walkers):
    with pytest.raises(ValueError, match="at least one record"):
        list(walkers.in_batches(of=0))


#
# Your field names, and dray's option names
#


# A restaurant's word for every option dray takes. `limit` lives in the blob
# because PostgreSQL keeps that word, so `create table sitting (limit bigint,
# ...)` is a syntax error however dray feels about it — dray does not quote the
# identifiers it writes. The rest are ordinary columns.
@record(table="sitting", collection="sittings")
class Sitting:
    label: str = field(default="")
    limit: int = field(default=0, stored_in="blob")  # minutes at the table
    of: int = field(default=0)                       # how many are sitting
    order_by: str = field(default="")                # what they asked for
    equals: str = field(default="")
    guarded: bool = field(default=False)
    given: str = field(default="")


@pytest.fixture
def sittings(store):
    store.create(Sitting)
    return store.sittings


def test_a_field_may_be_named_for_an_option_and_still_be_filtered_on(sittings):
    """`find(limit=90)` answered a different question from `count(limit=90)`
    and said nothing about it: `find` declared `limit` as an option and `count`
    did not, so the same filter gave the first ninety sittings from one and the
    three that ran ninety minutes from the other. With the filter inside
    `equals` there is one reading of it, and every read agrees."""
    sittings.add_all(
        [Sitting(label=f"table {n}", limit=90 if n < 3 else 60) for n in range(10)]
    )

    assert len(sittings.find(equals={"limit": 90})) == 3
    assert sittings.count(equals={"limit": 90}) == 3
    assert sittings.find_first(equals={"limit": 90}).limit == 90
    assert [
        len(batch) for batch in sittings.in_batches(of=2, equals={"limit": 90})
    ] == [2, 1]


def test_an_option_and_a_field_of_the_same_name_in_one_call(sittings):
    """Both meanings at once, which is the sentence the collision made
    unsayable: the window sittings of two, ordered by the field the diners call
    `order_by`, and only the first two of those."""
    sittings.add_all(
        [
            Sitting(label="second", of=2, order_by="b", equals="window"),
            Sitting(label="first", of=2, order_by="a", equals="window"),
            Sitting(label="other", of=3, order_by="c", equals="bar"),
        ]
    )

    found = sittings.find(
        equals={"of": 2, "equals": "window"}, order_by="order_by", limit=2
    )

    assert [sitting.label for sitting in found] == ["first", "second"]


def test_a_filter_outside_equals_is_refused(sittings):
    """The whole of what the two bags cost. A name dray does not take is not
    quietly a filter now, so a misspelt one is Python's own error against the
    call that was made rather than a read nobody asked for."""
    with pytest.raises(TypeError, match="unexpected keyword argument 'label'"):
        sittings.find(label="table 1")

    with pytest.raises(TypeError, match="unexpected keyword argument 'label'"):
        sittings.count(label="table 1")

    with pytest.raises(TypeError, match="unexpected keyword argument 'label'"):
        list(sittings.in_batches(label="table 1"))


def test_a_misspelt_save_option_is_refused_rather_than_assigned(sittings):
    """`save(etaG=...)` went out unguarded and said nothing — the misspelling
    fell into the assignment bag, matched no field, and was dropped. It is a
    `TypeError` now, and so is a value offered for assignment outside
    `given`."""
    sitting = sittings.add(Sitting(label="table 1"))

    with pytest.raises(TypeError, match="unexpected keyword argument 'etaG'"):
        sitting.save(etaG=sitting.etag)

    with pytest.raises(TypeError, match="unexpected keyword argument 'givne'"):
        sittings.save_all([sitting], givne={"given": "rod"})


def test_a_field_may_be_named_for_a_write_option_and_still_be_assigned(sittings):
    """`guarded` and `given` are a domain's words here as much as dray's. The
    option guards the write and the field takes what the write was told, in the
    same call, because one of them is inside `given` and the other beside it."""
    sitting = sittings.add(Sitting(label="table 1"), given={"given": "rod"})
    assert sitting.given == "rod"
    assert sitting.guarded is False

    sitting.label = "table 2"
    sittings.save_all([sitting], guarded=True, given={"guarded": True})

    again = sittings.by_id(sitting.id)
    assert (again.label, again.given, again.guarded) == ("table 2", "rod", True)


#
# The names dray owns
#


# A system being moved onto dray, which is where all five of dray's names turn
# up at once. The business has always called an employee number an `id`, mirrors
# an upstream API that sends an `etag`, and has a legacy `data` column nobody is
# willing to rename — so dray's stand somewhere else and wear the prefix, which
# is the right way round: the machinery is what should look like machinery.
@record(
    table="member", collection="members",
    key="ref", etag="dray_etag", blob="payload",
)
class Member:
    id: str = field(default="")                # the employee number
    etag: str = field(default=None)            # the upstream API's ETag
    data: dict | None = field(default=None)    # the old system's column
    family_name: str = field(default="")
    suburb: str | None = field(default=None, stored_in="blob")


@child(
    of=Member, name="remarks", table="remark", collection="remarks",
    key="ref", parent_type="about_kind", parent_id="about",
)
class Remark:
    body: str = field(default="")
    parent_id: str = field(
        default=""
    )         # the reference on the paper form


@pytest.fixture
def members(store):
    store.create(Member, Remark)
    return store.members


def test_a_class_can_take_back_every_word_dray_spells_in_english(store, members):
    """`id`, `etag` and `data` are the domain's words on a system being moved
    onto dray, and two of them were unusable: a declared `data` emitted the
    column twice and no database would take the `create table`, while a child's
    declared `parent_id` had dray's type and converter written over it at
    import. Each is a role now, with an option naming the column that fills
    it."""
    assert schema.create_table(Member) == (
        "create table if not exists member (\n"
        "    ref uuid primary key,\n"
        "    id text,\n"
        "    etag text,\n"
        "    data jsonb,\n"
        "    family_name text,\n"
        "    dray_etag text,\n"
        "    payload jsonb not null default '{}'::jsonb\n"
        ")"
    )

    member = members.add(
        Member(
            id="E1207",
            etag='W/"9f4c"',
            data={"legacy": "kept"},
            family_name="Hemingway",
            suburb="Katoomba",
        )
    )
    again = members.by_id(key_of(member))

    # Three columns read the way the business says them, and every one of them
    # is the class's own value rather than something dray filled in.
    assert (again.id, again.etag, again.data) == (
        "E1207", 'W/"9f4c"', {"legacy": "kept"}
    )
    assert again.suburb == "Katoomba"
    assert again.ref == member.ref
    # And `drift` watches all seven, because every one of them is on the class.
    assert schema.drift(store.conn, Member) == []


def test_the_key_column_comes_first_however_the_class_declared_it(members):
    """A table is read by people who never use dray, and the first column is
    where they look to find out what a row is. In declaration order a moved key
    sat in the middle and whatever came first read as though it were the key —
    here that would have been the employee number, which is not unique."""
    assert schema.create_table(Member).splitlines()[1] == (
        "    ref uuid primary key,"
    )

    @record(table="latekey", collection="latekeys")
    class LateKey:
        family_name: str = field(default="")
        id: str = field(default="")

    assert schema.create_table(LateKey).splitlines()[1] == (
        "    id text primary key,"
    )


def test_the_guard_is_drays_wherever_the_class_put_it(members):
    """`save(etag=...)` is the option naming the role, so it goes on meaning the
    guard on a record carrying an `etag` of its own — and the column it reads is
    whatever that class called dray's. Read off the field it happened to be
    named after, the guard compared an upstream API's token against itself and
    passed every time."""
    member = members.add(Member(id="E1", etag="upstream", family_name="Shelley"))
    shown = member.dray_etag

    member.family_name = "Shelley-Godwin"
    member.save()

    with pytest.raises(RecordHasChanged):
        members.save(member, etag=shown)

    members.save(member, etag=member.dray_etag)


def test_a_child_points_at_its_parent_under_the_names_the_class_gave(members):
    """A child declaring `parent_id` lost the field: `_declare` wrote dray's
    type, default and converter over whatever was there, so the value went in
    and the parent pointer came out. Moved, the field is the class's and dray's
    pointer is beside it — and the cascade, which crosses three tables, reads
    each one's own answer about what it calls them."""
    member = members.add(Member(id="E1", family_name="Stoker"))
    member.remarks.add("Rang about the Katoomba weekend.", parent_id="F/119")
    member.save()

    [stored] = member.remarks
    assert stored.parent_id == "F/119"
    assert stored.about_kind == "member"
    assert stored.about == member.ref

    # And the cross-parent read finds it under those names as well, which is
    # what `parent=` is for: neither column is typed anywhere.
    assert [r.body for r in members.store.remarks.find(parent=member)] == [
        "Rang about the Katoomba weekend."
    ]

    member.delete()
    assert members.store.conn.execute(
        "select count(*) from remark"
    ).fetchone()[0] == 0


def test_a_field_dray_would_have_written_over_is_refused(members):
    """Every one of them was accepted at import and broke later — at the
    `create table` for the blob, at the first construction for a child's parent
    columns. The refusal names the option that moves dray's, because renaming
    the field is exactly what a system being moved onto dray cannot do."""
    with pytest.raises(TypeError, match='blob="dray_data"'):
        @record(table="clash1", collection="clash1s")
        class Blobbed:
            data: str = field(default="")

    with pytest.raises(TypeError, match='etag="dray_etag"'):
        @record(table="clash2", collection="clash2s")
        class Guarded:
            etag: str = field(default="")

    with pytest.raises(TypeError, match='parent_id="dray_parent_id"'):
        @child(of=Member, name="clashes", table="clash3")
        class Pointed:
            parent_id: str = field(default="")


def test_two_of_drays_columns_cannot_land_on_one_name(members):
    """One column doing two jobs is the same defect from the other side — it
    would be emitted twice and hold whichever value was written last."""
    with pytest.raises(TypeError, match="key"):
        @record(table="clash4", collection="clash4s", etag="id")
        class Doubled:
            family_name: str = field(default="")


def test_key_of_answers_for_a_record_that_does_not_know_what_it_is(members):
    """An admin screen, a serialiser or an audit log works across record types
    and still needs the key. On a Member, `person.id` is somebody's employee
    number — a function rather than a member, so it costs the record no name."""
    member = members.add(Member(id="E1", family_name="Woolf"))
    walker = Walker(family_name="Woolf")

    assert key_of(member) == member.ref
    assert key_of(member) != member.id
    assert key_of(walker) == walker.id


def test_the_seven_names_off_a_class_are_the_seven_off_the_collection(
    store, members
):
    """A caller holding classes could reach none of the seven names a
    collection publishes, so it reached for `__dray_table__` — a spelling the
    page has never mentioned, and one of seven it would have gone on reaching
    for. Both doors are one object now, so they cannot come to differ."""
    both = ((Member, store.members), (Remark, store.remarks))
    for cls, of_the_store in both:
        for name in ("table", "columns", "blob", "id", "etag"):
            assert getattr(names_of(cls), name) == getattr(of_the_store, name)
    for name in ("parent_type", "parent_id"):
        assert getattr(names_of(Remark), name) == getattr(store.remarks, name)

    # Each is the class's own word rather than the plain one, which is the
    # whole reason a statement reads them rather than typing them out.
    assert (names_of(Member).id, names_of(Member).etag) == ("ref", "dray_etag")
    assert names_of(Member).blob == "payload"

    # A record answers as its class does, because the names are a fact about
    # the class either way.
    member = members.add(Member(id="E1", family_name="Austen"))
    assert names_of(member).table == names_of(Member).table == "member"

    # And the line that wanted this in the first place, with no dunder in it:
    # every write dray offers takes records, so emptying a set of tables is a
    # statement of your own, and these are the names it is built out of.
    with store.conn.cursor() as cur:
        for cls in (Remark, Member):
            cur.execute(f"delete from {names_of(cls).table}")
    assert members.count() == 0


def test_names_of_answers_for_a_child_with_no_collection_to_be_asked():
    """`@child` takes `collection=` and defaults to none, which is what almost
    every child wants — and `store.create(...)` names every class with a table,
    children included. So for most of the classes in exactly the tuple the page
    tells a caller to assemble there is no `store.<something>` to point at, and
    the route to the table name went through a dunder and then handed `getattr`
    a `None`."""

    @record(table="ledger", collection="ledgers")
    class Ledger:
        name: str = field(default="")

    @child(of=Ledger, name="lines", table="ledgerline")
    class Line:
        body: str = field(default="")

    assert Line.__dray_collection__ is None
    assert names_of(Line).table == "ledgerline"
    assert names_of(Line).parent_type == "parent_type"
    assert names_of(Line).parent_id == "parent_id"


def test_names_of_costs_a_record_none_of_the_seven_words_it_hands_back():
    """Two of the seven are words dray has already spent on every record it
    builds — `booking.id` is the key's value and `booking.etag` is the guard's
    — and the other five are words a domain may want, so a restaurant declaring
    a `table` field would have had a domain default going into an f-string that
    builds SQL. Nothing is bound on the record, so none of that can happen."""

    @record(table="booking3", collection="bookings3")
    class Booking:
        table: str = field(default="12")     # a restaurant has tables
        who: str = field(default="")

    booking = Booking(who="Rod")

    assert Booking.table == "12"
    assert booking.table == "12"
    assert names_of(Booking).table == "booking3"

    assert booking.id != "id"
    assert names_of(booking).id == "id"
    assert names_of(booking).etag == "etag"

    assert [n for n in dir(Booking) if not n.startswith("_")] == [
        "as_dict", "children", "delete", "parse", "save", "store",
        "table", "who",
    ]


def test_names_of_refuses_a_class_dray_has_never_met(store):
    """The call site is a tuple of classes assembled by hand, so one that never
    met the decorator is the near miss — and `AttributeError: __dray_table__`
    names an internal at somebody who has never heard of one. The two parent
    columns refuse the same way they refuse off a collection, and with the same
    words, because it is the same object saying them."""

    class Ledger:
        name: str = ""

    with pytest.raises(TypeError, match="names_of takes a record class"):
        names_of(Ledger)
    with pytest.raises(TypeError, match="names_of takes a record class"):
        names_of(Ledger())

    with pytest.raises(TypeError) as off_the_class:
        names_of(Member).parent_type
    with pytest.raises(TypeError) as off_the_collection:
        store.members.parent_type
    assert str(off_the_class.value) == str(off_the_collection.value)
    assert "Member is not a child" in str(off_the_class.value)

    with pytest.raises(TypeError, match="is not a child"):
        names_of(Member).parent_id


def test_sql_for_reads_a_field_wherever_the_class_decided_to_keep_it(store):
    """A statement whose fields are named by data cannot be written from the
    class alone: `volume` is right for a column and wrong for a field in the
    blob, and two of the shapes the blob needs are not a cast at all. Half the
    report that found this was one `group by`, and the other half loaded every
    record in the window and summed in Python, for a reason that had nothing to
    do with its domain."""

    @record(table="reading", collection="readings")
    class Reading:
        site: str = field(default="")
        volume: Decimal | None = field(default=None, stored_in="blob")
        taken_on: date | None = field(default=None, stored_in="blob")
        counted: int | None = field(default=None, stored_in="blob")
        ran_for: timedelta | None = field(default=None, stored_in="blob")
        seal: bytes | None = field(default=None, stored_in="blob")
        flags: list | None = field(default=None, stored_in="blob")

    store.create(Reading)
    written = store.readings.add(
        Reading(
            site="Katoomba",
            volume=Decimal("1.23456789"),
            taken_on=date(2026, 3, 14),
            counted=4,
            ran_for=timedelta(minutes=90),
            seal=b"\xde\xad",
            flags=["dry", "checked"],
        )
    )

    c = store.readings
    # A column is its own name, so the statement is character for character
    # what somebody would have typed.
    assert c.sql_for("site") == "site"

    row = c.select_rows(
        f"select {c.sql_for('site')} as site,"
        f" {c.sql_for('volume')} as volume,"
        f" {c.sql_for('taken_on')} as taken_on,"
        f" {c.sql_for('counted')} as counted,"
        f" {c.sql_for('ran_for')} as ran_for,"
        f" {c.sql_for('seal')} as seal,"
        f" {c.sql_for('flags')} as flags"
        f" from {c.table}"
    )[0]

    # The class's own types back, off a call that hydrates nothing — which is
    # what `->>` on its own cannot do, and what the two encoded ones cannot be
    # given by any cast at all.
    assert row["site"] == "Katoomba"
    assert row["taken_on"] == date(2026, 3, 14)
    assert row["counted"] == 4
    assert row["ran_for"] == timedelta(minutes=90)
    assert row["seal"] == b"\xde\xad"
    assert row["flags"] == ["dry", "checked"]

    # And the cost of matching the column rather than casting to a bare
    # `numeric`: the document holds every digit and `numeric(18,6)` does not.
    # Keeping them would make the report's answer move the day somebody
    # promotes the field, which is the one thing this is for.
    assert store.readings.by_id(written.id).volume == Decimal("1.23456789")
    assert row["volume"] == Decimal("1.234568")


def test_sql_for_is_the_expression_promote_builds_for_every_type_there_is():
    """Two readings of one table is a mirror that cracks the first time a type
    is added to `SQL_TYPES` — a promotion that works beside a report that does
    not, about the same field in the same table. Across the whole table by
    construction, so the next entry is covered without anybody remembering
    this test exists."""
    named = {f"f{n}": kind for n, kind in enumerate(schema.SQL_TYPES)}

    def declared(collection: str, **rules) -> type:
        plain = type(
            "Shape",
            (),
            {
                "__annotations__": {
                    name: kind | None for name, kind in named.items()
                },
                **{
                    name: field(default=None, **rules) for name in named
                },
            },
        )
        return record(table="shape", collection=collection)(plain)

    inside = declared("shapes_blob", stored_in="blob")
    outside = declared("shapes_column")

    for name in named:
        # `promote`'s second statement is the backfill, and what it sets the
        # column from is the same expression a report reads the blob with.
        backfill = schema.promote(outside, name)[1]
        held = backfill.split(" = ", 1)[1].split(" where ")[0]
        assert names_of(inside).sql_for(name) == held


def test_a_statement_built_from_the_class_survives_the_field_being_promoted(
    store,
):
    """A hand-written report reaching in with `data->>'region'` sees the rows
    written before the promotion until it sees none, and comes back as one
    group of nulls with nothing raised. `drift` does not look at statements, so
    the number on somebody's screen is simply wrong."""

    @record(table="claim", collection="claims_blob")
    class Before:
        region: str | None = field(default=None, stored_in="blob")

    store.create(Before)
    for region in ("north", "north", "south"):
        store.claims_blob.add(Before(region=region))

    def report(c) -> list[dict]:
        return c.select_rows(
            f"select {c.sql_for('region')} as g, count(*) as n"
            f" from {c.table} group by 1 order by 1"
        )

    by_hand = (
        f"select {store.claims_blob.blob}->>'region' as g, count(*) as n"
        f" from {store.claims_blob.table} group by 1 order by 1"
    )
    answer = [{"g": "north", "n": 2}, {"g": "south", "n": 1}]
    assert report(store.claims_blob) == answer
    assert store.claims_blob.select_rows(by_hand) == answer

    @record(table="claim", collection="claims_column")
    class After:
        region: str | None = field(default=None)

    for statement in schema.promote(After, "region"):
        with store.conn.cursor() as cur:
            cur.execute(statement)

    # Neither statement has been edited. One of them still answers.
    assert report(store.claims_column) == answer
    assert store.claims_column.select_rows(by_hand) == [{"g": None, "n": 3}]


def test_sql_for_answers_the_same_off_a_class_and_off_its_collection(
    store, members
):
    """Everything else a hand-written statement is built out of comes off both
    doors, so a collection method reaching through `dray.names_of(self.cls)`
    for this one would be the odd line in an f-string whose neighbour is
    `self.table`."""
    both = ((Member, store.members), (Remark, store.remarks))
    for cls, of_the_store in both:
        for name in cls.__dray_fields__:
            assert names_of(cls).sql_for(name) == of_the_store.sql_for(name)

    # The key and the guard are fields like any other and get no special case,
    # here on a class that has moved both of dray's off the plain words.
    assert names_of(Member).sql_for("ref") == "ref"
    assert names_of(Member).sql_for("dray_etag") == "dray_etag"
    # And `data` on this class is the domain's own column rather than the blob,
    # which is the whole reason the answer is read off the declaration.
    assert names_of(Member).sql_for("data") == "data"
    assert names_of(Member).sql_for("suburb") == "(payload->>'suburb')::text"


def test_sql_for_answers_for_a_child_with_no_collection_to_be_asked():
    """`@child` defaults to no collection, which is what almost every child
    wants — so for most children there is no `store.<something>` to ask, and
    the report is being written over exactly those tables."""

    @record(table="gauge", collection="gauges")
    class Gauge:
        name: str = field(default="")

    @child(of=Gauge, name="readings", table="gaugereading")
    class Sample:
        volume: Decimal | None = field(default=None, stored_in="blob")

    assert Sample.__dray_collection__ is None
    assert names_of(Sample).sql_for("volume") == (
        "(data->>'volume')::numeric(18,6)"
    )


def test_sql_for_refuses_a_field_the_class_never_declared(store):
    """The name arrives as data, so a typo in a config file is how this will be
    met — and `KeyError: 'subrub'` about `__dray_fields__` names an internal at
    somebody who has never heard of one. `promote`'s refusal is the model."""
    refused = "Member has no field 'subrub' to read"
    with pytest.raises(ValueError, match=refused):
        names_of(Member).sql_for("subrub")
    with pytest.raises(ValueError, match=refused):
        store.members.sql_for("subrub")


def test_a_record_saying_nothing_about_names_builds_the_table_it_always_did():
    """The defaults are the plain words and stay the plain words. The whole
    reason those columns are spelled in English is that a table is read by an
    analyst at a psql prompt who never uses dray, and nothing here makes one say
    `dray_id`."""
    @record(table="plainnames", collection="plainnamess",
            indexes=[index("suburb")])
    class Plain:
        family_name: str = field(default="")
        suburb: str | None = field(default=None)

    assert schema.create_table(Plain) == (
        "create table if not exists plainnames (\n"
        "    id uuid primary key,\n"
        "    family_name text,\n"
        "    suburb text,\n"
        "    etag text,\n"
        "    data jsonb not null default '{}'::jsonb\n"
        ")"
    )


#
# There is no reserved word
#


# A household has children and so has every record dray stores, and until now
# the word belonged to dray. The number is what this domain means by it.
@record(table="household", collection="households")
class Household:
    address: str = field(default="")
    children: int = field(default=0)     # how many live here


@child(of=Household, name="visits", table="visit")
class Visit:
    body: str = field(default="")


@pytest.fixture
def households(store):
    store.create(Household, Visit)
    return store.households


def test_a_field_may_take_a_word_dray_puts_on_every_record(households):
    """Eight ordinary domain words were refused at declaration, `children`
    among them, and a household that has some could not say so. dray holds its
    own copy under a second spelling instead, so the plain word is free and
    nothing dray does goes looking there."""
    household = households.add(Household(address="14 Lurline St", children=3))
    household.visits.add("Called in on the way past.")
    household.save()

    again = households.by_id(household.id)
    assert again.children == 3
    assert list(again._dray_children) == ["visits"]
    assert [v.body for v in again._dray_children["visits"]] == [
        "Called in on the way past."
    ]


def test_a_field_may_take_the_word_a_hook_reaches_its_store_by(store):
    """`store` is the newest of the words dray lends and the one a domain is
    most likely to want back — a chain has stores. Spelling it as a field takes
    the plain word and leaves dray's reading under the prefix, which is what a
    rule shared across record types has to write for exactly this reason."""

    @record(table="outlet", collection="outlets")
    class Outlet:
        store: str = field(default="")     # which shop this is

    store.create(Outlet)
    outlet = store.outlets.add(Outlet(store="Katoomba"))

    again = store.outlets.by_id(outlet.id)
    assert again.store == "Katoomba"
    assert again._dray_store is store


def test_a_record_that_has_never_been_stored_has_no_store_to_give(store):
    """The same sentence a save of one gets, because it is the same missing
    thing: a record built in memory has no collection, so it has no store to
    read the rest of the database through and nowhere to save to either."""

    @record(table="unheld", collection="unhelds")
    class Unheld:
        family_name: str = field(default="")

    with pytest.raises(RuntimeError, match="did not come from a store"):
        Unheld(family_name="Hemingway").store


def test_a_record_may_put_a_rule_in_front_of_what_dray_does(store):
    """A class defining `delete` kept it and lost dray's, which nothing public
    would give back — so the one thing anybody defines the method for turned it
    into a way of losing the behaviour rather than standing in front of it."""

    @record(table="lapser", collection="lapsers")
    class Lapser:
        family_name: str = field(default="")
        status: str = field(default="enquiry")

        def delete(self):
            """A volunteer is lapsed, never removed."""
            if self.status == "volunteer":
                raise ValueError("lapse a volunteer before deleting them")
            self._dray_delete()

        def save(self, **kw):
            self.family_name = self.family_name.strip()
            return self._dray_save(**kw)

    store.create(Lapser)
    lapser = store.lapsers.add(Lapser(family_name="  Hemingway  "))
    lapser.save()
    assert store.lapsers.by_id(lapser.id).family_name == "Hemingway"

    lapser.status = "volunteer"
    lapser.save()
    with pytest.raises(ValueError, match="lapse a volunteer"):
        lapser.delete()
    assert store.lapsers.by_id(lapser.id).status == "volunteer"

    lapser.status = "lapsed"
    lapser.save()
    lapser.delete()
    with pytest.raises(RecordNotFound):
        store.lapsers.by_id(lapser.id)


def test_a_rule_in_front_of_dray_may_live_on_a_base_class(store):
    """A rule is the same rule wherever it is written, and a base class is
    where it goes when several record types keep it. dray asked one class's
    `__dict__`, so an inherited `save` was replaced by dray's own without a
    word: everything imported, the suite stayed green, and every write went
    through a guard that was not there."""

    class LapsesRatherThanDeletes:
        """What two record types in this domain agree about."""

        def delete(self):
            if self.status == "volunteer":
                raise ValueError("lapse a volunteer before deleting them")
            self._dray_delete()

        def save(self, **kw):
            self.family_name = self.family_name.strip()
            return self._dray_save(**kw)

    @record(table="inheritor", collection="inheritors")
    class Inheritor(LapsesRatherThanDeletes):
        family_name: str = field(default="")
        status: str = field(default="enquiry")

    assert Inheritor.save is LapsesRatherThanDeletes.save

    store.create(Inheritor)
    one = store.inheritors.add(Inheritor(family_name="  Hemingway  "))
    one.save()
    assert store.inheritors.by_id(one.id).family_name == "Hemingway"

    one.status = "volunteer"
    one.save()
    with pytest.raises(ValueError, match="lapse a volunteer"):
        one.delete()
    assert store.inheritors.by_id(one.id).status == "volunteer"


def test_a_record_built_on_a_record_still_gets_drays_own(store):
    """The case deciding from the hierarchy could most easily break. A record
    may subclass a record, and the parent carries dray's method under the plain
    word — so a subclass that only asks whether anything up there defines
    `save` finds one, says the word is spent and ends up with no save of its
    own to bind."""

    @record(table="enquirer", collection="enquirers")
    class Enquirer:
        family_name: str = field(default="")

    @record(table="applicant", collection="applicants")
    class Applicant(Enquirer):
        referee: str = field(default="")

    assert Applicant.save is Applicant._dray_save

    store.create(Enquirer, Applicant)
    applicant = store.applicants.add(
        Applicant(family_name="Hemingway", referee="Woolf")
    )
    applicant.family_name = "Woolf"
    applicant.save()

    again = store.applicants.by_id(applicant.id)
    assert (again.family_name, again.referee) == ("Woolf", "Woolf")


def test_two_bases_spelling_one_lent_name_go_by_pythons_order(store):
    """`@check` refuses a name two bases both define, and its reason does not
    reach here: it turns on a marker, so a class that shares a word with a rule
    it never heard of is a collision with nothing to say which was meant. A
    lent name carries no marker — using the word is the whole of the intent —
    so two bases using it is a question Python has already answered."""

    class Stamped:
        def save(self, **kw):
            self.trail += "stamped "
            return self._dray_save(**kw)

    class Counted:
        def save(self, **kw):
            self.trail += "counted "
            return self._dray_save(**kw)

    @record(table="stamper", collection="stampers")
    class Stamper(Stamped, Counted):
        trail: str = field(default="")

    store.create(Stamper)
    stamper = store.stampers.add(Stamper())
    stamper.save()

    assert store.stampers.by_id(stamper.id).trail == "stamped "


def test_a_record_hands_out_every_field_it_declares(walkers):
    """Nothing went record-to-dict, so a page rendering one wrote its fields
    out by hand — a list that stops being right the day the class gains a field
    and stops silently, because nothing downstream is asking. The blob half is
    where that shows first: those fields have no column to remind anybody they
    are there."""
    walker = walkers.add(
        Walker(family_name="Hemingway", suburb="Katoomba", postcode="2780")
    )
    walker.save()

    handed = walker.as_dict()
    assert handed["family_name"] == "Hemingway"
    assert handed["suburb"] == "Katoomba"
    assert (handed["id"], handed["etag"]) == (walker.id, walker.etag)

    # The etag is in it deliberately: a form handed out without one cannot be
    # saved with a guard, which is the case this method was asked for.
    again = Walker.parse(handed)
    assert again == walker
    assert again.etag == walker.etag


def test_a_child_hands_out_the_two_columns_naming_its_parent(households):
    """Nobody declares `parent_type` and `parent_id`, but nobody declares `id`
    or `etag` either and those are in. Leaving them out would make the shape of
    what comes back depend on whether the record is a child — a rule every
    reader carries, for the benefit of two keys a caller can ignore."""
    household = households.add(Household(address="14 Lurline St", children=3))
    household.visits.add("Called in on the way past.")
    household.save()

    [visit] = list(households.by_id(household.id).visits)
    handed = visit.as_dict()

    assert handed["parent_type"] == "household"
    assert handed["parent_id"] == household.id
    assert Visit.parse(handed) == visit


def test_what_a_record_hands_out_is_a_snapshot_of_it(store):
    """Handing the values out by reference made one call two rules: a copy for
    a scalar, since writing a key back never touched the record, and a view for
    a `list`, since editing one in place did. The casualty was the change log —
    an edit through that reference assigned nothing, so `__setattr__` never
    ran and no `on_change` fired, and the next save wrote it to the row
    anyway."""

    @record(table="tagged", collection="taggeds")
    class Tagged:
        tags: list[str] | None = field(
            default=None,
            stored_in="blob",
            on_change=records_change(into="logs"),
        )
        # A list of dicts is where a shallow copy would leave residue, which is
        # why the copy is deep: the blob takes whatever jsonb takes.
        stints: list | None = field(default=None, stored_in="blob")

    @child(of=Tagged, name="logs", table="taggedlog")
    class TaggedLog:
        message: str = field(default="")

    store.create(Tagged, TaggedLog)
    tagged = store.taggeds.add(
        Tagged(tags=["walker"], stints=[{"year": 2026}])
    )
    tagged.save()

    handed = tagged.as_dict()
    assert handed is not tagged.as_dict()

    handed["tags"].append("guide")
    handed["stints"][0]["year"] = 1899
    assert tagged.tags == ["walker"]
    assert tagged.stints == [{"year": 2026}]

    # And nothing reached the row either, which is the failure worth naming:
    # the old shape wrote that edit on the next save with no log line for it,
    # because nothing was ever assigned.
    tagged.save()
    again = store.taggeds.by_id(tagged.id)
    assert again.tags == ["walker"]
    assert [line.message for line in again.logs] == []


def test_a_field_may_take_the_word_a_record_hands_itself_out_by(store):
    """The sixth word is lent the way the other five are, so a domain that
    wants it keeps it — and a handler written across record types still has
    dray's reading, without having to know what the class in hand declared."""

    @record(table="entry", collection="entries")
    class Entry:
        as_dict: str = field(default="")     # which dictionary it is in

    store.create(Entry)
    entry = store.entries.add(Entry(as_dict="Macquarie"))

    again = store.entries.by_id(entry.id)
    assert again.as_dict == "Macquarie"
    assert again._dray_as_dict()["as_dict"] == "Macquarie"


def test_a_field_declared_bare_is_still_required(store):
    """The trap in freeing these words. A bare `save: str` is in the
    annotations and not in `__dict__`, so binding dray's method over it handed
    `dataclasses` a function as that field's default — the field went optional
    without a word and a write carried a function object to the table."""

    @record(table="signoff", collection="signoffs")
    class Signoff:
        save: str

    [spec] = [f for f in dataclasses.fields(Signoff) if f.name == "save"]
    assert spec.default is dataclasses.MISSING

    with pytest.raises(TypeError, match="save"):
        Signoff()

    store.create(Signoff)
    signed = store.signoffs.add(Signoff(save="countersigned"))
    assert store.signoffs.by_id(signed.id).save == "countersigned"


def test_a_bare_as_dict_is_still_a_required_field(store):
    """The same trap, walked into by the newest of the words rather than by the
    oldest. Every word added to the lent set has to be read out of the
    annotations as well as out of `__dict__`, and the failure is quiet at both
    ends: the field goes optional without a word and the table takes a function
    object."""

    @record(table="filing", collection="filings")
    class Filing:
        as_dict: str

    [spec] = [f for f in dataclasses.fields(Filing) if f.name == "as_dict"]
    assert spec.default is dataclasses.MISSING

    with pytest.raises(TypeError, match="as_dict"):
        Filing()

    store.create(Filing)
    filed = store.filings.add(Filing(as_dict="Australian"))
    assert store.filings.by_id(filed.id).as_dict == "Australian"


def test_the_second_spelling_is_there_whether_or_not_the_class_took_the_word(
    households, walkers
):
    """The point of the second spelling is that it can be relied on. A handler
    or a base class calling `_dray_save` has no way to know whether the record
    in hand declared a `save` of its own, and it does not have to."""
    walker = Walker(family_name="Woolf")
    household = Household(address="14 Lurline St", children=3)

    for holder in (walker, household):
        for name in ("save", "delete", "parse", "as_dict", "children"):
            assert hasattr(holder, "_dray_" + name)
        # Off the class for this one. Reading it off a record nobody has stored
        # raises rather than answering, so `hasattr` would be asking a different
        # question of it than of the four above.
        assert isinstance(type(holder)._dray_store, property)

    # On a record that took none of the words, both spellings are the one thing.
    assert walker.save.__func__ is walker._dray_save.__func__


def test_a_field_under_drays_own_prefix_is_refused():
    """The only names left that a field may not take. `_dray_stored` is the
    keyword the constructor reads to say the values came out of the table, and
    a field of that name would take its default silently and switch conversion
    off for everything else in the same call — while `_setattr`, which passes
    any underscored name straight through, would never notice."""
    with pytest.raises(TypeError, match="_dray_stored"):
        @record(table="prefixed1", collection="prefixed1s")
        class Stored:
            _dray_stored: str = field(default="")

    with pytest.raises(TypeError, match="_dray_save"):
        @record(table="prefixed2", collection="prefixed2s")
        class Shadowed:
            _dray_save: str = field(default="")


def test_a_field_may_take_the_names_a_record_is_built_and_stored_with(store):
    """Three of dray's members sat under a single underscore rather than the
    prefix, outside the one rule that says what a field may not be called. A
    field named for any of them took the name off the machinery — and declared
    bare it hit the trap `save: str` hits, handing `dataclasses` a function as
    its default and going optional without a word."""

    @record(table="marker", collection="markers")
    class Marker:
        _load: str
        _validate: str = field(default="")
        _blob: str = field(default="")
        seen_at: str | None = field(default=None, stored_in="blob")

    [spec] = [f for f in dataclasses.fields(Marker) if f.name == "_load"]
    assert spec.default is dataclasses.MISSING
    with pytest.raises(TypeError, match="_load"):
        Marker()

    store.create(Marker)
    marker = store.markers.add(
        Marker(_load="a", _validate="b", _blob="c", seen_at="dusk")
    )
    marker.save()

    # dray's own three are still dray's, and still doing the work: this record
    # was validated, written and hydrated with the fields above in the way.
    assert marker._dray_blob() == {"seen_at": "dusk"}

    again = store.markers.by_id(marker.id)
    assert (again._load, again._validate, again._blob) == ("a", "b", "c")


def test_a_record_may_put_a_rule_in_front_of_assignment(store):
    """`__setattr__` is looked up by its own spelling, so the prefix could not
    cover it and a class defining one took dray's place rather than standing in
    front of it — every converter, validator and `on_change` on the record went
    with it, and nothing public gave them back."""

    @record(table="trimmer", collection="trimmers")
    class Trimmer:
        email: str = field(default="")

        def __setattr__(self, name, value):
            """The address the business types has a space on the end of it."""
            if name == "email" and value:
                value = value.strip()
            self._dray_setattr(name, value)

    store.create(Trimmer)
    trimmer = store.trimmers.add(Trimmer())
    trimmer.email = "  rod@example.com  "
    trimmer.save()

    assert store.trimmers.by_id(trimmer.id).email == "rod@example.com"

    # And dray's is underneath rather than gone, so the rest of what assignment
    # does is still there.
    with pytest.raises(AttributeError, match="has no field"):
        trimmer.postcode = "2780"
    with pytest.raises(AttributeError, match="cannot be changed"):
        trimmer.id = trimmer.id


def test_a_record_may_compare_its_own_way_and_still_ask_for_drays():
    """`__eq__` and `__hash__` are identity by key, which is the question a set
    and a dict key ask, and a class defining either lost that with nothing to
    call instead. Defining `__eq__` also leaves a class unhashable — Python's
    doing, not dray's — so the second spelling is how it asks for the hash
    back."""

    @record(table="alike", collection="alikes")
    class Alike:
        family_name: str = field(default="")

        def __eq__(self, other):
            """This screen groups people who look alike."""
            return self.family_name == other.family_name

        def __hash__(self):
            return self._dray_hash()

    one = Alike(family_name="Hemingway")
    other = Alike(family_name="Hemingway")

    assert one == other
    # dray's reading is still the one about which row this is.
    assert not one._dray_eq(other)
    assert len({one, other}) == 2


def test_the_second_spelling_covers_the_hooks_as_well_as_the_methods():
    """Same promise as for `save` and the rest: code that does not know what
    record it is holding can call dray's, and does not have to ask whether the
    class defined one of its own."""
    walker = Walker(family_name="Woolf")

    for name in ("setattr", "eq", "hash"):
        assert hasattr(walker, "_dray_" + name)

    # On a record that defined none of them, both spellings are the one thing.
    assert type(walker).__eq__ is walker._dray_eq.__func__


#
# SQL you wrote
#


def test_select_many_hydrates_a_statement_of_your_own(walkers):
    walkers.add_all(
        [
            Walker(family_name="Hemingway"),
            Walker(family_name="Shelley"),
            Walker(family_name="Frankenstein"),
        ]
    )
    found = walkers.select_many(
        f"select {walkers.columns} from {walkers.table}"
        " where family_name > %s order by family_name",
        ["G"],
    )
    assert [w.family_name for w in found] == ["Hemingway", "Shelley"]


def test_select_rows_hands_back_an_answer_that_is_not_records(walkers):
    """An aggregate has no record to become. `select_many` would refuse it as a
    partial select and be right to; this is the door it needs instead."""
    walkers.add_all(
        [
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="volunteer"),
            Walker(family_name="Frankenstein", status="enquiry"),
        ]
    )

    got = walkers.select_rows(
        f"select status, count(*) as people from {walkers.table}"
        " group by status order by status"
    )

    assert got == [
        {"status": "enquiry", "people": 1},
        {"status": "volunteer", "people": 2},
    ]


def test_select_rows_takes_parameters_like_any_other_statement(walkers):
    walkers.add_all(
        [
            Walker(family_name="Hemingway", status="volunteer"),
            Walker(family_name="Shelley", status="volunteer"),
            Walker(family_name="Frankenstein", status="enquiry"),
        ]
    )

    got = walkers.select_rows(
        f"select count(*) as people from {walkers.table} where status = %s",
        ["volunteer"],
    )

    assert got == [{"people": 2}]


def test_select_rows_reaches_the_blob_like_a_statement_of_yours(walkers):
    """`{self.blob}` is in reach here too, which is the point of the call
    existing rather than the caller dropping to `store.conn` and copying the
    blob's name out by hand."""
    walkers.add_all(
        [
            Walker(family_name="Hemingway", suburb="Leura"),
            Walker(family_name="Shelley", suburb="Leura"),
            Walker(family_name="Frankenstein", suburb="Katoomba"),
        ]
    )

    got = walkers.select_rows(
        f"select {walkers.blob}->>'suburb' as suburb, count(*) as people"
        f" from {walkers.table} group by 1 order by 2 desc"
    )

    assert got == [
        {"suburb": "Leura", "people": 2},
        {"suburb": "Katoomba", "people": 1},
    ]


def test_select_rows_does_not_ask_for_the_whole_record(walkers):
    """The partial-select guard is `select_many`'s and belongs there: it stops a
    record being built from half a row and saving its defaults over the rest.
    Nothing here builds a record, so a statement naming one column is the
    ordinary case rather than the refused one."""
    walkers.add(Walker(family_name="Hemingway"))

    assert walkers.select_rows(f"select family_name from {walkers.table}") == [
        {"family_name": "Hemingway"}
    ]


def test_a_bulk_write_is_one_call_however_many(walkers):
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(250)])
    assert walkers.count() == 250


def without(columns: str, *dropped: str) -> str:
    """A select list one column short, built off the real one so it stays a
    valid statement about a real table when the class changes."""
    return ", ".join(name for name in columns.split(", ") if name not in dropped)


def test_a_select_of_some_of_the_columns_is_refused(walkers):
    """It used to hydrate, and the record looked complete: `load` gives a key
    that did not come back the field's default, so `status` read 'enquiry'
    where the row said 'volunteer' — and saving that wrote the default over
    what was stored."""
    walkers.add(Walker(family_name="Hemingway", status="volunteer", suburb="Leura"))

    with pytest.raises(ValueError) as raised:
        walkers.select_many(f"select id, family_name from {walkers.table}")
    assert "status" in str(raised.value)
    assert "{self.columns}" in str(raised.value)


def test_a_select_without_an_id_is_refused(walkers):
    """The worst of it, because nothing about the record looks wrong: with no
    id `load` mints a fresh one, so the record belongs to no row at all and its
    next save either finds nothing or inserts a stranger."""
    walkers.add(Walker(family_name="Hemingway"))

    with pytest.raises(ValueError, match="belongs to no row"):
        walkers.select_many(
            f"select {without(walkers.columns, 'id')} from {walkers.table}"
        )


def test_a_select_without_the_blob_is_refused(walkers):
    """The jsonb column is not a field and is the easiest thing to leave off a
    list written by hand, and it carries every field that has no column of its
    own — so a select without it defaults `suburb` and `postcode` together and
    the save that follows empties both."""
    walkers.add(Walker(family_name="Hemingway", suburb="Leura"))

    with pytest.raises(ValueError, match=walkers.blob):
        walkers.select_many(
            f"select {without(walkers.columns, walkers.blob)} from {walkers.table}"
        )


def test_a_partial_select_is_refused_even_where_it_matched_nothing(walkers):
    """Read off what the statement asked for rather than off the rows it got
    back. An empty table is not what makes a statement right, and the one that
    says nothing today is the one that hydrates half a record tomorrow."""
    with pytest.raises(ValueError, match="selects part of a Walker"):
        walkers.select_many(f"select id, family_name from {walkers.table}")


def test_select_first_is_refused_the_same_way(walkers):
    """`select_first` is `select_many` with the first row taken, so the check
    belongs where the two share it rather than in front of either."""
    with pytest.raises(ValueError):
        walkers.select_first(f"select id from {walkers.table} limit 1")


def test_a_column_nobody_declared_still_comes_back_and_is_dropped(walkers):
    """The distinction this rests on, and the half `load` already had. A key
    the class does not recognise is dropped without a word — that is what lets
    a field be retired without a backfill — and nothing about the new
    strictness may touch it. Only a declared column *not* coming back is a
    mistake."""
    walkers.add(Walker(family_name="Hemingway"))

    found = walkers.select_many(
        f"select {walkers.columns}, upper(family_name) as shouted"
        f" from {walkers.table}"
    )
    assert [w.family_name for w in found] == ["Hemingway"]


def test_a_blob_field_is_not_a_column_the_select_has_to_name(walkers):
    """`suburb` and `postcode` live inside the jsonb and never appear in a
    result set under their own names, so a check made against every field
    rather than every column would refuse the statement every collection method
    on the page writes."""
    walkers.add(Walker(family_name="Hemingway", suburb="Leura"))

    found = walkers.select_many(f"select {walkers.columns} from {walkers.table}")
    assert [w.suburb for w in found] == ["Leura"]


#
# Fields the write fills in
#


def test_on_add_fires_once_and_is_never_written_again(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    first = walkers.by_id(walker.id).created_at

    walker.status = "volunteer"
    walker.save()

    assert walkers.by_id(walker.id).created_at == first


def test_what_the_database_computed_comes_back_in_hand(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    # Not the None it was carrying before the write.
    assert walker.created_at is not None
    assert walker.created_at == walkers.by_id(walker.id).created_at


def test_a_field_can_take_whoever_the_write_named(store):
    @record(table="signature", collection="signatures")
    class Signature:
        created_by: str | None = field(default=None, on_add=whoever)
        updated_by: str | None = field(
            default=None, on_add=whoever, on_save=whoever
        )

    store.create(Signature)
    signed = store.signatures.add(Signature(), given={"whom": "rod"})
    assert (signed.created_by, signed.updated_by) == ("rod", "rod")

    signed.save(given={"whom": "jo"})
    again = store.signatures.by_id(signed.id)
    assert (again.created_by, again.updated_by) == ("rod", "jo")


def test_a_handler_returning_nothing_leaves_the_field_alone(store):
    @record(table="quiet", collection="quiets")
    class Quiet:
        name: str = field(default="unnamed")
        touched_by: str | None = field(
            default=None, on_add=lambda w: w.given.get("whom")
        )

    store.create(Quiet)
    quiet = store.quiets.add(Quiet())
    assert store.quiets.by_id(quiet.id).touched_by is None


def test_a_write_can_fill_a_field_with_anything(store):
    @record(table="counter", collection="counters")
    class Counter:
        touched: int = field(default=0, on_save=lambda w: w.record.touched + 1)

    store.create(Counter)
    counter = store.counters.add(Counter())
    assert store.counters.by_id(counter.id).touched == 0

    counter.save()
    counter.save()
    assert store.counters.by_id(counter.id).touched == 2


def test_a_value_the_caller_set_is_not_replaced_by_the_write(walkers):
    """An `on_add` used to be filled whatever the record said, so a record
    carrying its own timestamp was stamped with the moment the script ran and
    nothing was raised about it. An import of 2019 records wants 2019 dates, and
    a report reading them back computed a wait of minus five months.

    Both doors, because a spreadsheet loop builds one way and a fixture the
    other."""
    then = datetime(2019, 3, 1, 9, 30, tzinfo=timezone.utc)

    built = walkers.add(Walker(family_name="Shelley", created_at=then))
    assert built.created_at == then
    assert walkers.by_id(built.id).created_at == then

    assigned = Walker(family_name="Stoker")
    assigned.created_at = then
    walkers.add(assigned)
    assert walkers.by_id(assigned.id).created_at == then


def test_a_field_nobody_named_is_filled_as_it_always_was(walkers):
    """The other half of the same rule, and the one that must not move: a
    handler is skipped for the field somebody chose and for no other."""
    then = datetime(2019, 3, 1, 9, 30, tzinfo=timezone.utc)

    walker = walkers.add(Walker(family_name="Shelley", created_at=then))

    assert walker.updated_at is not None
    assert walker.updated_at != then


def test_a_record_read_back_is_filled_by_a_save_as_it_would_have_been(walkers):
    """What keeps `on_save` working for the ordinary case. A row's values were
    not chosen by whoever is saving it now, so hydrating clears the said-set —
    without that, a record read back would carry its own `updated_at` as a
    choice and never be stamped again."""
    walker = walkers.add(Walker(family_name="Shelley"))
    first = walkers.by_id(walker.id).updated_at

    again = walkers.by_id(walker.id)
    again.status = "candidate"
    again.save()

    assert walkers.by_id(walker.id).updated_at > first


def test_a_derived_column_is_kept_true_and_is_nobody_to_set(store):
    """The shape the manual reaches for where DSQL refuses a partial index: the
    predicate goes in a column, and the column is worked out from the fields it
    is about rather than written by anybody, so no writer is left that can
    forget one — or set it wrong.

    Both directions matter, and the second is the one that bites. A booking that
    gives its table back has to stop matching a read on `held_table` — a handler
    can only do that by handing back a value meaning *not that*, because one
    handing back `None` has nothing to say and the field keeps what it had.
    """

    def holding_table(write):
        booking = write.record
        return booking.table_id if booking.holding else ""

    @record(table="booking", collection="bookings")
    class Booking:
        table_id: str = field(default="")
        holding: bool = field(default=False)
        held_table: str = field(default="", derived=holding_table)

    store.create(Booking)
    booking = store.bookings.add(Booking(table_id="t7", holding=True))
    assert store.bookings.by_id(booking.id).held_table == "t7"
    assert store.bookings.find(equals={"held_table": "t7"}) != []

    booking.holding = False
    booking.save()
    assert store.bookings.by_id(booking.id).held_table == ""
    assert store.bookings.find(equals={"held_table": "t7"}) == []

    # And the way the caller-wins rule above is silent about it: there is no
    # door to name it through, so the column cannot drift from what it is about.
    with pytest.raises(AttributeError, match="derived"):
        store.bookings.by_id(booking.id).held_table = "t7"


def test_a_write_told_to_set_a_derived_field_is_refused(store):
    """The door the constructor and assignment leave open if it is not shut.
    `given=` on this call names this class's field, on purpose, which is the
    same mistake as assigning to it — and it hears the same sentence rather than
    having the value quietly worked out from under it."""

    def holding_table(write):
        booking = write.record
        return booking.table_id if booking.holding else ""

    @record(table="counting", collection="countings")
    class Counting:
        table_id: str = field(default="")
        holding: bool = field(default=False)
        held_table: str = field(default="", derived=holding_table)

    store.create(Counting)

    with pytest.raises(ValidationError, match="derived"):
        store.countings.add(
            Counting(table_id="t7", holding=True), given={"held_table": "t9"}
        )

    # Refused before anything was written.
    assert store.conn.execute("select count(*) from counting").fetchone()[0] == 0


def test_a_store_default_that_lands_on_a_derived_field_says_nothing(store):
    """The other half, and the one that must stay quiet. A store's `defaults`
    exist to be applied by name to whatever declares a field of it, so a job
    carrying `whom` for every record it writes cannot be broken by one class
    happening to derive a field of that name. The value is not the caller
    naming this field on this write, so it is ignored the way it is for any
    field the handler works out."""

    @record(table="tallying", collection="tallyings")
    class Tallying:
        name: str = field(default="")
        whom: str = field(default="", derived=lambda w: w.record.name.upper())

    store.create(Tallying)
    store.defaults["whom"] = "System import"

    tallying = store.tallyings.add(Tallying(name="rod"))

    assert store.tallyings.by_id(tallying.id).whom == "ROD"


def test_a_derived_field_cannot_hand_back_sql(store):
    """`clock` is fine on `on_add` and cannot be right here. A value the
    statement works out never lands back on the object, so the record would read
    `None` while its row held a time — and a field whose whole point is to be
    true about the record cannot be one the record is wrong about."""

    @record(table="sitting", collection="sittings")
    class Sitting:
        name: str = field()
        seen_at: datetime | None = field(default=None, derived=clock)

    store.create(Sitting)

    with pytest.raises(TypeError) as raised:
        store.sittings.add(Sitting(name="A lyrebird."))

    assert "seen_at" in str(raised.value)
    assert "derived" in str(raised.value)
    # Refused before anything was written, not partway through.
    assert store.conn.execute("select count(*) from sitting").fetchone()[0] == 0


def test_a_write_refused_and_replayed_fills_a_field_once(store, monkeypatch):
    """The reason the filling happens outside the retry. A handler reading the
    record's own state would otherwise see, on the second attempt, what the
    first one left there, and count an attempt DSQL threw away."""
    import psycopg

    from dray import Collection, child

    @record(table="tally", collection="tallies")
    class Tally:
        touched: int = field(default=0, on_save=lambda w: w.record.touched + 1)

    @child(of=Tally, name="marks", table="mark")
    class Mark:
        note: str
        depth: int = field(default=0, on_add=lambda w: w.record.depth + 1)

    store.create(Tally, Mark)
    tally = store.tallies.add(Tally())

    # Refused once, at the last statement in the transaction, so the parent's
    # update and the child's insert are both replayed.
    refused = iter([True])
    real = Collection._insert_children

    def refusing_once(self, batch, prepared):
        sent = real(self, batch, prepared)
        if next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")
        return sent

    monkeypatch.setattr(Collection, "_insert_children", refusing_once)

    tally.marks.add("counted once")
    tally.save()

    written = store.tallies.by_id(tally.id)
    assert written.touched == 1
    assert [(m.note, m.depth) for m in written.marks] == [("counted once", 1)]


def test_a_blob_field_cannot_take_a_value_the_database_works_out(store):
    """`stored_in="blob"` and `on_add=clock` are each documented and each work,
    and together they cannot: the blob is written as one parameter, so there is
    no place in the statement for an expression and nothing to return it from.
    It used to reach the database as `returning seen_at` and come back as
    `column "seen_at" does not exist`, which points at the schema, where there
    is nothing wrong."""

    @record(table="sighting", collection="sightings")
    class Sighting:
        what: str = field()
        seen_at: datetime | None = field(
            default=None, stored_in="blob", on_add=clock
        )

    store.create(Sighting)

    with pytest.raises(TypeError) as raised:
        store.sightings.add(Sighting(what="A lyrebird."))

    assert "seen_at" in str(raised.value)
    assert "blob" in str(raised.value)
    # Refused before anything was written, not partway through.
    assert store.conn.execute("select count(*) from sighting").fetchone()[0] == 0


def test_a_handler_of_your_own_may_hand_back_an_expression(store):
    """`clock` is one expression and not the only one, which the page now says
    out loud — every other test here reaches for `clock`, so nothing was holding
    the caller's half of that: `Sql` exported, and a handler somebody wrote
    returning one of their own."""

    def when_the_day_started(write: Write) -> Sql:
        return Sql("date_trunc('day', clock_timestamp())")

    @record(table="shift", collection="shifts")
    class Shift:
        what: str = field()
        day_started: datetime | None = field(
            default=None, on_add=when_the_day_started
        )

    store.create(Shift)
    shift = store.shifts.add(Shift(what="Morning walk."))

    # Read back with the write, so the record holds the value rather than the
    # text that produced it.
    assert isinstance(shift.day_started, datetime)
    assert shift.day_started.hour == 0
    assert store.shifts.by_id(shift.id).day_started == shift.day_started


def test_a_blob_field_may_still_be_filled_from_python(store):
    """The rule is about `Sql` and not about the blob. A handler that hands back
    a value works on either side of the split, since the value goes onto the
    record and the blob is built from the record."""
    stamp = datetime(2026, 8, 11, 9, 30)

    @record(table="landing", collection="landings")
    class Landing:
        what: str = field()
        seen_at: datetime | None = field(default=None,
            stored_in="blob", on_add=lambda w: stamp
        )

    store.create(Landing)
    landing = store.landings.add(Landing(what="A lyrebird."))

    assert landing.seen_at == stamp
    assert store.landings.by_id(landing.id).seen_at == stamp


def test_a_child_ordered_by_something_it_does_not_declare_is_refused():
    from dray import child

    with pytest.raises(TypeError) as raised:
        @child(of=Walker, name="stamps", table="stamp", order_by="written_at")
        class Stamp:
            body: str = field()

    assert "written_at" in str(raised.value)


def test_a_child_ordered_by_a_blob_field_is_refused():
    """Declared, but with no column to sort on. Caught here rather than at
    every read, which is the only reason to check at declaration at all."""
    from dray import child

    with pytest.raises(TypeError) as raised:
        @child(of=Walker, name="tickets", table="ticket", order_by="written_at")
        class Ticket:
            body: str = field()
            written_at: datetime | None = field(default=None, stored_in="blob")

    assert "blob" in str(raised.value)


def test_finding_the_records_with_nothing_in_a_field(walkers):
    """`= null` is never true, so asking this way used to come back empty —
    which is a wrong answer rather than an empty one."""
    walkers.add(Walker(family_name="Nobody"))
    walkers.add(Walker(family_name="Somebody", suburb="Katoomba", postcode="2780"))

    nowhere = walkers.find(equals={"suburb": None})
    somewhere = walkers.find(equals={"suburb": "Katoomba"})
    assert [w.family_name for w in nowhere] == ["Nobody"]
    assert [w.family_name for w in somewhere] == ["Somebody"]
    assert walkers.count(equals={"suburb": None}) == 1


def test_finding_on_a_column_with_nothing_in_it(store):
    @record(table="visit", collection="visits")
    class Visit:
        name: str = field(default="")
        seen_at: datetime | None = field(default=None)

    store.create(Visit)
    store.visits.add(Visit(name="unseen"))
    store.visits.add(Visit(name="seen", seen_at=datetime(2026, 3, 14, 19, 0)))

    unseen = store.visits.find(equals={"seen_at": None})
    assert [v.name for v in unseen] == ["unseen"]


#
# What a field will take
#


def test_a_value_of_the_wrong_type_is_refused_on_assignment(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    with pytest.raises(ValidationError) as raised:
        walker.family_name = 4
    assert "expected str, got int" in str(raised.value)


def test_parse_refuses_what_a_form_actually_posts(store):
    """The whole point of `parse`: a form and a spreadsheet send strings, and
    that is the moment to hear about it — not three functions later when the
    arithmetic fails, and not never, because PostgreSQL cast it on the way in."""

    @record(table="booking", collection="bookings")
    class Booking:
        party_size: int = field(default=1)
        confirmed: bool = field(default=False)

    with pytest.raises(ValidationError) as raised:
        Booking.parse({"party_size": "4"})
    assert "expected int, got str" in str(raised.value)

    # A bool is an int to `isinstance`, which would make a party of `True`.
    with pytest.raises(ValidationError):
        Booking.parse({"party_size": True})

    assert Booking.parse({"party_size": 4, "confirmed": True}).party_size == 4


def test_a_number_is_taken_where_a_decimal_is_wanted(store):
    @record(table="bill", collection="bills")
    class Bill:
        total: Decimal | None = field(default=None)

    Bill.parse({"total": 40})._dray_validate()
    Bill.parse({"total": Decimal("40.50")})._dray_validate()
    with pytest.raises(ValidationError):
        Bill.parse({"total": "40.50"})


def test_a_row_written_under_an_older_type_still_loads(walkers):
    """Loading does not validate, and that has to keep holding for types too —
    otherwise tightening an annotation makes some of your history unreadable."""
    loaded = Walker._dray_load({"family_name": 4, "data": {"suburb": 7}})
    assert (loaded.family_name, loaded.suburb) == (4, 7)


def test_an_id_of_the_wrong_kind_is_refused(walkers):
    """A router that handed over whatever was in the URL. Refused here rather
    than at the database, which says nothing about where the value came from."""
    with pytest.raises(ValidationError) as raised:
        walkers.by_id(123)
    assert "a UUID or its text, not int" in str(raised.value)


def test_an_id_arrives_from_a_url_as_text(walkers):
    """Everything downstream of a request is a string, so the string form of an
    id has to find the record."""
    walker = walkers.add(Walker(family_name="Hemingway"))
    assert walkers.by_id(str(walker.id)).family_name == "Hemingway"


def test_text_that_is_not_an_id_is_refused_rather_than_reported_missing(walkers):
    """Taking text for a `uuid` key means a short string gets past the type and
    as far as the conversion, and what it raises there matters: a
    `RecordNotFound` would read as an id nobody answers to and send the caller
    looking for a row that was never the problem. A page that passes
    `by_id("k3Jf9")` over a class whose ids are minted `UUID`s is exactly how
    somebody arrives at that string in the first place."""
    with pytest.raises(ValidationError):
        walkers.by_id("k3Jf9")


#
# The guard against a stale write
#


def test_every_record_carries_an_etag(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    assert walker.etag
    assert walkers.by_id(walker.id).etag == walker.etag


def test_a_write_mints_a_new_one(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    first = walker.etag

    walker.status = "volunteer"
    walker.save()

    assert walker.etag != first
    assert walkers.by_id(walker.id).etag == walker.etag


def test_a_save_with_the_etag_it_was_shown_goes_through(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    shown = walker.etag

    walker.status = "volunteer"
    walker.save(etag=shown)

    assert walkers.by_id(walker.id).status == "volunteer"


def test_a_stale_form_is_refused(walkers):
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    shown = walker.etag

    # Somebody else gets there first.
    theirs = walkers.by_id(walker.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    mine = walkers.by_id(walker.id)
    mine.suburb = "Katoomba"
    with pytest.raises(RecordHasChanged):
        mine.save(etag=shown)

    # Theirs stands.
    assert walkers.by_id(walker.id).suburb == "Wentworth Falls"


def test_the_statement_catches_what_the_comparison_cannot(walkers):
    # The in-process check compares against the record in hand. This is the
    # narrower race: somebody commits between our read and our update, so the
    # record in hand still looks current and only the row disagrees.
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    mine = walkers.by_id(walker.id)
    shown = mine.etag

    walkers.conn.execute(
        "update walker set status = %s, etag = %s where id = %s",
        ["lapsed", "somebody-else", walker.id],
    )
    walkers.conn.commit()

    mine.suburb = "Katoomba"
    with pytest.raises(RecordHasChanged):
        mine.save(etag=shown)


def test_an_unguarded_save_still_wins_silently(walkers):
    # Without an etag the last write wins and nothing says so. That is the
    # behaviour the guard exists to opt out of.
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    theirs = walkers.by_id(walker.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    walker.suburb = "Katoomba"
    walker.save()
    assert walkers.by_id(walker.id).suburb == "Katoomba"


def test_an_etag_of_none_is_no_guard_at_all(walkers):
    """A caller reaches `etag=None` by a different road from the one that names
    no etag: `save(etag=form.get("etag"))` on the request whose form came back
    without a token. Both are the unguarded write, and a difference between them
    would only show on the day somebody's form lost it."""
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    theirs = walkers.by_id(walker.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    walker.suburb = "Katoomba"
    walker.save(etag=None)
    assert walkers.by_id(walker.id).suburb == "Katoomba"


def test_a_bulk_save_can_be_guarded_by_what_each_record_carries(walkers):
    """`save(etag=...)` needs the token a reader was shown, because only the
    caller knows what was displayed. A set read and written in one process needs
    nothing: every record already carries the etag it was loaded with, and that
    is the window a bulk edit sits in."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(4)])
    mine = walkers.find()

    # Somebody else writes one of them between this process's read and its write.
    theirs = walkers.by_id(mine[2].id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    for walker in mine:
        walker.status = "volunteer"

    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    assert raised.value.ids == (mine[2].id,)
    # All or nothing within the transaction: the three that could have been
    # written were not, because the one that could not be rolled the rest back.
    assert walkers.count(equals={"status": "volunteer"}) == 0
    assert walkers.by_id(mine[2].id).suburb == "Wentworth Falls"


def test_a_bulk_save_is_unguarded_unless_asked(walkers):
    """A deliberate overwrite is a legitimate thing a batch job does, so the
    guard is opt in — which is the opposite of `save`, where the etag being
    absent is the thing you have to notice."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(3)])
    mine = walkers.find()

    theirs = walkers.by_id(mine[1].id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    for walker in mine:
        walker.status = "volunteer"
    walkers.save_all(mine)

    # Nothing is refused: what somebody else did between this process's read
    # and its write is not this call's business. Nor is it overwritten — the
    # save named `status` and said nothing about the blob, so the blob was not
    # in the statement.
    assert walkers.count(equals={"status": "volunteer"}) == 3
    assert walkers.by_id(mine[1].id).suburb == "Wentworth Falls"


def test_a_guarded_bulk_save_names_every_record_that_moved(walkers):
    """One of these forty is a poor thing to hear forty times, and a caller
    about to re-read wants the list rather than the first name in it."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(5)])
    mine = walkers.find()

    for walker in (mine[0], mine[3]):
        theirs = walkers.by_id(walker.id)
        theirs.suburb = "Katoomba"
        theirs.save()

    for walker in mine:
        walker.status = "volunteer"

    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    assert set(raised.value.ids) == {mine[0].id, mine[3].id}
    assert "2 of 5" in str(raised.value)


def test_a_guarded_bulk_save_says_which_chunk_landed(walkers, monkeypatch):
    """A set above the row ceiling is several transactions, so the chunk holding
    the conflict rolls back and the ones before it do not. Reading a private
    attribute to find that out is what callers were doing."""
    import sys

    # `dray.collection` is the decorator, so the module has to come from
    # `sys.modules` rather than off the package.
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    walkers.add_all([Walker(family_name=f"Walker{n:02}") for n in range(6)])
    mine = sorted(walkers.find(), key=lambda w: w.id)

    # The conflict is in the last pair, so the first two pairs commit first.
    theirs = walkers.by_id(mine[5].id)
    theirs.suburb = "Katoomba"
    theirs.save()

    for walker in mine:
        walker.status = "volunteer"

    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    assert raised.value.ids == (mine[5].id,)
    assert raised.value.written == tuple(w.id for w in mine[:4])
    # Which is exactly what the table holds: four through, two rolled back.
    assert walkers.count(equals={"status": "volunteer"}) == 4


def test_a_clash_partway_through_a_bulk_add_says_which_chunk_landed(
    walkers, monkeypatch
):
    """The guard was the first refusal to need this and never the only one. A
    key clash stops a set exactly as a lost guard does — the chunk holding it
    rolls back, the ones before it are durable — and a caller left holding six
    records and four rows could work out which four only by reading a private
    attribute, which is what `written` exists to stop."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    landed = walkers.add_all(
        [Walker(family_name=f"Walker{n:02}") for n in range(2)]
    )

    # Six more, with the third pair carrying an id that is already taken. The
    # first two pairs are two whole transactions and commit before it is
    # reached.
    coming = [Walker(family_name=f"Later{n:02}") for n in range(6)]
    coming[5] = Walker(family_name="Later05", id=landed[0].id)

    with pytest.raises(DuplicateRecord) as raised:
        walkers.add_all(coming)

    assert raised.value.written == tuple(w.id for w in coming[:4])
    # Which is what the table holds: the original two, plus four of the six.
    assert walkers.count() == 6


def test_a_record_that_has_gone_says_which_chunk_landed(walkers, monkeypatch):
    """And the third of them. `RecordNotFound` on an unguarded `save_all` means
    a row went while this set was being written, and it stops the set in the
    same place and leaves the same question behind."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    walkers.add_all([Walker(family_name=f"Walker{n:02}") for n in range(6)])
    mine = sorted(walkers.find(), key=lambda w: w.id)

    # Removed by somebody else, and in the last pair so the first two commit.
    walkers.by_id(mine[5].id).delete()

    for walker in mine:
        walker.status = "volunteer"

    with pytest.raises(RecordNotFound) as raised:
        walkers.save_all(mine)

    assert raised.value.written == tuple(w.id for w in mine[:4])
    assert walkers.count(equals={"status": "volunteer"}) == 4


def test_a_refusal_with_nothing_behind_it_carries_an_empty_written(walkers):
    """Most of them. A single save, or a set that failed in its first chunk,
    has nothing durable to report — and empty is the answer rather than the
    absence of one, which is the whole reason this sits on `DrayError` rather
    than on the ones a bulk write can raise."""
    walker = walkers.add(Walker(family_name="Hemingway"))

    with pytest.raises(DuplicateRecord) as clashed:
        walkers.add(Walker(family_name="Shelley", id=walker.id))
    assert clashed.value.written == ()

    with pytest.raises(ValidationError) as refused:
        walkers.add(Walker(family_name="Shelley", status="unknown"))
    assert refused.value.written == ()


def test_a_set_inside_a_block_says_nothing_landed_because_nothing_did(
    store, monkeypatch
):
    """Inside a `store.transaction()` every chunk joins the caller's
    transaction, so a set that stopped partway landed none of itself and the
    keys of the chunks that "committed" name rows that rolled back — the one
    answer worse than no answer. It is reachable because the split and the
    in-block refusal count different things on purpose: the refusal counts
    rows, the split divides by the worst record's fanout, so a set small
    enough to be allowed in a block can still be several chunks."""
    import sys

    @record(table="crate", collection="crates")
    class Crate:
        label: str = field()

    @child(of=Crate, name="slots", table="slot")
    class Slot:
        note: str = field(default="")

    store.create(Crate, Slot)
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 8)

    first = store.crates.add(Crate(label="already here"))

    # Four records is two chunks, because the worst of them carries three.
    coming = [Crate(label=f"c{n}") for n in range(4)]
    for note in ("a", "b", "c"):
        coming[0].slots.add(note=note)
    coming[3] = Crate(label="clash", id=first.id)

    with pytest.raises(DuplicateRecord) as raised:
        with store.transaction():
            store.crates.add_all(coming)

    assert raised.value.written == ()
    # Which is the truth: the block rolled back and only the earlier row is
    # there. Filled per chunk, this named two records that do not exist.
    assert store.crates.count() == 1


def test_every_exception_dray_raises_can_be_asked_what_landed():
    """Asked of the hierarchy rather than of a list, because a list is exactly
    what this is meant to make unnecessary — a caller writes `except
    dray.DrayError as refused` and reads `refused.written` without knowing
    which arrived, and a written-out enumeration here would go stale the first
    time an exception is added and could never fail anyway, since the default
    is inherited. What it does catch is one of them shadowing the name or
    taking an `__init__` that drops it, which is a mistake somebody can make
    without noticing."""

    def everything(cls):
        for kind in cls.__subclasses__():
            yield kind
            yield from everything(kind)

    found = set(everything(DrayError))
    # The eight the manual names all arrive through the import above, so an
    # empty walk would mean this test had stopped testing anything.
    assert len(found) >= 8

    for kind in found:
        assert kind("said no").written == (), kind.__name__


def test_a_guarded_save_of_one_record_reads_the_same(walkers):
    """`save(etag=...)` goes down the same path, so it gets the same exception
    carrying one id — and the message stays the sentence it always was."""
    walker = walkers.add(Walker(family_name="Hemingway"))
    shown = walker.etag

    theirs = walkers.by_id(walker.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    with pytest.raises(RecordHasChanged) as raised:
        walker.save(etag=shown)
    assert raised.value.ids == (walker.id,)
    assert str(raised.value) == "this Walker was changed by someone else"


def test_a_refused_save_hands_back_the_record_as_the_table_now_has_it(walkers):
    """`RecordHasChanged` named the ids and nothing else, so every recovery went
    back for the rows — and the shape the page taught was `except
    RecordHasChanged:` and then a read. The one statement that answers is sent
    from inside the transaction that found out, on the path that was already
    raising."""
    walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    mine = walkers.find()

    theirs = walkers.by_id(mine[0].id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    mine[0].status = "volunteer"
    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    (moved,) = raised.value.records
    assert isinstance(moved, Walker)
    assert moved.id == mine[0].id
    assert moved.suburb == "Wentworth Falls"
    assert moved.etag == theirs.etag
    # A record and not a row, so it is attached and can be asked anything a
    # record answers.
    assert moved.status == "enquiry"


def test_a_record_that_has_gone_is_an_id_with_no_record_beside_it(walkers):
    """The commonest collision in a booking domain is a cancellation racing a
    seating: the guest cancels from their phone, the row goes, and the host at
    the podium was told somebody had changed the booking. A guarded save answers
    for a deletion because a row that is no longer there is the furthest a row
    can have changed — but the sentence and the payload have to say which."""
    walker = walkers.add(Walker(family_name="Hemingway"))
    shown = walker.etag
    walkers.by_id(walker.id).delete()

    walker.status = "volunteer"
    with pytest.raises(RecordHasChanged) as raised:
        walker.save(etag=shown)

    assert raised.value.ids == (walker.id,)
    assert raised.value.records == ()
    assert str(raised.value) == "this Walker was removed by someone else"


def test_a_form_refused_before_the_write_has_nothing_to_carry(walkers):
    """`save(etag=...)` compares the token a reader was shown against the record
    in hand before it sends a statement, and a refusal there has asked the
    database nothing. Empty rather than a read dray went and invented: the
    record and its id are both already in the caller's hand at that point."""
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))
    shown = walker.etag

    theirs = walkers.by_id(walker.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save()

    mine = walkers.by_id(walker.id)
    mine.suburb = "Katoomba"
    with pytest.raises(RecordHasChanged) as raised:
        mine.save(etag=shown)

    assert raised.value.ids == ()
    assert raised.value.records == ()


def test_a_batch_says_which_of_them_moved_and_which_of_them_went(walkers):
    """Both kinds in one refusal, which is the case that makes a second
    exception the wrong answer: there is one guard, it was lost twice over, and
    `records` is what tells the two apart."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(4)])
    mine = walkers.find()

    theirs = walkers.by_id(mine[1].id)
    theirs.suburb = "Katoomba"
    theirs.save()
    walkers.by_id(mine[2].id).delete()

    for walker in mine:
        walker.status = "volunteer"

    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    # `records` follows `ids`, so the pair line up for a caller walking them.
    assert raised.value.ids == (mine[1].id, mine[2].id)
    assert [each.id for each in raised.value.records] == [mine[1].id]
    assert raised.value.records[0].suburb == "Katoomba"
    assert (
        str(raised.value)
        == "2 of 4 Walker records were changed or removed by someone else"
    )


def test_a_guarded_save_that_goes_through_sends_no_extra_statement(walkers):
    """The read belongs to the refusal and nowhere else. A write that lands must
    not pay a statement for a recovery it is not having."""
    walkers.add_all([Walker(family_name=f"Walker{n}") for n in range(3)])
    mine = walkers.find()
    for walker in mine:
        walker.status = "volunteer"

    with walkers.store.watching() as seen:
        walkers.save_all(mine, guarded=True)

    assert [span.sql.split()[0] for span in seen] == ["update"] * 3


def test_a_lookup_that_fails_is_still_a_refused_write(walkers, monkeypatch):
    """The read sits inside the transaction `@retrying` owns, and that replays
    on `SerializationFailure`. Let out raw it would spend five attempts and then
    report a commit DSQL never refused — sending somebody who has lost a write
    off to run the whole thing again."""
    walkers.add(Walker(family_name="Hemingway"))
    mine = walkers.find()

    theirs = walkers.by_id(mine[0].id)
    theirs.suburb = "Katoomba"
    theirs.save()

    def broken(self, ids):
        raise psycopg.errors.SerializationFailure("the read conflicted")

    monkeypatch.setattr(type(walkers), "_as_stored", broken)

    mine[0].status = "volunteer"
    with pytest.raises(RecordHasChanged) as raised:
        walkers.save_all(mine, guarded=True)

    assert raised.value.ids == (mine[0].id,)
    assert raised.value.records == ()
    assert "could not be read" in str(raised.value)


def test_a_clashing_child_does_not_cost_a_guarded_refusal_its_records(store):
    """An id in `ids` with no record among `records` is how a caller is told
    the row has gone, so a refusal that cannot read is a refusal that lies
    about a row somebody merely edited. Sending the queued children with their
    parents put a statement that can fail ahead of that read: a child that
    clashed killed the transaction the read had to happen in, and the whole
    conflict came back carrying nothing."""

    @record(table="lodge", collection="lodges")
    class Lodge:
        name: str = field()

    @child(
        of=Lodge,
        name="beds",
        table="bed",
        collection="beds",
        indexes=[index("room", unique=True)],
    )
    class Bed:
        room: str = field(default="")

    store.create(Lodge, Bed)
    one = store.lodges.add(Lodge(name="one"))
    two = store.lodges.add(Lodge(name="two"))
    two.beds.add(room="room 1")
    two.save()

    # Somebody else between this process's read and its write, which is the
    # window `guarded=True` is for.
    theirs = store.lodges.by_id(one.id)
    theirs.name = "theirs"
    theirs.save()

    one.name = "one again"
    two.name = "two again"
    two.beds.add(room="room 1")

    with pytest.raises(RecordHasChanged) as raised:
        store.lodges.save_all([one, two], guarded=True)

    assert raised.value.ids == (one.id,)
    assert [each.name for each in raised.value.records] == ["theirs"]


def test_a_guarded_save_that_goes_through_still_writes_its_children(store):
    """The other half of the same shape. A guarded write holds its children
    back until the guards have answered, and holding them back must not mean
    dropping them."""

    @record(table="depot", collection="depots")
    class Depot:
        name: str = field()

    @child(of=Depot, name="crates", table="crate", collection="crates")
    class Crate:
        label: str = field(default="")

    store.create(Depot, Crate)
    depot = store.depots.add(Depot(name="Bourke Street"))

    depot.name = "Bourke St"
    depot.crates.add(label="one")
    depot.crates.add(label="two")
    store.depots.save_all([depot], guarded=True)

    # Sorted, because an unordered `find` is the table's order to choose and
    # what is being asked here is only whether both crates are in it.
    assert sorted(each.label for each in depot.crates.find()) == ["one", "two"]


def test_a_missing_record_is_not_reported_as_a_conflict(walkers):
    walker = walkers.add(Walker(family_name="Hemingway"))
    walker.delete()

    walker.status = "volunteer"
    with pytest.raises(RecordNotFound):
        walker.save()


def test_deleting_a_record_that_is_already_gone_says_so(walkers):
    """The same pair asked the other way round. A save of a removed record
    raised and a delete of one returned `None`, so a cancellation submitted
    twice — by the guest and then by the host — read as two removals. Somebody
    building on dray hit exactly that and wrote a status check of its own
    rather than reading the silence as the record layer's."""
    walker = walkers.add(Walker(family_name="Hemingway"))
    theirs = walkers.by_id(walker.id)
    walker.delete()

    # The other object holding the same row says it too: it is the row that
    # decides, not which copy of the record did the asking.
    with pytest.raises(RecordNotFound) as raised:
        theirs.delete()
    assert str(raised.value) == f"no Walker {walker.id!r} to delete"


def test_a_record_cannot_declare_its_own_etag():
    with pytest.raises(TypeError) as raised:
        @record(table="clash", collection="clashes")
        class Clash:
            etag: str = field(default="")

    assert "etag" in str(raised.value)


#
# How much of the row a save writes
#
# The section above is two writers racing. This is the quieter one: one thread,
# two objects for the same row, and a save that wrote every column whatever
# moved — so a caller setting one flag put back everything else the object had
# read minutes earlier, over the top of whoever had written it since.
#


def _updates(monkeypatch) -> list[str]:
    """Every `update` statement sent after this call, in order.

    What a save leaves out is only visible here. A round trip cannot tell a
    column that was left alone from one written back with the value it already
    held — which is the whole difficulty, since the two differ only when
    somebody else is writing too."""
    ran: list[str] = []
    real = psycopg.Cursor.execute

    def watching(self, statement, params=None, **rest):
        if statement.startswith("update "):
            ran.append(statement)
        return real(self, statement, params, **rest)

    monkeypatch.setattr(psycopg.Cursor, "execute", watching)
    return ran


def test_a_save_leaves_alone_a_column_it_was_never_told_about(walkers):
    """The write it reverts is one nobody in the call stack ever mentioned, and
    no exception is raised and nothing is logged. Silent both ways is what
    makes it worth pinning: the lesson a caller draws from losing data once is
    to re-read at the top of every mutating function, which is a rule dray
    should never have taught anybody."""
    walker = walkers.add(Walker(family_name="Hemingway"))

    held = walkers.by_id(walker.id)      # the caller's
    other = walkers.by_id(walker.id)     # a helper's, read before that

    other.given_names = "Ernest"
    other.save()

    held.status = "volunteer"            # one unrelated field
    held.save()

    again = walkers.by_id(walker.id)
    assert (again.given_names, again.status) == ("Ernest", "volunteer")


def test_a_save_writes_the_columns_that_moved_and_no_others(
    walkers, monkeypatch
):
    """Against the statement rather than a round trip, because that is the only
    place the difference shows. What used to go over was every column: `id` set
    to itself, `family_name` and the whole document rewritten from what the
    object read minutes ago, and one field actually moving."""
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))

    ran = _updates(monkeypatch)
    held = walkers.by_id(walker.id)
    held.status = "volunteer"
    held.save()

    assert ran == [
        "update walker set status = %s, updated_at = clock_timestamp(), "
        "etag = %s where id = %s returning updated_at"
    ]


def test_a_save_writes_what_the_write_was_told_as_an_add_does(postgresql):
    """The store's `defaults` reach a field through `object.__setattr__`, so
    they enter neither the set of what was said nor what a handler filled. A
    column list built from those two alone leaves them off the statement, and
    the write silently loses the value the defaults exist to stamp — which a
    test asserting only that a narrow save is narrow will not catch."""
    from dray import Store

    store = Store(postgresql, defaults={"whom": "rod"})

    @record(table="ticket", collection="tickets")
    class Ticket:
        subject: str = field(default="")
        whom: str | None = field(default=None)

    store.create(Ticket)
    ticket = store.tickets.add(Ticket(subject="the roof"))
    assert store.tickets.by_id(ticket.id).whom == "rod"

    held = store.tickets.by_id(ticket.id)
    held.subject = "the gutter"
    held.save(given={"whom": "jo"})
    assert store.tickets.by_id(ticket.id).whom == "jo"


def test_a_save_that_moved_no_blob_field_leaves_the_document_alone(
    walkers, monkeypatch
):
    """The other half of what this costs. A domain whose descriptions run to
    several pages was rewriting the largest field in the row every time
    somebody marked a job done, which is the commonest write in the system."""
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))

    ran = _updates(monkeypatch)
    held = walkers.by_id(walker.id)
    held.status = "volunteer"
    held.save()

    assert "data" not in ran[0]
    assert walkers.by_id(walker.id).suburb == "Leura"


def test_a_blob_field_moving_sends_the_whole_document(walkers):
    """The narrowing stops at the column, and this is the cost of that rather
    than an oversight. The blob is one jsonb parameter, so it goes whole or not
    at all, and fields sharing it keep exactly the behaviour every column had
    before. Merging with `data = data || %s` would narrow inside the document
    and has to answer what removing a key means; nobody has asked for it."""
    walker = walkers.add(Walker(family_name="Hemingway", suburb="Leura"))

    held = walkers.by_id(walker.id)
    other = walkers.by_id(walker.id)

    other.postcode = "2780"
    other.save()

    held.suburb = "Katoomba"
    held.save()

    again = walkers.by_id(walker.id)
    assert (again.suburb, again.postcode) == ("Katoomba", None)


def test_a_save_of_a_record_nobody_touched_still_writes_the_row(walkers):
    """Hydrating a row empties the set of what was said, so a record read back
    and saved with nothing assigned narrows to the etag and whatever the
    handlers filled — and it is still a write. `save` means the row was
    touched: `updated_at` moves and anything watching for movement sees it. A
    caller who wants that conditional writes their own `save` and calls
    `_dray_save` when they decide to."""
    walker = walkers.add(Walker(family_name="Hemingway"))

    held = walkers.by_id(walker.id)
    held.save()

    again = walkers.by_id(walker.id)
    assert again.etag == held.etag != walker.etag
    assert again.updated_at > walker.updated_at


#
# Whether a record is editable is the application's decision
#


def test_a_class_that_writes_its_own_save_keeps_it(store):
    @record(table="carved", collection="carveds")
    class Carved:
        body: str = field(default="")

        def save(self, **assigned):
            raise TypeError("a Carved is written or deleted, never edited")

    store.create(Carved)
    carved = store.carveds.add(Carved(body="as written"))

    with pytest.raises(TypeError):
        carved.save()

    # Everything dray did not have to stand aside for still works.
    assert store.carveds.by_id(carved.id).body == "as written"
    carved.delete()


#
# What a field makes of a value before looking at it
#


def test_a_converter_takes_what_a_form_posts(store):
    """The other half of `parse` refusing a string for an int. dray never
    guesses a conversion; a field says exactly what one is."""

    @record(table="booking2", collection="bookings2")
    class Booking2:
        party_size: int = field(default=1, converter=int)
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )

    posted = Booking2.parse({"party_size": "4", "email": "  Rod@Example.COM "})
    assert (posted.party_size, posted.email) == (4, "rod@example.com")

    # And on assignment, which is the other door outside data comes through.
    posted.party_size = "6"
    assert posted.party_size == 6


def test_a_converter_that_cannot_names_the_field(store):
    @record(table="booking3", collection="bookings3")
    class Booking3:
        party_size: int = field(default=1, converter=int)

    with pytest.raises(ValidationError) as raised:
        Booking3.parse({"party_size": "four"})
    assert "party_size" in str(raised.value)


def test_a_converter_leaves_none_alone(store):
    @record(table="booking4", collection="bookings4")
    class Booking4:
        party_size: int | None = field(default=None, converter=int)

    assert Booking4.parse({"party_size": None}).party_size is None


def test_loading_a_row_does_not_convert(store):
    """`load` is a row this table already holds, and stays as lenient as it was
    — otherwise a converter added this year rewrites what was read last year."""

    @record(table="booking5", collection="bookings5")
    class Booking5:
        party_size: int = field(default=1, converter=int)

    assert Booking5._dray_load({"party_size": "4"}).party_size == "4"


def test_a_record_your_own_code_builds_is_converted(store):
    """The half that was quietly missing. A service layer builds its records
    with the constructor — that is what a constructor is for — so a converter
    that only ran for `parse` did nothing for every record the application
    made itself, and an application whose whole write path was `Guest(...)`
    got no normalising at all."""

    @record(table="guest1", collection="guests1")
    class Guest1:
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )
        party_size: int = field(default=1, converter=int)

    built = Guest1(email="  Amrita.Sen@Example.COM ", party_size="4")

    assert built.email == "amrita.sen@example.com"
    assert built.party_size == 4


def test_a_converter_runs_on_a_positional_argument_too(store):
    """Positionals are matched to names by the same `dataclasses.fields`
    order the dataclass builds `__init__` from, so a value is converted by the
    rules of the field it is actually landing on."""
    @record(table="guest2", collection="guests2")
    class Guest2:
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )

    assert Guest2("  ROD@example.com ").email == "rod@example.com"


def test_a_filter_is_converted_the_same_way_the_value_was(store):
    """The half that produced wrong data. The field somebody normalises is the
    one they normalised *in order to look it up*, so a filter that skipped the
    converter could not find what the write had put there — and said nothing
    about it."""

    @record(table="guest3", collection="guests3")
    class Guest3:
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )
        suburb: str | None = field(default=None,
            stored_in="blob", converter=lambda v: v.strip().title()
        )

    store.create(Guest3)
    store.guests3.add(Guest3(email="rod@example.com", suburb="katoomba"))

    assert store.guests3.find(equals={"email": "  ROD@Example.COM "})
    assert store.guests3.find(equals={"suburb": "  katoomba "})
    assert store.guests3.count(equals={"email": "ROD@EXAMPLE.COM"}) == 1


def test_any_of_converts_every_value_it_was_given(store):
    """Every member, not the first — `any_of` is several values where a filter
    takes one, and each of them is a value the field has rules about."""
    @record(table="guest4", collection="guests4")
    class Guest4:
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )

    store.create(Guest4)
    store.guests4.add_all(
        [Guest4(email="rod@example.com"), Guest4(email="amrita@example.com")]
    )

    found = store.guests4.find(
        equals={"email": any_of("ROD@Example.COM", "  Amrita@EXAMPLE.com")}
    )

    assert len(found) == 2


def test_a_filter_a_converter_will_not_take_names_the_field(store):
    """A query is a door like any other, so a value it cannot make sense of is
    refused here rather than sent to the database to be compared as text."""

    @record(table="guest5", collection="guests5")
    class Guest5:
        party_size: int = field(default=1, converter=int)

    store.create(Guest5)

    with pytest.raises(ValidationError, match="party_size"):
        store.guests5.find(equals={"party_size": "four"})


def test_a_converter_runs_on_a_later_positional_too(store):
    """The first argument proves nothing about the mapping. If index and name
    ever came apart, a value would be converted by another field's rules and
    stored wrong with nothing said — so the test is a field whose converter
    would be visibly wrong applied anywhere else."""

    @record(table="guest6", collection="guests6")
    class Guest6:
        family_name: str = field(default="", converter=lambda v: v.strip())
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )
        party_size: int = field(default=1, converter=int)

    built = Guest6("  Hemingway  ", "  ROD@Example.COM  ", "4")

    assert built.family_name == "Hemingway"
    assert built.email == "rod@example.com"
    assert built.party_size == 4


def test_a_child_set_filters_what_is_queued_the_way_it_filters_what_is_stored(
    store,
):
    """`find` and `count` on a child set answer over two halves — the rows, and
    what is queued for the next save — and promise the same answer either side
    of it. The stored half goes through `_conditions` and is converted, so the
    queued half has to be too, or one filter gets two answers depending on
    whether anybody has saved yet."""

    @record(table="host", collection="hosts")
    class Host:
        name: str = field(default="")

    @child(of=Host, name="notes", table="host_note")
    class HostNote:
        body: str = field(default="")
        kind: str = field(default="", converter=lambda v: v.strip().lower())

    store.create(Host, HostNote)
    host = store.hosts.add(Host(name="Hemingway"))
    host.notes.add(body="rang about the booking", kind="  PHONE ")

    def asked() -> tuple[int, int]:
        return (
            len(host.notes.find(equals={"kind": "  PHONE "})),
            host.notes.count(equals={"kind": "  PHONE "}),
        )

    queued = asked()
    host.save()
    stored = asked()

    assert queued == (1, 1)
    assert queued == stored


def test_a_child_set_answers_any_of_the_same_way_either_side_of_a_save(store):
    """`find` on a child set answers over two halves, so every filter shape it
    accepts has to work on both. `any_of` is a tuple of values where the others
    take one — handed to a converter it is not a value, and compared for
    equality it is never equal to the one thing inside it."""

    @record(table="host2", collection="hosts2")
    class Host2:
        name: str = field(default="")

    @child(of=Host2, name="notes", table="host_note2")
    class HostNote2:
        body: str = field(default="")
        kind: str = field(default="", converter=lambda v: v.strip().lower())

    store.create(Host2, HostNote2)
    host = store.hosts2.add(Host2(name="Hemingway"))
    host.notes.add(body="rang", kind="  PHONE ")
    host.notes.add(body="wrote", kind="LETTER")

    wanted = any_of("phone", "  EMAIL ")
    asked = {"kind": wanted}
    queued = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))
    host.save()

    stored = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))

    assert queued == (1, 1)
    assert queued == stored


def test_nothing_inside_any_of_matches_nothing_on_either_side(store):
    """`= any(%s)` is equality against each member and nothing equals NULL, so
    a `None` in there matches no row. Python's `in` would match a child holding
    `None`, which would make one filter answer differently before and after a
    save — so the memory half follows the rows rather than the other way
    about. `find(equals={"x": None})` is the spelling that means it, on both
    sides."""

    @record(table="host4", collection="hosts4")
    class Host4:
        name: str = field(default="")

    @child(of=Host4, name="notes", table="host_note4")
    class HostNote4:
        body: str = field(default="")
        room: str | None = field(default=None, stored_in="blob")

    store.create(Host4, HostNote4)
    host = store.hosts4.add(Host4(name="Hemingway"))
    host.notes.add(body="no room given", room=None)
    host.notes.add(body="in the attic", room="Attic")

    wanted = any_of(None, "Attic")
    asked = {"room": wanted}
    queued = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))
    host.save()
    stored = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))

    assert queued == (1, 1), "the attic one, and not the one holding nothing"
    assert queued == stored
    # And the way to ask for the other one, which works either side too.
    assert len(host.notes.find(equals={"room": None})) == 1


def test_a_child_set_answers_none_of_the_same_way_either_side_of_a_save(store):
    """The half of `none_of` that a statement cannot be trusted to carry on its
    own: the queued children are matched in Python, and Python's `!=` says
    nothing about a child holding `None` unless somebody writes the branch. The
    two halves have drifted three times before and every drift was a filter that
    answered differently depending on whether anybody had saved yet."""

    @record(table="host5", collection="hosts5")
    class Host5:
        name: str = field(default="")

    @child(of=Host5, name="notes", table="host_note5")
    class HostNote5:
        body: str = field(default="")
        kind: str | None = field(
            default=None, converter=lambda v: v and v.strip().lower()
        )
        room: str | None = field(default=None, stored_in="blob")

    store.create(Host5, HostNote5)
    host = store.hosts5.add(Host5(name="Hemingway"))
    host.notes.add(body="rang", kind="  PHONE ", room="Attic")
    host.notes.add(body="wrote", kind="LETTER", room="Study")
    host.notes.add(body="unrecorded", kind=None, room=None)

    asked = {"kind": none_of("  PHONE ")}
    queued = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))
    inside = {"room": none_of("Attic")}
    queued_blob = len(host.notes.find(equals=inside))
    host.save()

    stored = (len(host.notes.find(equals=asked)), host.notes.count(equals=asked))

    assert queued == (2, 2), "the letter, and the one that never answered"
    assert queued == stored
    assert queued_blob == len(host.notes.find(equals=inside)) == 2


def test_a_child_set_refuses_a_field_it_does_not_declare(store):
    """Either side of a save, and on a parent that has never been written. A
    dropped condition matches everything, so a typo that used to find nothing
    would have started finding the lot."""

    @record(table="host3", collection="hosts3")
    class Host3:
        name: str = field(default="")

    @child(of=Host3, name="notes", table="host_note3")
    class HostNote3:
        body: str = field(default="")

    store.create(Host3, HostNote3)

    unsaved = Host3(name="Hemingway")
    unsaved.notes.add(body="queued against a record with no row")
    with pytest.raises(ValidationError, match="no field 'nope'"):
        unsaved.notes.find(equals={"nope": "x"})

    host = store.hosts3.add(unsaved)
    with pytest.raises(ValidationError, match="no field 'nope'"):
        host.notes.count(equals={"nope": "x"})


def test_filtering_for_nothing_still_means_is_null_on_a_converted_field(store):
    """A guard on `convert`'s `None` exemption rather than on the filter path,
    and it would pass without the filter converting at all — `None` went
    through untouched before. What changed is that the exemption became
    load-bearing: a filter now reaches the converter, so removing it would take
    `find(equals={"x": None})` with it, and this is what would notice."""

    @record(table="guest7", collection="guests7")
    class Guest7:
        email: str | None = field(
            default=None, converter=lambda v: v.strip().lower()
        )
        suburb: str | None = field(default=None,
            stored_in="blob", converter=lambda v: v.strip().title()
        )

    store.create(Guest7)
    store.guests7.add(Guest7(email="rod@example.com", suburb="katoomba"))
    store.guests7.add(Guest7())

    assert len(store.guests7.find(equals={"email": None})) == 1
    assert len(store.guests7.find(equals={"suburb": None})) == 1


def test_a_converter_is_one_function_not_a_list(store):
    with pytest.raises(TypeError) as raised:
        @record(table="booking6", collection="bookings6")
        class Booking6:
            party_size: int = field(default=1, converter=[int, str])

    assert "one function" in str(raised.value)


def test_a_converter_complaining_about_the_value_names_the_field(store):
    """The four ways a short converter says it was handed the wrong thing.
    A bug inside the converter is left alone, because a message about somebody's
    data would hide it."""
    codes = {"AU": "Australia"}

    @record(table="booking7", collection="bookings7")
    class Booking7:
        size: int | None = field(
            default=None, converter=int
        )               # ValueError
        starts: str | None = field(
            default=None, converter=lambda v: v.upper()
        )  # AttributeError
        country: str | None = field(
            default=None, converter=lambda v: codes[v]
        )   # KeyError

    for name, value in (("size", "four"), ("starts", 7), ("country", "NZ")):
        with pytest.raises(ValidationError) as raised:
            Booking7.parse({name: value})
        assert name in str(raised.value)

    # A converter that is simply broken is not a rejected value.
    @record(table="booking8", collection="bookings8")
    class Booking8:
        size: int | None = field(
            default=None, converter=lambda v: undefined_name(v)
        )  # `undefined_name` is undefined on purpose

    with pytest.raises(NameError):
        Booking8.parse({"size": 4})


def test_a_converter_runs_on_what_a_write_handler_returned(store):
    """A handler chooses which value; the field says what shape it takes. Which
    is why `whoever` can hand back whatever the application calls a person."""

    class User:
        def __init__(self, username: str) -> None:
            self.username = username

        def __str__(self) -> str:
            return self.username

    @record(table="signature2", collection="signatures2")
    class Signature2:
        made_at: datetime | None = field(default=None, on_add=clock)
        made_by: str | None = field(default=None,
            on_add=lambda w: w.given.get("whom"), converter=str
        )

    store.create(Signature2)
    signed = store.signatures2.add(Signature2(), given={"whom": User("rod")})

    assert signed.made_by == "rod"
    assert store.signatures2.by_id(signed.id).made_by == "rod"
    # `Sql` is exempt: a converter would make nonsense of clock_timestamp().
    assert signed.made_at is not None


def test_a_field_holding_another_record_s_id_takes_one_or_its_text(store):
    """`as_uuid` was dray's own key converter and was not exported, so the
    manual told people to write `converter=UUID` for a field pointing at
    another record. That takes the string off the form and refuses the
    ordinary case: `UUID(a_uuid)` raises, so `Booking9(host_id=host.id)` came
    back as `'UUID' object has no attribute 'replace'` — a message naming
    nothing the caller can act on, about the commonest way such a field is
    filled."""
    from uuid import UUID, uuid4

    @record(table="booking9", collection="bookings9")
    class Booking9:
        host_id: UUID | None = field(default=None, converter=as_uuid)

    host_id = uuid4()

    assert Booking9(host_id=str(host_id)).host_id == host_id
    assert Booking9(host_id=host_id).host_id == host_id

    # And what `UUID` would have said `'int' object has no attribute
    # 'replace'` about.
    with pytest.raises(ValidationError) as raised:
        Booking9(host_id=7)
    assert "host_id" in str(raised.value)


def test_finding_records_matching_any_of_several_values(store):
    """The commonest thing `find` could not do. A lifecycle has a list of live
    statuses and a page has a list of ids, and both were a hand-written
    statement or a loop of round trips."""

    @record(table="job", collection="jobs")
    class Job:
        status: str = field(default="queued")
        crew: str | None = field(default=None, stored_in="blob")

    store.create(Job)
    for status, crew in (
        ("queued", "red"),
        ("running", "blue"),
        ("done", "red"),
        ("failed", "green"),
    ):
        store.jobs.add(Job(status=status, crew=crew))

    live = store.jobs.find(equals={"status": any_of("queued", "running")})
    assert sorted(job.status for job in live) == ["queued", "running"]

    # Loose or in one iterable, and the same either way.
    assert (
        store.jobs.count(equals={"status": any_of(["queued", "running"])}) == 2
    )

    # Inside the blob, where each value goes over as jsonb exactly as a single
    # one would.
    assert store.jobs.count(equals={"crew": any_of("red", "green")}) == 3

    # And it composes with an ordinary equality, because it is still equality.
    assert (
        store.jobs.count(
            equals={"status": any_of("queued", "done"), "crew": "red"}
        )
        == 2
    )


def test_finding_by_a_list_of_ids(store):
    """Forty records by id was forty round trips. The same shape as a DynamoDB
    `BatchGetItem`, and reached for as constantly."""

    @record(table="parcel2", collection="parcels2")
    class Parcel:
        label: str = field(default="")

    store.create(Parcel)
    written = store.parcels2.add_all([Parcel(label=str(n)) for n in range(5)])
    wanted = [parcel.id for parcel in written[:3]]

    found = store.parcels2.find(equals={"id": any_of(wanted)})
    assert sorted(p.label for p in found) == ["0", "1", "2"]


def test_any_of_nothing_matches_nothing(store):
    """What `= any('{}')` does, and the right reading of "in an empty set". A
    caller whose list turned out to be empty wants no rows — not every row, and
    not an exception to write a special case around."""

    @record(table="crate", collection="crates")
    class Crate:
        label: str = field(default="")
        colour: str | None = field(default=None, stored_in="blob")

    store.create(Crate)
    store.crates.add(Crate(label="one", colour="red"))

    assert store.crates.find(equals={"label": any_of()}) == []
    assert store.crates.find(equals={"colour": any_of([])}) == []
    assert store.crates.count(equals={"label": any_of()}) == 0


def test_a_bare_list_still_means_equal_to_that_list(store):
    """Which is why `any_of` had to be a value rather than a bare list: the two
    readings sit on the same call site and only the annotation tells them
    apart, and nothing at the call site shows the annotation."""

    @record(table="hamper", collection="hampers")
    class Hamper:
        tags: list | None = field(default=None)

    store.create(Hamper)
    store.hampers.add(Hamper(tags=["fragile", "heavy"]))

    assert len(store.hampers.find(equals={"tags": ["fragile", "heavy"]})) == 1
    assert store.hampers.find(equals={"tags": ["fragile"]}) == []
    assert len(
        store.hampers.find(equals={"tags": any_of(["fragile", "heavy"], ["light"])})
    ) == 1


def test_finding_records_matching_none_of_several_values(store):
    """"Everything except cancelled and no-show" had to be written positively,
    as `any_of` with the live states listed out — correct on the day it was
    written and silently wrong the day somebody added a state, at every call
    site that was not updated and with nothing to grep for."""

    @record(table="job2", collection="jobs2")
    class Job:
        status: str = field(default="queued")
        crew: str | None = field(default=None, stored_in="blob")

    store.create(Job)
    for status, crew in (
        ("queued", "red"),
        ("running", "blue"),
        ("done", "red"),
        ("failed", "green"),
    ):
        store.jobs2.add(Job(status=status, crew=crew))

    live = store.jobs2.find(equals={"status": none_of("done", "failed")})
    assert sorted(job.status for job in live) == ["queued", "running"]

    # Loose or in one iterable, and inside the blob each value goes over as
    # jsonb exactly as a single one would.
    assert store.jobs2.count(equals={"crew": none_of(["red"])}) == 2

    # It composes with an ordinary equality and with its opposite, since all
    # three describe one field each.
    assert (
        store.jobs2.count(
            equals={"status": none_of("done"), "crew": any_of("red", "blue")}
        )
        == 2
    )

    # Every read that takes a filter takes this one.
    assert store.jobs2.find_first(
        equals={"status": none_of("queued", "running", "done")}
    ).status == "failed"
    walk = store.jobs2.in_batches(of=2, equals={"status": none_of("done")})
    assert [len(batch) for batch in walk] == [2, 1]


def test_a_record_that_never_answered_is_none_of_the_values(store):
    """The decision `none_of` exists to make, and the reason it is a helper
    rather than three words of SQL. `status <> all(...)` drops the row holding
    nothing — not because it is cancelled but because `null <> 'cancelled'` is
    unknown — so the row that vanishes is exactly the one where the question was
    never answered. `equals` describes a row and `None` in a filter already
    means *unset*, so a booking nobody has decided about is a booking whose
    status is none of these."""

    @record(table="booking2", collection="bookings2")
    class Booking:
        status: str | None = field(default=None)
        table_id: str | None = field(default=None, stored_in="blob")

    store.create(Booking)
    store.bookings2.add(Booking(status="cancelled", table_id="t1"))
    store.bookings2.add(Booking(status="seated", table_id="t2"))
    store.bookings2.add(Booking(status=None, table_id=None))

    live = store.bookings2.find(equals={"status": none_of("cancelled")})
    assert sorted(str(booking.status) for booking in live) == ["None", "seated"]

    # The blob side agrees, and it is the sharper case: a field holding `None`
    # is left out of the document entirely, so the key is absent rather than
    # null and both readings have to be caught.
    assert store.bookings2.count(equals={"table_id": none_of("t1")}) == 2

    # And the way to ask the other question is unchanged.
    assert store.bookings2.count(equals={"status": None}) == 1


def test_none_of_nothing_matches_everything(store):
    """The mirror of `any_of()` matching nothing, and the same reading of the
    same empty list — so a list that came back empty asks for no rows one way
    round and every row the other. Both are right and the pair is worth
    knowing about where the list arrives from outside."""

    @record(table="crate2", collection="crates2")
    class Crate:
        label: str = field(default="")
        colour: str | None = field(default=None, stored_in="blob")

    store.create(Crate)
    store.crates2.add(Crate(label="one", colour="red"))
    store.crates2.add(Crate(label="two", colour=None))

    assert store.crates2.count(equals={"label": none_of()}) == 2
    assert store.crates2.count(equals={"colour": none_of([])}) == 2
    assert store.crates2.count(equals={"label": any_of()}) == 0


def test_none_of_refuses_none_among_its_values(store):
    """A field holding nothing already matches `none_of`, so a `None` in there
    asks for the opposite of the rest of the call. It is also the one member
    the two sides of the storage split would answer differently: a column
    compares against SQL null and gets *unknown*, while a blob field compares
    against a jsonb `null` and gets a plain *true*."""

    with pytest.raises(ValidationError, match="none_of"):
        none_of(None)
    with pytest.raises(ValidationError, match="none_of"):
        none_of(["done", None])


def test_a_blob_field_filters_on_more_than_strings(store):
    """`->>` extracts as text, so a filter on `size` of 4 was `text = smallint`
    and no such operator exists. A list quietly matched nothing at all."""

    @record(table="parcel", collection="parcels")
    class Parcel:
        size: int | None = field(default=None, stored_in="blob")
        express: bool | None = field(default=None, stored_in="blob")
        label: str | None = field(default=None, stored_in="blob")
        tags: list | None = field(default=None, stored_in="blob")

    store.create(Parcel)
    store.parcels.add(
        Parcel(size=4, express=True, label="Leura", tags=["fragile", "heavy"])
    )

    assert len(store.parcels.find(equals={"size": 4})) == 1
    assert len(store.parcels.find(equals={"express": True})) == 1
    assert len(store.parcels.find(equals={"label": "Leura"})) == 1
    assert len(store.parcels.find(equals={"tags": ["fragile", "heavy"]})) == 1

    # And a value that is not there still matches nothing.
    assert store.parcels.find(equals={"size": 5}) == []
    assert store.parcels.find(equals={"tags": ["fragile"]}) == []
    assert store.parcels.count(equals={"size": 4}) == 1


def test_a_blob_field_holding_nothing_is_an_absent_key(store):
    """The document holds what somebody put in it and nothing else.

    Which matters because `data ? 'x'` is the only thing a hand-written
    statement can ask that a column has no analogue for. Writing a null for
    every unassigned field made it answer "has this row been saved since the
    field was declared" — right on the day it was written, wrong afterwards, and
    it reads like "was this ever recorded"."""

    @record(table="parcel", collection="parcels")
    class Parcel:
        label: str | None = field(default=None, stored_in="blob")
        express: bool | None = field(default=None, stored_in="blob")

    store.create(Parcel)
    said = store.parcels.add(Parcel(label="Leura"))
    nothing = store.parcels.add(Parcel())

    stored = dict(
        store.conn.execute(
            "select id, data from parcel", []
        ).fetchall()
    )
    assert stored[said.id] == {"label": "Leura"}
    assert stored[nothing.id] == {}

    # `?` now says on the blob what `is not null` says on a column, and the two
    # sides of the split answer alike.
    present = store.conn.execute(
        "select count(*) from parcel where data ? 'label'", []
    ).fetchone()
    assert present[0] == 1

    # And nothing above the statement can tell: an absent key loads as `None`,
    # which is what it was.
    assert store.parcels.by_id(nothing.id).label is None
    assert len(store.parcels.find(equals={"label": None})) == 1


def test_clearing_a_blob_field_removes_its_key(store):
    """A value taken away leaves no trace of having been there, the same way a
    column set back to null does."""

    @record(table="parcel", collection="parcels")
    class Parcel:
        label: str | None = field(default=None, stored_in="blob")

    store.create(Parcel)
    parcel = store.parcels.add(Parcel(label="Leura"))
    parcel.label = None
    parcel.save()

    row = store.conn.execute("select data from parcel", []).fetchone()
    assert row[0] == {}


def test_a_blob_field_that_defaults_to_something_keeps_its_null(store):
    """The one exception, and the whole of the care this needs.

    `load` gives an absent key the field's declared default, so leaving the key
    out round-trips as `None` only where `None` is the default. A field
    defaulting to anything else keeps its stored null, or clearing it would read
    back as the default and the record would be lying about itself."""

    @record(table="parcel", collection="parcels")
    class Parcel:
        whom: str | None = field(default="System", stored_in="blob")

    store.create(Parcel)
    parcel = store.parcels.add(Parcel())
    assert store.conn.execute(
        "select data from parcel", []
    ).fetchone()[0] == {"whom": "System"}

    parcel.whom = None
    parcel.save()

    assert store.conn.execute(
        "select data from parcel", []
    ).fetchone()[0] == {"whom": None}
    assert store.parcels.by_id(parcel.id).whom is None


def test_a_blob_holds_the_types_json_cannot(store):
    """A column gets these for nothing — psycopg turns a timestamptz back into
    a datetime. jsonb has no such types, so dray is the adapter."""
    from decimal import Decimal as D

    @record(table="sitting", collection="sittings")
    class Sitting:
        on_day: date | None = field(default=None, stored_in="blob")
        at_moment: datetime | None = field(default=None, stored_in="blob")
        takings: D | None = field(default=None, stored_in="blob")
        label: str | None = field(default=None, stored_in="blob")

    store.create(Sitting)
    written = store.sittings.add(
        Sitting(
            on_day=date(2026, 3, 14),
            at_moment=datetime(2026, 3, 14, 19, 30),
            takings=D("4.99"),
            label="2026-03-14",
        )
    )

    back = store.sittings.by_id(written.id)
    assert back.on_day == date(2026, 3, 14)
    assert back.at_moment == datetime(2026, 3, 14, 19, 30)
    # Through str, not float — 4.99 as a float does not survive the trip.
    assert back.takings == D("4.99")
    # Driven by the annotation, so a str field that looks like a date stays one.
    assert back.label == "2026-03-14"

    # And they filter, which means the parameter is encoded the same way.
    assert len(store.sittings.find(equals={"on_day": date(2026, 3, 14)})) == 1
    assert len(store.sittings.find(equals={"takings": D("4.99")})) == 1


def test_a_blob_value_that_will_not_parse_still_loads(store):
    """`load` does not raise. A row written before the field was a date has to
    keep loading, the same as one written under a looser rule."""

    @record(table="sitting2", collection="sitting2s")
    class Sitting2:
        on_day: date | None = field(default=None, stored_in="blob")

    loaded = Sitting2._dray_load({"data": {"on_day": "the fourteenth"}})
    assert loaded.on_day == "the fourteenth"


def test_every_type_dray_knows_survives_both_sides(store):
    """A column and a blob field of the same type come back the same. `time`
    was the one that did not: a text column reading back a string, in a domain
    made of times of day."""
    from decimal import Decimal as D
    from datetime import time as clock_time, timedelta as span
    from uuid import UUID, uuid4

    ref = uuid4()

    @record(table="every", collection="everys")
    class Every:
        as_time: clock_time | None = field(default=None)
        as_time_blob: clock_time | None = field(default=None, stored_in="blob")
        as_span: span | None = field(default=None)
        as_span_blob: span | None = field(default=None, stored_in="blob")
        as_uuid: UUID | None = field(default=None)
        as_uuid_blob: UUID | None = field(default=None, stored_in="blob")
        as_bytes: bytes | None = field(default=None)
        as_bytes_blob: bytes | None = field(default=None, stored_in="blob")
        as_money: D | None = field(default=None)

    ddl = schema.create_table(Every)
    assert "as_time time" in ddl
    assert "as_span interval" in ddl
    assert "as_uuid uuid" in ddl
    assert "as_bytes bytea" in ddl

    store.create(Every)
    written = store.everys.add(
        Every(
            as_time=clock_time(19, 30), as_time_blob=clock_time(19, 30),
            as_span=span(hours=1, minutes=30), as_span_blob=span(hours=1, minutes=30),
            as_uuid=ref, as_uuid_blob=ref,
            as_bytes=b"\x00\x01", as_bytes_blob=b"\x00\x01",
            as_money=D("4.99"),
        )
    )

    back = store.everys.by_id(written.id)
    assert back.as_time == back.as_time_blob == clock_time(19, 30)
    assert back.as_span == back.as_span_blob == span(hours=1, minutes=30)
    assert back.as_uuid == back.as_uuid_blob == ref
    assert back.as_bytes == back.as_bytes_blob == b"\x00\x01"
    assert back.as_money == D("4.99")

    # And a range on a time column, which is what a text column could not do.
    assert store.everys.select_many(
        f"select {store.everys.columns} from every where as_time > %s",
        [clock_time(18, 0)],
    )


def test_a_list_or_dict_column_is_usable(store):
    """`SQL_TYPES` maps both to jsonb, so the DDL was right and the write was
    not: psycopg sends a list as a Postgres array and cannot adapt a dict."""

    @record(table="basket", collection="baskets")
    class Basket:
        tags: list | None = field(default=None)
        meta: dict | None = field(default=None)

    assert "tags jsonb" in schema.create_table(Basket)
    store.create(Basket)

    written = store.baskets.add(Basket(tags=["fragile", "heavy"], meta={"aisle": 3}))
    back = store.baskets.by_id(written.id)
    assert back.tags == ["fragile", "heavy"]
    assert back.meta == {"aisle": 3}

    # And findable, which is the same defect one step further along: the write
    # learned to send jsonb and the filter did not, so a declared column type
    # could be written and never asked about.
    assert len(store.baskets.find(equals={"tags": ["fragile", "heavy"]})) == 1
    assert len(store.baskets.find(equals={"meta": {"aisle": 3}})) == 1
    assert store.baskets.count(equals={"tags": ["fragile", "heavy"]}) == 1
    assert store.baskets.find(equals={"tags": ["fragile"]}) == []


def test_a_list_against_a_scalar_column_is_refused(store):
    """`find(equals={"status": ["booked", "seated"]})` is the first thing a
    lifecycle reaches for. The list was rendered `{booked,seated}` and compared as text,
    so it matched nothing and said nothing — a wrong answer rather than an
    empty one. Sending it as jsonb made it a refusal, but the driver's:
    `text = jsonb` has no operator, and the message named neither dray nor the
    field. The field's own annotation answers it now, before any statement is
    built."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        status: str = ""

    store.create(Ticket)
    store.tickets.add(Ticket(status="booked"))

    assert len(store.tickets.find(equals={"status": "booked"})) == 1
    with pytest.raises(ValidationError, match="status"):
        store.tickets.find(equals={"status": ["booked", "seated"]})


def test_a_filter_of_a_type_the_field_will_not_take_is_refused(store):
    """The write path checked a value against its field and the read path did
    not, so the same mistake raised `ValidationError` naming the field at `add`
    and reached the driver untouched from `find` —
    `operator does not exist: text = uuid`, which names neither dray, nor the
    field, nor which of the two types the class declared. And it was
    asymmetric: a `str` against a `uuid` column is inferred to the column's
    type and quietly works, so which direction was fatal turned on a column
    type nobody is thinking about while writing a filter."""
    from uuid import uuid4

    @record(table="player", collection="players")
    class Player:
        team_id: str | None = field(default=None)
        shirt: int | None = field(default=None, stored_in="blob")

    store.create(Player)
    ref = uuid4()

    # All four reads go through one funnel, and all four refuse it.
    with pytest.raises(ValidationError, match="team_id"):
        store.players.find(equals={"team_id": ref})
    with pytest.raises(ValidationError, match="team_id"):
        store.players.find_first(equals={"team_id": ref})
    with pytest.raises(ValidationError, match="team_id"):
        store.players.count(equals={"team_id": ref})
    with pytest.raises(ValidationError, match="team_id"):
        list(store.players.in_batches(equals={"team_id": ref}))

    # The blob side is no different — the annotation is the rule either side of
    # the storage split, and `->` sends the parameter as jsonb rather than text
    # precisely so the type is the one the field declared.
    with pytest.raises(ValidationError, match="shirt"):
        store.players.find(equals={"shirt": "nine"})

    # A member of `any_of` is a value like any other, and is checked as one.
    # So is a member of `none_of`, which is the same funnel and the same rule
    # read the other way round.
    with pytest.raises(ValidationError, match="team_id"):
        store.players.find(equals={"team_id": any_of("keeper", ref)})
    with pytest.raises(ValidationError, match="team_id"):
        store.players.find(equals={"team_id": none_of("keeper", ref)})

    # None is exempt, here as everywhere else: it is what asks for *is null*.
    store.players.add(Player(team_id="keepers", shirt=9))
    assert store.players.find(equals={"team_id": None}) == []
    assert len(store.players.find(equals={"team_id": "keepers", "shirt": 9})) == 1


def test_a_filter_is_held_to_the_type_and_to_nothing_else_the_field_says(store):
    """`choices` and the validators must not run on a filter. Loading a row
    does not validate it, so a record written under a rule that has since been
    tightened stays readable — and a filter is how somebody goes and finds
    those rows. If `choices` ran here, narrowing a status list would make the
    rows holding the old value unaskable-for on the same day it made them
    unwritable, and the migration meant to fix them could not select them."""

    def not_blank(value: str) -> None:
        if not value.strip():
            raise ValueError("cannot be blank")

    @record(table="applicant", collection="applicants")
    class Applicant:
        family_name: str = field(default="", validator=not_blank)
        status: str = field(
            default="enquiry", choices=("enquiry", "volunteer")
        )

    store.create(Applicant)
    store.applicants.add(Applicant(family_name="Hemingway"))
    # A row from before the rule was tightened, which is the whole case: the
    # class has never accepted 'lapsed', and the row holds it anyway.
    store.conn.execute("update applicant set status = 'lapsed'", [])

    assert len(store.applicants.find(equals={"status": "lapsed"})) == 1
    assert store.applicants.count(equals={"status": "lapsed"}) == 1
    # And a validator no more than `choices`: this is precisely the query
    # somebody runs after adding `not_blank`.
    assert store.applicants.find(equals={"family_name": ""}) == []


def test_a_choices_function_is_not_asked_by_a_filter_either(store):
    """A `choices` function is asked wherever a value is checked, and a filter
    is not one of those. It matters more for a function than for a tuple: the
    exemption is what keeps a retired value findable, and a vocabulary that has
    just dropped one would otherwise make its rows unaskable-for the moment the
    application reloaded. It is also why asking one costs nothing per row."""
    asked = []

    def statuses() -> tuple[str, ...]:
        asked.append(1)
        return ("enquiry", "volunteer")

    @record(table="applicant", collection="applicants")
    class Applicant:
        family_name: str = field(default="")
        status: str = field(default="enquiry", choices=statuses)

    store.create(Applicant)
    store.applicants.add(Applicant(family_name="Hemingway"))
    store.conn.execute("update applicant set status = 'lapsed'", [])

    before = len(asked)
    assert len(store.applicants.find(equals={"status": "lapsed"})) == 1
    assert store.applicants.count(equals={"status": "lapsed"}) == 1
    assert len(asked) == before


#
# What comes back when it goes wrong
#


def test_adding_an_id_that_is_taken_is_refused(walkers):
    """The only unique constraint dray makes is the primary key, so this is
    what `DuplicateRecord` is for: a clash on the way in."""
    walker = walkers.add(Walker(family_name="Hemingway"))

    # Given at construction, which is where an id somebody else chose arrives:
    # an import carrying its own keys, a record rebuilt from a backup. It cannot
    # be moved afterwards, since that would move which row the save writes to.
    same = Walker(family_name="Shelley", id=walker.id)
    with pytest.raises(DuplicateRecord) as raised:
        walkers.add(same)
    assert str(walker.id) in str(raised.value)

    # And the first one is untouched.
    assert walkers.by_id(walker.id).family_name == "Hemingway"


def test_a_clash_in_the_middle_of_a_set_names_the_record_that_clashed(walkers):
    """A set goes out as one batch, so every statement in it is sent before any
    result is read back — and an error taken off the connection rather than off
    the statement that caused it names whichever record the reader happened to
    be looking at. A key clash is the one message that says a *record* out
    loud, so blaming the wrong statement says the wrong id rather than merely
    saying less."""
    written = walkers.add_all(
        [Walker(family_name=f"W{n}") for n in range(10)]
    )

    # Position 6 of 10, and its neighbours innocent: an id somebody else chose
    # is how this happens for real, an import carrying its own keys or a record
    # rebuilt from a backup.
    taken = written[6].id
    clashing = [Walker(family_name=f"X{n}") for n in range(10)]
    clashing[6] = Walker(family_name="X6", id=taken)

    with pytest.raises(DuplicateRecord) as raised:
        walkers.add_all(clashing)
    assert str(taken) in str(raised.value)
    assert all(str(one.id) not in str(raised.value) for one in written[:6])

    # And none of the set landed, the nine good ones with the tenth.
    assert walkers.count() == 10


def test_every_record_of_a_set_gets_back_what_the_database_filled_in(walkers):
    """Not only the first. A batched write reads its results back one statement
    at a time and matches each to the record that sent it — get that mapping
    wrong by one and every record after the first holds another record's value,
    which is the kind of wrong nothing raises about."""
    landed = walkers.add_all(
        [Walker(family_name=f"W{n}") for n in range(20)]
    )

    assert all(one.created_at is not None for one in landed)
    # Against the rows rather than against each other, since twenty records all
    # holding the first one's value would pass a test that only asked whether
    # something arrived.
    stored = {one.id: one.created_at for one in walkers.find()}
    assert {one.id: one.created_at for one in landed} == stored


def test_a_set_of_any_size_waits_for_the_database_once(walkers, monkeypatch):
    """A hundred records used to be a hundred round trips, inside the
    transaction that is also the conflict window — microseconds under test and
    four seconds against a cluster, on whichever request copies a list. The
    statements are unchanged and the waits are one, which is the whole of the
    change and very nearly the only part of it visible from anywhere."""
    from dray import store as store_module

    waits = []
    real = store_module._Batch._flush

    def counting(self):
        if not self._flushed:
            waits.append(len(self._sent))
        real(self)

    monkeypatch.setattr(store_module._Batch, "_flush", counting)
    walkers.add_all([Walker(family_name=f"W{n}") for n in range(50)])

    assert waits == [50]


def test_a_clash_on_the_way_out_is_the_databases_to_report(store):
    """Documented rather than desirable. `DuplicateRecord` is raised by the
    insert and nowhere else, so the same constraint reads differently depending
    on whether the record was being added or saved."""
    import psycopg

    @record(table="ticketed", collection="ticketeds")
    class Ticketed:
        serial: str = field(default="")

    store.create(Ticketed)
    store.conn.execute("create unique index on ticketed (serial)")

    store.ticketeds.add(Ticketed(serial="A1"))
    second = store.ticketeds.add(Ticketed(serial="A2"))

    second.serial = "A1"
    with pytest.raises(psycopg.errors.UniqueViolation):
        second.save()


def test_a_record_can_declare_its_own_id(store):
    """`id` is added rather than declared, but a record may declare it — and a
    `uuid` primary key is a real one on DSQL, indexable and 16 bytes."""
    from uuid import UUID, uuid4

    @record(table="native", collection="natives")
    class Native:
        id: UUID = field(default_factory=uuid4)
        name: str = field(default="")

    assert "id uuid primary key" in schema.create_table(Native)

    store.create(Native)
    written = store.natives.add(Native(name="Hemingway"))
    assert isinstance(written.id, UUID)

    # And the guard on `by_id` reads the field rather than assuming a string.
    assert store.natives.by_id(written.id).name == "Hemingway"
    with pytest.raises(ValidationError) as raised:
        store.natives.by_id("not-a-uuid")
    assert "a Native id is UUID, not str" in str(raised.value)


#
# Two services, one cluster, and both of them have a `person`
#


def test_a_store_works_inside_the_namespace_it_was_given(postgresql):
    """Every statement dray writes names its table bare, so a namespace is a
    `search_path` and nothing else — which is why `select_many`, written by
    hand, lands in the same place as everything dray generates."""
    from dray import Store

    @record(table="person", collection="folk")
    class Person:
        family_name: str = field()

    admin = Store(postgresql)
    with admin.conn.cursor() as cur:
        cur.execute(schema.create_namespace("orders"))
        cur.execute(schema.create_namespace("billing"))

    orders = Store(postgresql, namespace="orders", records=[Person])
    orders.create(Person)
    orders.folk.add(Person(family_name="Hemingway"))

    assert orders.namespace == "orders"
    with orders.conn.cursor() as cur:
        cur.execute("select count(*) from orders.person")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "select count(*) from information_schema.tables"
            " where table_schema = 'public' and table_name = 'person'"
        )
        assert cur.fetchone()[0] == 0


def test_the_same_record_serves_two_namespaces_at_once(postgresql, postgresql_proc):
    """Two stores on two connections, the same class, different schemas — which
    is what separates this from a name on the record. A migration or a test
    suite would use it, and so does the argument for putting it on the store."""
    import psycopg
    from dray import Store

    @record(table="person", collection="citizens")
    class Person:
        family_name: str = field()

    def fresh():
        return psycopg.connect(
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            user=postgresql_proc.user,
            dbname=postgresql.info.dbname,
            autocommit=True,
        )

    admin = Store(postgresql)
    for name in ("orders", "billing"):
        with admin.conn.cursor() as cur:
            cur.execute(schema.create_namespace(name))

    orders = Store(fresh(), namespace="orders", records=[Person])
    billing = Store(fresh(), namespace="billing", records=[Person])
    orders.create(Person)
    billing.create(Person)

    orders.citizens.add(Person(family_name="Hemingway"))
    billing.citizens.add(Person(family_name="Shelley"))
    billing.citizens.add(Person(family_name="Frankenstein"))

    assert [p.family_name for p in orders.citizens.find()] == ["Hemingway"]
    assert orders.citizens.count() == 1
    assert billing.citizens.count() == 2

    # And drift answers for the schema each store is in, not for the cluster.
    assert schema.drift(orders.conn, Person) == []
    assert schema.drift(billing.conn, Person) == []


def test_no_namespace_touches_nothing(store):
    """`None` by default, meaning whatever `search_path` the connection already
    carried — which for a connection handed over is the caller's business."""
    assert store.namespace is None
    store.create(Walker)
    store.walkers.add(Walker(family_name="Hemingway"))
    with store.conn.cursor() as cur:
        cur.execute("select count(*) from public.walker")
        assert cur.fetchone()[0] == 1


def test_a_namespace_that_is_not_a_name_is_refused(postgresql):
    """The one identifier dray takes from a caller, and `set search_path`
    cannot take a parameter."""
    from dray import Store

    with pytest.raises(ValueError, match="not a name a schema can have"):
        Store(postgresql, namespace="public; drop table walker")
    with pytest.raises(ValueError, match="not a name a schema can have"):
        schema.create_namespace("orders-and-billing")


#
# The indexes a class declares
#


def test_a_class_declares_the_indexes_its_table_carries(store):
    """One field at a time and one column wide cannot say the question a
    table exists to answer, which is two or three columns matched together.
    Four single-column indexes spend four of DSQL's 24 slots to buy a worse
    plan than one composite would have given, and nothing on the class says
    so."""

    @record(table="member", collection="members",
            indexes=[index("joined_on", "family_name")])
    class Member:
        email: str = field(default="")
        joined_on: date | None = field(default=None)
        family_name: str = field(default="")

    assert schema.create_indexes(Member) == [
        "create index async if not exists member_joined_on_family_name"
        " on member (joined_on, family_name)"
    ]
    # PostgreSQL's form is the same schema, and is what local PostgreSQL takes.
    assert schema.create_indexes(Member, asynchronous=False) == [
        "create index if not exists member_joined_on_family_name"
        " on member (joined_on, family_name)"
    ]

    store.create(Member)
    store.members.add(Member(email="rod@example.com", joined_on=date(2026, 3, 14)))
    assert store.members.count(equals={"joined_on": date(2026, 3, 14)}) == 1


def test_one_column_keeps_the_name_it_has_always_had():
    """`drift` matches an index by the name dray chose, so a table built by an
    earlier dray would have reported the index it has as missing if a composite
    naming rule had changed what one column is called."""

    @record(table="ledger", collection="ledgers", indexes=[index("entered_on")])
    class Ledger:
        entered_on: date | None = field(default=None)

    assert schema.create_indexes(Ledger) == [
        "create index async if not exists ledger_entered_on on ledger (entered_on)"
    ]


def test_a_unique_index_is_a_constraint_on_a_table_being_created(store):
    """Where the DDL goes is dray's to decide, and the two places are not the
    same promise: a constraint enforces from the moment the table exists, where
    `create unique index async` is a background job that takes a duplicate
    written before it finishes. So the table being created gets the constraint,
    and the table that is already there gets the index."""

    @record(table="badge", collection="badges", indexes=[index("code", unique=True)])
    class Badge:
        code: str = field(default="")

    assert "constraint badge_code unique (code)" in schema.create_table(Badge)
    assert schema.create_indexes(Badge) == [
        "create unique index async if not exists badge_code on badge (code)"
    ]
    # And said once. The `create table` already carries it, so `statements` for
    # a new table would otherwise ask for a second index over the same column.
    assert schema.statements(Badge) == [schema.create_table(Badge)]

    store.create(Badge)
    store.badges.add(Badge(code="A1"))
    with pytest.raises(DuplicateRecord):
        store.badges.add(Badge(code="A1"))


def test_a_unique_index_over_two_columns_is_the_pair(store):
    """A name and a date together are one event, which no field could say on its
    own — `unique=True` on either column would have refused the second spring
    intake as well as the second event on that Tuesday."""

    @record(table="event", collection="events",
            indexes=[index("name", "starts_on", unique=True)])
    class Event:
        name: str = field(default="")
        starts_on: date | None = field(default=None)

    store.create(Event)
    store.events.add(Event(name="Spring intake", starts_on=date(2026, 3, 14)))
    store.events.add(Event(name="Working bee", starts_on=date(2026, 3, 14)))
    store.events.add(Event(name="Spring intake", starts_on=date(2026, 3, 15)))
    with pytest.raises(DuplicateRecord):
        store.events.add(Event(name="Spring intake", starts_on=date(2026, 3, 14)))


def test_a_childs_declared_index_is_beside_its_parents_and_not_inside_it(store):
    """The index a child gets for nothing serves reads through a parent, and
    that is the door the accessor uses. The other one — the collection's, which
    names no parent — cannot be served by an index leading with the parent, and
    a declaration that had the two columns silently put in front of it would
    have answered the question it was written for."""
    from dray import child

    @record(table="crew", collection="crews")
    class Crew:
        name: str = field(default="")

    @child(of=Crew, name="shifts", table="shift", collection="shifts",
           indexes=[index("starts_at")])
    class Shift:
        label: str = field(default="")
        starts_at: datetime | None = field(default=None)

    assert schema.create_indexes(Shift) == [
        "create index async if not exists shift_parent"
        " on shift (parent_type, parent_id)",
        "create index async if not exists shift_starts_at"
        " on shift (starts_at)",
    ]


def test_a_child_declaring_nothing_is_still_indexed_on_its_parent(store):
    """`delete_batch` filters on those two columns once per generation, so
    dray's own cascade walks the table without it."""
    from dray import child

    @record(table="troupe", collection="troupes")
    class Troupe:
        name: str = field(default="")

    @child(of=Troupe, name="turns", table="turn")
    class Turn:
        label: str = field(default="")

    assert schema.create_indexes(Turn) == [
        "create index async if not exists turn_parent"
        " on turn (parent_type, parent_id)"
    ]


def test_a_child_can_index_the_columns_naming_its_parent(store):
    """The two columns are fields like any other, which is how a child says it
    wants a parent's reads served by more than the pair alone — and the
    declaration leads with them, so dray's own index over the pair would have
    been a second slot spent on reads this one already answers."""
    from dray import child

    @record(table="ward", collection="wards")
    class Ward:
        name: str = field(default="")

    @child(of=Ward, name="rounds", table="round",
           indexes=[index("parent_type", "parent_id", "walked_at")])
    class Round:
        walked_at: datetime | None = field(default=None)

    assert schema.create_indexes(Round) == [
        "create index async if not exists round_parent_type_parent_id_walked_at"
        " on round (parent_type, parent_id, walked_at)"
    ]

    # And the reads it was standing in for still go through it.
    store.create(Ward, Round)
    ward = store.wards.add(Ward(name="North"))
    ward.rounds.add(Round(walked_at=datetime(2026, 3, 14, 9, 0)))
    assert len(ward.rounds.find()) == 1


def test_a_child_indexing_the_parent_columns_backwards_keeps_the_implicit_one(
    store,
):
    """Only dray's own order counts as covering. `(parent_id, parent_type)`
    would in fact serve a read matching both columns, but the rule everywhere
    else here is about a leading run rather than about a set of columns, and
    reading it the strict way costs an index nobody needed where reading it
    loosely would cost a read nobody indexed."""
    from dray import child

    @record(table="depot", collection="depots")
    class Depot:
        name: str = field(default="")

    @child(of=Depot, name="hauls", table="haul",
           indexes=[index("parent_id", "parent_type")])
    class Haul:
        label: str = field(default="")

    assert schema.create_indexes(Haul) == [
        "create index async if not exists haul_parent"
        " on haul (parent_type, parent_id)",
        "create index async if not exists haul_parent_id_parent_type"
        " on haul (parent_id, parent_type)",
    ]


def test_a_child_indexing_its_parent_uniquely_gets_no_second_index(store):
    """A unique btree serves a leading run like any other, so a child holding
    one child per parent has already indexed the pair — and the implicit index
    beside it would enforce nothing extra and answer nothing extra."""
    from dray import child

    @record(table="rider", collection="riders")
    class Rider:
        name: str = field(default="")

    @child(of=Rider, name="passes", table="boarding",
           indexes=[index("parent_type", "parent_id", unique=True)])
    class Boarding:
        code: str = field(default="")

    assert schema.create_indexes(Boarding) == [
        "create unique index async if not exists boarding_parent_type_parent_id"
        " on boarding (parent_type, parent_id)"
    ]
    # The unique one is a constraint on a table being created, so `statements`
    # asks for no index at all here — and still not the implicit pair.
    assert schema.statements(Boarding) == [schema.create_table(Boarding)]


def test_an_index_that_is_the_leading_run_of_another_is_refused():
    """Both were created, and the narrower one answered no question the wider
    one was not already answering — a slot out of the 23 a table has and a write
    on every insert, spent for nothing and visible nowhere but the list they are
    both declared in."""

    with pytest.raises(ValueError, match="a leading run of"):

        @record(table="sitting", collection="sittings",
                indexes=[index("on_date"), index("on_date", "name")])
        class Sitting:
            name: str = field(default="")
            on_date: date | None = field(default=None)


def test_an_index_declared_twice_is_refused_like_any_other_leading_run():
    """The same declaration said twice is the extreme of the same mistake, and
    `create index if not exists` made it silent: the second statement found the
    first index and did nothing, so the list said two and the table had one."""

    with pytest.raises(ValueError, match="a leading run of"):

        @record(table="parcel", collection="parcels",
                indexes=[index("sent_on"), index("sent_on")])
        class Parcel:
            sent_on: date | None = field(default=None)


def test_a_narrower_unique_index_stands_beside_a_wider_one():
    """A unique index is not made redundant by anything: the wider index serves
    its reads and enforces none of its uniqueness, so dropping it would drop a
    rule rather than a cost."""

    @record(table="courier", collection="couriers",
            indexes=[index("email", unique=True), index("email", "joined_on")])
    class Courier:
        email: str = field(default="")
        joined_on: date | None = field(default=None)

    assert schema.create_indexes(Courier) == [
        "create unique index async if not exists courier_email"
        " on courier (email)",
        "create index async if not exists courier_email_joined_on"
        " on courier (email, joined_on)",
    ]


def test_a_plain_index_inside_a_wider_unique_one_is_still_refused():
    """The rule is about what a btree can be searched by, and a unique one is
    searched the same way — so uniqueness saves the index that carries it and
    not the one beside it."""

    with pytest.raises(ValueError, match="a leading run of"):

        @record(
            table="docket", collection="dockets",
            indexes=[index("code"), index("code", "issued_on", unique=True)])
        class Docket:
            code: str = field(default="")
            issued_on: date | None = field(default=None)


def test_indexes_that_are_not_a_leading_run_of_each_other_are_both_kept():
    """Only a true leading run is redundant. `(a, b)` and `(b, a)` answer
    different questions, and so do `(a, b)` and `(a, c)` — refusing either pair
    would be refusing an index somebody's read needs."""

    @record(table="shipment", collection="shipments",
            indexes=[index("origin", "sent_on"), index("sent_on", "origin")])
    class Shipment:
        origin: str = field(default="")
        sent_on: date | None = field(default=None)

    assert len(schema.create_indexes(Shipment)) == 2

    @record(table="crate", collection="crates",
            indexes=[index("origin", "sent_on"), index("origin", "weight")])
    class Crate:
        origin: str = field(default="")
        sent_on: date | None = field(default=None)
        weight: int = field(default=0)

    assert len(schema.create_indexes(Crate)) == 2


def test_drift_sees_an_index_that_was_never_made(store):
    """Drift was the argument for dray knowing about DDL at all, and it was
    blind in the one dimension that decides whether the thing is usable: a table
    with every column and none of its indexes reads exactly like a right one."""

    @record(table="tally", collection="tallies", indexes=[index("entered_on")])
    class Tally:
        entered_on: date | None = field(default=None)

    with store.conn.cursor() as cur:
        cur.execute(schema.create_table(Tally))

    assert schema.drift(store.conn, Tally) == [
        "tally has no index 'tally_entered_on', which the class asks for"
    ]

    for statement in schema.create_indexes(Tally, asynchronous=False):
        with store.conn.cursor() as cur:
            cur.execute(statement)
    assert schema.drift(store.conn, Tally) == []


def test_drift_finds_a_unique_index_that_arrived_as_a_constraint(store):
    """The two places a unique index is written are one declaration, so drift
    has to recognise what `create_table` made — which is why the constraint is
    named rather than left to the database, whose own name for it is
    `roster_code_key` and is not a name dray ever asks about."""

    @record(table="roster", collection="rosters",
            indexes=[index("code", unique=True)])
    class Roster:
        code: str = field(default="")

    store.create(Roster)
    assert schema.drift(store.conn, Roster) == []


def test_an_index_key_carries_the_null_placement_it_was_given(store):
    """An index and the `order_by` it serves are one decision. A plain
    `(area_id, due_on)` serves the two orders a bare name and `desc` give, since
    a backward scan reverses everything; the other two — up with the empty ones
    first, down with them last — are served by this one and by nothing dray
    could say before."""

    @record(table="task", collection="tasks",
            indexes=[index("area_id", asc("due_on", nulls="first"))])
    class Task:
        area_id: str = field(default="")
        due_on: date | None = field(default=None)

    assert schema.create_indexes(Task) == [
        "create index async if not exists task_area_id_due_on"
        " on task (area_id, due_on nulls first)"
    ]
    # The name is the table and the bare columns, which is what `drift` goes
    # looking for — a placement that reached it would rename an index the day
    # somebody added one.
    store.create(Task)
    assert schema.drift(store.conn, Task) == []


def test_an_index_key_saying_nothing_about_nulls_is_the_column_it_always_was():
    """`asc` with no `nulls=` is the bare name, so a class that reaches for the
    word for symmetry does not quietly ask for a different index than the one
    its table is already carrying."""

    @record(table="duty", collection="duties", indexes=[index(asc("due_on"))])
    class Duty:
        due_on: date | None = field(default=None)

    assert schema.create_indexes(Duty) == [
        "create index async if not exists duty_due_on on duty (due_on)"
    ]


def test_an_index_key_may_not_say_a_direction():
    """DSQL answers `specifying sort order not supported for index keys`, and
    local PostgreSQL builds `(due_on desc)` without a word — so a suite that
    was green said nothing, and the deployment was where this would have been
    learnt. Nothing is lost: a btree is scanned backwards, so the bare name is
    already the index the descending read wants."""

    with pytest.raises(ValueError, match="not supported for index keys"):

        @record(table="job", collection="jobs", indexes=[index(desc("due_on"))])
        class Job:
            due_on: date | None = field(default=None)


def test_a_unique_index_may_not_say_where_its_nulls_go():
    """dray's own limit rather than the database's, and the reason is that the
    unique kind is a constraint inside a `create table` where `unique (due_on
    nulls first)` has no grammar. Left alone, a table being created and a table
    already there would come out indexed differently with nothing to say so."""

    with pytest.raises(ValueError, match="unique=True together"):

        @record(table="slot", collection="slots",
                indexes=[index(asc("due_on", nulls="first"), unique=True)])
        class Slot:
            due_on: date | None = field(default=None)


def test_a_blob_field_cannot_be_indexed():
    """There is no column to index, and on DSQL `jsonb` carries no index support
    at all — which is what makes promoting a field the only way to make it fast
    rather than merely the tidier one."""

    with pytest.raises(TypeError, match="no column to index"):

        @record(table="lodge", collection="lodges", indexes=[index("suburb")])
        class Lodge:
            suburb: str | None = field(default=None, stored_in="blob")


def test_an_index_on_a_field_the_class_does_not_have_is_refused():
    """A name that is not a field is a `create index` that fails during
    somebody's deployment, and the line that got it wrong is here."""

    with pytest.raises(TypeError, match="does not declare"):

        @record(table="hall", collection="halls", indexes=[index("suburb")])
        class Hall:
            family_name: str = field(default="")


def test_an_index_over_no_columns_is_refused():
    """`index()` on its own reads as a default and is not one."""
    with pytest.raises(ValueError, match="index over none of them"):
        index()


def test_a_column_dsql_cannot_index_is_refused_at_declaration():
    """DSQL's table of supported types carries an index support column, and
    `interval`, `bytea` and `jsonb` have none — so an index over a field of one
    of those annotations is one the cluster refuses. Local PostgreSQL builds all
    three happily, which is the whole problem: the suite was green and the
    deployment was where it would have been learnt."""

    with pytest.raises(ValueError, match="no index support for interval"):

        @record(table="lap", collection="laps", indexes=[index("took")])
        class Lap:
            took: timedelta | None = field(default=None)

    with pytest.raises(ValueError, match="no index support for bytea"):

        @record(table="seal", collection="seals", indexes=[index("digest")])
        class Seal:
            digest: bytes | None = field(default=None)

    with pytest.raises(ValueError, match="no index support for jsonb"):

        @record(table="form", collection="forms", indexes=[index("answers")])
        class Form:
            answers: dict | None = field(default=None)

    with pytest.raises(ValueError, match="no index support for jsonb"):

        @record(table="basket", collection="baskets", indexes=[index("items")])
        class Basket:
            items: list | None = field(default=None)


def test_a_unique_index_is_refused_on_the_same_columns_as_a_plain_one():
    """A unique index is not a lesser one — it is backed by exactly the index
    DSQL will not build. Refusing one and allowing the other would move the
    failure from `create index async` to `create table` and leave it just as far
    away."""

    with pytest.raises(ValueError, match="no index support for bytea"):

        @record(table="ticket", collection="tickets",
                indexes=[index("token", unique=True)])
        class Ticket:
            token: bytes | None = field(default=None)


def test_a_column_dsql_can_index_is_untouched():
    """The refusal is read off the annotation, so the check has to leave the
    types either side of it alone — `date` and `str` index on DSQL, and a
    `timedelta` nobody asked to index is an ordinary column."""

    @record(table="permit", collection="permits",
            indexes=[index("issued_on"), index("number", unique=True)])
    class Permit:
        number: str = field(default="")
        issued_on: date | None = field(default=None)
        valid_for: timedelta | None = field(default=None)

    assert schema.create_indexes(Permit) == [
        "create index async if not exists permit_issued_on on permit (issued_on)",
        "create unique index async if not exists permit_number on permit (number)",
    ]
    assert "valid_for interval" in schema.create_table(Permit)


def test_an_index_name_past_the_limit_is_cut_where_the_database_cuts_it(store):
    """An identifier holds 63 bytes and a longer one is stored at 63 without a
    word, so dray asked for `…renewed_by_volunteer`, the table carried
    `…renew`, and `drift` reported an index missing for ever on a table that had
    exactly what the class asked for — the worst shape a drift finding takes,
    since it teaches a reader to stop believing the tool."""

    @record(table="organisation_membership", collection="memberships",
            indexes=[index("effective_from", "contribution_level",
                           "renewed_by_volunteer")])
    class Membership:
        effective_from: date | None = field(default=None)
        contribution_level: str = field(default="")
        renewed_by_volunteer: str = field(default="")

    cut = "organisation_membership_effective_from_contribution_level_renew"
    assert len(cut.encode()) == 63
    assert schema.create_indexes(Membership) == [
        f"create index async if not exists {cut} on organisation_membership"
        " (effective_from, contribution_level, renewed_by_volunteer)"
    ]

    store.create(Membership)
    assert schema.drift(store.conn, Membership) == []


def test_an_index_name_is_cut_on_a_character_boundary(store):
    """`name[:63]` is the same bug arriving by another road. It agrees with the
    database for as long as every identifier is ASCII, and the moment one is not
    it splits a multi-byte character and produces a name no table has — so drift
    goes back to reporting an index the table is carrying as missing."""

    @record(table="réunion_générale", collection="reunions",
            indexes=[index("débute", "responsable_désigné",
                           "numéro_de_référence")])
    class Réunion:
        débute: date | None = field(default=None)
        responsable_désigné: str = field(default="")
        numéro_de_référence: str = field(default="")

    made = schema.create_indexes(Réunion)[0]
    cut = made.split(" if not exists ")[1].split(" on ")[0]
    assert cut == "réunion_générale_débute_responsable_désigné_numéro_de_r"
    assert len(cut.encode()) <= 63

    # 63 characters rather than 63 bytes, which is what makes this worth its own
    # test: the name is short enough that slicing the string would have cut
    # nothing at all and handed the database 72 bytes to shorten itself.
    whole = "réunion_générale_débute_responsable_désigné_numéro_de_référence"
    assert len(whole) == 63 and len(whole.encode()) == 72

    store.create(Réunion)
    assert schema.drift(store.conn, Réunion) == []


def test_the_constraint_and_the_index_form_are_cut_the_same_way(store):
    """One declaration is written in two places — a constraint inside the
    `create table` and `create unique index async` against a table already
    there — and drift asks about one name. Shortening on one side only would
    have made a unique index and its drift check disagree about a table built
    the other way."""

    @record(table="organisation_membership", collection="memberships",
            indexes=[index("effective_from", "contribution_level",
                           "renewed_by_volunteer", unique=True)])
    class Membership:
        effective_from: date | None = field(default=None)
        contribution_level: str = field(default="")
        renewed_by_volunteer: str = field(default="")

    cut = "organisation_membership_effective_from_contribution_level_renew"
    assert f"constraint {cut} unique (" in schema.create_table(Membership)
    assert schema.create_indexes(Membership) == [
        f"create unique index async if not exists {cut}"
        " on organisation_membership"
        " (effective_from, contribution_level, renewed_by_volunteer)"
    ]

    store.create(Membership)
    assert schema.drift(store.conn, Membership) == []


def test_two_indexes_cut_to_the_same_name_are_refused():
    """The second was never built and nothing said so. `create index async if
    not exists` matched the first one's truncated name, so the statement
    succeeded — on DSQL as a submitted background job, with an id for the build
    of nothing — and the class asked for two indexes while the table carried
    one, until a query timed out in production."""

    with pytest.raises(ValueError, match="dray calls the same thing"):

        @record(table="organisation_membership", collection="memberships",
                indexes=[index("effective_from", "contribution_level",
                               "renewed_by_volunteer"),
                         index("effective_from", "contribution_level",
                               "renewed_at")])
        class Membership:
            effective_from: date | None = field(default=None)
            contribution_level: str = field(default="")
            renewed_by_volunteer: str = field(default="")
            renewed_at: datetime | None = field(default=None)


def test_two_indexes_named_the_same_without_being_cut_are_refused_too():
    """A column name may hold an underscore, so two indexes reach one name
    without going anywhere near the limit — `(sent, on_paper)` and `(sent_on,
    paper)` are both `parcel_sent_on_paper`. It is the same silence for the same
    reason, and the check is on the name rather than on the cut for exactly
    this."""

    with pytest.raises(ValueError, match="dray calls the same thing"):

        @record(table="parcel", collection="parcels",
                indexes=[index("sent", "on_paper"), index("sent_on", "paper")])
        class Parcel:
            sent: date | None = field(default=None)
            on_paper: str = field(default="")
            sent_on: date | None = field(default=None)
            paper: str = field(default="")


def test_an_index_name_inside_the_limit_is_untouched():
    """The cut renames nothing that was not already being renamed by the
    database. Every name a table is carrying today is under the limit, and one
    byte of difference here would report all of them missing."""

    @record(table="event", collection="events",
            indexes=[index("name", "starts_on")])
    class Event:
        name: str = field(default="")
        starts_on: date | None = field(default=None)

    assert schema.create_indexes(Event) == [
        "create index async if not exists event_name_starts_on"
        " on event (name, starts_on)"
    ]


def test_an_id_dsql_cannot_put_in_a_key_is_refused_at_declaration():
    """The same three column types one door along. A record may declare its own
    `id`, and `id: bytes` asks for no index at all — so nothing checked it, and
    `store.create` emitted `id bytea primary key`, which local PostgreSQL takes
    and a cluster refuses. The refusal the cluster gives is about keys rather
    than indexes, and a primary key is a key."""

    with pytest.raises(ValueError, match="will not have interval in a key"):

        @record(table="stint", collection="stints")
        class Stint:
            id: timedelta = field()

    with pytest.raises(ValueError, match="will not have bytea in a key"):

        @record(table="thing", collection="things")
        class Thing:
            id: bytes = field()

    with pytest.raises(ValueError, match="will not have jsonb in a key"):

        @record(table="compound", collection="compounds")
        class Compound:
            id: dict = field()

    with pytest.raises(ValueError, match="will not have jsonb in a key"):

        @record(table="tuple_ish", collection="tuple_ishes")
        class TupleIsh:
            id: list = field()

    # And `bytes | None` with it, since the column is the same one either way.
    with pytest.raises(ValueError, match="will not have bytea in a key"):

        @record(table="sealed", collection="sealeds")
        class Sealed:
            id: bytes | None = field(default=None)


def test_a_child_declaring_its_own_id_is_checked_like_any_other_record():
    """A child is a record with a primary key of its own, and it reaches the
    same declaration. Checking only `@record` would have left the refusal true
    of a person and not of their notes."""
    from dray import child

    @record(table="troupe", collection="troupes")
    class Troupe:
        name: str = field(default="")

    with pytest.raises(ValueError, match="will not have bytea in a key"):

        @child(of=Troupe, name="scenes", table="scene")
        class Scene:
            body: str = field(default="")
            id: bytes = field()


def test_an_id_of_a_type_a_key_can_hold_is_untouched():
    """The refusal is read off the annotation, so it has to leave alone the
    types an id is usually made of — including the `uuid` a record gets when it
    declares none, which is the primary key every table here already has."""
    from uuid import UUID, uuid4

    @record(table="numbered", collection="numbereds")
    class Numbered:
        id: int = field(default=0)

    @record(table="coded", collection="codeds")
    class Coded:
        id: str = field(default="")

    @record(table="minted", collection="minteds")
    class Minted:
        id: UUID = field(default_factory=uuid4)

    @record(table="given", collection="givens")
    class Given:
        name: str = field(default="")

    assert "id bigint primary key" in schema.create_table(Numbered)
    assert "id text primary key" in schema.create_table(Coded)
    assert "id uuid primary key" in schema.create_table(Minted)
    assert "id uuid primary key" in schema.create_table(Given)


def test_the_unindexable_annotations_are_the_columns_the_schema_emits():
    """Two lists that have to agree and are written down apart: `model` names
    the column each refused annotation becomes, because the message has to say
    it and because nothing in that module may ask the schema. A column type
    changed on one side and not the other would refuse the wrong fields, or
    quietly stop refusing."""
    from dray.model import UNINDEXABLE

    for annotation, column in UNINDEXABLE.items():
        assert schema._sql_type(annotation) == column

    # And nothing the schema sends to one of those three is missing from it.
    assert {
        annotation
        for annotation, column in schema.SQL_TYPES.items()
        if column in ("interval", "bytea", "jsonb")
    } == set(UNINDEXABLE)


def test_a_size_past_what_dsql_holds_is_refused_at_declaration():
    """The cluster's ceiling is a precision of 38 and a scale of 37, where local
    PostgreSQL takes a precision of 1,000 — so a column declared past it is the
    familiar shape one notch worse: not a value that differs by where it ran,
    but a `create table` local PostgreSQL builds and a deployment stops on."""
    with pytest.raises(ValueError, match="precision 39 is outside"):
        field(precision=39, scale=6)

    with pytest.raises(ValueError, match="scale 38 is outside"):
        field(precision=38, scale=38)

    with pytest.raises(ValueError, match="more than precision"):
        field(precision=8, scale=12)


def test_a_precision_and_a_scale_are_said_together():
    """`numeric(12)` is a scale of zero, so a field given only a precision would
    round every digit after the point away — which is the opposite of what the
    only reason to say either is."""
    with pytest.raises(ValueError, match="together or not at all"):
        field(precision=12)

    with pytest.raises(ValueError, match="together or not at all"):
        field(scale=8)


def test_a_size_on_a_field_that_is_not_a_decimal_is_refused():
    """Refused as the class is built, since `field` is on the right-hand side of
    the annotation and never sees it. Nothing else dray writes is a column with
    a size, so accepting it would be accepting a line that does nothing."""
    with pytest.raises(ValueError, match="not a Decimal"):

        @record(table="levy", collection="levies")
        class Levy:
            code: str = field(precision=12, scale=8)


def test_a_size_on_a_field_in_the_blob_is_refused():
    """A `Decimal` goes into the document as its own text and comes back whole,
    so a size declared there is a promise about rounding that nothing keeps —
    the same shape as asking to index a blob field, and the same answer: the
    size belongs to a column, so give the field one."""
    with pytest.raises(ValueError, match="no column to size"):

        @record(table="surcharge", collection="surcharges")
        class Surcharge:
            rate: Decimal | None = field(
                default=None, stored_in="blob", precision=12, scale=8
            )


def test_a_bulk_write_too_big_for_one_transaction_is_refused_inside_a_block(
    walkers, monkeypatch
):
    """`save_all` fits a set to the row ceiling by splitting it across
    transactions, and there is nothing to split into inside a block somebody
    opened — every chunk joins theirs. Refused up front rather than at the
    database with half of it already sent."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)
    made = [Walker(family_name=f"Walker{n}") for n in range(3)]

    with pytest.raises(ValueError, match="cannot be split"):
        with walkers.store.transaction():
            walkers.add_all(made)

    assert walkers.count() == 0


def test_one_record_carrying_more_than_a_transaction_holds_is_refused(
    store, monkeypatch
):
    """The chunk arithmetic has a floor of one record, and one record can be
    over the ceiling on its own — dividing the ceiling by a fanout larger than
    it gives nought, and the floor makes that one. So a set that cannot fit
    went to the database looking sized, and came back
    `transaction row limit exceeded` with nothing written. Inside a block the
    same case has always been refused up front; this is the other door."""
    import sys

    @record(table="depot", collection="depots")
    class Depot:
        name: str = field()

    @child(of=Depot, name="crates", table="crate")
    class Crate:
        label: str = field(default="")

    store.create(Depot, Crate)
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 4)

    depot = Depot(name="Bourke Street")
    for n in range(4):
        depot.crates.add(label=f"crate {n}")

    with pytest.raises(ValueError, match="cannot be split from its own"):
        store.depots.add(depot)

    assert store.depots.count() == 0


def test_a_record_carrying_what_a_transaction_holds_is_written(
    store, monkeypatch
):
    """The boundary, so the refusal above cannot creep. A record and its
    children that come to exactly the ceiling have nothing wrong with them."""
    import sys

    @record(table="yard", collection="yards")
    class Yard:
        name: str = field()

    @child(of=Yard, name="bins", table="bin")
    class Bin:
        label: str = field(default="")

    store.create(Yard, Bin)
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 4)

    yard = Yard(name="Bourke Street")
    for n in range(3):
        yard.bins.add(label=f"bin {n}")

    store.yards.add(yard)

    assert [each.label for each in sorted(
        yard.bins.find(), key=lambda one: one.label
    )] == ["bin 0", "bin 1", "bin 2"]


def test_a_bulk_write_that_fits_is_fine_inside_a_block(walkers, monkeypatch):
    """The guard is about splitting, not about bulk. A set that fits in one
    transaction has nothing to split and goes in with everything else."""
    import sys

    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    with walkers.store.transaction():
        walkers.add_all(
            [Walker(family_name="Hemingway"), Walker(family_name="Woolf")]
        )

    assert walkers.count() == 2


def test_everything_dray_raises_is_a_dray_error(store):
    """One name to catch when what you want is "dray said no" rather than one
    particular no. Without it every `except` lists the lot, and every list
    written today misses whatever the next version adds — so this asserts over
    what dray exports rather than over a list written here, which would have
    the same problem."""
    import dray

    named = [getattr(dray, name) for name in dray.__all__]
    raised = [
        thing
        for thing in named
        if isinstance(thing, type)
        and issubclass(thing, Exception)
        and thing is not DrayError  # the base is not one of the things raised
    ]

    assert len(raised) == 8, "a new one arrived; it wants a row on the page too"
    assert [c for c in raised if not issubclass(c, DrayError)] == []


def test_a_dray_error_is_still_what_it_always_was(store):
    """The base is a second one rather than a replacement. Code that handles a
    bad value, a failed lookup or a dead connection generally is right to catch
    these too, and should not have to know dray is underneath."""
    assert issubclass(ValidationError, ValueError)
    assert issubclass(RecordNotFound, LookupError)
    assert issubclass(DuplicateRecord, ValueError)
    assert issubclass(ConnectionLost, psycopg.OperationalError)


def test_a_clash_names_the_columns_and_not_the_key_it_just_minted(store):
    """`DuplicateRecord` used to say `Slot UUID('b2e0…') exists`, naming the id
    dray had minted a moment earlier — the one value certain not to be the
    clash. The discretised slot pattern the page recommends is where that bites
    hardest, since the whole point of it is a constraint over a pair of
    ordinary columns."""

    @record(table="held", collection="helds",
            indexes=[index("table_id", "slot_at", unique=True)])
    class Held:
        table_id: int = field(default=0)
        slot_at: str = field(default="")

    store.create(Held)
    first = store.helds.add(Held(table_id=4, slot_at="19:00"))

    with pytest.raises(DuplicateRecord) as raised:
        store.helds.add(Held(table_id=4, slot_at="19:00"))

    assert "table_id, slot_at" in str(raised.value)
    assert str(first.id) not in str(raised.value), "named a key again"
    assert raised.value.columns == ("table_id", "slot_at")
    assert raised.value.constraint == "held_table_id_slot_at"


def test_a_clash_an_index_can_not_be_read_from_still_says_which_index(store):
    """The columns come out of a sentence PostgreSQL writes, and an index over
    an expression writes it differently — `Key (lower(email))=`. Parsing that
    is not worth doing, and the fallback must not go back to naming the key,
    which is the false sentence the whole translation exists to stop."""

    @record(table="mailed", collection="maileds")
    class Mailed:
        email: str = field(default="")

    store.create(Mailed)
    store.conn.execute("create unique index mailed_lower on mailed (lower(email))")
    store.conn.commit()
    posted = store.maileds.add(Mailed(email="a@b.com"))

    with pytest.raises(DuplicateRecord) as raised:
        store.maileds.add(Mailed(email="A@B.com".lower()))

    assert "mailed_lower" in str(raised.value)
    assert str(posted.id) not in str(raised.value), "named a key again"
    assert raised.value.columns is None
    assert raised.value.constraint == "mailed_lower"
