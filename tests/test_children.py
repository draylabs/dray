"""
Children: queued on a parent, written by its save, gone when it goes.
"""

import pytest

from datetime import date, datetime, timezone
from uuid import uuid4

from dray import (
    Change,
    DuplicateRecord,
    RecordNotFound,
    ValidationError,
    after_commit,
    asc,
    before_delete,
    check,
    child,
    clock,
    desc,
    describe,
    field,
    index,
    record,
    records_change,
    schema,
)

STATUSES = ("planned", "cancelled")


# A handler names the child it writes to, and only reaches it when a value
# actually moves — so it can be declared before the child exists. That is what
# keeps the record, its handler and its children out of a circular reference.
def cancellation(change: Change) -> None:
    # `children` rather than an assumption, so the same handler can go on a
    # record that takes no notes without that being a crash.
    if change.new != "cancelled":
        return
    if "notes" in change.record.children:
        change.record.notes.add(f"Cancelled — was {change.old}.")


@record(table="hiker", collection="hikers")
class Hiker:
    family_name: str = field()
    status: str = field(default="enquiry")
    suburb: str | None = field(default=None,
        stored_in="blob", on_change=records_change(into="logs")
    )
    # The same field name a note declares, which is the whole of what makes a
    # write reach both: dray has never heard the word, and two classes did.
    whom: str = field(default="System")


@record(table="outing", collection="outings")
class Outing:
    name: str = field()
    status: str = field(
        default="planned", choices=STATUSES, on_change=cancellation
    )


@child(of=(Hiker, Outing), name="notes", table="note", order_by="written_at")
class Note:
    body: str = field()
    whom: str = field(default="System")
    written_at: datetime | None = field(default=None, on_add=clock)


@child(of=Hiker, name="logs", table="hiker_log", order_by="written_at")
class HikerLog:
    message: str = field()
    whom: str = field(default="System")
    written_at: datetime | None = field(default=None, on_add=clock)


@pytest.fixture
def hikers(store):
    store.create(Hiker, Outing, Note, HikerLog)
    return store.hikers


@pytest.fixture
def outings(store):
    store.create(Hiker, Outing, Note, HikerLog)
    return store.outings


#
# Queue and flush
#


def test_nothing_is_written_until_the_parent_is_saved(hikers, store):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Called about the Katoomba weekend.")

    assert store.conn.execute("select count(*) from note").fetchone()[0] == 0
    hiker.save()
    assert store.conn.execute("select count(*) from note").fetchone()[0] == 1


def test_a_note_and_the_change_it_explains_land_together(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.status = "volunteer"
    hiker.notes.add("Cleared to start after the June training.")
    hiker.save()

    again = hikers.by_id(hiker.id)
    assert again.status == "volunteer"
    assert [note.body for note in again.notes] == [
        "Cleared to start after the June training."
    ]


def test_children_queued_before_the_parent_exists_are_written_by_add(hikers):
    hiker = Hiker(family_name="Shelley")
    hiker.notes.add("Imported from the 2019 membership spreadsheet.")
    hikers.add(hiker)

    assert len(hikers.by_id(hiker.id).notes) == 1


def test_a_child_reads_the_same_before_and_after_its_save(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Queued but not written.")
    assert len(hiker.notes) == 1
    hiker.save()
    assert len(hiker.notes) == 1


def test_a_queued_child_is_found_by_the_id_a_page_printed(hikers):
    """The same rule, through `by_id`. Everything downstream of a request is
    text, so the id a page hands out comes back as a string — and the queued
    child holds a `UUID`, which is never equal to one. The lookup was converting
    for the statement and comparing the raw argument against what was queued, so
    the fallback that exists to make a child read the same before and after its
    save was dead for every caller who had been through a form."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    note = hiker.notes.add("Corrected from the paper enrolment form.")
    printed = str(note.id)

    assert hiker.notes.by_id(printed) is note
    assert hiker.notes.by_id(note.id) is note

    # And unchanged once there is a row to find, which is the point of it.
    hiker.save()
    assert hikers.by_id(hiker.id).notes.by_id(printed).body == (
        "Corrected from the paper enrolment form."
    )


def test_a_queued_child_of_somebody_else_is_still_not_reachable(hikers):
    """The guard is the parent, and converting the id first does not loosen it."""
    mine = hikers.add(Hiker(family_name="Hemingway"))
    note = mine.notes.add("Mine.")

    theirs = hikers.add(Hiker(family_name="Shelley"))
    with pytest.raises(RecordNotFound):
        theirs.notes.by_id(str(note.id))


def test_notes_come_back_oldest_first(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for line in ("First.", "Second.", "Third."):
        hiker.notes.add(line)
        hiker.save()

    assert [note.body for note in hikers.by_id(hiker.id).notes] == [
        "First.",
        "Second.",
        "Third.",
    ]


def test_the_first_note_is_the_oldest_one(hikers):
    """A child declares its order once, so `find_first` needs no `order_by` of
    its own — which is the one place a child set says less than a collection
    and means the same thing."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for line in ("First.", "Second.", "Third."):
        hiker.notes.add(line)
    hiker.save()

    again = hikers.by_id(hiker.id)
    assert again.notes.find_first().body == "First."
    assert again.notes.find_first(equals={"body": "Second."}).body == "Second."
    assert again.notes.find_first(equals={"body": "Never written."}) is None


def test_a_queued_note_is_reachable_before_its_parent_was_ever_written():
    """A record built and not yet saved has no collection to reach a table
    through, so asking the database first would fail on the parent before ever
    looking at the child being held."""
    hiker = Hiker(family_name="Hemingway")
    note = hiker.notes.add("Queued against a record that has never been saved.")

    assert hiker.notes.by_id(note.id) is note
    with pytest.raises(RecordNotFound):
        hiker.notes.by_id(uuid4())


def test_a_queued_note_is_the_first_one_when_nothing_is_stored(hikers):
    """`find_first` reads the same before and after a save, which is why it
    builds the set rather than asking the database for one row: a `limit 1`
    could not see what is queued."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Queued and not yet written.")

    assert hiker.notes.find_first().body == "Queued and not yet written."


#
# The other door: a child written without writing its parent
#


@pytest.fixture
def packing(store):
    """A list several people add items to, which is the case the second door
    exists for. Through the parent, everybody adding an item writes the list's
    own row — and on DSQL a row two writers want is a conflict rather than a
    queue."""

    @record(table="triplist", collection="triplists")
    class TripList:
        title: str = field()

    @child(
        of=TripList, name="items", table="triplist_item", collection="trip_items"
    )
    class Item:
        label: str = field()
        whom: str = field(default="System")

    store.create(TripList, Item)
    return store, TripList, Item


def test_a_child_is_written_without_a_statement_for_its_parent(packing):
    """The only door was `list.items.add(...)` then `list.save()`, so a caller
    who wanted a row for the item was made to write a row for the list as well —
    and the list is the row everybody shares. The alternative was typing
    `parent_type="triplist"` by hand, which is dray's bookkeeping copied into an
    application and no longer right the day the record is renamed."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    was = trip.etag

    with store.watching() as seen:
        store.trip_items.add(Item(label="Gas canister"), parent=trip)

    assert [span.sql.split()[0] for span in seen] == ["insert"]
    assert "triplist_item" in seen[0].sql
    assert trip.etag == was
    assert store.triplists.by_id(trip.id).etag == was


def test_a_child_written_that_way_reads_back_through_its_parent(packing):
    """Both columns filled and filled the same way the reads resolve them, so
    the item is the parent's by every door that asks — otherwise this would be a
    write nothing could find."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    store.trip_items.add(Item(label="Gas canister"), parent=trip)

    assert [item.label for item in trip.items] == ["Gas canister"]
    assert [item.label for item in store.trip_items.find(parent=trip)] == [
        "Gas canister"
    ]


def test_a_whole_set_goes_under_one_parent(packing):
    """`add_all` takes the same argument, because forty items about one list is
    exactly the write that should not be forty statements against the list."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    store.trip_items.add_all(
        [Item(label="Gas canister"), Item(label="Head torch")], parent=trip
    )

    assert sorted(item.label for item in trip.items) == [
        "Gas canister",
        "Head torch",
    ]


def test_a_child_already_naming_another_parent_is_refused(packing):
    """`parent=` is an easier way to say which parent, not a second answer that
    argues with the record's own — and it is not a way to move a child, which
    dray settles once and never hands on. Refused where it is written, naming
    both, rather than picking one and writing a row somebody has to work out
    afterwards."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    other = store.triplists.add(TripList(title="Mount Solitary"))
    item = Item(label="Gas canister", parent_type="triplist", parent_id=other.id)

    with pytest.raises(ValueError, match=str(other.id)):
        store.trip_items.add(item, parent=trip)

    assert store.trip_items.count() == 0


def test_a_child_written_under_nobody_is_refused(packing):
    """The row this used to write is the one state `@child` says cannot exist:
    an ordinary call put it in the table, and then no read through a parent
    reached it, no parent's delete took it away, and `find` could not even ask
    for it — `parent_type=None` in a filter means *unset* and so filters on
    nothing at all. The other door has never been able to make one, since
    `list.items.add(...)` always has a list."""
    store, TripList, Item = packing

    with pytest.raises(ValueError, match="written under a parent"):
        store.trip_items.add(Item(label="Gas canister"))

    assert store.trip_items.count() == 0


def test_a_child_naming_its_parent_by_hand_is_still_written(packing):
    """The two columns are ordinary fields, and an importer parsing rows holds
    raw ids rather than the records they point at — so it has nothing to pass
    to `parent=` and must not be shut out by the refusal above."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    store.trip_items.add(
        Item(label="Gas canister", parent_type="triplist", parent_id=trip.id)
    )

    assert [each.label for each in trip.items] == ["Gas canister"]


def test_half_a_parent_is_no_parent(packing):
    """Both columns or neither. A read through a parent matches on the pair, so
    a child holding an id and no table name is as unreachable as one holding
    nothing — and it would look filled in to anybody checking one column."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))

    with pytest.raises(ValueError, match="written under a parent"):
        store.trip_items.add(Item(label="Gas canister", parent_id=trip.id))


def test_a_set_is_refused_before_any_of_it_is_written_under_nobody(packing):
    """The promise `add_all` makes about a bad value at position 4,000, kept
    for this refusal too."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))

    with pytest.raises(ValueError, match="written under a parent"):
        store.trip_items.add_all(
            [Item(label="Gas canister"), Item(label="Head torch")], parent=None
        )

    assert store.trip_items.count(parent=trip) == 0


def test_a_child_carrying_half_a_parent_is_refused_in_a_whole_sentence(
    packing,
):
    """The two columns are ordinary fields, so a record parsed from a row or
    built by hand arrives with whichever of them somebody filled. Printing both
    regardless said this Item already belongs to None <id>, which reads as a
    fault in dray rather than in the record it is complaining about."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    other = store.triplists.add(TripList(title="Mount Solitary"))

    with pytest.raises(ValueError) as raised:
        store.trip_items.add(
            Item(label="Gas canister", parent_id=other.id), parent=trip
        )

    assert "None" not in str(raised.value)
    assert str(other.id) in str(raised.value)


def test_a_child_naming_the_same_parent_is_not_an_argument(packing):
    """The two columns are ordinary fields and a caller may set them, so saying
    the same thing twice has to be allowed — a refusal here would make `parent=`
    unusable to exactly the importer that parses the columns out of a row."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    item = Item(label="Gas canister", parent_type="triplist", parent_id=trip.id)
    store.trip_items.add(item, parent=trip)

    assert [each.label for each in trip.items] == ["Gas canister"]


def test_a_set_is_refused_before_any_of_it_is_written(packing):
    """The promise `add_all` already makes about a bad value at position 4,000,
    kept for this refusal too: the parent is checked against every record before
    a column is set on the first of them."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    other = store.triplists.add(TripList(title="Mount Solitary"))
    good = Item(label="Gas canister")

    with pytest.raises(ValueError):
        store.trip_items.add_all(
            [
                good,
                Item(
                    label="Head torch",
                    parent_type="triplist",
                    parent_id=other.id,
                ),
            ],
            parent=trip,
        )

    assert store.trip_items.count() == 0
    assert good.parent_id is None


def test_a_parent_on_a_collection_of_plain_records_is_refused(packing):
    """A record that is not a child has no columns to fill, so this is a
    mistake in the call rather than a value to ignore — and ignoring it would
    write the record under nothing and say so nowhere."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))

    with pytest.raises(TypeError, match="not a child"):
        store.triplists.add(TripList(title="Mount Solitary"), parent=trip)


def test_a_parent_of_a_kind_the_child_does_not_hang_off_is_refused(packing):
    """The other door cannot make this mistake — `walker.items` is not an
    attribute, because `Item` never named `Walker` in `of=`. Taken as an
    argument it went through: the row was written under a walker, `parent_id`
    was filled with a key of a type that column was never sized for, and the
    walker's own delete left it behind, since the cascade is `of=` too. So a
    row nothing removes and nothing much reaches, from one wrong name."""
    store, TripList, Item = packing

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    store.create(Walker)
    walker = store.walkers.add(Walker(family_name="Hemingway"))

    with pytest.raises(TypeError, match="hangs off TripList, not Walker"):
        store.trip_items.add(Item(label="Gas canister"), parent=walker)

    assert store.trip_items.count(parent=walker) == 0


def test_the_taught_spelling_still_writes_the_parent(packing):
    """The second door is a second door. Queueing on the parent goes on meaning
    what it meant — the item and the change it explains in one transaction, the
    parent's own row written and its etag moved — because a note explaining a
    change is the ordinary case and this must not have quietly made it cost
    more."""
    store, TripList, Item = packing

    trip = store.triplists.add(TripList(title="Six Foot Track"))
    was = trip.etag
    trip.items.add("Gas canister")
    trip.save()

    assert [item.label for item in trip.items] == ["Gas canister"]
    assert store.triplists.by_id(trip.id).etag != was


def test_a_second_field_settles_what_the_first_one_ties(store):
    """`id` breaks the last tie and is random, so rows sharing the field they
    are ordered by come back in an order that means nothing — and a different
    one for every fresh set of them. A second name is where that gets decided."""

    @record(table="service", collection="services")
    class Service:
        name: str = ""

    @child(
        of=Service,
        name="seatings",
        table="seating",
        order_by=("starts_at", desc("label")),
    )
    class Seating:
        label: str = ""
        starts_at: datetime | None = field(default=None)

    store.create(Service, Seating)
    seven = datetime(2026, 8, 28, 19, 0)
    eight = datetime(2026, 8, 28, 20, 0)

    service = Service(name="Friday dinner")
    for label, at in (("M2", seven), ("B1", eight), ("M1", seven)):
        service.seatings.add(label, starts_at=at)
    store.services.add(service)

    # The two at seven are settled by `label` backwards; `B1` is later and
    # sorts after both however its label reads.
    assert [s.label for s in store.services.by_id(service.id).seatings] == [
        "M2",
        "M1",
        "B1",
    ]


def test_a_child_can_read_backwards(store):
    """A thread on a page wants the newest line at the top. `desc` wraps the
    one field it applies to, so directions can be mixed."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        subject: str = ""

    @child(of=Ticket, name="replies", table="reply", order_by=desc("written_at"))
    class Reply:
        message: str = ""
        written_at: datetime | None = field(default=None, on_add=clock)

    store.create(Ticket, Reply)
    ticket = store.tickets.add(Ticket(subject="Locked out"))
    for line in ("First.", "Second.", "Third."):
        ticket.replies.add(line)
        ticket.save()

    assert [r.message for r in store.tickets.by_id(ticket.id).replies] == [
        "Third.",
        "Second.",
        "First.",
    ]


def test_a_child_says_where_its_empty_ones_go_the_same_way_find_does(store):
    """One function reads both, so this is the same terms arriving by the other
    door — worth a test because a child says it once at declaration where `find`
    says it per call, and the two had no reason to stay in step beyond sharing
    the code."""

    @record(table="area", collection="areas")
    class Area:
        name: str = ""

    @child(
        of=Area,
        name="tasks",
        table="task",
        order_by=asc("due_on", nulls="first"),
    )
    class Task:
        label: str = ""
        due_on: date | None = field(default=None)

    store.create(Area, Task)
    area = Area(name="Kitchen")
    for label, due in (("paint", date(2026, 3, 14)), ("sweep", None),
                       ("mend", date(2026, 3, 1))):
        area.tasks.add(label, due_on=due)
    store.areas.add(area)

    assert [t.label for t in store.areas.by_id(area.id).tasks] == [
        "sweep",
        "mend",
        "paint",
    ]


def test_an_order_naming_a_field_the_child_does_not_have_is_refused():
    """Checked at declaration, because the statement is built once per read —
    so this would otherwise fail on every read of every child, forever."""

    @record(table="anchor", collection="anchors")
    class Anchor:
        name: str = ""

    with pytest.raises(TypeError, match="does not declare"):

        @child(of=Anchor, name="marks", table="mark", order_by=("body", "when"))
        class Mark:
            body: str = ""

    with pytest.raises(TypeError, match="stored in the blob"):

        @child(of=Anchor, name="tags", table="tag", order_by=("body", desc("at")))
        class Tag:
            body: str = ""
            at: datetime | None = field(default=None, stored_in="blob")

    with pytest.raises(TypeError, match="ordered by nothing"):

        @child(of=Anchor, name="stamps", table="stamp", order_by=())
        class Stamp:
            body: str = ""


#
# What a clash on the way in comes back as
#


def test_a_queued_child_refused_by_a_unique_index_is_a_duplicate_record(store):
    """Which exception you got read off how the row had arrived. The same pair
    of columns declared on a record raised `DuplicateRecord`; queued on a
    parent and written by its save, the identical clash arrived as psycopg's,
    so `except DuplicateRecord` around a save that carries children missed
    it."""

    @record(
        table="berth",
        collection="berths",
        indexes=[index("night", "hut", unique=True)],
    )
    class Berth:
        night: date | None = field(default=None)
        hut: str = field(default="")

    @record(table="party", collection="parties")
    class Party:
        name: str = field()

    @child(
        of=Party,
        name="bunks",
        table="bunk",
        collection="bunks",
        indexes=[index("night", "hut", unique=True)],
    )
    class Bunk:
        night: date | None = field(default=None)
        hut: str = field(default="")

    store.create(Berth, Party, Bunk)

    # The record for contrast, and unchanged: one bed in one hut on one night,
    # so the second booking is a clash the database adjudicates and dray names.
    store.berths.add(Berth(night=date(2026, 3, 14), hut="Kanangra"))
    with pytest.raises(DuplicateRecord):
        store.berths.add(Berth(night=date(2026, 3, 14), hut="Kanangra"))

    party = store.parties.add(Party(name="Hemingway"))
    party.bunks.add(night=date(2026, 3, 14), hut="Kanangra")
    party.bunks.add(night=date(2026, 3, 14), hut="Kanangra")
    with pytest.raises(DuplicateRecord) as raised:
        party.save()
    assert "Bunk" in str(raised.value)

    # And the save rolled back whole, the same as it did when the driver's
    # error was what came out of it.
    assert store.conn.execute("select count(*) from bunk").fetchone()[0] == 0


def test_the_same_child_is_refused_the_same_way_inside_a_transaction(store):
    """A block the caller opened takes the parent back with the children, so
    what the `except` around it sees is the only thing left to tell it what
    happened."""

    @record(table="camp", collection="camps")
    class Camp:
        name: str = field()

    @child(
        of=Camp,
        name="pitches",
        table="pitch",
        collection="pitches",
        indexes=[index("night", "site", unique=True)],
    )
    class Pitch:
        night: date | None = field(default=None)
        site: str = field(default="")

    store.create(Camp, Pitch)

    with pytest.raises(DuplicateRecord):
        with store.transaction():
            camp = store.camps.add(Camp(name="Hemingway"))
            camp.pitches.add(night=date(2026, 3, 14), site="Riverbank")
            camp.pitches.add(night=date(2026, 3, 14), site="Riverbank")
            camp.save()

    assert store.camps.count() == 0
    assert store.conn.execute("select count(*) from pitch").fetchone()[0] == 0


def test_a_clash_in_the_middle_of_a_set_of_children_still_names_the_child(
    store,
):
    """The second way that name could have gone missing. A parent's children go
    out as one batch now, so a hundred statements are sent before any result is
    read — and an error read off the connection rather than off the statement
    that caused it would have arrived detached from the child it belongs to,
    which is exactly what a queued child losing `DuplicateRecord` was the first
    time. In the middle rather than at the head, because a batch blamed for its
    first statement would look right at the head and be wrong everywhere
    else."""

    @record(table="lodge", collection="lodges")
    class Lodge:
        name: str = field()

    @child(
        of=Lodge,
        name="beds",
        table="bed",
        collection="beds",
        indexes=[index("night", "room", unique=True)],
    )
    class Bed:
        night: date | None = field(default=None)
        room: str = field(default="")

    store.create(Lodge, Bed)

    lodge = store.lodges.add(Lodge(name="Hemingway"))
    for n in range(10):
        lodge.beds.add(night=date(2026, 3, 14), room=f"room {n}")
    # The clash is with the sixth of them and nothing before it, so a batch
    # matching results back by position has to land on the eleventh statement
    # and no other.
    lodge.beds.add(night=date(2026, 3, 14), room="room 5")

    with pytest.raises(DuplicateRecord) as raised:
        lodge.save()
    assert raised.value.columns == ("night", "room")
    assert "Bed" in str(raised.value)

    # And the whole save rolled back, the ten good ones with the eleventh.
    assert store.conn.execute("select count(*) from bed").fetchone()[0] == 0


def test_a_child_saved_after_the_fact_leaves_the_clash_to_the_database(store):
    """The edge the manual draws, and a child sits on the same side of it as a
    record. Once a pitch has a row its save is an ordinary update through the
    collection over its table, so a unique index refusing that is psycopg's to
    report exactly as `person.save()` is — the translation is for the way in
    only, and this is what would have told us it had gone further."""
    import psycopg

    @record(table="ground", collection="grounds")
    class Ground:
        name: str = field()

    @child(
        of=Ground,
        name="sites",
        table="site",
        collection="sites",
        indexes=[index("night", "label", unique=True)],
    )
    class Site:
        night: date | None = field(default=None)
        label: str = field(default="")

    store.create(Ground, Site)

    ground = store.grounds.add(Ground(name="Hemingway"))
    ground.sites.add(night=date(2026, 3, 14), label="Riverbank")
    second = ground.sites.add(night=date(2026, 3, 14), label="Ridge")
    ground.save()

    second.label = "Riverbank"
    with pytest.raises(psycopg.errors.UniqueViolation):
        second.save()


#
# Asking about them without reading them
#


def test_counting_does_not_read_every_child(hikers):
    """`len(self.find())` built every note to return a number, which a list page
    asking once per row pays for on every row. The scope is the same either way
    and `(parent_type, parent_id)` is the index that serves it."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for line in ("First.", "Second.", "Third."):
        hiker.notes.add(line)
    hiker.save()

    again = hikers.by_id(hiker.id)
    assert again.notes.count() == 3
    assert len(again.notes) == 3


def test_a_count_includes_what_is_queued(hikers):
    """The same rule `find` follows, so a set reads the same before and after
    its parent's save."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Written and saved.")
    hiker.save()

    hiker.notes.add("Queued and not yet written.")
    assert hiker.notes.count() == 2
    hiker.save()
    assert hikers.by_id(hiker.id).notes.count() == 2


def test_finding_among_a_parents_children(hikers):
    """The conditions go into the statement rather than over a full read — a
    filter applied in Python has already paid for every row it discards."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Rod wrote this.", whom="rod")
    hiker.notes.add("Jo wrote this.", whom="jo")
    hiker.save()

    again = hikers.by_id(hiker.id)
    found = again.notes.find(equals={"whom": "rod"})
    assert [n.body for n in found] == ["Rod wrote this."]
    assert again.notes.count(equals={"whom": "rod"}) == 1
    assert again.notes.find(equals={"whom": "nobody"}) == []


def test_finding_matches_what_is_queued_too(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Written.", whom="rod")
    hiker.save()
    hiker.notes.add("Queued.", whom="rod")

    found = hiker.notes.find(equals={"whom": "rod"})
    assert [n.body for n in found] == ["Written.", "Queued."]


def test_finding_cannot_reach_another_parents_children(hikers):
    """The parent is in the statement, the same as it is in `by_id`."""
    mine = hikers.add(Hiker(family_name="Hemingway"))
    mine.notes.add("Mine.", whom="rod")
    mine.save()

    theirs = hikers.add(Hiker(family_name="Shelley"))
    theirs.notes.add("Theirs.", whom="rod")
    theirs.save()

    found = hikers.by_id(mine.id).notes.find(equals={"whom": "rod"})
    assert [n.body for n in found] == ["Mine."]
    assert hikers.by_id(mine.id).notes.count(equals={"whom": "rod"}) == 1


def test_finding_on_a_field_the_child_does_not_declare_is_refused(hikers):
    """It used to compare the attribute to `None` and hand back nothing, which
    is a misspelling answered with an empty list. A statement asks the class
    first, exactly as `Collection.find` does."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Something.")
    hiker.save()

    with pytest.raises(ValidationError, match="no field 'auhtor'"):
        hikers.by_id(hiker.id).notes.find(equals={"auhtor": "rod"})


def test_a_filter_of_the_wrong_type_is_refused_either_side_of_a_save(hikers):
    """The sharpest version of a wrong-typed filter, because a child set
    answers over two halves: the queued half matched in Python said `[]`, and
    the stored half raised `operator does not exist: text = smallint` out of
    the driver. One filter, two answers, decided by whether anybody had saved
    — which is the one thing a child set promises not to do."""
    hiker = Hiker(family_name="Hemingway")
    hiker.notes.add("Rod wrote this.", whom="rod")

    with pytest.raises(ValidationError, match="whom"):
        hiker.notes.find(equals={"whom": 7})

    hiker = hikers.add(hiker)

    with pytest.raises(ValidationError, match="whom"):
        hiker.notes.find(equals={"whom": 7})
    with pytest.raises(ValidationError, match="whom"):
        hiker.notes.count(equals={"whom": 7})

    # And the filter it was a typo for still reads both halves.
    hiker.notes.add("Queued.", whom="rod")
    assert hiker.notes.count(equals={"whom": "rod"}) == 2


def test_a_filter_on_a_child_set_lives_in_equals_too(hikers):
    """A child set has no options of its own for a filter to collide with, so
    it takes `equals` for the other reason: a question asked through a parent
    and the same one asked through the store must not be spelled two ways, or
    reading one teaches you nothing about the other."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Rod wrote this.", whom="rod")
    hiker.save()

    with pytest.raises(TypeError, match="unexpected keyword argument 'whom'"):
        hiker.notes.find(whom="rod")

    with pytest.raises(TypeError, match="unexpected keyword argument 'whom'"):
        hiker.notes.count(whom="rod")

    with pytest.raises(TypeError, match="unexpected keyword argument 'whom'"):
        hiker.notes.find_first(whom="rod")


#
# One child, many kinds of parent
#


def test_the_same_child_hangs_off_two_kinds_of_record(hikers, outings):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("A person's note.")
    hiker.save()

    outing = outings.add(Outing(name="Blue Mountains working bee"))
    outing.notes.add("An event's note.")
    outing.save()

    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["A person's note."]
    assert [n.body for n in outings.by_id(outing.id).notes] == ["An event's note."]


#
# What the parent's key makes of parent_id
#


def test_a_child_follows_a_parent_that_declared_its_own_key(store):
    """`parent_id` was a `uuid` column with `as_uuid` on it whatever the parent
    was keyed by, so every child of a record that had declared its own id was
    unusable — the pointer refused the key it was handed, and the column could
    not have held it. A migrated system is where a declared key turns up
    and is also where the child tables are."""

    @record(table="guide", collection="guides")
    class Guide:
        id: str = field(default="")            # the employee number
        family_name: str = field(default="")

    @child(of=Guide, name="shifts", table="shift", collection="shifts")
    class Shift:
        route: str = field(default="")

    assert "    parent_id text" in schema.create_table(Shift)

    store.create(Guide, Shift)
    guide = store.guides.add(Guide(id="E1207", family_name="Hemingway"))
    guide.shifts.add("Katoomba to Blackheath")
    guide.save()

    [shift] = store.guides.by_id("E1207").shifts
    assert shift.parent_id == "E1207"
    # And the other door, which reads the same column without a parent object
    # in hand to take the type from.
    assert [s.route for s in store.shifts.find(parent=guide)] == [
        "Katoomba to Blackheath"
    ]

    guide.delete()
    assert store.conn.execute("select count(*) from shift").fetchone()[0] == 0


def test_a_key_that_is_neither_text_nor_a_uuid_reaches_the_column_too(store):
    """A date is the case that says the column follows the key rather than
    falling back to a string: `parent_id date`, holding the day itself, so a
    child reads back through a parent whose id is one."""

    @record(table="day", collection="days")
    class Day:
        id: date = field(default=None)

    @child(of=Day, name="timelogs", table="timelog")
    class Timelog:
        task: str = field(default="")

    assert "    parent_id date" in schema.create_table(Timelog)

    store.create(Day, Timelog)
    day = store.days.add(Day(id=date(2026, 9, 14)))
    day.timelogs.add("Track clearing above Leura")
    day.save()

    [timelog] = store.days.by_id(date(2026, 9, 14)).timelogs
    assert timelog.parent_id == date(2026, 9, 14)


def test_parents_whose_keys_are_of_different_types_cannot_share_a_table():
    """One column holds one type, so a child table cannot point at both a day
    and a person. Refused where the declaration is, naming both parents and
    both key types — the alternative is a `create table` that builds and a
    pointer that silently will not reach half of what it is for."""

    @record(table="roster", collection="rosters")
    class Roster:
        id: date = field(default=None)

    @record(table="ranger", collection="rangers")
    class Ranger:
        name: str = field(default="")

    with pytest.raises(TypeError, match=r"Roster \(date\), Ranger \(UUID\)"):

        @child(of=(Roster, Ranger), name="marks", table="mark")
        class Mark:
            body: str = field(default="")


def test_parents_whose_keys_agree_still_share_one_table(store):
    """Which the refusal must not narrow: several parents in one table is what
    `parent_type` is for, and two records keyed the same way have nothing to
    reconcile — including two that both declared their own."""

    @record(table="depot", collection="depots")
    class Depot:
        id: str = field(default="")

    @record(table="hut", collection="huts")
    class Hut:
        id: str = field(default="")

    @child(of=(Depot, Hut), name="checks", table="inspection", collection="checks")
    class Check:
        finding: str = field(default="")

    assert "    parent_id text" in schema.create_table(Check)

    store.create(Depot, Hut, Check)
    depot = store.depots.add(Depot(id="D1"))
    depot.checks.add("Extinguisher replaced.")
    depot.save()
    hut = store.huts.add(Hut(id="H1"))
    hut.checks.add("Tank full.")
    hut.save()

    assert [c.finding for c in store.depots.by_id("D1").checks] == [
        "Extinguisher replaced."
    ]
    assert [c.finding for c in store.huts.by_id("H1").checks] == ["Tank full."]


def test_parents_whose_keys_convert_differently_cannot_share_a_table():
    """The type refusal let two parents through whose keys agreed on it and
    disagreed on how they normalise it, and the field silently took the first
    parent's — so which rule a key handed in from outside was held to came down
    to the order somebody wrote `of=` in."""

    def upper(value: str) -> str:
        return value.strip().upper()

    def lower(value: str) -> str:
        return value.strip().lower()

    @record(table="shed", collection="sheds")
    class Shed:
        id: str = field(default="", converter=upper)

    @record(table="gate", collection="gates")
    class Gate:
        id: str = field(default="", converter=lower)

    with pytest.raises(TypeError, match=r"Shed \(upper\), Gate \(lower\)"):

        @child(of=(Shed, Gate), name="faults", table="fault")
        class Fault:
            body: str = field(default="")

    # And one converter against none, which is the same disagreement written
    # by leaving the option off rather than by saying something else.
    @record(table="stile", collection="stiles")
    class Stile:
        id: str = field(default="")

    with pytest.raises(TypeError, match=r"Shed \(upper\), Stile \(none\)"):

        @child(of=(Shed, Stile), name="faults", table="fault")
        class Fault2:
            body: str = field(default="")


def test_a_child_of_something_that_is_not_a_record_is_refused():
    """It raised `AttributeError: '__dray_annotations__'` — dray's own
    bookkeeping, named at a line whose author has never heard of it — since
    `parent_id` started reading the key it points at off the parent."""

    class NotARecord:
        pass

    with pytest.raises(TypeError, match="of= takes a record class"):

        @child(of=NotARecord, name="tallies", table="tally")
        class Tally:
            body: str = field(default="")

    with pytest.raises(TypeError, match="of= takes a record class"):

        @child(of=(Hiker, "outing"), name="tallies", table="tally")
        class Tally2:
            body: str = field(default="")

    # An instance is the near miss: it answers to `__dray_table__` through its
    # class, so it would have declared and then pointed at nothing in
    # particular.
    hemingway = Hiker(family_name="Hemingway")
    with pytest.raises(TypeError, match="rather than a person"):

        @child(of=hemingway, name="tallies", table="tally")
        class Tally3:
            body: str = field(default="")


#
# A child of a child
#


@pytest.fixture
def gear(store):
    """A note with attachments, and a version of each — three generations, so
    the walk has to be a walk rather than one more level hard-coded."""

    @record(table="climb", collection="climbs")
    class Climb:
        name: str = field()

    @child(of=Climb, name="notes", table="climb_note")
    class ClimbNote:
        body: str = field()

    @child(of=ClimbNote, name="attachments", table="climb_attachment")
    class Attachment:
        filename: str = field()

    @child(of=Attachment, name="versions", table="climb_version")
    class Version:
        label: str = field()

    store.create(Climb, ClimbNote, Attachment, Version)
    return store, Climb


def rows(store, table):
    return store.conn.execute(f"select count(*) from {table}").fetchone()[0]


def tables_of(chains):
    """`_descendants` hands back chains of classes, because a delete crossing
    three tables needs each one's own answer about what its key and its parent
    columns are called. Tables are what the walk is legible as."""
    return [tuple(kind.__dray_table__ for kind in chain) for chain in chains]


def test_a_grandchild_rides_the_same_save_as_its_grandparent(gear):
    """Which is the promise children carry, and it stopped at the first
    generation: the note was written and the attachment queued on it was not,
    silently, waiting for a save of the note that might never come."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    attachment = note.attachments.add("consent-2026-03.pdf")
    attachment.versions.add("scanned")
    climb.save()

    assert rows(store, "climb_note") == 1
    assert rows(store, "climb_attachment") == 1
    assert rows(store, "climb_version") == 1


def test_a_grandchild_is_written_once(gear):
    """`_settle` cleared the first generation only, so an attachment stayed
    queued against a note that had been written — and the note's next save
    wrote it a second time."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    note.attachments.add("consent-2026-03.pdf")
    climb.save()

    note.body = "Consent form received and filed."
    note.save()

    assert rows(store, "climb_attachment") == 1


def test_a_grandchild_reads_back_through_its_own_parent(gear):
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    note.attachments.add("consent-2026-03.pdf")
    climb.save()

    [stored_note] = store.climbs.by_id(climb.id).notes
    assert [a.filename for a in stored_note.attachments] == ["consent-2026-03.pdf"]


def test_a_grandchild_can_write_itself_after_the_save(gear):
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    attachment = note.attachments.add("consent-2026-03.pdf")
    climb.save()

    attachment.filename = "consent-2026-03-signed.pdf"
    attachment.save()

    [stored_note] = store.climbs.by_id(climb.id).notes
    assert [a.filename for a in stored_note.attachments] == [
        "consent-2026-03-signed.pdf"
    ]


def test_deleting_takes_every_generation_with_it(gear):
    """There is no foreign key, so nothing in an attachment's row says which
    climb it belongs to — only which note. Reaching it means going through the
    note, and the cascade stopped one level short, leaving rows pointing at a
    note that no longer existed."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    attachment = note.attachments.add("consent-2026-03.pdf")
    attachment.versions.add("scanned")
    climb.save()

    climb.delete()

    assert rows(store, "climb") == 0
    assert rows(store, "climb_note") == 0
    assert rows(store, "climb_attachment") == 0
    assert rows(store, "climb_version") == 0


def test_deleting_a_note_takes_its_own_descendants_and_leaves_the_rest(gear):
    """The same walk from halfway down, and scoped: a note's delete takes its
    attachments and nothing belonging to the note beside it."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    kept = climb.notes.add("Kept.")
    kept.attachments.add("kept.pdf")
    going = climb.notes.add("Going.")
    going.attachments.add("going.pdf")
    climb.save()

    going.delete()

    assert rows(store, "climb_note") == 1
    assert [a.filename for a in store.climbs.by_id(climb.id).notes[0].attachments] == [
        "kept.pdf"
    ]


def test_a_transaction_is_sized_by_rows_and_not_by_records(gear):
    """A record costs its own row plus everything queued below it, so what fits
    depends on the tree and not the count. Measured, since that is what decides
    how many go in one transaction."""
    from dray.collection import _queued

    store, Climb = gear

    climb = Climb(name="Bardens Lookout")
    for n in range(3):
        note = climb.notes.add(f"Note {n}.")
        for m in range(2):
            note.attachments.add(f"{n}-{m}.pdf")

    # Three notes, six attachments — not three.
    assert len(_queued(climb)) == 9
    assert store.climbs._rows_per([climb]) == 10


def test_the_cascade_is_one_statement_per_generation():
    """Depth is the depth of the statement and nothing else about it changes,
    which is what makes the number of generations not a limit."""
    from dray.collection import _cascade

    @record(table="person", collection="cascade_people")
    class Person:
        name: str = field()

    @child(of=Person, name="notes", table="note")
    class Note:
        body: str = field()

    @child(of=Note, name="attachments", table="attachment")
    class Attachment:
        filename: str = field()

    assert _cascade((Person, Note)) == (
        "delete from note where parent_type = 'person' and parent_id in (%s)"
    )
    assert _cascade((Person, Note, Attachment)) == (
        "delete from attachment where parent_type = 'note' and parent_id in ("
        "select id from note where parent_type = 'person' and parent_id in (%s))"
    )


def test_the_descendants_of_a_record_come_back_deepest_first(gear):
    """So an attachment goes before the note it hangs off, and nothing is
    orphaned by the row that would have found it being gone."""
    from dray.collection import _descendants

    store, Climb = gear
    assert tables_of(_descendants(Climb)) == [
        ("climb", "climb_note", "climb_attachment", "climb_version"),
        ("climb", "climb_note", "climb_attachment"),
        ("climb", "climb_note"),
    ]


@pytest.fixture
def crag(store):
    """The shape a real record has, which a single chain does not exercise:
    more than one kind of child, one of them shared with a second record, and
    the branches not the same depth."""

    @record(table="crag", collection="crags")
    class Crag:
        name: str = field()

    @record(table="route", collection="routes")
    class Route:
        name: str = field()

    @child(of=(Crag, Route), name="notes", table="crag_note")
    class CragNote:
        body: str = field()

    @child(of=Crag, name="visits", table="crag_visit")
    class Visit:
        went_on: str = field()

    @child(of=CragNote, name="photos", table="crag_photo")
    class Photo:
        filename: str = field()

    store.create(Crag, Route, CragNote, Visit, Photo)
    return store, Crag, Route, Photo


def test_descendants_walks_every_branch_and_not_only_the_deepest(crag):
    """Deepest first is what matters *within* a chain, because a chain's own
    prefix is always shorter than it. Between branches the order is free, and
    what has to hold is that every branch is there at all."""
    from dray.collection import _descendants

    _, Crag, Route, Photo = crag

    assert tables_of(_descendants(Crag)) == [
        ("crag", "crag_note", "crag_photo"),
        ("crag", "crag_note"),
        ("crag", "crag_visit"),
    ]

    # A child declared against two records is below each of them, carrying its
    # own children either way — `of=` decides which records get an accessor and
    # never what the table can hold.
    assert tables_of(_descendants(Route)) == [
        ("route", "crag_note", "crag_photo"),
        ("route", "crag_note"),
    ]

    # And a record nothing hangs off has nothing below it, which is the answer
    # every ordinary delete gets.
    assert tables_of(_descendants(Photo)) == []


def test_deleting_follows_every_branch(crag):
    """A shallow branch beside a deep one is where a walk that only followed
    the longest chain would leave rows behind, with no foreign key anywhere to
    notice."""
    store, Crag, Route, _ = crag

    crag_record = Crag(name="Bardens Lookout")
    note = crag_record.notes.add("Access is through the gate.")
    note.photos.add("gate.jpg")
    crag_record.visits.add("2026-03-14")
    store.crags.add(crag_record)

    # A second record sharing the child table, which must be left alone.
    route = Route(name="Sweet Dreams")
    route.notes.add("Bolts replaced 2025.")
    store.routes.add(route)

    assert rows(store, "crag_note") == 2
    assert (rows(store, "crag_photo"), rows(store, "crag_visit")) == (1, 1)

    store.crags.by_id(crag_record.id).delete()

    assert rows(store, "crag") == 0
    assert rows(store, "crag_photo") == 0
    assert rows(store, "crag_visit") == 0
    # The route's note survives: it hangs off a different record in the same
    # table, and the cascade goes through the parent rather than over the table.
    assert rows(store, "crag_note") == 1


#
# Removing
#


def test_a_note_is_deleted_by_id(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Written in error.")
    hiker.save()

    [note] = hikers.by_id(hiker.id).notes
    hiker = hikers.by_id(hiker.id)

    # A row that already exists, so it goes now. No parent save needed.
    hiker.notes.by_id(note.id).delete()
    assert list(hikers.by_id(hiker.id).notes) == []


def test_deleting_is_scoped_to_the_parent(hikers):
    mine = hikers.add(Hiker(family_name="Hemingway"))
    mine.notes.add("Mine.")
    mine.save()
    [note] = hikers.by_id(mine.id).notes

    theirs = hikers.add(Hiker(family_name="Shelley"))
    with pytest.raises(RecordNotFound):
        theirs.notes.by_id(note.id)

    # An id from a form cannot reach somebody else's note.
    assert [n.body for n in hikers.by_id(mine.id).notes] == ["Mine."]


def test_deleting_a_note_that_is_already_gone_says_so(hikers):
    """A child goes down the same path a record does, so it answers the same
    way: the second `delete()` of the object still in hand raised nothing,
    where the `save()` beside it had always raised. The two ends of one page's
    double submit read as one success and one silence."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Written in error.")
    hiker.save()

    [note] = hikers.by_id(hiker.id).notes
    note.delete()

    with pytest.raises(RecordNotFound) as raised:
        note.delete()
    assert str(raised.value) == f"no Note {note.id!r} to delete"


def test_a_record_with_no_children_is_still_there_to_delete(hikers):
    """Only the record's own row decides. A hiker who never took a note deletes
    no note rows, and a cascade that deleted nothing must not be mistaken for a
    record that was never there — it is one statement per generation and loads
    not a row of them, so it could not tell the difference anyway."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    assert list(hiker.notes) == []

    hiker.delete()
    with pytest.raises(RecordNotFound):
        hikers.by_id(hiker.id)


def test_a_child_writes_itself_like_any_other_record(hikers):
    # It came from a store, so it knows where it lives — the same backref that
    # makes `person.save()` work.
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("As written.")
    hiker.save()

    [note] = hikers.by_id(hiker.id).notes
    note.body = "As corrected."
    note.save()
    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["As corrected."]

    note.delete()
    assert list(hikers.by_id(hiker.id).notes) == []


def test_the_note_add_handed_back_writes_itself_too(hikers):
    # The same as above without the re-read. `add` hands back the note, the
    # parent's save writes it, and from that moment it is an ordinary row — so
    # the object in hand has to be able to say so. It was left unattached, and
    # `note.save()` answered that it had never come from a store, about a note
    # whose row was sitting in the table.
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    note = hiker.notes.add("As written.")
    hiker.save()

    note.body = "As corrected."
    note.save()
    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["As corrected."]

    note.delete()
    assert list(hikers.by_id(hiker.id).notes) == []


def test_a_queued_child_is_attached_to_the_collection_written_for_it(hikers):
    # Through whatever `@collection(of=...)` declared, the same as the store
    # and the parent's own reads — not a plain one that would drop the
    # vocabulary the class was given.
    from dray.collection import _collection_for

    hiker = hikers.add(Hiker(family_name="Hemingway"))
    note = hiker.notes.add("As written.")
    hiker.save()

    assert type(note._dray_collection) is type(_collection_for(hikers.store, Note))


def test_a_queued_child_comes_back_holding_what_the_database_computed(hikers):
    # `written_at` is filled by `clock`, which is SQL for the database to
    # evaluate rather than a value — so the row has a time and the object had
    # nothing to put there until the insert said. A record gets that back; a
    # child did not, and disagreed with its own row until somebody re-read it.
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    note = hiker.notes.add("As written.")
    hiker.save()

    [stored] = hikers.by_id(hiker.id).notes
    assert note.written_at == stored.written_at


def test_deleting_a_record_takes_its_children_with_it(hikers, store):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Goes with them.")
    hiker.save()

    hiker.delete()
    assert store.conn.execute("select count(*) from note").fetchone()[0] == 0


def test_a_child_can_write_into_its_parent_as_it_goes(store):
    """The case the hook exists for. `delete` opens its own transaction, so
    "remove this note and write down what it said" cannot be wrapped from
    outside — a line written before the call is its own transaction and may end
    up the only half that landed."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="logs", table="walker_log")
    class WalkerLog:
        message: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def keep_what_it_said(self):
            walker = store.walkers.by_id(self.parent_id)
            walker.logs.add(f"mark removed: {self.body}")
            walker.save()

    store.create(Walker, WalkerLog, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("Written in error.")
    walker.save()

    [mark] = store.walkers.by_id(walker.id).marks
    mark.delete()

    walker = store.walkers.by_id(walker.id)
    assert list(walker.marks) == []
    assert [log.message for log in walker.logs] == [
        "mark removed: Written in error."
    ]


def test_a_rule_can_read_the_children_that_are_about_to_go(store):
    """It runs before any of the statements, so the tree below the record is
    still there to be asked about — which is what makes "say what this had"
    something the record itself can answer."""
    counted = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

        @before_delete
        def say_what_they_had(self):
            counted.append(len(self.marks.find()))

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("One.")
    walker.marks.add("Two.")
    walker.save()

    store.walkers.by_id(walker.id).delete()

    assert counted == [2]
    assert store.conn.execute("select count(*) from walker_mark").fetchone()[0] == 0


def test_a_cascade_does_not_run_the_rules_of_what_it_takes(store):
    """Somebody will assume it does. A delete takes each generation below the
    record with one statement and loads not a row of them, so reaching their
    hooks would mean reading the whole tree first — fifty notes becoming fifty
    reads and fifty deletes, which is the cost this shape exists to avoid. Only
    the record `delete` was called on runs its own."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def not_this_one(self):
            ran.append(self.body)
            raise ValueError("a mark of mine is never removed")

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("Goes with them.")
    walker.save()

    walker.delete()

    assert ran == []
    assert store.conn.execute("select count(*) from walker_mark").fetchone()[0] == 0

    # And it is not that the rule is inert — a mark deleted on its own account
    # refuses, which is the same rule reached through the door that loads it.
    walker = store.walkers.add(Walker(family_name="Hemingway"))
    walker.marks.add("Stays.")
    walker.save()
    [mark] = store.walkers.by_id(walker.id).marks

    with pytest.raises(ValueError, match="never removed"):
        mark.delete()
    assert ran == ["Stays."]


#
# Emptying a set
#


def test_a_set_is_emptied_by_one_statement(hikers, store):
    """The act this exists for: a generation replaced whole. There was no door
    for it, so it was `for note in hiker.notes: note.delete()` — a transaction
    and a round trip each, for a set that is going away entire."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(5):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with store.watching() as seen:
        went = hiker.notes.clear()

    assert went == 5
    assert [span.sql.split()[0] for span in seen] == ["delete"]
    assert list(hikers.by_id(hiker.id).notes) == []
    assert hikers.by_id(hiker.id).family_name == "Hemingway"


def test_clearing_a_set_takes_the_generations_below_it(gear):
    """The whole reason this is a call rather than the statement written out by
    hand. A loop that thins one generation leaves the attachments hanging off
    notes that are gone, which nothing will ever reach again — the chains come
    off the declaration and go deepest first, the same walk a delete uses."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    attachment = note.attachments.add("consent-2026-03.pdf")
    attachment.versions.add("scanned")
    climb.save()

    assert store.climbs.by_id(climb.id).notes.clear() == 1

    assert rows(store, "climb") == 1
    assert rows(store, "climb_note") == 0
    assert rows(store, "climb_attachment") == 0
    assert rows(store, "climb_version") == 0


def test_what_a_clear_hands_back_is_this_generations_rowcount(gear):
    """How many children went, not how many rows the tree under them was. It is
    free from the statement that removes them, and it is the number a caller
    replacing a generation is about to compare against what it wrote."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    for n in range(2):
        note = climb.notes.add(f"Note {n}.")
        note.attachments.add(f"{n}.pdf")
    climb.save()

    assert store.climbs.by_id(climb.id).notes.clear() == 2


def test_a_clear_reaches_this_parents_children_and_nobody_elses(crag):
    """The safety of the whole thing is that a set is bounded by one parent by
    construction — the same two columns every read of a child already carries.
    A clear that went by table would empty the notes of every crag there is."""
    store, Crag, Route, _ = crag

    mine = store.crags.add(Crag(name="Bardens Lookout"))
    mine.notes.add("Mine.")
    mine.visits.add(went_on="2026-03-14")
    mine.save()

    theirs = store.crags.add(Crag(name="Mount Piddington"))
    theirs.notes.add("Theirs.")
    theirs.save()

    route = store.routes.add(Route(name="Sweet Dreams"))
    route.notes.add("A route's, in the same table.")
    route.save()

    assert store.crags.by_id(mine.id).notes.clear() == 1

    assert [n.body for n in store.crags.by_id(theirs.id).notes] == ["Theirs."]
    assert [n.body for n in store.routes.by_id(route.id).notes] == [
        "A route's, in the same table."
    ]
    # And the crag's other kind of child, which is neither in the statement nor
    # in the chains it was built from.
    assert len(store.crags.by_id(mine.id).visits) == 1


def test_clearing_drops_what_is_queued_as_well(hikers):
    """A set is stored plus queued everywhere else on this class, and a clear
    that emptied one half would be the one method that disagreed — `find` and
    `count` would go on reporting a child that the next save was going to write
    into a generation somebody had just removed."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Written.")
    hiker.save()
    hiker.notes.add("Queued.")

    assert hiker.notes.count() == 2
    assert hiker.notes.clear() == 1
    assert hiker.notes.count() == 0
    assert hiker.notes.find() == []

    hiker.save()
    assert list(hikers.by_id(hiker.id).notes) == []


def test_clearing_and_then_adding_writes_the_new_generation(hikers):
    """Which is the order the act has: the old set goes, the new one is queued,
    and the parent's save writes it. The other order writes nothing, and reads
    that way too."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("As imported.")
    hiker.save()

    hiker = hikers.by_id(hiker.id)
    hiker.notes.clear()
    hiker.notes.add("As corrected.")
    hiker.save()

    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["As corrected."]


def test_clearing_a_set_with_nothing_in_it_is_not_an_error(hikers):
    """`Collection.delete` raises `RecordNotFound` because an id is a
    belief about one row and the raise says the belief was wrong. A set carries
    no such belief, so clearing nothing out of one is a set operation that
    worked."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    assert hikers.by_id(hiker.id).notes.clear() == 0


def test_clearing_a_set_on_a_parent_that_was_never_saved(hikers, store):
    """No collection is no table to reach, which is the state `count` already
    answers out of the queue alone. It drops what is queued and asks the
    database nothing."""
    hiker = Hiker(family_name="Shelley")
    hiker.notes.add("Queued against nobody yet.")

    with store.watching() as seen:
        assert hiker.notes.clear() == 0

    assert list(seen) == []
    assert hiker.notes.count() == 0


def test_a_childs_rule_runs_on_every_child_a_clear_removes(store):
    """The declaration decides and the caller cannot turn it off, which is what
    separates this from the `delete_all` that was refused: a call that runs the
    one rule a removal has is not a second meaning of the verb."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def say_what_it_said(self):
            ran.append(self.body)

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("One.")
    walker.marks.add("Two.")
    walker.save()

    assert store.walkers.by_id(walker.id).marks.clear() == 2

    assert sorted(ran) == ["One.", "Two."]
    assert rows(store, "walker_mark") == 0


def test_a_rule_that_refuses_leaves_the_whole_generation(store):
    """It runs inside the transaction and in front of every statement, so a set
    of forty where the fourth refuses is forty rows still there — the same
    promise `@before_delete` makes about one record, over a set."""
    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def not_the_first_one(self):
            if self.body == "One.":
                raise ValueError("a mark of mine is never removed")

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("One.")
    walker.marks.add("Two.")
    walker.save()

    with pytest.raises(ValueError, match="never removed"):
        store.walkers.by_id(walker.id).marks.clear()

    assert rows(store, "walker_mark") == 2


def test_a_class_declaring_no_rule_is_never_read(store):
    """What keeps the ordinary case one round trip whatever the size of
    the set. The declaration is asked off the class, so a set of two thousand
    children that nobody wrote a rule about costs a dictionary lookup and one
    statement."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("One.")
    walker.marks.add("Two.")
    walker.save()
    walker = store.walkers.by_id(walker.id)

    with store.watching() as seen:
        walker.marks.clear()

    assert [span.sql.split()[0] for span in seen] == ["delete"]


def test_a_clear_does_not_run_the_rules_of_the_generations_below(store):
    """`delete`'s answer about a cascade, inherited: the generations
    under the children are taken with one statement each and loaded not a row,
    so reaching their rules would mean reading the whole tree — which is the
    cost this shape exists to avoid."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

    @child(of=Mark, name="scans", table="walker_scan")
    class Scan:
        filename: str = field()

        @before_delete
        def not_this_one(self):
            ran.append(self.filename)
            raise ValueError("a scan of mine is never removed")

    store.create(Walker, Mark, Scan)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    mark = walker.marks.add("One.")
    mark.scans.add("one.pdf")
    walker.save()

    store.walkers.by_id(walker.id).marks.clear()

    assert ran == []
    assert rows(store, "walker_mark") == 0
    assert rows(store, "walker_scan") == 0


def test_a_clear_inside_a_block_is_part_of_it(hikers, store):
    """It happens where it is called, which inside a block somebody
    opened means inside their transaction like any other write — so a rollback
    leaves the generation exactly where it was."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Stays.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with pytest.raises(ValueError, match="thought better of it"):
        with store.transaction():
            hiker.notes.clear()
            raise ValueError("thought better of it")

    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["Stays."]


def test_a_rolled_back_clear_puts_the_queued_half_back(hikers, store):
    """The rows come back with the rollback, and a queue that stayed emptied
    would leave the set reading as something no transaction ever did — the same
    bookkeeping a rolled-back save already puts back."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Stored.")
    hiker.save()
    hiker.notes.add("Queued.")

    with pytest.raises(ValueError, match="thought better of it"):
        with store.transaction():
            hiker.notes.clear()
            raise ValueError("thought better of it")

    assert sorted(n.body for n in hiker.notes) == ["Queued.", "Stored."]


#
# Thinning a set that does not fit in one transaction
#


def counts(store):
    """The three generations of `gear`, so a pass can be read as a movement
    between them rather than as a number."""
    return tuple(
        rows(store, table)
        for table in ("climb_note", "climb_attachment", "climb_version")
    )


def test_a_pass_takes_no_more_rows_than_it_was_asked_for(hikers):
    """The number `at_a_time` carries is a row count and a real one, which is
    the whole argument for bounding a generation rather than the set: a caller
    choosing it is choosing what one transaction will hold."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(5):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    assert hiker.notes.thin(at_a_time=2) == 2
    assert hiker.notes.count() == 3
    assert hiker.notes.thin(at_a_time=2) == 2
    assert hiker.notes.thin(at_a_time=2) == 1
    assert hiker.notes.thin(at_a_time=2) == 0
    assert hiker.notes.count() == 0


def test_a_pass_takes_from_the_deepest_generation_that_still_has_rows(gear):
    """Deepest first is what makes stopping half way a shortened tree rather
    than a broken one. A pass that took notes while attachments hung off them
    would leave rows nothing could ever reach again, which is exactly what the
    hand-rolled loop this replaces did."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    for n in range(2):
        note = climb.notes.add(f"Note {n}.")
        for m in range(2):
            note.attachments.add(f"{n}-{m}.pdf").versions.add("scanned")
    climb.save()
    notes = store.climbs.by_id(climb.id).notes

    assert counts(store) == (2, 4, 4)
    assert notes.thin(at_a_time=3) == 3
    assert counts(store) == (2, 4, 1)
    assert notes.thin(at_a_time=3) == 1
    assert counts(store) == (2, 4, 0)
    assert notes.thin(at_a_time=3) == 3
    assert counts(store) == (2, 1, 0)


def test_looping_a_thin_empties_the_set_and_everything_under_it(gear):
    """What the loop is for, and the only promise the call makes about where it
    ends: nought means no rows left, in this generation or in any below it."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    for n in range(4):
        note = climb.notes.add(f"Note {n}.")
        for m in range(3):
            note.attachments.add(f"{n}-{m}.pdf").versions.add("scanned")
    climb.save()

    notes = store.climbs.by_id(climb.id).notes
    while notes.thin(at_a_time=5):
        pass

    assert counts(store) == (0, 0, 0)
    assert rows(store, "climb") == 1


def test_a_child_added_under_a_thinned_generation_is_still_taken(gear):
    """A pass remembers nothing, and that is the correctness of the loop rather
    than a detail of it. Draining a generation and then moving up as a phase
    would walk past a row inserted under an already-drained level and leave it
    pointing at a note that is about to go — the orphaning this call exists to
    end, rebuilt inside it."""
    store, Climb = gear

    climb = store.climbs.add(Climb(name="Bardens Lookout"))
    note = climb.notes.add("Consent form received.")
    note.attachments.add("consent.pdf")
    climb.save()

    notes = store.climbs.by_id(climb.id).notes
    assert notes.thin(at_a_time=5) == 1          # the attachment
    assert counts(store) == (1, 0, 0)

    late = store.climbs.by_id(climb.id).notes[0]
    late.attachments.add("consent-signed.pdf")
    late.save()

    while notes.thin(at_a_time=5):
        pass

    assert counts(store) == (0, 0, 0)


def test_a_pass_over_a_set_with_nothing_under_it_is_one_statement(hikers, store):
    """A generation with no children is one round trip a pass, whatever the
    size of the set — the statement per empty generation is bounded by the
    depth of the tree and there is none here to pay for."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(3):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with store.watching() as seen:
        assert hiker.notes.thin(at_a_time=2) == 2

    assert [span.sql.split()[0] for span in seen] == ["delete"]


def test_thinning_a_set_with_nothing_in_it_takes_nothing(hikers):
    """Nought is what ends the loop, so an empty set has to answer it rather
    than raise — the same reasoning that leaves `clear` without a
    `RecordNotFound`."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    assert hikers.by_id(hiker.id).notes.thin() == 0


def test_thinning_a_set_leaves_what_is_queued_alone(hikers, store):
    """Where `clear` empties the set as `find` and `count` define it, this takes
    rows — and a queued child is not one. So the loop reaching nought means no
    rows left rather than an empty set, and a child queued for the save that
    follows the loop is still there to be written."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Stored.")
    hiker.save()
    hiker.notes.add("Queued.")

    assert hiker.notes.thin(at_a_time=5) == 1
    assert [n.body for n in hiker.notes] == ["Queued."]

    hiker.save()
    assert [n.body for n in hikers.by_id(hiker.id).notes] == ["Queued."]


def test_thinning_a_set_on_a_parent_that_was_never_saved(hikers, store):
    """No collection is no table, so there are no rows to take and nothing to
    ask. The queue stays where it is, which is what keeps the loop on an unsaved
    parent one that ends rather than one that empties it."""
    hiker = Hiker(family_name="Shelley")
    hiker.notes.add("Queued against nobody yet.")

    with store.watching() as seen:
        assert hiker.notes.thin() == 0

    assert list(seen) == []
    assert hiker.notes.count() == 1


def test_a_pass_reaches_this_parents_children_and_nobody_elses(crag):
    """Bounded by one parent by construction, the same as every other read and
    write on a set — a limit applied to the table would thin somebody else's
    notes and hand back a number that looked right."""
    store, Crag, Route, _ = crag

    mine = store.crags.add(Crag(name="Bardens Lookout"))
    for n in range(3):
        mine.notes.add(f"Mine {n}.")
    mine.visits.add(went_on="2026-03-14")
    mine.save()

    theirs = store.crags.add(Crag(name="Mount Piddington"))
    theirs.notes.add("Theirs.")
    theirs.save()

    route = store.routes.add(Route(name="Sweet Dreams"))
    route.notes.add("A route's, in the same table.")
    route.save()

    notes = store.crags.by_id(mine.id).notes
    while notes.thin(at_a_time=2):
        pass

    assert [n.body for n in store.crags.by_id(theirs.id).notes] == ["Theirs."]
    assert [n.body for n in store.routes.by_id(route.id).notes] == [
        "A route's, in the same table."
    ]
    assert len(store.crags.by_id(mine.id).visits) == 1


def test_a_rule_runs_for_the_children_a_pass_takes_and_not_the_rest(store):
    """The new thing this call does that no other door in dray can: a handler
    that runs for some rows of a generation and never for the others, because
    the passes are separate transactions and the loop may stop between two of
    them."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def say_what_it_said(self):
            ran.append(self.body)

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    for n in range(3):
        walker.marks.add(f"Mark {n}.")
    walker.save()

    marks = store.walkers.by_id(walker.id).marks
    assert marks.thin(at_a_time=1) == 1

    assert len(ran) == 1
    assert rows(store, "walker_mark") == 2


def test_a_rule_that_refuses_leaves_that_pass_and_not_the_ones_before_it(store):
    """The half of `clear`'s promise a walk can keep and the half it cannot. A
    refusal rolls back the pass it refused, so those rows are still there — and
    the passes that committed before it are gone, which is the state the page
    has to describe rather than the one `clear` promises."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        # Counted rather than named, because which rows a pass takes is the one
        # thing this deliberately does not promise — a rule refusing a
        # particular body would be a test of the order the keys fell in.
        @before_delete
        def never_more_than_two(self):
            ran.append(self.body)
            if len(ran) > 2:
                raise ValueError("a mark of mine is never removed")

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    for n in range(3):
        walker.marks.add(f"Mark {n}.")
    walker.save()

    marks = store.walkers.by_id(walker.id).marks
    assert marks.thin(at_a_time=1) == 1
    assert marks.thin(at_a_time=1) == 1
    with pytest.raises(ValueError, match="never removed"):
        marks.thin(at_a_time=1)

    assert rows(store, "walker_mark") == 1


def test_a_class_declaring_no_rule_is_never_read_by_a_pass(store):
    """What keeps the ordinary pass one statement: the declaration is asked off
    the class, so a generation nobody wrote a rule about is bounded in the
    statement that removes it and not by a read in front of it."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

    store.create(Walker, Mark)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    walker.marks.add("One.")
    walker.save()
    walker = store.walkers.by_id(walker.id)

    with store.watching() as seen:
        walker.marks.thin(at_a_time=1)

    assert [span.sql.split()[0] for span in seen] == ["delete"]


def test_a_pass_does_not_run_the_rules_of_the_generations_below(store):
    """`clear`'s division, one pass at a time: the generation being asked for is
    read where the class declares a rule, and everything under it goes by a
    statement that loads not a row — so reaching their rules would mean reading
    the tree this shape exists not to read."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

    @child(of=Mark, name="scans", table="walker_scan")
    class Scan:
        filename: str = field()

        @before_delete
        def not_this_one(self):
            ran.append(self.filename)
            raise ValueError("a scan of mine is never removed")

    store.create(Walker, Mark, Scan)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    mark = walker.marks.add("One.")
    mark.scans.add("one.pdf")
    walker.save()

    marks = store.walkers.by_id(walker.id).marks
    while marks.thin(at_a_time=5):
        pass

    assert ran == []
    assert rows(store, "walker_mark") == 0
    assert rows(store, "walker_scan") == 0


def test_a_rule_writing_under_the_child_it_is_losing_orphans_nothing(store):
    """The one way a pass could leave a row nothing reaches, and the reason it
    does not. A pass gets to the children only once every generation below them
    has answered empty, so a row a rule writes under a child it is losing
    arrives *after* the statement that would have taken it — and the children's
    own removal is by id and does not cascade. So the pass cascades through the
    ids it read, which is what `clear` gets for nothing by running every rule in
    front of every statement."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @before_delete
        def file_what_it_said(self):
            self.scans.add(f"{self.body}.pdf")
            self.save()

    @child(of=Mark, name="scans", table="walker_scan")
    class Scan:
        filename: str = field()

    store.create(Walker, Mark, Scan)
    walker = store.walkers.add(Walker(family_name="Shelley"))
    for n in range(2):
        walker.marks.add(f"Mark {n}.")
    walker.save()

    marks = store.walkers.by_id(walker.id).marks
    while marks.thin(at_a_time=1):
        pass

    assert rows(store, "walker_mark") == 0
    assert rows(store, "walker_scan") == 0


def test_a_pass_bigger_than_a_transaction_holds_is_refused_where_it_is_written(
    hikers,
):
    """A pass is one transaction and cannot be split, so a number past what one
    holds describes a pass the database would never take. Answered where
    somebody wrote it rather than on the first trip out, the same as a record
    carrying more queued children than the ceiling."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    with pytest.raises(ValueError, match="more rows than one transaction"):
        hiker.notes.thin(at_a_time=2_001)

    with pytest.raises(ValueError, match="at least one row"):
        hiker.notes.thin(at_a_time=0)

    with pytest.raises(TypeError, match="a number of rows"):
        hiker.notes.thin(at_a_time="500")


def test_thinning_inside_a_block_rebuilds_the_transaction_it_escapes(
    hikers, store
):
    """Every write on a child set joins a block the caller opened and this is
    no exception — which is the trap worth knowing about, because the passes
    stop being separate transactions and the loop goes back under the one
    ceiling it was written to get past. Nothing is durable until the block
    commits, so a rollback puts every pass back."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(4):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with pytest.raises(ValueError, match="thought better of it"):
        with store.transaction():
            while hiker.notes.thin(at_a_time=1):
                pass
            raise ValueError("thought better of it")

    assert hikers.by_id(hiker.id).notes.count() == 4


def test_the_pass_that_would_take_a_block_past_the_ceiling_is_refused(
    hikers, store
):
    """Which is what the block does to the loop, said before the database says
    it: the passes are one transaction in there, so the ceiling is the loop's
    and not a pass's. dray can see that total — it is what the passes handed
    back — so this stands on the same count every other in-block refusal stands
    on, and arrives instead of `transaction row limit exceeded` from the middle
    of a loop written to avoid exactly that."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(3):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with pytest.raises(ValueError, match="passes in this block"):
        with store.transaction():
            while hiker.notes.thin(at_a_time=2_000):
                pass

    assert hikers.by_id(hiker.id).notes.count() == 3


def test_a_loop_whose_passes_fit_in_a_block_is_left_alone(hikers, store):
    """A count and not a rule about where the call may be written. A set small
    enough to be thinned inside a block is a set `clear` would have taken in
    one, so nothing is being bought here — but it works today and a flat
    refusal would take it away for a sentence nobody needed."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(4):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with store.transaction():
        while hiker.notes.thin(at_a_time=1):
            pass

    assert hikers.by_id(hiker.id).notes.count() == 0


def test_the_rows_a_block_counts_start_again_with_the_next_block(hikers, store):
    """It counts one transaction, so it goes back to nought when that one ends.
    A store outlives its blocks, and a count that carried would refuse the
    second block for rows the first one committed."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    for n in range(4):
        hiker.notes.add(f"Note {n}.")
    hiker.save()
    hiker = hikers.by_id(hiker.id)

    with store.transaction():
        assert hiker.notes.thin(at_a_time=2_000) == 4

    hiker.notes.add("Written after.")
    hiker.save()
    with store.transaction():
        assert hiker.notes.thin(at_a_time=2_000) == 1


#
# A child that says what happens once it has landed
#


def test_a_child_written_with_its_parent_is_told_that_it_landed(store):
    """A child is a record and takes the marked methods a record takes, which is
    how `check` already works. There is no save of its own to hang this on — a
    queued child is written by whatever writes its parent — so the parent's
    write is what runs it, once the whole of that write is durable. A child that
    was not told would be the odd one out for having ridden in rather than
    having been saved."""
    told = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

        @after_commit
        def say_it_landed(self):
            told.append(self.family_name)

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()

        @after_commit
        def say_it_landed(self):
            told.append(self.body)

    store.create(Walker, Mark)
    walker = Walker(family_name="Shelley")
    walker.marks.add("Rode in with them.")
    store.walkers.add(walker)

    # The parent first, because that is the order the rows were written in.
    assert told == ["Shelley", "Rode in with them."]

    # And a child saved on its own account goes through its own collection, so
    # it is told the same way rather than only when its parent is written.
    [mark] = store.walkers.by_id(walker.id).marks
    mark.body = "Written again."
    mark.save()
    assert told == ["Shelley", "Rode in with them.", "Written again."]


#
# What a bulk write checks, and when
#


def test_a_bad_child_stops_a_bulk_write_before_anything_lands(hikers, store):
    """`add_all` validates every record before the first transaction opens, so
    that a bad value cannot leave a set half written. Children were outside that
    pass: the first check on a note was inside the chunk carrying it, so a bad
    one was found on the second transaction with the first already committed —
    which is the import in the manual, one note per person, exactly."""
    people = [Hiker(family_name=f"Hiker {n}") for n in range(1200)]
    for hiker in people:
        hiker.notes.add("Imported from the 2019 membership spreadsheet.")

    # Well past the first chunk, and a body the class will not take.
    people[-1].notes.add(body=12345)

    with pytest.raises(ValidationError):
        hikers.add_all(people)

    assert store.conn.execute("select count(*) from hiker").fetchone()[0] == 0
    assert store.conn.execute("select count(*) from note").fetchone()[0] == 0


def test_the_check_sees_what_the_write_was_told(store):
    """The up-front check has to apply what the write was told before looking,
    or it refuses a child whose field the write was always going to fill. A
    store carrying an author for everything it writes is the ordinary case, and
    this would otherwise be a write that worked until it was checked."""

    def not_blank(value: str) -> None:
        if not value.strip():
            raise ValueError("cannot be blank")

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()
        whom: str = field(default="", validator=not_blank)

    store.create(Walker, Mark)
    store.defaults["whom"] = "System import"

    walker = Walker(family_name="Shelley")
    walker.marks.add("Imported.")
    store.walkers.add_all([walker])

    assert [mark.whom for mark in store.walkers.by_id(walker.id).marks] == [
        "System import"
    ]


def test_a_queued_child_carries_its_own_rules_into_the_write(store):
    """A child is a record and its rules are run where every other child's are —
    once the whole write has been worked out and before the first transaction
    opens, so a note the record refuses stops the write it was riding with
    rather than being found halfway through it."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()
        signed_off_by: str | None = field(default=None)

        @check
        def a_clearance_is_signed(self):
            if self.body.startswith("Cleared") and not self.signed_off_by:
                raise ValueError("a clearance says who gave it")

    store.create(Walker, Mark)

    walker = Walker(family_name="Shelley")
    walker.marks.add("Cleared to start.")

    with pytest.raises(ValueError, match="says who gave it"):
        store.walkers.add(walker)
    assert store.conn.execute("select count(*) from walker").fetchone()[0] == 0
    assert store.conn.execute("select count(*) from walker_mark").fetchone()[0] == 0


def test_a_queued_child_rule_reads_what_the_write_filled_in(store):
    """A child's rules used to run before its `on_add` handlers had, twice over
    — once in the up-front pass and again inside the chunk — so a rule about a
    field the write fills refused a child that was never wrong. They now run
    once, on children the write has finished with."""
    ran = []

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()
        signed_off_by: str | None = field(
            default=None, on_add=lambda write: "the clerk"
        )

        @check
        def a_clearance_is_signed(self):
            ran.append(self.body)
            if not self.signed_off_by:
                raise ValueError("a clearance says who gave it")

    store.create(Walker, Mark)

    walker = Walker(family_name="Shelley")
    walker.marks.add("Cleared to start.")
    store.walkers.add(walker)

    assert ran == ["Cleared to start."]
    assert [mark.signed_off_by for mark in store.walkers.by_id(walker.id).marks] == [
        "the clerk"
    ]


def test_a_queued_child_built_by_parse_is_judged_at_that_door_too(store):
    """A child is a record and `Note.parse(row)` is how an importer builds one,
    so a rule about what the row supplied belongs where the row is read. Judged
    only by the write, a bad row was found when its parent was saved — by which
    point the importer had attached the rest of the sheet to it."""

    @record(table="walker", collection="walkers")
    class Walker:
        family_name: str = field()

    @child(of=Walker, name="marks", table="walker_mark")
    class Mark:
        body: str = field()
        signed_off_by: str | None = field(default=None)

        @check
        def a_clearance_is_signed(self):
            if self.body.startswith("Cleared") and not self.signed_off_by:
                raise ValueError("a clearance says who gave it")

    store.create(Walker, Mark)

    with pytest.raises(ValueError, match="says who gave it"):
        Mark.parse({"body": "Cleared to start."})

    walker = Walker(family_name="Shelley")
    walker.marks.add(Mark.parse({"body": "Cleared to start.", "signed_off_by": "rod"}))
    store.walkers.add(walker)

    assert [mark.signed_off_by for mark in store.walkers.by_id(walker.id).marks] == [
        "rod"
    ]


#
# Early and late assignment
#


def test_a_child_field_takes_its_value_from_the_save(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Cleared to start.")
    hiker.save(given={"whom": "rod"})

    assert hikers.by_id(hiker.id).notes[-1].whom == "rod"


def test_the_declared_default_stands_when_nobody_says(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Membership lapsed.")
    hiker.save()

    assert hikers.by_id(hiker.id).notes[-1].whom == "System"


def test_a_store_carries_a_default_for_everything_it_writes(store):
    store.create(Hiker, Outing, Note, HikerLog)
    store.defaults["whom"] = "System import"

    hiker = Hiker(family_name="Shelley")
    hiker.notes.add("Imported from the 2019 membership spreadsheet.")
    store.hikers.add(hiker)

    assert store.hikers.by_id(hiker.id).notes[-1].whom == "System import"


def test_the_narrowest_wins(store):
    store.create(Hiker, Outing, Note, HikerLog)
    store.defaults["whom"] = "System import"

    hiker = store.hikers.add(Hiker(family_name="Hemingway"))
    hiker.notes.add("Said by the store.")
    hiker.notes.add("Said by the note itself.", whom="rod")
    hiker.save(given={"whom": "the save"})

    written = {n.body: n.whom for n in store.hikers.by_id(hiker.id).notes}
    assert written["Said by the store."] == "the save"
    assert written["Said by the note itself."] == "rod"


def test_a_record_takes_what_the_write_was_told_too(store):
    """Only the children used to listen. `add(given={"whom": "rod"})` set it on
    every note queued against a person and left the person's own `whom` on its
    declared default, with nothing said — one store, one write, two
    answers."""
    store.create(Hiker, Outing, Note, HikerLog)

    hiker = store.hikers.add(
        Hiker(family_name="Hemingway"), given={"whom": "rod"}
    )
    assert store.hikers.by_id(hiker.id).whom == "rod"


def test_a_record_that_named_its_own_beats_the_write(store):
    """The narrowest wins, on a record for the same reason it does on a note."""
    store.create(Hiker, Outing, Note, HikerLog)

    hiker = store.hikers.add(
        Hiker(family_name="Hemingway", whom="typed in"), given={"whom": "rod"}
    )
    assert store.hikers.by_id(hiker.id).whom == "typed in"


def test_a_field_assigned_after_construction_counts_as_named(store):
    """Which fields were chosen is not only what `__init__` was handed — an
    assignment is a choice too, and a write must not undo one."""
    store.create(Hiker, Outing, Note, HikerLog)

    hiker = Hiker(family_name="Hemingway")
    hiker.whom = "typed in later"
    store.hikers.add(hiker, given={"whom": "rod"})

    assert store.hikers.by_id(hiker.id).whom == "typed in later"


def test_a_record_read_back_can_still_be_filled_by_a_later_write(store):
    """A row's values were not named by whoever is saving it now. A record that
    counted everything it hydrated with as chosen would be unfillable for the
    rest of its life, which is the opposite failure."""
    store.create(Hiker, Outing, Note, HikerLog)
    hiker = store.hikers.add(
        Hiker(family_name="Hemingway"), given={"whom": "rod"}
    )

    again = store.hikers.by_id(hiker.id)
    again.family_name = "Hemingway, E."
    again.save(given={"whom": "jo"})

    assert store.hikers.by_id(hiker.id).whom == "jo"


def test_one_default_reaches_the_record_and_its_children_alike(store):
    """The import from the manual, which said this worked and did not: one key,
    said once on the store, on the person and on the note it wrote."""
    store.create(Hiker, Outing, Note, HikerLog)
    store.defaults["whom"] = "System import"

    hiker = Hiker(family_name="Shelley")
    hiker.notes.add("Imported from the 2019 membership spreadsheet.")
    store.hikers.add(hiker)

    written = store.hikers.by_id(hiker.id)
    assert (written.whom, written.notes[-1].whom) == (
        "System import",
        "System import",
    )


#
# Attaching a child you already built
#


def test_a_child_can_be_handed_over_rather_than_described(hikers):
    """`add` takes the object on a collection and took only keywords here, so a
    note parsed out of a spreadsheet could not be attached to the person it was
    for — the object went into the first declared field and the complaint came
    at the save, naming a field rather than the call."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    note = Note.parse({"body": "Imported.", "whom": "rod"})
    assert hiker.notes.add(note) is note
    hiker.save()

    assert [(n.body, n.whom) for n in hikers.by_id(hiker.id).notes] == [
        ("Imported.", "rod")
    ]


def test_a_child_handed_over_keeps_what_it_named(hikers):
    """It knows what it was told at construction, so the write fills in the rest
    and leaves alone what somebody chose — exactly as for one queued by
    keyword, and without `add` having watched it being made."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    hiker.notes.add(Note(body="Said its own.", whom="rod"))
    hiker.notes.add(Note(body="Said nothing."))
    hiker.save(given={"whom": "the save"})

    written = {n.body: n.whom for n in hikers.by_id(hiker.id).notes}
    assert written["Said its own."] == "rod"
    assert written["Said nothing."] == "the save"


def test_a_child_built_then_assigned_then_attached(hikers):
    """Three ways of choosing a value and all of them count: handed to the
    constructor, assigned afterwards, and the field nobody touched. `add` sees
    none of it happen and does not need to — the object has been keeping the
    set since it was made."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    note = Note(body="Built elsewhere.")
    note.whom = "assigned afterwards"
    hiker.notes.add(note)
    hiker.save(
        given={"whom": "the save", "status": "ignored, not a field of a note"}
    )

    assert sorted(note._dray_said) == ["body", "whom"]
    assert [(n.body, n.whom) for n in hikers.by_id(hiker.id).notes] == [
        ("Built elsewhere.", "assigned afterwards")
    ]


def test_a_child_that_named_a_filled_in_field_keeps_it(hikers):
    """The same set decides what a handler may fill as decides what the write
    may assign, so a note carrying the time it was actually written is not
    stamped with the moment the import ran. Every queued child goes through the
    same function a record does, which is why this is one rule rather than two
    that have to be kept agreeing."""
    then = datetime(2019, 3, 1, 9, 30, tzinfo=timezone.utc)
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    hiker.notes.add("Carried its own date.", written_at=then)
    hiker.notes.add("Said nothing about one.")
    hiker.save()

    written = {n.body: n.written_at for n in hikers.by_id(hiker.id).notes}
    assert written["Carried its own date."] == then
    assert written["Said nothing about one."] > then


def test_a_write_told_to_set_a_childs_derived_field_is_refused(store):
    """A child carries what the write was told, so it has to answer this the
    same way its parent does. A name refused on the record and taken on the note
    would be one write with two rules in it, and which one applied would depend
    on where the field happened to be declared."""

    @record(table="walk", collection="walks")
    class Walk:
        name: str = field(default="")

    @child(of=Walk, name="stops", table="stop")
    class Stop:
        place: str = field()
        sorting: str = field(
            default="", derived=lambda w: w.record.place.lower()
        )

    store.create(Walk, Stop)
    walk = store.walks.add(Walk(name="Grand Canyon"))
    walk.stops.add("Evans Lookout")

    with pytest.raises(ValidationError, match="derived"):
        walk.save(given={"sorting": "typed in"})

    # The parent knows nothing about `sorting`, so the refusal came off the
    # queued child rather than off the record the call was made on.
    assert "sorting" not in Walk.__dray_fields__


def test_a_child_handed_over_with_keywords_as_well_is_refused(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    with pytest.raises(TypeError, match="keywords as well"):
        hiker.notes.add(Note(body="Imported."), whom="rod")


def test_a_grandchild_can_be_handed_over_too(hikers):
    """A child is a record, so this is the same rule one generation down."""
    hiker = hikers.add(Hiker(family_name="Hemingway"))

    note = hiker.notes.add(Note(body="Consent form received."))
    hiker.save()

    assert [n.body for n in hikers.by_id(hiker.id).notes] == [
        "Consent form received."
    ]
    assert note.whom == "System"


#
# A log is a child with a change handler pointed at it
#


def test_a_tracked_field_writes_its_own_account(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway", suburb="Leura"))
    hiker.suburb = "Katoomba"
    hiker.save(given={"whom": "rod"})

    [line] = hikers.by_id(hiker.id).logs
    assert line.message == "suburb changed from 'Leura' to 'Katoomba'."
    assert line.whom == "rod"


def test_an_untracked_field_records_nothing(hikers):
    hiker = hikers.add(Hiker(family_name="Hemingway"))
    hiker.status = "volunteer"
    hiker.save()

    assert list(hikers.by_id(hiker.id).logs) == []


def test_a_handler_of_your_own_decides_what_is_worth_recording(outings):
    outing = outings.add(Outing(name="Working bee"))
    outing.status = "cancelled"
    outing.save()

    assert [n.body for n in outings.by_id(outing.id).notes] == [
        "Cancelled — was planned."
    ]


def test_a_handler_that_does_not_fire_records_nothing(outings):
    outing = outings.add(Outing(name="Working bee"))
    outing.name = "Blue Mountains working bee"
    outing.save()

    assert list(outings.by_id(outing.id).notes) == []


#
# Which children a record has at all
#


def test_children_names_every_kind_declared_for_the_record():
    hiker = Hiker(family_name="Hemingway")
    assert set(hiker.children) == {"notes", "logs"}

    outing = Outing(name="Working bee")
    assert set(outing.children) == {"notes"}


def test_children_answers_a_different_question_from_whether_there_are_any():
    hiker = Hiker(family_name="Hemingway")
    assert "notes" in hiker.children
    assert len(hiker.notes) == 0


def test_children_gives_back_the_same_set_the_attribute_does():
    hiker = Hiker(family_name="Hemingway")
    assert hiker.children["notes"] is hiker.notes


def test_a_handler_can_be_shared_by_records_that_do_not_all_take_notes(store):
    # `cancellation` guards on `children`, so putting it on a record with no
    # notes is a decision rather than a crash.
    @record(table="booking", collection="bookings")
    class Booking:
        status: str = field(default="planned", on_change=cancellation)

    store.create(Booking)
    booking = store.bookings.add(Booking())
    booking.status = "cancelled"
    booking.save()

    assert store.bookings.by_id(booking.id).status == "cancelled"


def test_a_handler_of_your_own_may_write_the_sentence_dray_would_have(store):
    """The page now tells a handler to reach for `describe`, and nothing here
    was holding the caller's half of it: every use was `records_change`'s own,
    one call deep, so an export a reader is being pointed at was pinned only by
    the thing that made it unnecessary. What it buys is the sentence *and* the
    values, which is the case `records_change` cannot serve."""

    kept = []

    def sentence_and_values(change) -> None:
        kept.append((describe(change), change.old, change.new))

    @record(table="permit", collection="permits")
    class Permit:
        status: str = field(default="applied", on_change=sentence_and_values)

    store.create(Permit)
    permit = store.permits.add(Permit())
    permit.status = "granted"

    said, was, now = kept[0]
    assert said == "status changed from 'applied' to 'granted'."
    assert (was, now) == ("applied", "granted")

    # The three cases are `describe`'s, so a handler that borrows it borrows
    # them — a value arriving reads differently from one being replaced.
    permit.status = ""
    assert kept[1][0] == "status of 'granted' cleared."


def test_records_change_says_so_when_the_child_is_not_there(store):
    from dray import records_change as records

    @record(table="ledger", collection="ledgers")
    class Ledger:
        note: str = field(default="", on_change=records(into="lgos"))

    store.create(Ledger)
    ledger = store.ledgers.add(Ledger())
    with pytest.raises(AttributeError) as raised:
        ledger.note = "typo in the child name"
    assert "lgos" in str(raised.value)


def test_records_change_still_says_so_on_a_record_with_a_children_field(store):
    """`records_change` asks what kinds of child a record has, and it asks
    under dray's own spelling — so a class that took `children` for a number of
    its own still gets told which child it misnamed, rather than a `TypeError`
    about an int not being iterable."""

    @record(table="terrace", collection="terraces")
    class Terrace:
        children: int = field(default=0)
        note: str = field(default="", on_change=records_change(into="lgos"))

    store.create(Terrace)
    terrace = store.terraces.add(Terrace(children=2))
    with pytest.raises(AttributeError) as raised:
        terrace.note = "typo in the child name"
    assert "lgos" in str(raised.value)


#
# A child with a name on the store
#


def test_a_child_is_only_reachable_through_a_parent_by_default(store):
    """Which is what almost every child wants. `name=` is the parent's word for
    them and says nothing at all about the store."""

    @record(table="walker4", collection="walker4s")
    class Walker4:
        name: str = field(default="")

    @child(of=Walker4, name="scribbles", table="scribble4")
    class Scribble4:
        body: str = field(default="")

    store.create(Walker4, Scribble4)
    assert Scribble4.__dray_collection__ is None

    with pytest.raises(AttributeError) as raised:
        store.scribbles
    assert "scribbles" in str(raised.value)


def test_a_child_that_names_a_collection_gets_one(store):
    """`collection=` is the other door: the questions that are about the
    children themselves rather than about one parent's."""

    @record(table="walker2", collection="walker2s")
    class Walker2:
        name: str = field(default="")

    @record(table="ramble", collection="rambles")
    class Ramble:
        name: str = field(default="")

    @child(
        of=(Walker2, Ramble), name="marks", table="mark2", collection="marks"
    )
    class Mark2:
        body: str = field(default="")

    store.create(Walker2, Ramble, Mark2)

    walker = store.walker2s.add(Walker2(name="Hemingway"))
    walker.marks.add("about a walker")
    walker.save()
    ramble = store.rambles.add(Ramble(name="Blue Mountains"))
    ramble.marks.add("about a ramble")
    ramble.save()

    # Through the parent, as always: only that parent's.
    assert [m.body for m in walker.marks] == ["about a walker"]

    # And across every parent, which is the point of naming one.
    assert store.marks.count() == 2
    assert [
        m.body for m in store.marks.find(equals={"parent_type": "walker2"})
    ] == ["about a walker"]

    # What comes back is a record like any other.
    found = store.marks.find(equals={"parent_type": "ramble"})[0]
    found.body = "corrected"
    found.save()
    assert [m.body for m in ramble.marks] == ["corrected"]


@pytest.fixture
def marks(store):
    """Two kinds of record, one kind of child hanging off both, and more than
    one parent of the first kind — which is the shape a read across parents has
    to be able to tell apart."""

    @record(table="walker5", collection="walker5s")
    class Walker5:
        name: str = field(default="")

    @record(table="ramble5", collection="ramble5s")
    class Ramble5:
        name: str = field(default="")

    @child(
        of=(Walker5, Ramble5), name="marks", table="mark5", collection="mark5s"
    )
    class Mark5:
        body: str = field(default="")
        kind: str = field(default="note")

    store.create(Walker5, Ramble5, Mark5)

    hemingway = store.walker5s.add(Walker5(name="Hemingway"))
    hemingway.marks.add("about one walker", kind="call")
    hemingway.save()
    shelley = store.walker5s.add(Walker5(name="Shelley"))
    shelley.marks.add("about another walker")
    shelley.save()
    ramble = store.ramble5s.add(Ramble5(name="Blue Mountains"))
    ramble.marks.add("about a ramble")
    ramble.save()

    return store, Walker5, hemingway


def test_a_childs_collection_is_read_by_parent_without_any_sql(marks):
    """`equals={"parent_type": "walker5"}` asks the same question and goes on
    working, since the parent columns are ordinary fields. What the options add
    is that neither name is typed: the table comes off the class, so a record
    renamed tomorrow moves every read with it rather than leaving a filter that
    finds nothing and says nothing about it."""
    store, Walker5, hemingway = marks

    assert [m.body for m in store.mark5s.find(parent=hemingway)] == [
        "about one walker"
    ]
    assert sorted(m.body for m in store.mark5s.find(parent_type=Walker5)) == [
        "about another walker",
        "about one walker",
    ]
    assert [
        m.body
        for m in store.mark5s.find(parent_type=Walker5, equals={"kind": "call"})
    ] == ["about one walker"]


def test_reading_by_parent_refuses_what_cannot_be_read_for_one(marks):
    """A record answers to `__dray_table__` through its class, so an instance
    handed to `parent_type=` would quietly have meant *every* walker's — which
    is the same silent widening as the string it replaced."""
    store, Walker5, hemingway = marks

    with pytest.raises(TypeError, match="not both"):
        store.mark5s.find(parent=hemingway, parent_type=Walker5)

    with pytest.raises(TypeError, match="parent's record class"):
        store.mark5s.find(parent_type="walker5")

    with pytest.raises(TypeError, match="parent's record class"):
        store.mark5s.find(parent_type=hemingway)

    with pytest.raises(TypeError, match="is not a child"):
        store.walker5s.find(parent=hemingway)


def test_a_collection_with_no_parent_refuses_to_name_the_columns(marks):
    """Both names are on every class, child or not, so a record's collection
    answered `parent_type` with the word `parent_type` — and the idiom these
    properties exist for, `f"where {c.parent_type} = %s"`, came back from the
    database saying *column does not exist* about a column nobody has."""
    store, _, _ = marks

    assert store.mark5s.parent_type == "parent_type"
    assert store.mark5s.parent_id == "parent_id"

    with pytest.raises(TypeError, match="Walker5 is not a child"):
        store.walker5s.parent_type

    with pytest.raises(TypeError, match="Walker5 is not a child"):
        store.walker5s.parent_id


def test_every_read_on_a_collection_narrows_to_a_parent_the_same_way(marks):
    """`parent=` and `parent_type=` landed on `find` alone, so the same
    question asked four ways came back scoped one way and unscoped three:
    `find(parent=hemingway)` handed back his one mark and `count(...)` would
    not take the argument at all, leaving a caller to count every mark in the
    table or write the two columns out by hand."""
    store, Walker5, hemingway = marks

    assert store.mark5s.count() == 3
    assert store.mark5s.count(parent_type=Walker5) == 2
    assert store.mark5s.count(parent=hemingway) == 1

    assert store.mark5s.find_first(parent=hemingway).body == "about one walker"
    assert (
        store.mark5s.find_first(parent_type=Walker5, order_by="body").body
        == "about another walker"
    )
    assert (
        store.mark5s.find_first(parent=hemingway, equals={"kind": "x"}) is None
    )

    # A batch at a time reaches the same records as `find` does, which is what
    # a script walking one kind of parent's children needs.
    walked = [
        m.body
        for batch in store.mark5s.in_batches(of=1, parent_type=Walker5)
        for m in batch
    ]
    assert sorted(walked) == ["about another walker", "about one walker"]
    assert [
        m.body
        for batch in store.mark5s.in_batches(parent=hemingway)
        for m in batch
    ] == ["about one walker"]


def test_reading_by_parent_refuses_the_same_things_however_it_is_asked(marks):
    """One scope shared by the four reads, rather than four signatures that
    happen to accept the same words — so a mistake is named the same way
    wherever it is made, and none of them can drift into accepting a table
    name or a record where the others take a class."""
    store, Walker5, hemingway = marks

    with pytest.raises(TypeError, match="not both"):
        store.mark5s.count(parent=hemingway, parent_type=Walker5)

    with pytest.raises(TypeError, match="parent's record class"):
        store.mark5s.find_first(parent_type="walker5")

    with pytest.raises(TypeError, match="is not a child"):
        store.walker5s.count(parent=hemingway)

    # A generator refuses on the first step rather than at the call, as it
    # already does for `of=0`.
    with pytest.raises(TypeError, match="parent's record class"):
        list(store.mark5s.in_batches(parent_type=hemingway))


def test_a_parent_column_named_outright_beats_the_scope(marks):
    """`parent=` and `equals={"parent_id": ...}` can be written in one call,
    and the explicit filter wins on every read that takes both — the same rule
    a child set has always followed, rather than a second one to remember."""
    store, Walker5, hemingway = marks
    shelley = store.walker5s.find_first(equals={"name": "Shelley"})

    for read in (
        lambda **kw: [m.body for m in store.mark5s.find(**kw)],
        lambda **kw: [store.mark5s.find_first(**kw).body],
    ):
        assert read(
            parent=hemingway, equals={"parent_id": shelley.id}
        ) == ["about another walker"]

    assert (
        store.mark5s.count(parent=hemingway, equals={"parent_id": shelley.id})
        == 1
    )


def test_a_child_set_takes_no_parent_because_it_is_already_one_parents(marks):
    """`person.notes` is scoped by construction, so `parent=` there could only
    ever name the parent it was reached through or contradict it. It stays
    absent, and the argument is refused as any other misspelt option is."""
    store, Walker5, hemingway = marks

    assert hemingway.marks.count() == 1
    assert [m.body for m in hemingway.marks.find()] == ["about one walker"]

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        hemingway.marks.find(parent=hemingway)

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        hemingway.marks.count(parent_type=Walker5)


#
# A count for a page of parents at once
#


@pytest.fixture
def tallies(store):
    """A page's worth of parents: two with children, one with none at all, and
    a second kind of record sharing the child table. The parent with nothing is
    the one this whole question is about — it is the row a `group by` drops."""

    @record(table="walker6", collection="walker6s")
    class Walker6:
        name: str = field(default="")

    @record(table="ramble6", collection="ramble6s")
    class Ramble6:
        name: str = field(default="")

    @child(
        of=(Walker6, Ramble6), name="marks", table="mark6", collection="mark6s"
    )
    class Mark6:
        body: str = field(default="")
        kind: str = field(default="note")

    store.create(Walker6, Ramble6, Mark6)

    hemingway = store.walker6s.add(Walker6(name="Hemingway"))
    hemingway.marks.add("rang about the weekend", kind="call")
    hemingway.marks.add("sent the induction pack")
    hemingway.save()
    woolf = store.walker6s.add(Walker6(name="Woolf"))
    woolf.marks.add("asked to be called back")
    woolf.save()
    nobody = store.walker6s.add(Walker6(name="Nobody"))
    ramble = store.ramble6s.add(Ramble6(name="Blue Mountains"))
    ramble.marks.add("about a ramble")
    ramble.save()

    return store, Walker6, [hemingway, woolf, nobody], ramble


def _watching(monkeypatch):
    """Every statement that reaches a cursor, in order."""
    import psycopg

    ran = []
    real = psycopg.Cursor.execute

    def watch(self, statement, params=None, **rest):
        ran.append(statement)
        return real(self, statement, params, **rest)

    monkeypatch.setattr(psycopg.Cursor, "execute", watch)
    return ran


def test_a_count_for_a_page_of_parents_is_one_statement(tallies, monkeypatch):
    """`{w.id: w.marks.count() for w in walkers}` is a round trip per row, so a
    list of two thousand parents was two thousand statements for two thousand
    numbers. The answer comes back in the order the parents were passed, which
    is the order the page is already iterating them in."""
    store, _, walkers, _ = tallies
    hemingway, woolf, nobody = walkers

    ran = _watching(monkeypatch)
    counts = store.mark6s.counts_for(walkers)

    assert counts == {hemingway.id: 2, woolf.id: 1, nobody.id: 0}
    assert list(counts) == [hemingway.id, woolf.id, nobody.id]
    assert len(ran) == 1


def test_a_parent_with_no_children_comes_back_as_zero(tallies):
    """The defect this exists for as much as the round trips do. A `group by`
    has nothing to group for a parent with no children, so the dict a caller
    builds by hand is missing exactly the rows a template will index into — a
    KeyError at render time, in the half of the page nobody tested because it
    had no data."""
    store, _, walkers, _ = tallies
    nobody = walkers[2]

    grouped = store.mark6s.select_rows(
        "select parent_id, count(*) from mark6 group by parent_id"
    )
    assert nobody.id not in {row["parent_id"] for row in grouped}

    assert store.mark6s.counts_for(walkers)[nobody.id] == 0


def test_a_count_across_parents_is_narrowed_the_way_a_child_set_is(tallies):
    """`equals` is spelled here as it is on `ChildSet.count`, because *how many
    unanswered each* is the question a summary column actually asks. The
    parents that match none of them are still in the answer."""
    store, _, walkers, _ = tallies
    hemingway, woolf, nobody = walkers

    assert store.mark6s.counts_for(walkers, equals={"kind": "call"}) == {
        hemingway.id: 1,
        woolf.id: 0,
        nobody.id: 0,
    }

    with pytest.raises(ValidationError, match="no field 'kimd'"):
        store.mark6s.counts_for(walkers, equals={"kimd": "call"})


def test_a_count_across_parents_includes_what_is_queued(tallies):
    """The rule `ChildSet.count` follows, so the same question does not answer
    differently depending on which door it came in by: a list rendered inside a
    transaction with children queued against it would otherwise show numbers
    disagreeing with the objects on the same screen."""
    store, _, walkers, _ = tallies
    hemingway, woolf, nobody = walkers

    hemingway.marks.add("queued, and not yet written")
    nobody.marks.add("queued against a parent with no rows at all")

    assert store.mark6s.counts_for(walkers) == {
        hemingway.id: 3,
        woolf.id: 1,
        nobody.id: 1,
    }
    assert hemingway.marks.count() == 3

    # And the filter reaches the queued half too, which is `_queued` doing the
    # matching in memory that the statement does in SQL.
    assert store.mark6s.counts_for(walkers, equals={"kind": "call"}) == {
        hemingway.id: 1,
        woolf.id: 0,
        nobody.id: 0,
    }

    hemingway.save()
    nobody.save()
    assert store.mark6s.counts_for(walkers) == {
        hemingway.id: 3,
        woolf.id: 1,
        nobody.id: 1,
    }


def test_a_count_across_parents_reaches_more_than_one_kind_of_them(tallies):
    """One child table holds the children of every record that declared them,
    so a list may hold both kinds — and each parent is counted under its own
    table name rather than by id alone."""
    store, _, walkers, ramble = tallies
    hemingway = walkers[0]

    assert store.mark6s.counts_for([hemingway, ramble]) == {
        hemingway.id: 2,
        ramble.id: 1,
    }


def test_two_kinds_of_parent_keyed_alike_are_not_counted_as_one(store):
    """Ids are dray's and are random, but a record can declare its own — and
    then a depot and a hut can both be `E1207`. One dict cannot answer for both
    under that key, so it is refused rather than answered with a number that is
    two parents' children added together."""

    @record(table="depot6", collection="depot6s")
    class Depot6:
        id: str = field(default="")

    @record(table="hut6", collection="hut6s")
    class Hut6:
        id: str = field(default="")

    @child(
        of=(Depot6, Hut6), name="checks", table="check6", collection="check6s"
    )
    class Check6:
        note: str = field(default="")

    store.create(Depot6, Hut6, Check6)
    depot = store.depot6s.add(Depot6(id="E1207"))
    depot.checks.add("gate needs a new latch")
    depot.save()
    hut = store.hut6s.add(Hut6(id="E1207"))
    hut.checks.add("tank low")
    hut.checks.add("stove serviced")
    hut.save()

    # Asked about one at a time, the other's children are not in the answer,
    # though both statements ask about the key they share.
    assert store.check6s.counts_for([depot]) == {"E1207": 1}
    assert store.check6s.counts_for([hut]) == {"E1207": 2}

    with pytest.raises(ValueError, match="one dict cannot answer for both"):
        store.check6s.counts_for([depot, hut])


def test_counting_across_parents_refuses_what_it_cannot_count_for(tallies):
    """Records rather than ids, which is the whole of why `parent_type` never
    has to be written down by a caller — and a collection whose record is not a
    child has no parents to be asked about at all."""
    store, _, walkers, _ = tallies

    with pytest.raises(TypeError, match="is not a child"):
        store.walker6s.counts_for(walkers)

    with pytest.raises(TypeError, match="records the children hang off"):
        store.mark6s.counts_for([walker.id for walker in walkers])


def test_asking_about_no_parents_answers_without_asking(tallies, monkeypatch):
    """A page with nothing on it is an ordinary page. Nothing to ask about is
    an empty answer rather than an empty statement, and rather than a `where`
    with nothing in it counting the whole table."""
    store, *_ = tallies

    ran = _watching(monkeypatch)
    assert store.mark6s.counts_for([]) == {}
    assert ran == []


def test_a_named_child_takes_a_collection_class_of_its_own(store):
    from dray import collection

    @record(table="walker3", collection="walker3s")
    class Walker3:
        name: str = field(default="")

    @child(of=Walker3, name="stamps", table="stamp3", collection="stamps")
    class Stamp3:
        body: str = field(default="")

    @collection(of=Stamp3)
    class Stamps:
        def shouting(self) -> list:
            return self.select_many(
                f"select {self.columns} from {self.table}"
                " where body = upper(body)"
            )

    store.create(Walker3, Stamp3)
    walker = store.walker3s.add(Walker3(name="Shelley"))
    walker.stamps.add("quiet")
    walker.stamps.add("LOUD")
    walker.save()

    assert type(store.stamps).__name__ == "Stamps"
    assert [s.body for s in store.stamps.shouting()] == ["LOUD"]


def test_a_write_converts_what_it_was_given(store):
    """`save(given={"whom": me})` hands over whatever the application calls a
    person. The field says what to make of one, so no call site has to."""

    class User:
        def __init__(self, username: str) -> None:
            self.username = username

        def __str__(self) -> str:
            return self.username

    @record(table="walker5", collection="walker5s")
    class Walker5:
        name: str = field(default="")

    @child(of=Walker5, name="marks", table="mark5")
    class Mark5:
        body: str = field(default="")
        whom: str = field(default="System", converter=str)

    store.create(Walker5, Mark5)
    walker = store.walker5s.add(Walker5(name="Hemingway"))
    walker.marks.add("as written")
    walker.save(given={"whom": User("rod")})

    assert store.walker5s.by_id(walker.id).marks[0].whom == "rod"
