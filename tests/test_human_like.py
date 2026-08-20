"""
The manual, run.

Stories rather than a suite of small checks: somebody joins and eventually goes,
a season of events loses a day to the weather, an import signs its own work, two
coordinators edit the same person at once, a coordinator asks what was written
today across everything, two records that have to agree go into one transaction,
and a volunteer is rostered to two things at once and the database says no. Each
builds a record up over several steps the way an application does, so what is on
the page is dray being used rather than dray being probed.

The transaction ones are this domain rather than the page's booking, which is a
restaurant and has no records here. The shapes are the page's, step for step —
what a block spans, what a failed one leaves you holding, and a service function
that cannot see whether it was wrapped.

It is a demo that happens to run under pytest, which is the cheapest way to get
a real database made and thrown away around it. The narrow tests — one
behaviour, one assertion — live in the other files.

If this file and the manual disagree, one of them is wrong.
"""

from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pytest

from dray import (
    Change,
    DuplicateRecord,
    RecordHasChanged,
    RecordNotFound,
    ValidationError,
    Write,
    after_commit,
    before_delete,
    before_save,
    check,
    child,
    clock,
    collection,
    field,
    index,
    record,
    records_change,
)

STATUSES = ("enquiry", "candidate", "volunteer", "lapsed")

# Standing in for a queue a worker reads. It is another process on another
# connection, so what it is told about has to have committed before it hears.
QUEUED: list[str] = []


def not_blank(value: str) -> None:
    """
    A validator: raise to reject, say nothing to accept.

    There is no return value to remember and no convention to look up — one that
    finishes quietly has accepted the value. A plain `ValueError` is enough;
    dray catches it and re-raises a `ValidationError` naming the field, so the
    message here says what was wrong and not where.

    It never sees `None`. A field with no value is not validated, so this needs
    no guard, and "must have a value" stays a rule of its own rather than
    something every optional field has to opt out of.

    Called on assignment, so a value that fails here never reaches the record,
    let alone the database — and never reaches `on_change` either, because
    nothing moved.
    """
    if not value.strip():
        raise ValueError("cannot be blank")


def cancellation(change: Change) -> None:
    """
    A handler of your own decides when a change is worth an entry, and what the
    entry should say. This one cares about a single transition.

    `children` is asked rather than assumed, because the same handler can be put
    on records of different kinds and not all of them need be noteable. It asks
    whether this kind of record has notes at all, which is a different question
    from whether it has any.
    """
    if change.new != "cancelled":
        return
    if "notes" in change.record.children:
        change.record.notes.add(f"Cancelled — was {change.old}.")


#
# Defining a person
#
# Three of these get columns and one does not. `family_name`, `given_names` and
# `status` are what we filter, sort and count on; `suburb` lives in a jsonb blob
# because we only ever read it back out.
#


def whoever(write: Write) -> object | None:
    """
    Whoever the write said was doing the work, exactly as it was given.

    Ours, not dray's — it has never heard the word `whom` and has no opinion
    about what a write should be told. Note what this does not do: it does not
    look at the value. A handler chooses *which* value and the field says what
    shape it takes, which is `converter=str` below.
    """
    return write.given.get("whom")


def tidy_suburb(value: str) -> str:
    """A form posts what somebody typed. This is what the field makes of it,
    wherever it came from — a parsed form, a record this file built by hand, an
    assignment, or the value a `find` is looking for."""
    return value.strip().title()


def flag_for_review(change: Change) -> None:
    """Not every handler is about recording. Somebody leaving volunteer status
    wants looking at, so this sets a field, which the same save writes."""
    if change.old == "volunteer" and change.new != "volunteer":
        change.record.needs_review = True


@record(table="person", collection="people")
class Person:
    family_name: str = field(validator=not_blank)
    given_names: str = field(default="")
    # Two handlers, run in the order listed. One records what moved and the
    # other reacts to it, and neither knows about the other.
    status: str = field(
        default="enquiry",
        choices=STATUSES,
        on_change=[records_change(into="logs"), flag_for_review],
    )
    suburb: str | None = field(default=None,
        stored_in="blob",
        converter=tidy_suburb,
        on_change=records_change(into="logs"),
    )
    # Set by a handler rather than by a person, so it names none of its own —
    # a line saying it moved would be about the bookkeeping, not the person.
    needs_review: bool = field(default=False)

    # Nobody assigns these; the write fills them in. `on_add` fires the first
    # time a record is written and `on_save` every time it is saved, so saying
    # both is what makes "created" and "updated" different declarations rather
    # than one with a caveat.
    created_at: datetime | None = field(default=None, on_add=clock)
    created_by: str | None = field(default=None, on_add=whoever, converter=str)
    updated_at: datetime | None = field(
        default=None, on_add=clock, on_save=clock
    )
    updated_by: str | None = field(default=None,
        on_add=whoever, on_save=whoever, converter=str
    )

    @before_delete
    def a_volunteer_is_lapsed_rather_than_removed(self) -> None:
        """
        What this domain says about its own records going.

        It runs inside the transaction `delete` opens, which is what makes it
        different from the same `if` written above each of the calls: there is
        one of these and there are several of those, and the day one of those
        is written without it the row is gone for good.
        """
        if self.status == "volunteer":
            raise ValueError("lapse a volunteer before deleting them")


@record(table="event", collection="events")
class Event:
    name: str = field()
    starts_on: date | None = field(default=None)
    status: str = field(default="planned", on_change=cancellation)
    suburb: str | None = field(default=None, stored_in="blob")
    # How many people this one has room for, where anybody has said. A cap is
    # a fact about the event and the rule it implies is about the rosterings,
    # which is why the field is here and the rule is on `Shift`.
    volunteers_wanted: int | None = field(default=None)

    @check
    def a_planned_event_has_a_date(self) -> None:
        """
        A rule about the record rather than about one of its values.

        A validator is handed the value and nothing else, so a rule reading two
        fields has nowhere to sit — and this is only true or false once both of
        them have been set, which is why it runs when the record is whole and
        not on the assignment that got it half way there.

        dray finds it by the decorator and never by what it is called, so the
        name is ours to spend on saying what the rule is. Raise to reject, as a
        validator does; a plain `ValueError` would do, and a `ValidationError`
        is what a caller catching dray's refusals is already looking for.
        """
        if self.status == "planned" and self.starts_on is None:
            raise ValidationError("a planned event needs a date")

    @after_commit
    def tell_the_listing_page(self) -> None:
        """
        The other kind of marked method: not a rule about the record but a step
        that has to wait for the record's rows.

        The page listing what is on is rebuilt by a worker on another
        connection, which cannot see a row this transaction has not committed.
        Queuing the job from inside the write would be a race with something
        that is not waiting for us — it would read the event as it was, or not
        find it at all — and putting it after the save only works where the
        save is not inside somebody's block. The record says it once instead.
        """
        QUEUED.append(self.name)


#
# One volunteer, one hour. A rostering is not a record of its own here — the
# hours are the rostering, and the pair being unique is what makes an overlap
# something the database refuses rather than something a caller has to look for
# first. A record and not a child of either: it is about a volunteer and an
# event both, and a child hangs off one parent.
#


@record(table="shift", collection="shifts",
        indexes=[index("person_id", "hour", unique=True)])
class Shift:
    person_id: UUID = field()
    event_id: UUID = field()
    hour: datetime = field()

    @before_save
    def the_event_has_room_for_another_volunteer(self, write) -> None:
        """
        The third kind of marked method: not a rule about the values in front
        of it and not a step waiting for the rows, but a read and a refusal
        that have to happen with the write's transaction already open.

        A cap is over a set. No field carries it, so a validator cannot be
        handed enough to judge it, and a `@check` would ask the database in a
        transaction that has committed by the time the row lands — a rule
        about a moment that has passed. This runs inside the write, so what it
        found is still true when the shift is written.

        Every door reaches it, which is the other half. `roster` below writes
        with `add_all` and never calls `Shift.save`, so the same rule written
        as a method on the class would be walked past by the one call this
        system actually rosters through.

        What it costs is a read per row, inside the transaction. Nothing at
        all where the event named no cap, which is most of them.
        """
        event = self.store.events.by_id(self.event_id)
        if event.volunteers_wanted is None:
            return
        rostered = {
            shift.person_id
            for shift in self.store.shifts.find(
                equals={"event_id": self.event_id}
            )
        }
        # Somebody already on is not a new volunteer, and the hours of one
        # `roster` call are one person — so the four rules of a four-hour
        # rostering agree with each other about a set none of them can see.
        if self.person_id in rostered:
            return
        if len(rostered) >= event.volunteers_wanted:
            raise ValueError(f"{event.name} has all the volunteers it wants")


def hours(starts_at: datetime, ends_at: datetime) -> list[datetime]:
    """The slots a period covers, half open at the end — so a shift beginning
    where the last one finished names no hour in common with it."""
    at, slots = starts_at, []
    while at < ends_at:
        slots.append(at)
        at += timedelta(hours=1)
    return slots


def roster(
    store, person: Person, event: Event, starts_at: datetime, ends_at: datetime
) -> list[Shift]:
    """One call, one transaction — `add_all` fits a set to the row ceiling and
    four hours is nowhere near it, so the rows land together or not at all."""
    return store.shifts.add_all(
        [
            Shift(person_id=person.id, event_id=event.id, hour=at)
            for at in hours(starts_at, ends_at)
        ]
    )


#
# Children. A note hangs off either kind of record; a log is the same thing with
# a change handler pointed at it.
#


# `name=` is what a parent calls them and `collection=` is what the store does.
# The second is optional and most children want nothing to do with it — it is
# for the questions that are about the children rather than about one parent's.
@child(
    of=(Person, Event),
    name="notes",
    table="note",
    collection="notes",
    order_by="written_at",
)
class Note:
    body: str = field()
    whom: str = field(default="System", converter=str)
    written_at: datetime | None = field(default=None, on_add=clock)


@child(of=Person, name="logs", table="person_log", order_by="written_at")
class PersonLogs:
    message: str = field()
    whom: str = field(default="System", converter=str)
    written_at: datetime | None = field(default=None, on_add=clock)


#
# Collections. `store.people` is the default one. When a record has vocabulary
# of its own, write it down.
#


@collection(of=Note)
class Notes:
    """A child's table is a table, so it takes vocabulary of its own in exactly
    the way a record's does."""

    def since(self, when: datetime) -> list[Note]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where written_at >= %s order by written_at",
            [when],
        )

    def on_cancelled_events(self) -> list[Note]:
        """
        Everything written about an event that was called off.

        `unheard_from` below reaches from a record's collection into the child
        table; this is the same seam the other way round, and it is the
        direction people look for and do not find. The outer select stays on
        the note table so `{self.columns}` still names a whole `Note`, and the
        events are reached in the subquery.

        `parent_type` is narrowed as well as `parent_id`, and that is not
        decoration: this table holds the notes of people and events alike, so
        an event id is only unique among events.
        """
        events = self.store.events
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.parent_type} = '{events.table}'"
            f"   and {self.parent_id} in ("
            f"     select {events.id} from {events.table}"
            "        where status = 'cancelled'"
            "   )"
            " order by written_at"
        )


@collection(of=Person)
class People:
    def unheard_from(self, since: datetime) -> list[Person]:
        """
        Volunteers nobody has written about since.

        A child table is an ordinary table, so a question about people can be
        asked in terms of their notes. Not a name in it is typed out — the
        notes' collection knows its own table and what it calls the two columns
        naming a parent, and the one holding the parent's *table* name holds
        `self.table` here. So the only parameter is the one thing that varies.

        The child is reached in a subquery and the outer select stays on one
        table, which is what keeps `{self.columns}` usable — a join proper
        cannot use it, because both tables have a key, a guard and a blob.
        """
        notes = self.store.notes
        return self.select_many(
            f"select {self.columns} from {self.table} p"
            " where p.status = 'volunteer'"
            "   and not exists ("
            f"     select 1 from {notes.table}"
            f"      where {notes.parent_type} = '{self.table}'"
            f"        and {notes.parent_id} = p.{self.id}"
            "        and written_at >= %s"
            "   )"
            " order by p.family_name",
            [since],
        )


@collection(of=Event)
class Events:
    def on(self, day: date) -> list[Event]:
        return self.find(equals={"starts_on": day})

    def upcoming(self) -> list[Event]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where status = 'planned' and starts_on >= current_date"
            " order by starts_on"
        )


@pytest.fixture
def store(store):
    """
    The store every test below uses, with the tables these records imply.

    Overrides the plain one from `conftest.py` and asks for it by the same name,
    which pytest allows: the argument is the outer fixture, the return value is
    what the tests get.
    """
    store.create(Person, Event, Note, PersonLogs, Shift)
    QUEUED.clear()
    return store


#
# One
#
# Somebody enquires, is taken on, moves house, and eventually goes. The record
# keeps an account of all of it without being asked to.
#


def test_a_person_from_enquiry_to_gone(store):
    class User:
        """
        Whoever is doing the work: the application's own idea of a person, about
        which dray knows nothing at all.

        The object goes to the write as it is. Every field that stores one says
        `converter=str`, so `__str__` here decides what lands in the column and
        no call site has to remember.
        """

        def __init__(self, username: str, name: str) -> None:
            self.username = username
            self.name = name

        def __str__(self) -> str:
            return self.username

    me = User(username="rod", name="Rod Stein")

    # An enquiry arrives as a form post, so it is parsed rather than built —
    # strictly, because a key nobody declared is a typo in the form. What was
    # typed into the suburb box was "  leura ", and the field says what to make
    # of that.
    person = store.people.add(
        Person.parse(
            {
                "family_name": "Hemingway",
                "given_names": "Ernest",
                "suburb": "  leura ",
            }
        ),
        given={"whom": me},
    )

    assert person.status == "enquiry"
    assert person.suburb == "Leura"

    # Nobody set these. `created_by` took the name the write was given and
    # `created_at` came back from the database, which computed it.
    assert person.created_by == "rod"
    assert person.created_at is not None
    # The same moment, not the same value. `clock_timestamp()` advances inside a
    # transaction — which is the whole reason it is what `clock` returns — so two
    # fields filled by one write land microseconds apart, and asserting they are
    # equal fails a couple of times in a hundred.
    assert person.updated_at - person.created_at < timedelta(milliseconds=1)

    # Records compare by value, so ids are what you want here — otherwise this
    # quietly asserts that every field survived the round trip byte for byte,
    # which is a different claim and is tested elsewhere.
    # And the same tidying on the way into the filter, so a suburb typed into a
    # search box finds the one that was typed into a form.
    found = store.people.find(
        equals={"status": "enquiry", "suburb": "  leura "}
    )
    assert [other.id for other in found] == [person.id]

    # A column and a jsonb field, set the same way and written together.
    person.status = "volunteer"
    person.suburb = "Katoomba"
    person.notes.add("Cleared to start after the June training.")
    person.save(given={"whom": me})

    # Nobody wrote either line. Both fields name a handler, so becoming a
    # volunteer and moving house each account for themselves, in the same
    # transaction as the change.
    person = store.people.by_id(person.id)
    assert [(line.message, line.whom) for line in person.logs] == [
        ("status changed from 'enquiry' to 'volunteer'.", "rod"),
        ("suburb changed from 'Leura' to 'Katoomba'.", "rod"),
    ]
    assert [(note.body, note.whom) for note in person.notes] == [
        ("Cleared to start after the June training.", "rod"),
    ]

    # `needs_review` names no handler, so nothing was written about it — and
    # nothing has set it yet either.
    assert person.needs_review is False

    # `created_by` is written once and never again; `updated_by` moves with
    # every save. Both from the same word the write was given.
    assert (person.created_by, person.updated_by) == ("rod", "rod")
    assert person.updated_at > person.created_at

    # A note that already exists is a record like any other, so correcting one
    # is a save on the note itself rather than anything to do with the person.
    [note] = person.notes
    note.body = "Cleared to start after the June training day."
    note.save()
    assert [n.body for n in store.people.by_id(person.id).notes] == [
        "Cleared to start after the June training day."
    ]

    # A log is a child like any other, so one can simply be added.
    person.logs.add("Called and confirmed the new address.")
    person.save(given={"whom": me})
    assert len(store.people.by_id(person.id).logs) == 3

    # What the rules refuse, they refuse on assignment — before the record
    # holds it, let alone the database, and before any handler hears about it.
    with pytest.raises(ValidationError):
        person.status = "voluntear"
    with pytest.raises(ValidationError):
        person.family_name = "   "
    assert store.people.by_id(person.id).status == "volunteer"
    assert len(store.people.by_id(person.id).logs) == 3

    # A note listed in one request is deleted in the next, by the id that
    # request handed out. The row already exists, so it goes now rather than
    # waiting for the parent — and the parent is still part of the statement,
    # so an id arriving from a form cannot reach somebody else's note.
    listed = [(note.id, note.body) for note in person.notes]
    [(note_id, _)] = listed
    person = store.people.by_id(person.id)
    person.notes.by_id(note_id).delete()
    assert list(store.people.by_id(person.id).notes) == []

    # Two calls in a week, and then somebody replaces both with one line that
    # says it properly. Correcting a set is `clear` and then `add`: the removal
    # happens where it is written, the additions wait for the save, and the
    # number back is how many rows went.
    person.notes.add("Rang back about the Leura working bee.")
    person.notes.add("Left a message.")
    person.save(given={"whom": me})

    person = store.people.by_id(person.id)
    assert person.notes.clear() == 2
    person.notes.add("Rang back about the working bee, and left a message.")
    person.save(given={"whom": me})
    assert [n.body for n in store.people.by_id(person.id).notes] == [
        "Rang back about the working bee, and left a message."
    ]

    # They leave the mountains and nobody has the new address. A value going
    # away is not the same event as one being replaced, and the line written
    # says which happened.
    person.suburb = None
    person.save(given={"whom": me})
    assert [line.message for line in store.people.by_id(person.id).logs][-1] == (
        "suburb of 'Katoomba' cleared."
    )

    # Somebody tidying the register tries to remove them, and the record itself
    # says no — a rule about its own removal, run inside the transaction the
    # delete opens, so the refusal leaves everything where it was.
    with pytest.raises(ValueError, match="lapse a volunteer"):
        person.delete()
    assert store.people.by_id(person.id).status == "volunteer"

    # They lapse, and two handlers fire on the one assignment: the first writes
    # the line, the second decides somebody should look at this. Neither knows
    # about the other, and `needs_review` is written by the same save.
    person.status = "lapsed"
    assert person.needs_review is True
    person.save(given={"whom": me})

    lapsed = store.people.by_id(person.id)
    assert lapsed.needs_review is True
    assert [line.message for line in lapsed.logs][-1] == (
        "status changed from 'volunteer' to 'lapsed'."
    )

    # And when they go, their history goes with them. DSQL has no foreign keys
    # and therefore no cascade of its own.
    lapsed.delete()
    with pytest.raises(RecordNotFound):
        store.people.by_id(person.id)
    assert store.conn.execute("select count(*) from person_log").fetchone()[0] == 0

    # The tidying request arrives twice, and the second one is asking by
    # identity for somebody who is not there — the same answer the read above
    # gives, rather than a second quiet success.
    with pytest.raises(RecordNotFound):
        lapsed.delete()


#
# Two
#
# A season of events, a page that lists them, and a day the weather takes.
#


def test_a_season_of_events_and_a_day_the_weather_takes(store):
    store.events.add_all(
        [
            Event(name="Blue Mountains working bee", starts_on=date(2099, 9, 14),
                  suburb="Katoomba"),
            Event(name="Spring intake day", starts_on=date(2099, 10, 2),
                  suburb="Leura"),
            Event(name="Volunteer training", starts_on=date(2099, 10, 19),
                  suburb="Penrith"),
            Event(name="Last year's thing", starts_on=date(2000, 1, 1)),
        ]
    )

    # Four written, four jobs — one for each event, queued once the rows were
    # durable rather than while they were being written.
    assert QUEUED == [
        "Blue Mountains working bee",
        "Spring intake day",
        "Volunteer training",
        "Last year's thing",
    ]
    QUEUED.clear()

    # `upcoming` is SQL the collection wrote, hydrated back into records — so a
    # page asks the question it has, and reads columns and jsonb alike.
    listing = [
        f"{event.starts_on:%d %b} · {event.name} · {event.suburb}"
        for event in store.events.upcoming()
    ]
    assert listing == [
        "14 Sep · Blue Mountains working bee · Katoomba",
        "02 Oct · Spring intake day · Leura",
        "19 Oct · Volunteer training · Penrith",
    ]

    # The forecast turns. Records come back changeable and go back as a set, in
    # one transaction — several, if the day were busier than DSQL will take.
    storms = store.events.on(date(2099, 9, 14))
    for event in storms:
        event.status = "cancelled"
        event.notes.add("Storms forecast across the Blue Mountains.")

    # Nothing converts it here — the note's `whom` is a text column and this is
    # already text. dray stores what it is handed.
    store.events.save_all(storms, given={"whom": "rod"})

    # Two lines each: one the handler queued when the status moved, one written
    # by hand. Both belong to the event they are about, so a reader next year
    # cannot tell it was cancelled alongside anything else.
    [cancelled] = store.events.on(date(2099, 9, 14))
    assert [(note.body, note.whom) for note in cancelled.notes] == [
        ("Cancelled — was planned.", "rod"),
        ("Storms forecast across the Blue Mountains.", "rod"),
    ]

    # And the page that lists what is on hears about it, once, from the record
    # rather than from whoever happened to make the change.
    assert QUEUED == ["Blue Mountains working bee"]

    # And it is gone from the page that lists what is on.
    assert "Blue Mountains" not in " ".join(
        event.name for event in store.events.upcoming()
    )

    # Somebody's good idea, typed in without a date. Neither field is wrong on
    # its own, which is exactly what no validator could have said — and the
    # write is refused rather than the page filling up with events nobody can
    # be told about.
    with pytest.raises(ValidationError, match="needs a date"):
        store.events.add(Event(name="A weekend in the Wolgan"))

    # Nothing was written, so nothing was announced.
    assert QUEUED == ["Blue Mountains working bee"]


#
# Three
#
# An import, which knows who it is before it starts and says so once.
#


def test_an_import_signs_its_own_work(store):
    store.defaults["whom"] = "System import"

    joined = datetime(2019, 3, 1, 9, 30, tzinfo=timezone.utc)
    spreadsheet = [
        {"family_name": "Shelley", "given_names": "Mary", "suburb": "Leura"},
        {"family_name": "Frankenstein", "given_names": "Victor"},
        {
            "family_name": "Stoker",
            "given_names": "Bram",
            "suburb": "Katoomba",
            "created_at": joined,
        },
    ]
    people = [Person.parse(row) for row in spreadsheet]
    for person in people:
        person.notes.add("Imported from the 2019 membership spreadsheet.")

    store.people.add_all(people)

    # Nothing in that loop mentioned who was doing the work.
    assert store.people.count() == 3
    for person in people:
        assert store.people.by_id(person.id).notes[-1].whom == "System import"

    # The spreadsheet knew when Bram joined and the write did not. `created_at`
    # names an `on_add`, and a handler fills in what nobody said rather than
    # what somebody did — otherwise an import of 2019 records is three people
    # who all joined this afternoon.
    bram = store.people.find(equals={"family_name": "Stoker"})[0]
    mary = store.people.find(equals={"family_name": "Shelley"})[0]
    assert bram.created_at == joined
    assert mary.created_at > joined

    # Victor's row gave no address, and "who did not tell us where they live" is
    # a question the same `find` asks — on the jsonb side here, and it would read
    # the same if `suburb` had a column.
    [victor] = store.people.find(equals={"suburb": None})
    assert victor.family_name == "Frankenstein"

    # He rings in with one. A value arriving for the first time reads
    # differently to whoever looks this up next year, so the line says so rather
    # than claiming it changed from nothing.
    victor.suburb = "Blackheath"
    victor.save(given={"whom": "rod"})
    assert [line.message for line in store.people.by_id(victor.id).logs] == [
        "suburb set to 'Blackheath'."
    ]

    # A row that says something the class does not declare fails here, where
    # somebody can still fix the spreadsheet.
    with pytest.raises(ValidationError) as raised:
        Person.parse({"family_name": "Dracula", "surbub": "Whitby"})
    assert "surbub" in str(raised.value)

    # So does a value of the wrong type, which is how a column mapped to the
    # wrong field arrives. It is caught before `not_blank` is called, which
    # would otherwise ask an integer for its whitespace.
    with pytest.raises(ValidationError) as raised:
        Person.parse({"family_name": 1961})
    assert "expected str" in str(raised.value)

    # A record written under a rule that has since been tightened still loads.
    # Reading is lenient where parsing is strict, or history becomes unreadable
    # the day a rule changes.
    old = Person._dray_load(
        {"family_name": "Hemingway", "status": "retired-in-1961"}
    )
    assert old.status == "retired-in-1961"


#
# Four
#
# Two coordinators with the same person open. DSQL's own concurrency control
# spans a transaction — microseconds — and a form sits open for minutes, so the
# gap between them is the application's to mind.
#


def test_two_people_with_the_same_form_open(store):
    # Built here rather than parsed, and the suburb arrives as somebody typed
    # it — the field tidies it wherever it came from, so the record this file
    # made by hand holds what a parsed one would.
    person = store.people.add(
        Person(family_name="Hemingway", suburb="  leura "), given={"whom": "rod"}
    )
    assert person.suburb == "Leura"

    # A page renders the form and hands the token out with the values.
    form = {
        "family_name": person.family_name,
        "suburb": person.suburb,
        "etag": store.people.by_id(person.id).etag,
    }

    # Jo opens the same person a minute later, changes the address and saves.
    # Their write mints a new token.
    theirs = store.people.by_id(person.id)
    theirs.suburb = "Wentworth Falls"
    theirs.save(given={"whom": "jo"})
    assert theirs.etag != form["etag"]

    # Now the first form comes back, carrying the token it was shown.
    mine = store.people.by_id(person.id)
    mine.suburb = "Katoomba"
    with pytest.raises(RecordHasChanged):
        mine.save(etag=form["etag"], given={"whom": "rod"})

    # What was rolled back is the row, not the object. `mine` still holds the
    # value it was refused, and the line `on_change` queued for that value is
    # still queued — waiting for a save that succeeds. Which is why the answer
    # to a refusal is to re-read rather than to put the field back by hand.
    assert mine.suburb == "Katoomba"
    assert [line.message for line in mine.logs] == [
        "suburb changed from 'Leura' to 'Wentworth Falls'.",
        "suburb changed from 'Wentworth Falls' to 'Katoomba'.",
    ]

    # Jo's work stands. Without the token this would have reverted it silently,
    # across every field on the form and not just the one that clashed.
    again = store.people.by_id(person.id)
    assert again.suburb == "Wentworth Falls"
    assert again.updated_by == "jo"

    # Re-read, re-apply, and the token now matches.
    mine = store.people.by_id(person.id)
    shown = mine.etag
    mine.suburb = "Katoomba"
    mine.save(etag=shown, given={"whom": "rod"})

    again = store.people.by_id(person.id)
    assert (again.suburb, again.updated_by) == ("Katoomba", "rod")
    assert again.etag != shown

    # And the person's own history has all of it, because `suburb` names a
    # handler and neither coordinator had to remember to write a line.
    assert [line.message for line in again.logs] == [
        "suburb changed from 'Leura' to 'Wentworth Falls'.",
        "suburb changed from 'Wentworth Falls' to 'Katoomba'.",
    ]


#
# Five
#
# A coordinator at the end of the day. Everything so far has been asked of one
# person or one event; this is the question that is about the notes themselves.
#


def test_a_days_notes_across_everything(store):
    started = datetime.now().astimezone() - timedelta(minutes=1)

    person = store.people.add(
        Person(family_name="Hemingway"), given={"whom": "rod"}
    )
    person.notes.add("Called about the Katoomba weekend.")
    person.save(given={"whom": "rod"})

    event = store.events.add(
        Event(name="Spring intake day", starts_on=date(2099, 10, 2))
    )
    event.notes.add("Hall booked.")
    event.save(given={"whom": "jo"})

    # Through a parent, a note set is that parent's and nobody else's. That is
    # the guard, and it has not moved.
    assert [note.body for note in person.notes] == [
        "Called about the Katoomba weekend."
    ]

    # `collection="notes"` on the child is the other door: the same table asked
    # about as a whole, answering a question no single parent can. One note
    # belongs to a person and the other to an event, and the reading does not
    # care.
    assert [note.body for note in store.notes.since(started)] == [
        "Called about the Katoomba weekend.",
        "Hall booked.",
    ]

    # Which kind of record a note hangs off is asked for by naming the class,
    # so the read follows the table's name rather than repeating it.
    people_notes = store.notes.find(parent_type=Person)
    assert [note.body for note in people_notes] == [
        "Called about the Katoomba weekend."
    ]

    # And what comes back is a record, so correcting one is its own save — and
    # the parent it belongs to sees it, because there is only ever one row.
    [booked] = store.notes.find(parent_type=Event)
    booked.body = "Hall booked and paid."
    booked.save()

    assert [note.body for note in store.events.by_id(event.id).notes] == [
        "Hall booked and paid."
    ]

    # The same seam runs the other way round: a question about notes, narrowed
    # by something true of the parents rather than by anything on the note. The
    # event is called off, which queues a line of its own, and both of its notes
    # come back where the person's — on the same table — does not.
    event = store.events.by_id(event.id)
    event.status = "cancelled"
    event.save(given={"whom": "jo"})

    assert [note.body for note in store.notes.on_cancelled_events()] == [
        "Hall booked and paid.",
        "Cancelled — was planned.",
    ]

    # The other question a coordinator has at the end of a week: who has nobody
    # written about. That one spans two tables — people, asked in terms of their
    # notes — and still comes back as `Person` records.
    hemingway = store.people.by_id(person.id)
    hemingway.status = "volunteer"
    hemingway.save(given={"whom": "rod"})

    store.people.add(
        Person(family_name="Shelley", status="volunteer"), given={"whom": "rod"}
    )
    store.people.add(Person(family_name="Stoker"), given={"whom": "rod"})

    # Hemingway was written about today. Stoker is not a volunteer. Shelley is
    # the one nobody has said anything about.
    assert [p.family_name for p in store.people.unheard_from(started)] == ["Shelley"]

    # And the list itself, which shows a number about notes it is not
    # displaying. One statement for the page rather than one per row, and the
    # two people nobody has written about come back as zero rather than being
    # absent — which is the row a `group by` drops and a template indexes into.
    everybody = store.people.find(order_by="family_name")
    counts = store.notes.counts_for(everybody)
    assert [(p.family_name, counts[p.id]) for p in everybody] == [
        ("Hemingway", 1),
        ("Shelley", 0),
        ("Stoker", 0),
    ]

    # A note queued against one of the records in hand counts too, the same as
    # it does through the parent — so a page rendered before the save agrees
    # with the objects it was rendered from.
    [listed] = [p for p in everybody if p.family_name == "Hemingway"]
    listed.notes.add("Rang back about the Leura working bee.")
    assert store.notes.counts_for(everybody)[listed.id] == 2
    assert listed.notes.count() == 2


def test_two_records_that_have_to_agree(store):
    """
    A sixth story, and the one the page's *Two collections in one transaction*
    is about: an outcome that spans two kinds of record, where half of it
    landing is worse than none of it.

    A person is cleared as a volunteer and the event they were waiting on is
    marked as staffed. Neither is much on its own; a volunteer cleared against
    an event that never took them, or an event staffed by nobody, is the sort of
    thing somebody finds a fortnight later.
    """
    person = store.people.add(
        Person(family_name="Hemingway"), given={"whom": "rod"}
    )
    event = store.events.add(
        Event(name="Katoomba working bee", starts_on=date(2026, 6, 6))
    )

    # The note explaining the change is queued on the person, so it is in the
    # transaction too without being mentioned in it.
    person.status = "volunteer"
    person.notes.add("Cleared after the June training.", whom="rod")

    QUEUED.clear()
    with store.transaction():
        person.save(given={"whom": "rod"})
        event.status = "staffed"
        event.save()
        # The event's own step waits for the block rather than for its save:
        # inside here the row is not durable, and the worker rebuilding the
        # page would be reading a transaction it cannot see.
        assert QUEUED == []

    assert QUEUED == ["Katoomba working bee"]
    assert store.people.by_id(person.id).status == "volunteer"
    assert store.events.by_id(event.id).status == "staffed"
    assert [n.body for n in store.people.by_id(person.id).notes] == [
        "Cleared after the June training."
    ]


def test_a_block_that_fails_leaves_the_work_runnable(store):
    """The other half of that section. Nothing lands, and — the part worth
    testing, because it is the part that used to be false — the records in hand
    are still the records you would need to run it again."""
    person = store.people.add(
        Person(family_name="Shelley"), given={"whom": "rod"}
    )
    was = person.etag

    person.status = "volunteer"
    person.notes.add("Cleared after the June training.", whom="rod")

    with pytest.raises(RuntimeError):
        with store.transaction():
            person.save(given={"whom": "rod"})
            raise RuntimeError("the second half of the work failed")

    assert store.people.by_id(person.id).status == "enquiry"
    assert person.etag == was
    assert [n.body for n in person.notes] == [
        "Cleared after the June training."
    ]

    # So running it again writes the whole of it, note included.
    with store.transaction():
        person.save(given={"whom": "rod"})

    again = store.people.by_id(person.id)
    assert again.status == "volunteer"
    assert [n.body for n in again.notes] == ["Cleared after the June training."]


def test_a_service_function_that_does_not_know_it_was_wrapped(store):
    """*Once it has committed*, run. The same function both ways: on its own the
    save has committed by the time the job is queued, and inside a block it has
    committed nothing at all — which is a race with a worker that is not waiting
    for you."""
    queued = []

    def clear(person: Person) -> None:
        person.status = "volunteer"
        person.save(given={"whom": "rod"})
        store.after_commit(lambda: queued.append(person.family_name))

    on_its_own = store.people.add(
        Person(family_name="Hemingway"), given={"whom": "rod"}
    )
    clear(on_its_own)
    assert queued == ["Hemingway"]

    wrapped = store.people.add(
        Person(family_name="Shelley"), given={"whom": "rod"}
    )
    with pytest.raises(RuntimeError):
        with store.transaction():
            clear(wrapped)
            assert queued == ["Hemingway"], "still waiting on the block"
            raise RuntimeError("boom")

    assert queued == ["Hemingway"], "the block rolled back, so it never ran"


def test_a_volunteer_rostered_to_two_things_at_once(store):
    """
    The last story, and the one *Indexes, and the one unique thing* is about:
    a scarce resource held over a period, which PostgreSQL would say with an
    exclusion constraint over a range and DSQL cannot be told at all.

    Without something to declare, the shape left is a `clashing()` read and
    then an `add` — two callers reading the same gap and both writing into it,
    which no amount of wrapping in a transaction fixes.
    Cutting the volunteer's day into hours takes the range out of the question,
    and the second rostering is refused by the database rather than by a read
    somebody remembered to write.

    What is asserted here is the shape rather than the race. Two writers on two
    connections is the same refusal arrived at the long way round, and on DSQL —
    which holds no locks — the road is longer still and has not been watched.
    """
    ada = store.people.add(
        Person(family_name="Lovelace"), given={"whom": "rod"}
    )
    babbage = store.people.add(
        Person(family_name="Babbage"), given={"whom": "rod"}
    )
    bee = store.events.add(
        Event(name="Katoomba working bee", starts_on=date(2026, 6, 6))
    )
    intake = store.events.add(
        Event(name="Spring intake day", starts_on=date(2026, 6, 6))
    )

    nine = datetime(2026, 6, 6, 9)
    noon = datetime(2026, 6, 6, 12)
    one = datetime(2026, 6, 6, 13)
    four = datetime(2026, 6, 6, 16)

    roster(store, ada, bee, noon, four)
    assert store.shifts.count() == 4

    # Nine to one overlaps by an hour: three free slots and then noon, which is
    # taken. The clash is the fourth row of the four, so what this asserts is
    # the rollback as much as the refusal — a write that kept what fitted would
    # have left three rows behind and a rostering that half happened.
    with pytest.raises(DuplicateRecord):
        roster(store, ada, intake, nine, one)
    assert store.shifts.count() == 4

    # Half open at the end, so the shift that finishes where the next begins
    # names no hour in common with it.
    roster(store, ada, intake, nine, noon)
    assert store.shifts.count() == 7

    # The pair is the constraint, so the same hours are somebody else's to work.
    roster(store, babbage, bee, noon, four)
    assert store.shifts.count() == 11

    # Dropping a rostering is deleting its rows — read through the unique index,
    # which `person_id` leads, and taken together in a block of our own.
    with store.transaction():
        for shift in store.shifts.find(
            equals={"person_id": ada.id, "event_id": bee.id}
        ):
            store.shifts.delete(shift)

    assert store.shifts.count() == 7
    roster(store, ada, intake, noon, four)
    assert store.shifts.count() == 11


def test_an_event_that_has_all_the_volunteers_it_wants(store):
    """
    A rule the database cannot be told and no field can carry: an event that
    wants three people takes three, and the fourth is refused.

    Every guard this file has used so far misses it. The unique index is about
    one volunteer and one hour, and these are four different people. The etag
    is about a record written twice, and each rostering is new rows. A
    `@check` would read the count in a transaction of its own, which has
    committed and stopped meaning anything by the time the shift is written.
    So the rule is a read and a refusal inside the write's own transaction,
    which is what `@before_save` is, and `roster` reaches it without knowing
    it exists.
    """
    small = store.events.add(
        Event(
            name="Leura tool library",
            starts_on=date(2026, 7, 4),
            volunteers_wanted=2,
        )
    )
    ada = store.people.add(Person(family_name="Lovelace"))
    babbage = store.people.add(Person(family_name="Babbage"))
    hopper = store.people.add(Person(family_name="Hopper"))

    nine = datetime(2026, 7, 4, 9)
    noon = datetime(2026, 7, 4, 12)

    roster(store, ada, small, nine, noon)
    roster(store, babbage, small, nine, noon)
    assert store.shifts.count() == 6

    # The rule refuses before a statement is sent, so the three hours this call
    # would have written are not there to tidy up afterwards.
    with pytest.raises(ValueError, match="all the volunteers it wants"):
        roster(store, hopper, small, nine, noon)
    assert store.shifts.count() == 6

    # Somebody already on is not a new volunteer, so more of their hours is a
    # rostering the cap has nothing to say about.
    roster(store, ada, small, noon, datetime(2026, 7, 4, 15))
    assert store.shifts.count() == 9

    # And an event that named no cap is untouched by any of it.
    open_day = store.events.add(
        Event(name="Spring intake day", starts_on=date(2026, 7, 4))
    )
    roster(store, hopper, open_day, nine, noon)
    assert store.shifts.count() == 12
