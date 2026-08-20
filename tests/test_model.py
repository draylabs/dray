"""
The declaration layer, with no database anywhere near it.

Everything here is about what a class becomes when `@record` is applied to it:
which fields earn columns, what they will accept, and when `on_change` fires.
"""

import ast
import pathlib
from collections.abc import Callable
from datetime import date
from enum import Enum

import pytest

from uuid import uuid4

from dray import Change, Record, ValidationError, check, field, record
from dray import model
from dray.model import BLOB_COLUMN

STATUSES = ("enquiry", "candidate", "volunteer", "lapsed")


def not_blank(value: str) -> None:
    if not value.strip():
        raise ValueError("cannot be blank")


@record(table="person", collection="people")
class Person:
    family_name: str = field(validator=not_blank)
    given_names: str = field(default="")
    status: str = field(default="enquiry", choices=STATUSES)
    suburb: str | None = field(default=None, stored_in="blob")


#
# What the declaration produces
#


def test_columns_and_blob_are_separated():
    assert Person.__dray_blob__ == ("suburb",)
    assert "family_name" in Person.__dray_columns__
    assert "suburb" not in Person.__dray_columns__


def test_a_record_gets_an_id_and_nothing_else():
    # An id is the one thing added rather than declared: a record without one
    # cannot be found again, and there is nothing to decide. Timestamps are a
    # decision, so they are not here unless the class asked for them.
    person = Person(family_name="Hemingway")
    assert person.id
    assert "id" in Person.__dray_columns__
    assert "created_at" not in Person.__dray_columns__
    assert "updated_at" not in Person.__dray_columns__


def test_ids_are_not_shared():
    assert Person(family_name="Hemingway").id != Person(family_name="Shelley").id


def test_blob_holds_only_the_fields_without_columns():
    person = Person(family_name="Hemingway", suburb="Leura")
    assert person._dray_blob() == {"suburb": "Leura"}


#
# Assignment is the one interception point
#


def test_a_value_outside_choices_is_refused():
    person = Person(family_name="Hemingway")
    with pytest.raises(ValidationError) as raised:
        person.status = "voluntear"
    assert "voluntear" in str(raised.value)
    assert person.status == "enquiry"


def test_a_validator_refuses_by_raising():
    person = Person(family_name="Hemingway")
    with pytest.raises(ValidationError):
        person.family_name = "   "


def test_an_undeclared_field_is_refused():
    person = Person(family_name="Hemingway")
    with pytest.raises(AttributeError):
        person.postcode = "2780"


def test_a_field_spelled_with_a_leading_underscore_is_still_a_field():
    """Every underscored name went straight to the object, so a declared
    `_ref` was a real column that was never converted, never validated and
    never reported to `on_change` — a field managed everywhere except at the
    one door assignment comes through. dray's own names wear `_dray_`, and one
    underscore is a spelling rather than a request to be left alone."""
    changes: list[Change] = []

    @record(table="marker", collection="markers")
    class Marker:
        _ref: str = field(
            default="north",
            converter=str.strip,
            choices=("north", "south"),
            on_change=seen(changes),
        )

    marker = Marker()
    marker._ref = "  south  "

    assert marker._ref == "south"
    assert [(c.field_name, c.old, c.new) for c in changes] == [
        ("_ref", "north", "south")
    ]
    # Assigned, so a write may no longer fill it in.
    assert "_ref" in marker._dray_said

    with pytest.raises(ValidationError):
        marker._ref = "east"
    assert marker._ref == "south"


def test_a_key_spelled_with_a_leading_underscore_cannot_be_moved_either():
    """The refusal that protects the hash was reached by name, so a class
    keying itself on `_id` could point an object at somebody else's row and
    save over it."""

    @record(table="marker", collection="markers", key="_id")
    class Marker:
        _id: str = field(default="k3Jf9")

    with pytest.raises(AttributeError, match="cannot be changed"):
        Marker()._id = "somebody-elses"


def test_a_name_the_class_never_declared_is_the_callers_to_hang_things_on():
    """Narrowing the door dray lets through must not close this one: parking a
    transient on an object in hand is an ordinary idiom, and a leading
    underscore is how Python has always said *mine, not the record's*."""
    person = Person(family_name="Hemingway")
    person._rendered = "<li>Hemingway</li>"

    assert person._rendered == "<li>Hemingway</li>"
    # And it is nothing to do with the record, so it is not stored.
    assert "_rendered" not in Person.__dray_columns__
    assert "_rendered" not in person._dray_blob()


def test_construction_is_exempt_from_validation():
    # A row written under an older rule has to stay loadable, so nothing is
    # checked on the way in.
    person = Person(family_name="Hemingway", status="retired-in-1961")
    assert person.status == "retired-in-1961"


def test_validate_checks_everything_at_once():
    person = Person(family_name="   ", status="retired-in-1961")
    with pytest.raises(ValidationError) as raised:
        person._dray_validate()
    assert "family_name" in str(raised.value)
    assert "status" in str(raised.value)


#
# The vocabulary a field checks against
#


def test_a_choices_collection_no_longer_moves_when_the_list_does():
    """The field kept the list object it was handed, so appending to that list
    changed what was accepted and rebinding the name did not. Two ways of
    writing the same update, one of them working, and nothing anywhere saying
    which one you had written."""
    statuses = ["enquiry", "volunteer"]

    @record(table="applicant", collection="applicants")
    class Applicant:
        status: str = field(default="enquiry", choices=statuses)

    statuses.append("alumnus")

    applicant = Applicant()
    with pytest.raises(ValidationError):
        applicant.status = "alumnus"


def test_a_choices_function_is_asked_at_every_check():
    """A vocabulary that moves now has a way to say so. Both ways of updating
    it read the same from here, which is the point: the function is asked at
    the check rather than the declaration, so neither is a trap."""
    statuses = ["enquiry", "volunteer"]

    @record(table="applicant", collection="applicants")
    class Applicant:
        status: str = field(default="enquiry", choices=lambda: statuses)

    applicant = Applicant()
    with pytest.raises(ValidationError):
        applicant.status = "alumnus"

    statuses.append("alumnus")
    applicant.status = "alumnus"
    assert applicant.status == "alumnus"

    statuses = statuses + ["trustee"]
    applicant.status = "trustee"
    assert applicant.status == "trustee"


def test_an_enum_is_a_vocabulary_rather_than_something_to_call():
    """An `Enum` class is a legal `choices` and it is also callable, so the
    obvious reading of *ask a callable* calls one — which is a lookup of a
    member by value, and raises at the first check rather than at the
    declaration that caused it."""

    class Section(Enum):
        main = "main"
        courtyard = "courtyard"

    assert callable(Section)

    @record(table="seating", collection="seatings")
    class Seating:
        section: str = field(default="main", choices=Section)

    seating = Seating()
    seating.section = "courtyard"
    assert seating.section == "courtyard"

    with pytest.raises(ValidationError):
        seating.section = "bar"


def test_a_choices_function_handing_back_nothing_to_look_in_is_named():
    """A function that hands back the wrong thing is a declaration's fault
    rather than a value's, and it fails on every check from anywhere. So the
    refusal names the field and the function, not whichever value happened to
    be assigned when somebody noticed."""

    @record(table="applicant", collection="applicants")
    class Applicant:
        status: str = field(default="enquiry", choices=lambda: 3)

    with pytest.raises(TypeError) as raised:
        Applicant().status = "volunteer"
    assert "status" in str(raised.value)
    assert "lambda" in str(raised.value)


def test_choices_that_is_neither_a_collection_nor_a_function_is_refused():
    """Freezing the collection is where this can be noticed at last, and the
    declaration is the place for it: the alternative is `argument of type 'int'
    is not iterable` out of a check months later, naming neither the field nor
    what was declared."""
    with pytest.raises(TypeError, match="choices"):

        @record(table="applicant", collection="applicants")
        class Applicant:
            status: str = field(default="enquiry", choices=3)


def test_a_string_of_choices_is_refused_rather_than_read_as_its_characters():
    """A string is iterable, so freezing one would turn `"abc"` into three
    one-character values without a word. It was no better before the freeze:
    `value not in "abc"` is a substring test, so `"ab"` was accepted as a
    status. Neither is a vocabulary, and both are somebody meaning a
    collection."""
    with pytest.raises(TypeError, match="not one value"):

        @record(table="applicant", collection="applicants")
        class Applicant:
            status: str = field(default="e", choices="abc")


def test_a_choices_function_is_not_asked_for_a_row_from_the_table():
    """`choices` does not run on a load, so a record written under a vocabulary
    since narrowed stays readable, and a function inherits that exemption
    rather than quietly undoing it. Which is also why asking one costs nothing
    per row: it is asked where a value is checked and nowhere else."""
    asked = []

    def statuses() -> tuple[str, ...]:
        asked.append(1)
        return ("enquiry",)

    @record(table="applicant", collection="applicants")
    class Applicant:
        status: str = field(default="enquiry", choices=statuses)

    loaded = Applicant._dray_load({"status": "lapsed"})

    assert loaded.status == "lapsed"
    assert asked == []


#
# A field the caller does not have
#
# What the write does with one is `test_storage.py`. This is the half that needs
# no database: which doors refuse a value for it, and which one does not.
#


def folded(write) -> str:
    person = write.record
    return f"{person.family_name} {person.given_names}".strip().casefold()


@record(table="searcher", collection="searchers")
class Searcher:
    family_name: str = field(default="")
    given_names: str = field(default="")
    search_name: str = field(default="", derived=folded)


def test_a_derived_field_cannot_be_assigned():
    """The guarantee the column is declared for. A search name set by hand is a
    claim about the name it is folded from, and the next save would work it out
    again — so the value is either overwritten or a lie about the fields it
    reads, and neither is worth being able to write."""
    searcher = Searcher(family_name="Hemingway")
    with pytest.raises(AttributeError, match="derived"):
        searcher.search_name = "hemingway, e"


def test_a_derived_field_is_not_a_value_the_constructor_takes():
    """The same refusal at the door `parse` builds through, so a spreadsheet
    with a column of folded names is told about it where somebody can still fix
    the spreadsheet."""
    with pytest.raises(ValidationError, match="derived"):
        Searcher(family_name="Hemingway", search_name="hemingway, e")

    with pytest.raises(ValidationError, match="derived"):
        Searcher.parse({"family_name": "Hemingway", "search_name": "h"})


def test_a_row_arriving_from_the_table_carries_what_was_derived():
    """The one door that is not a caller. Those values are what the last write
    worked out, and a record that refused them could not be read back at all."""
    searcher = Searcher._dray_load(
        {"family_name": "Hemingway", "search_name": "hemingway ernest"}
    )
    assert searcher.search_name == "hemingway ernest"


def test_a_derived_field_beside_a_handler_of_its_own_is_refused():
    """Two answers to when the field is the caller's, in one declaration. A
    derived field is nobody's to set and one naming `on_add` is filled in where
    nobody said, and there is no rule for a field that says both."""
    with pytest.raises(ValueError, match="derived"):

        @record(table="muddle", collection="muddles")
        class Muddle:
            name: str = field(default="")
            folded: str = field(default="", derived=folded, on_save=folded)


#
# on_change
#


def seen(changes: list) -> Callable[[Change], None]:
    def handler(change: Change) -> None:
        changes.append(change)

    return handler


def test_on_change_is_handed_the_before_and_after():
    changes: list[Change] = []

    @record(table="event", collection="events")
    class Event:
        name: str = field(default="")
        status: str = field(default="planned", on_change=seen(changes))

    event = Event(name="Working bee")
    event.status = "cancelled"

    assert len(changes) == 1
    assert changes[0].record is event
    assert changes[0].field_name == "status"
    assert changes[0].old == "planned"
    assert changes[0].new == "cancelled"


def test_on_change_is_silent_during_construction():
    changes: list[Change] = []

    @record(table="event", collection="events")
    class Event:
        status: str = field(default="planned", on_change=seen(changes))

    Event(status="cancelled")
    assert changes == []


def test_on_change_is_silent_when_the_value_did_not_move():
    changes: list[Change] = []

    @record(table="event", collection="events")
    class Event:
        status: str = field(default="planned", on_change=seen(changes))

    event = Event()
    event.status = "planned"
    assert changes == []


def test_a_refused_value_never_reaches_on_change():
    changes: list[Change] = []

    @record(table="event", collection="events")
    class Event:
        status: str = field(
            default="planned", choices=("planned", "cancelled"), on_change=seen(changes)
        )

    event = Event()
    with pytest.raises(ValidationError):
        event.status = "postponed"
    assert changes == []


def test_on_change_fires_for_a_blob_field_too():
    changes: list[Change] = []

    @record(table="person", collection="people")
    class Walker:
        suburb: str | None = field(
            default=None, stored_in="blob", on_change=seen(changes)
        )

    walker = Walker(suburb="Leura")
    walker.suburb = "Katoomba"
    assert [(c.old, c.new) for c in changes] == [("Leura", "Katoomba")]


#
# parse is strict, load is lenient
#


def test_parse_takes_a_dict():
    person = Person.parse({"family_name": "Hemingway", "suburb": "Leura"})
    assert person.family_name == "Hemingway"
    assert person.suburb == "Leura"


def test_parse_refuses_a_key_the_class_does_not_declare():
    with pytest.raises(ValidationError) as raised:
        Person.parse({"family_name": "Hemingway", "surbub": "Leura"})
    assert "surbub" in str(raised.value)


def test_parse_validates():
    with pytest.raises(ValidationError):
        Person.parse({"family_name": "Hemingway", "status": "voluntear"})


def test_load_drops_a_key_the_class_has_retired():
    person = Person._dray_load(
        {"family_name": "Hemingway", "wwcc_number": "no longer declared"}
    )
    assert person.family_name == "Hemingway"


def test_load_does_not_validate():
    person = Person._dray_load(
        {"family_name": "Hemingway", "status": "retired-in-1961"}
    )
    assert person.status == "retired-in-1961"


def test_load_lifts_blob_fields_out_of_the_data_column():
    person = Person._dray_load(
        {"family_name": "Hemingway", BLOB_COLUMN: {"suburb": "Leura"}}
    )
    assert person.suburb == "Leura"


#
# A rule about the whole record
#
# Which methods a class offered dray, and in what order, and the one door a rule
# runs at with no database anywhere near it. The other door is the write, and
# what happens there is `test_storage.py`.
#


@record(table="booking", collection="bookings")
class Booking:
    starts_on: date | None = field(default=None)
    ends_on: date | None = field(default=None)
    seats: int = field(default=1)

    @check
    def ends_after_it_starts(self):
        if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
            raise ValueError("a booking cannot end before it starts")


def test_a_rule_is_collected_and_the_method_left_as_it_was():
    """The decorator marks and hands back, so the class keeps an ordinary
    method: still callable by hand, still what a traceback says it is. A rule
    that had been wrapped would be a rule nobody could call themselves."""
    assert Booking.__dray_hooks__ == {"check": ("ends_after_it_starts",)}

    booking = Booking(starts_on=date(2026, 3, 14), ends_on=date(2026, 3, 1))
    with pytest.raises(ValueError, match="cannot end before it starts"):
        booking.ends_after_it_starts()


def test_a_method_called_check_without_the_marker_is_not_collected():
    """The reason a hook is found by a decorator at all. `check` is an ordinary
    domain word — a booking's is checking a party in — and dray reaching for
    that spelling would start calling it before every write, on a class written
    before dray had ever heard of the idea."""

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        def check(self):
            ...

    assert Sitting.__dray_hooks__ == {}


def test_every_marked_method_is_collected_in_the_order_it_was_written():
    """More than one rule is allowed and all of them are kept. Declaration order
    is the only order a reader can predict from looking at the class."""

    @record(table="sitting", collection="sittings")
    class Sitting:
        at: str = field(default="19:00")

        @check
        def a_table_is_free(self):
            ...

        @check
        def the_kitchen_is_open(self):
            ...

    assert Sitting.__dray_hooks__["check"] == (
        "a_table_is_free",
        "the_kitchen_is_open",
    )


def test_a_rule_a_base_class_wrote_counts_on_everything_built_on_it():
    """A rule on a mixin is a rule about every record that mixes it in, which is
    the reason to put it there. The bases come first, so a record reading its own
    class top to bottom is reading the tail of the order rather than the whole
    of it."""

    class Sittable:
        @check
        def the_kitchen_is_open(self):
            ...

    @record(table="sitting", collection="sittings")
    class Sitting(Sittable):
        at: str = field(default="19:00")

        @check
        def a_table_is_free(self):
            ...

    assert Sitting.__dray_hooks__["check"] == (
        "the_kitchen_is_open",
        "a_table_is_free",
    )


def test_a_rule_an_unrelated_class_has_taken_the_name_of_is_refused():
    """The one promise the decorator exists to keep, and the one place names
    rather than functions could break it. Python's method order knows nothing
    about markers, so a record built on a mixin that marked `the_kitchen_is_open`
    and on a class that merely spells a method that way ran the second — a method
    dray was never shown, on a class whose author never heard of the rule."""

    class Sittable:
        @check
        def the_kitchen_is_open(self):
            raise AssertionError("the marked one, and not the one that ran")

    class Rota:
        def the_kitchen_is_open(self):
            ...

    with pytest.raises(TypeError, match="never shown"):

        @record(table="sitting", collection="sittings")
        class Sitting(Rota, Sittable):
            at: str = field(default="19:00")


def test_a_rule_overridden_by_a_class_that_inherited_it_is_allowed():
    """The other side of the refusal above, and the reason it is drawn where it
    is. A class overriding a rule it inherited is saying something about that
    rule whether or not it repeats the decorator, and a second base standing
    beside it does not make that a collision."""

    class Sittable:
        @check
        def the_kitchen_is_open(self):
            ...

    class Timed:
        def opens_at(self):
            ...

    @record(table="sitting", collection="sittings")
    class LateSitting(Timed, Sittable):
        at: str = field(default="22:00")

        def the_kitchen_is_open(self):
            ...

    assert LateSitting.__dray_hooks__["check"] == ("the_kitchen_is_open",)


def test_a_rule_spanning_two_supplied_fields_is_run_by_parse():
    """A rule judged only at the write made `parse` the one door that could
    accept a record `add` then refused. Both dates came off the form, so nothing
    the write does could change the answer, and the caller heard about it a call
    later — after the handler had gone on to do other work."""
    with pytest.raises(ValueError, match="cannot end before it starts"):
        Booking.parse({"starts_on": date(2026, 3, 14), "ends_on": date(2026, 3, 1)})


def test_a_rule_reading_a_field_the_write_fills_sees_nothing_at_parse():
    """The cost of the second door, and what a rule has to do about it. `parse`
    is handed what the caller supplied and the write has filled in nothing, so a
    rule that read `filed_by` without guarding would refuse every form post —
    and the record it refused would have been written perfectly well."""
    seen = []

    @record(table="memo", collection="memos")
    class Memo:
        body: str = field()
        filed_by: str | None = field(
            default=None, on_add=lambda write: "the clerk"
        )

        @check
        def says_who_filed_it(self):
            seen.append(self.filed_by)
            if self.filed_by is not None and not self.filed_by.strip():
                raise ValueError("a memo says who filed it")

    Memo.parse({"body": "Cleared to start."})

    assert seen == [None]


def test_hydrating_a_row_does_not_run_a_rule():
    """The same promise `load` makes about validators, and for the same reason:
    a booking written before anybody thought to compare the two dates has to
    keep loading, or the rule that was added this morning makes some of the
    history unreadable."""
    booking = Booking._dray_load(
        {"starts_on": date(2026, 3, 14), "ends_on": date(2026, 3, 1)}
    )
    assert booking.ends_on == date(2026, 3, 1)


def test_marking_something_that_cannot_be_called_is_refused():
    """`@check` above a `field(...)` reads plausibly and would otherwise be a
    `'Field' object is not callable` at somebody's first save, a long way from
    the line that got it wrong."""
    with pytest.raises(TypeError, match="cannot be called"):

        @record(table="sitting", collection="sittings")
        class Sitting:
            at: str = check(field(default="19:00"))


#
# Which record this is
#


def test_two_objects_for_the_same_record_are_equal():
    """Identity is the id, and so is settled before anything is written. It is
    not whether two records are alike: one may have been read hours ago and the
    other just now, one may be carrying edits nobody has saved, and they are
    still the same record."""
    one = Person(family_name="Hemingway")
    other = Person._dray_load({"id": one.id, "family_name": "Hemingway, E."})

    assert one == other


def test_a_record_still_equals_itself_after_its_values_move():
    """A dataclass compares every field, so a record stopped equalling itself
    the moment somebody wrote it — the token in the row moved and the copy in
    hand did not, and `in`, `index` and `remove` all answered on how stale
    something was."""
    person = Person(family_name="Hemingway")
    same = Person._dray_load({"id": person.id, "family_name": "Hemingway"})

    person.family_name = "Hemingway, E."
    object.__setattr__(same, "etag", "a token from a later write")

    assert person == same
    assert [same].index(person) == 0


def test_two_records_that_are_alike_are_not_the_same_record():
    assert Person(family_name="Hemingway") != Person(family_name="Hemingway")


def test_different_kinds_are_never_the_same_record():
    """Two tables can hold the same id and mean nothing by it."""

    @record(table="other", collection="others")
    class Other:
        family_name: str = field()

    person = Person(family_name="Hemingway")
    assert person != Other(family_name="Hemingway", id=person.id)


def test_a_record_can_go_in_a_set_and_be_a_key():
    """Deduplicating a list of records and keying a lookup by one are ordinary,
    and every record dray built was unhashable: a dataclass that compares by
    value sets `__hash__` to None."""
    person = Person(family_name="Hemingway")
    same = Person._dray_load({"id": person.id, "family_name": "Hemingway"})
    other = Person(family_name="Shelley")

    assert len({person, same, other}) == 2
    assert {person: "notes"}[same] == "notes"
    assert set([person, other]) - set([same]) == {other}


def test_an_id_is_given_when_the_record_is_built():
    """An id somebody else chose arrives at construction — an import carrying
    its own keys, a record rebuilt from a backup."""
    chosen = uuid4()
    assert Person(family_name="Hemingway", id=chosen).id == chosen
    assert Person.parse({"family_name": "Hemingway", "id": str(chosen)}).id == chosen


def test_an_id_cannot_be_moved_afterwards():
    """Which row a save writes to is whatever the object currently says, so
    assigning somebody else's id and saving overwrote their row with these
    values and left this one behind, silently. It is also what the record
    hashes on, and a key that moves while it sits in a set is one the set can
    no longer find."""
    person = Person(family_name="Hemingway")
    with pytest.raises(AttributeError, match="cannot be changed"):
        person.id = uuid4()


#
# The base a record inherits so an editor can see it
#


def test_the_base_is_empty_when_the_program_runs():
    """Everything on it is declared under `TYPE_CHECKING`, and that is
    load-bearing rather than tidy. `_claimed` asks the whole hierarchy whether
    a word has been spoken for, so a base carrying real methods would read as
    the domain having claimed all six of them — dray would step aside from
    every one, and the record would come out with dray's behaviour missing."""
    assert [
        name for name in vars(Record) if not name.startswith("__")
    ] == []


def test_a_record_inheriting_the_base_keeps_every_member_dray_lends():
    """The trap the line above avoids, from the other side. A base whose
    `as_dict` was a real method — even one whose body is `...` — left
    `person.as_dict()` handing back `None`, on a record that looked entirely
    ordinary and passed every other test."""

    @record(table="reader", collection="readers")
    class Reader(Record):
        family_name: str = field()

    reader = Reader(family_name="Hemingway")

    assert reader.as_dict()["family_name"] == "Hemingway"
    assert reader.id is not None
    assert reader.etag is not None
    assert reader.children == {}
    assert Reader.parse({"family_name": "Shelley"}).family_name == "Shelley"


def test_a_record_that_leaves_the_base_off_is_unchanged():
    """It is opt-in, and a record declaring nothing about it must behave
    identically — otherwise this is a migration rather than an option."""

    @record(table="writer", collection="writers")
    class Writer:
        family_name: str = field()

    plain = Writer(family_name="Hemingway")

    assert plain.as_dict()["family_name"] == "Hemingway"
    assert plain.id is not None
    assert not isinstance(plain, Record)


def test_a_rule_in_front_of_a_write_still_reaches_drays_own():
    """The pattern the manual teaches, on a class carrying the base. The base
    declares `_dray_save` so an editor can see the call; the record has to
    still find the real one underneath."""

    @record(table="trimmer", collection="trimmers")
    class Trimmer(Record):
        family_name: str = field()

        def save(self, **kw):
            self.family_name = self.family_name.strip()
            return self._dray_save(**kw)

    trimmer = Trimmer(family_name="  Hemingway  ")

    assert Trimmer.save.__qualname__.endswith("Trimmer.save")
    assert callable(trimmer._dray_save)
    assert trimmer._dray_save.__func__ is model._save


def test_the_base_says_what_the_real_member_says():
    """Two spellings of one docstring, and a hover shows the base's. Its
    opening line is copied from the member dray actually binds, so the pair
    have to be read together — a member reworded on one side and not the other
    would leave an editor telling a caller something that used to be true."""
    source = ast.parse(pathlib.Path(model.__file__).read_text())
    declared = {
        node.name: ast.get_docstring(node)
        for node in source.body
        if isinstance(node, ast.FunctionDef)
    }
    # Two of them: the one a checker reads and the empty one it is at
    # runtime. The declarations are on the first.
    (checked,) = [
        node
        for node in ast.walk(source)
        if isinstance(node, ast.ClassDef)
        and node.name == "Record"
        and any(isinstance(each, ast.FunctionDef) for each in node.body)
    ]

    lent = {
        "save": "_save",
        "delete": "_delete",
        "as_dict": "_as_dict",
        "parse": "_parse",
        "children": "_children",
        "store": "_store",
        "_dray_save": "_save",
        "_dray_delete": "_delete",
        "_dray_as_dict": "_as_dict",
        "_dray_parse": "_parse",
        "_dray_validate": "_validate",
        "_dray_blob": "_blob",
        "_dray_hash": "_hash",
    }
    said = {
        node.name: ast.get_docstring(node)
        for node in checked.body
        if isinstance(node, ast.FunctionDef)
    }

    # Every one of them, so a member added to the base without a docstring is
    # this test failing rather than a blank hover nobody notices.
    assert set(said) == set(lent)
    for name, real in lent.items():
        # Compared with the wrapping taken out, since the base's copy sits four
        # levels in and breaks its lines somewhere else.
        opening = declared[real].split("\n\n")[0]
        assert " ".join(said[name].split()) == " ".join(opening.split()), name
