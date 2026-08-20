"""
The same declarations, in a module that stringifies its annotations.

`from __future__ import annotations` is per file, which is what makes it worth
a module of its own: every other test here declares records without it, so
nothing else in the suite can see what it does. It turned every annotation into
a string, and a string told `accepts` and `restorer` nothing — so a field took
any value it was given and a blob field never came back as what it declared,
for every record in the file, with the schema still correct and `drift` still
clean.

Nothing here needs a database. What broke was the declaration.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from dray import ValidationError, field, record, schema
from dray.model import BLOB_COLUMN


@record(table="booking", collection="stringified_bookings")
class Booking:
    party_size: int = field(default=1)
    label: str = field(default="")
    price: Decimal | None = field(default=None)
    starts_on: date | None = field(default=None, stored_in="blob")
    settled_on: date | None = field(default=None, stored_in="blob")


def test_a_field_still_takes_only_what_its_annotation_says():
    booking = Booking()
    with pytest.raises(ValidationError):
        booking.party_size = "4"


def test_parse_is_still_strict_about_values():
    with pytest.raises(ValidationError):
        Booking.parse({"party_size": "4"})


def test_a_blob_field_still_comes_back_as_what_it_declared():
    # The one that is data rather than a check: stored as a string, because
    # jsonb has no date, and brought back by the annotation. Left as a string it
    # is wrong everywhere it is used and raises nowhere.
    loaded = Booking._dray_load(
        {"party_size": 2, BLOB_COLUMN: {"starts_on": "2026-03-14"}}
    )
    assert loaded.starts_on == date(2026, 3, 14)


def test_a_blob_value_that_will_not_parse_is_still_handed_back():
    # Leniency survives the fix: `load` does not raise, so a row written before
    # the field was a date keeps loading.
    loaded = Booking._dray_load(
        {BLOB_COLUMN: {"settled_on": "sometime in March"}}
    )
    assert loaded.settled_on == "sometime in March"


def test_a_union_still_reads_as_the_type_beside_the_none():
    booking = Booking()
    booking.price = Decimal("4.99")
    with pytest.raises(ValidationError):
        booking.price = "4.99"


def test_the_columns_are_still_the_types_the_class_declared():
    kinds = dict(schema._columns(Booking))
    assert kinds["party_size"] == "bigint"
    assert kinds["label"] == "text"
    assert kinds["price"] == "numeric(18,6)"


def test_an_id_a_key_cannot_hold_is_still_refused():
    # The refusal reads the annotation, and here the annotation is the string
    # `"bytes"` — which matches nothing, so a class declared in a file like this
    # one would have been built without a word and its `create table` refused by
    # the cluster. It is read through the same resolved annotations the schema
    # uses, so the two cannot disagree about what the column is.
    with pytest.raises(ValueError, match="will not have bytea in a key"):

        @record(table="stringified_seal", collection="stringified_seals")
        class Seal:
            id: bytes = field()


def test_a_record_declared_in_a_function_keeps_working():
    # Why the annotations are resolved one at a time rather than with
    # `get_type_hints`, which is all-or-nothing: `Ticket` is not visible from
    # this module, and taking it as unreadable must cost that field alone.
    class Ticket:
        pass

    @record(table="entry", collection="stringified_entries")
    class Entry:
        seats: int = field(default=1)
        ticket: Ticket | None = field(default=None)

    entry = Entry()
    with pytest.raises(ValidationError):
        entry.seats = "2"

    # The one field nothing can read is not checked, which is the safe way to
    # be wrong — and the schema still has a column for it.
    entry.ticket = "anything at all"
    assert dict(schema._columns(Entry))["seats"] == "bigint"
