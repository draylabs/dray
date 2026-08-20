"""
The handlers dray supplies.

A hook is a method a record marks and `hooks` is next door; a handler is a
function a field names, which is what `on_add`, `on_save` and `on_change` each
take. Nearly all of those are yours, because dray has no opinion about your
domain — `whoever` in the manual is four lines you write, and it stays yours
because dray has never heard of `whom`.

These three are the exceptions, offered because most records want them and
nobody should have to write the same three sentences again. What collects them
here is who wrote the function rather than when it runs, so `clock` sits with
`records_change` although one fills a field and the other queues a child.
"""

from collections.abc import Callable

from dray.model import Change, Sql, Write


def clock(write: Write) -> Sql:
    """
    The database's clock, for a field that records when something happened.

        created_at: datetime | None = field(default=None, on_add=clock)

    `clock_timestamp()` rather than a Python `datetime`, because it advances
    within a transaction where `now()` does not. Rows written by one save would
    otherwise share a timestamp exactly, leaving anything ordered on it to break
    the tie on a random id.

    Which means the field needs a column: this is text for the statement rather
    than a value, and the blob goes over as a parameter with nowhere to put an
    expression. A blob field wanting a time says so from Python instead.
    """
    return Sql("clock_timestamp()")


def describe(change: Change) -> str:
    """
    What happened, in a sentence.

    Three cases rather than one, because "given_names changed from 'None' to
    'Ernest'" is not what happened — a value arriving for the first time and a
    value being replaced read differently to whoever is looking this up later.
    """
    if change.old in (None, ""):
        return f"{change.field_name} set to '{change.new}'."
    if change.new in (None, ""):
        return f"{change.field_name} of '{change.old}' cleared."
    return f"{change.field_name} changed from '{change.old}' to '{change.new}'."


def records_change(*, into: str) -> Callable[[Change], None]:
    """
    Queue a line about the change, in the child named.

        family_name: str = field(on_change=records_change(into="logs"))

    Told where to write rather than guessing. It fills the child's first
    declared field, so a log that calls it `message` and a note that calls it
    `body` both work without either being named here.
    """

    def handler(change: Change) -> None:
        if into not in change.record._dray_children:
            # Loudly, because the alternative is a field that has been quietly
            # recording nothing since the day the name was mistyped.
            kind = type(change.record).__name__
            raise AttributeError(
                f"records_change(into={into!r}) but {kind} has no such child. "
                f"Declare it with @child(of={kind}, name={into!r}, table=...)"
            )
        getattr(change.record, into).add(describe(change))

    return handler
