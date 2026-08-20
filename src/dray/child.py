"""
Children: records that belong to another record.

A child is reached through its parent, written by the parent's save, and gone
when the parent goes. Notes are children; so is history — a log is not a special
case, it is a child with a change handler pointed at it.
"""

from collections.abc import Callable, Iterator, Sequence
from typing import Any, ClassVar, Generic, TypeVar, dataclass_transform

import psycopg

from dray.model import (
    BLOB_COLUMN,
    CACHE_MOST,
    ETAG,
    KEY,
    PARENT_ID,
    PARENT_TYPE,
    AnyOf,
    Index,
    NoneOf,
    Sql,
    _Class,
    _declare,
    convert,
    field,
    key_of,
    normalised,
)
from dray.store import RecordNotFound

# Parent class to the children declared against it, so a delete knows what to
# take with it and nothing has to keep a list by hand.
CHILDREN: dict[type, list[type]] = {}


# The same two as `@record`, for the same reasons — a child is a record that
# knows its parent, and an editor has even less to go on here: without them a
# child class is not merely opaque but unremarked.
@dataclass_transform(field_specifiers=(field,))
def child(
    *,
    of: type | tuple,
    name: str,
    table: str,
    collection: str | None = None,
    order_by: str | tuple | list | None = None,
    indexes: Index | Sequence[Index] | None = None,
    cached_for: float | None = None,
    cache_most: int = CACHE_MOST,
    key: str = KEY,
    etag: str = ETAG,
    blob: str = BLOB_COLUMN,
    parent_type: str = PARENT_TYPE,
    parent_id: str = PARENT_ID,
) -> Callable[[_Class], _Class]:
    """
    Declare a child of one or more kinds of record.

        @child(of=(Person, Event), name="notes", table="note")
        class Note:
            body: str

    `name` is what a parent calls them, so the same word both reads them and
    takes new ones. `of` decides which records get that accessor, and mostly it
    is not a storage constraint: every child table carries its parent's type in
    a column, so attaching to one more record keyed the way these are is a
    change to this line and nothing else.

    The column beside it is the exception. `parent_id` holds the key it points
    at, and takes that key's type and converter — so a child of a record that
    declared its own id points at it with a column of that type, and parents
    keyed differently want a `@child` each:

        @child(of=Day, name="timelogs", table="timelog")   # a date for a key
        @child(of=Person, name="notes", table="note")      # a uuid for a key

    Naming both in one `of` is refused where it is written, because one column
    holds one type and no table could serve them.

    `collection` is a different word for a different door, and optional. `name`
    reaches a parent's own children; `collection` is what the store calls the
    whole table, for the questions that are about the children rather than about
    one parent's — every note written this week, everyone carrying a tag.

        @child(of=Person, name="notes", table="note", collection="notes")

    Say it and `store.notes` is a collection like any other, `@collection(of=Note)`
    included. Leave it off and a child is reachable only through a parent, which
    is what most of them want.

    `order_by` is how they come back. It defaults to the key, which is total and
    stable and means nothing — a key is random unless a class said otherwise. A
    thread that should read as a conversation declares a timestamp and orders on
    it:

        @child(of=Person, name="notes", table="note", order_by="written_at")
        class Note:
            body: str
            written_at: datetime | None = field(default=None, on_add=clock)

    The key goes on the end of whatever is named, so a read is always total —
    and what that promises is narrower than it sounds. The same rows come back
    in the same order every time, which is what stops a page reshuffling when
    somebody refreshes it. Rows tied on everything named fall through to a
    random key, so among themselves the order is one nobody chose, and the same
    rows written again anywhere else fall differently.

    Name more than one field where that matters, and `desc` for the ones that
    read backwards:

        @child(of=Parcel, name="scans", table="scan",
               order_by=("depot", desc("scanned_at")))

    `indexes` is what the table is indexed for, and on a child it answers the
    question the accessor cannot. Every child table is indexed on
    `(parent_type, parent_id)` whether anything is declared or not, because
    reading a parent's children is what a child is for and dray's own cascading
    delete filters on those two columns — and a declared index that leads with
    those two columns is built in place of that one rather than beside it,
    since it serves the same reads. What is declared here is usually for the
    other door — the question that names no parent, which an index leading with
    one cannot serve:

        @child(of=Parcel, name="holds", table="customs_hold",
               collection="customs_holds",
               indexes=[index("depot_id", "raised_at")])

    Those are the columns the index covers and all of them: nothing is put in
    front of a declaration, which is the whole of what makes it able to answer
    that question.

    `cached_for` and `cache_most` are what they are on `@record`. They are
    worth less here than there, because a parent's children are read through
    `find` and a set is not something dray can invalidate — what they cache is
    a child asked for by its own id, through the collection a `collection=`
    gives it.

    `key`, `etag` and `blob` are what they are on `@record`, and `parent_type`
    and `parent_id` name the two columns holding the parent. A child whose
    domain wants one of those words says where dray's goes:

        @child(of=Person, name="notes", table="note", parent_id="belongs_to")
        class Note:
            body: str
            parent_id: str = field(default="")   # the reference on the form
    """
    parents = of if isinstance(of, tuple) else (of,)
    # A class, and one dray has already built. Everything a child takes from
    # its parent — the table it points at, the type and converter of the key it
    # points with — is read off the class here at the decorator, so anything
    # else is refused here too rather than announcing itself as a missing dray
    # internal on the same line. An instance is the near miss worth naming: it
    # answers to `__dray_table__` through its class and would have declared
    # perfectly well.
    for parent in parents:
        known = isinstance(parent, type) and hasattr(parent, "__dray_table__")
        if not known:
            raise TypeError(
                f"of= takes a record class, or a tuple of them, not "
                f"{parent!r}. A child hangs off a class @record or @child has "
                "been run over, which is what gives it a table to point at "
                "and a key to point with — pass Person rather than a person."
            )

    def wrap(cls: _Class) -> _Class:
        built = _declare(
            cls,
            table=table,
            collection=collection,
            order_by=order_by,
            indexes=indexes,
            cached_for=cached_for,
            cache_most=cache_most,
            parent_types=parents,
            key=key,
            etag=etag,
            blob=blob,
            parent_type=parent_type,
            parent_id=parent_id,
        )
        built.__dray_name__ = name
        for parent in parents:
            CHILDREN.setdefault(parent, []).append(built)
            setattr(parent, name, _accessor(built, name))
        return built

    return wrap



def _accessor(cls: type, name: str) -> property:
    """The parent's attribute. One `ChildSet` per record per kind, made when
    first asked for and kept, so what you queue on it is still there at save."""

    def get(record: Any) -> "ChildSet":
        sets = record._dray_sets
        if name not in sets:
            sets[name] = ChildSet(record, cls)
        return sets[name]

    return property(get)


# What kind of child this set holds, so `person.notes.find()` hands back notes
# rather than `Any`. The parameter is only ever filled in by the declaration on
# the parent — `notes: ChildSet[Note]` — since the runtime object is built
# from a class the decorator captured and has never needed to say so.
_Child = TypeVar("_Child")


class ChildSet(Generic[_Child]):
    """
    One record's children of one kind.

    Reads go to the database scoped to this parent, which is the point of
    reaching children through their parent: an id arriving from a form cannot
    reach somebody else's note, because the parent is part of every lookup.

    A new child is queued and written by the parent's save, so a note and the
    change it explains land in one transaction. `clear` and `thin` are the other
    way round and happen where they are called: a removal has no change to ride
    with.
    """

    def __init__(self, record: Any, cls: type) -> None:
        self.record = record
        self.cls = cls
        self._adding: list[Any] = []

    #
    # Queuing
    #

    def add(self, *content: Any, **values: Any) -> _Child:
        """
        Queue a child, written by the parent's next save.

        One positional argument is allowed and fills the first field the class
        declared — the body of a note, the message of a log. Everything else is
        by name, because nothing else about a child is obvious from position.

        Or hand over one you already built, which is what `add` means on a
        collection and had no meaning here:

            person.notes.add(Note.parse(row))

        A child is a record, so an importer that parses rows into notes should
        be able to attach one. It knows what it named, having been told at
        construction, so the write fills in the rest exactly as it would for a
        note queued by keyword.
        """
        if len(content) > 1:
            raise TypeError("add takes at most one positional value")

        if content and isinstance(content[0], self.cls):
            if values:
                raise TypeError(
                    f"add was given a {self.cls.__name__} and keywords as well. "
                    "Set them on the object, or pass keywords instead of it."
                )
            self._adding.append(content[0])
            return content[0]

        if content:
            field = self.cls.__dray_content__
            if field is None:
                raise TypeError(f"{self.cls.__name__} declares no field to fill")
            if field in values:
                raise TypeError(f"{field!r} given twice")
            values[field] = content[0]

        item = self.cls(**values)
        self._adding.append(item)
        return item

    #
    # Emptying
    #

    def clear(self) -> int:
        """
        Remove every child in this set, and say how many rows went.

            person.notes.clear()

        The whole set as `find` and `count` define it: the stored rows go
        and whatever is queued on the set is dropped with them. So clearing
        and then adding writes the new generation, and the other order writes
        nothing — which is the shape of the act this is for, a generation
        replaced rather than edited.

        It happens now rather than at the parent's next save. A new child
        queues because it is usually explaining the change being saved beside
        it, and a removal has nothing to ride with — `note.delete()` is
        immediate for the same reason. Inside a block you opened it is part of
        that transaction like any other write, and outside one it commits on
        its own.

        **What it runs is decided by the child class and not by the caller.**
        Where the class declares no `@before_delete` this is one statement per
        generation and reads not a row, whatever the size of the set. Where it
        declares one, the children are read first and the rule runs on each of
        them inside the transaction, so a rule that refuses leaves every row
        where it was. Their own descendants' rules do not run, which is what a
        parent's delete already promises about a cascade and for the same
        reason: reaching them means reading the whole tree.

        The number is this generation's — how many children went, not how many
        rows the tree under them was. A parent that has never been saved has no
        rows to remove, so it drops what is queued and hands back nought.

        Nothing here is sized. Every row counts against DSQL's 3,000 and a
        generation with children of its own multiplies, so a set too big for
        one transaction is refused whole with nothing removed — `delete`'s
        answer, inherited rather than solved. `thin` is the call for a set that
        size, and pays for it in transactions.
        """
        collection = getattr(self.record, "_dray_collection", None)
        if collection is None:
            # No collection is no table, so there is nothing to send and
            # nothing to count — the state `count` already answers out of the
            # queue alone.
            self._adding.clear()
            return 0

        # Held in case the block this is inside rolls back. The rows come back
        # with it, and a queue that stayed emptied would leave the set reading
        # as something no transaction ever did.
        dropped = list(self._adding)
        removed = self._theirs()._clear_batch(self.record)
        self._adding.clear()
        if collection.store.in_transaction:
            collection.store._undo_on_rollback(
                lambda: self._requeue(dropped)
            )
        return removed

    def thin(self, *, at_a_time: int = 500) -> int:
        """
        Take some of this set away, and say how many rows went.

            while person.notes.thin(at_a_time=500):
                pass

        One call is one pass: up to `at_a_time` rows from **one** generation, in
        a transaction of its own, deepest first. Nought means there is nothing
        left to take, so the loop above ends with this set and everything under
        it gone. The loop is yours, and so is the trade it makes — several
        transactions rather than the one `clear` and `delete` promise, which is
        the only way past a ceiling that counts a transaction.

        Which is the whole of when to reach for it. `clear` is the call for a
        set that fits in one transaction and this is the call for the set that
        does not, and nothing else divides them.

        **The bound is a generation and not the set**, because the 3,000 is a
        limit on a transaction rather than on a statement. A pass bounded at 200
        notes with twenty attachments under each is 4,200 rows and a refusal,
        however the limit on the notes is phrased. One generation a pass is a
        real row count with no fanout for you to know, and it is why an
        `at_a_time` past what a transaction holds is refused here rather than at
        the database.

        **Stopping half way leaves a shortened tree and not a broken one.**
        Every attachment goes before any note does, so a parent is left standing
        with fewer children, nothing is orphaned, and nothing about the set says
        a loop was ever running. Every pass starts again at the deepest
        generation, which is what keeps a child added under an already-thinned
        note from being left behind.

        **What is queued is left alone**, where `clear` drops it: this takes
        rows and a queued child is not one. So the loop reaching nought means no
        rows left rather than an empty set, and a parent that has never been
        saved has nothing to take and answers nought without asking.

        A `@before_delete` on the child class runs for the children each pass
        takes, inside that pass's transaction, and a rule that refuses leaves
        that pass's rows where they were — and the passes before it committed.
        **So a rule can run for some of a generation and never for the rest**,
        which no other door in dray can do. A rule that writes rides on the
        pass's budget too: 500 notes whose rule adds a line is 1,000 rows in
        that transaction, so `at_a_time` bounds what dray removes rather than
        what the pass costs. Their descendants' rules do not run, exactly as
        `clear` says.

        **Inside a block you opened, every pass joins your transaction** — as
        every write on this class does — which rebuilds the one transaction this
        exists to escape and puts the whole loop back under the ceiling. So the
        passes in a block are added up, and the one that would take the total
        past what a transaction holds is refused here rather than by the
        database a few passes later. A short loop in there still works, and a
        set that fits in one transaction is what `clear` is for — a loop long
        enough to be refused is a set that wanted thinning outside the block.
        """
        from dray.collection import MAX_ROWS

        if not isinstance(at_a_time, int) or isinstance(at_a_time, bool):
            raise TypeError(
                f"at_a_time is a number of rows, not {at_a_time!r}"
            )
        if at_a_time < 1:
            raise ValueError(
                f"a pass takes at least one row, not {at_a_time}. A pass that "
                "takes none is a loop that never ends."
            )
        # The same refusal a set of queued children over the ceiling gets, and
        # here for the same reason: this is a number somebody wrote down, and a
        # pass the database can never take should be answered where it was
        # written rather than on the first trip out.
        if at_a_time > MAX_ROWS:
            raise ValueError(
                f"at_a_time={at_a_time} is more rows than one transaction "
                f"holds, which is {MAX_ROWS}. A pass is one transaction and "
                "cannot be split, so no pass that size would ever be taken — "
                "loop more times with fewer."
            )

        collection = getattr(self.record, "_dray_collection", None)
        if collection is None:
            # No collection is no table, so there are no rows to take. The queue
            # is untouched here as it is everywhere else in this call, which is
            # what keeps `while thin(): ...` on an unsaved parent a loop that
            # ends rather than one that empties it.
            return 0

        store = collection.store
        # Inside a block every pass joins the caller's transaction, so the loop
        # is one transaction and the ceiling applies to the whole of it rather
        # than to a pass. That total is a number dray can see — it is the sum of
        # what the passes handed back — so it is refused on the same terms every
        # other in-block write is refused on, before the pass that would cross
        # it is sent. Outside a block there is nothing to add up: each pass
        # commits on its own and `at_a_time` already bounds it.
        if store.in_transaction and store._thinned + at_a_time > MAX_ROWS:
            raise ValueError(
                f"the passes in this block have taken {store._thinned} rows "
                f"and another {at_a_time} would be more than the {MAX_ROWS} "
                "one transaction holds. Every pass joins the block you opened, "
                "so the loop is one transaction and cannot be split — run it "
                "outside the block, where a pass commits on its own. A set "
                "that does fit in one transaction is what `clear` is for."
            )

        took = self._theirs()._thin_batch(self.record, at_a_time)
        if store.in_transaction:
            store._thinned += took
        return took

    #
    # Writing one that already exists
    #
    # A child queued here rides with its parent, because an added note is
    # usually explaining the change being saved alongside it — the child's own
    # collection is the door for the case where it is not. One that already
    # exists is a row like any other and looks after itself, which is why there
    # is no `save` here — `note.save()` is the same write, and the note already
    # knows where it lives.
    #

    def _mounted(self) -> Any:
        """The parent's own collection, which is also where the parent's table
        name comes from. A child of an unsaved record has neither."""
        collection = getattr(self.record, "_dray_collection", None)
        if collection is None:
            raise RuntimeError(
                f"this {type(self.record).__name__} did not come from a store, "
                "so it has no children to write yet"
            )
        return collection

    def _theirs(self) -> Any:
        """A collection over the child's own table — the one written for it
        where there is one, since a child that declared a collection should be
        read through it here as much as through the store."""
        from dray.collection import _collection_for

        return _collection_for(self._mounted().store, self.cls)

    #
    # Reading
    #

    def by_id(self, record_id: Any) -> _Child:
        """
        One child of this parent, by id.

        An id is whatever the child class made its key — a `UUID` where it
        declared none, and text is taken for that one, which is what lets the
        id a page printed come straight back from the form. A child that
        declares its own key takes text only where that field said
        `converter=`, exactly as a collection's does.

        The parent is in the statement, so an id arriving from somebody else's
        form finds nothing. That is the whole of the guard: a child can only be
        acted on by whoever can reach it through its parent, and reaching it is
        this. `save` and `delete` then need no scoping of their own, because
        holding the object means having come through here.

        A statement rather than a filter over `find()`, since this is the gate
        every write goes through and reading a hundred notes to find one is a
        poor thing to put in front of it.

        Raises where `find_first` returns None, which is the same division a
        collection makes: an id is something the caller believes in, and a
        search is a question about what exists.
        """
        from dray.collection import _checked_id

        # Converted once, and used for both. Everything downstream of a request
        # is text, so the id a page printed comes back as a string — and the
        # queued child it is looking for holds a `UUID`, which is never equal to
        # one however identical they read.
        wanted = _checked_id(self.cls, record_id)

        # What is queued is asked about first, because it can be answered
        # without a store. A parent that has never been written has no
        # collection to reach a table through, and going to the database first
        # would fail on that before ever looking at the child being held.
        for item in self._adding:
            if key_of(item) == wanted:
                return item

        missing = RecordNotFound(
            f"no {self.cls.__name__} {record_id!r} belonging to this record"
        )
        collection = getattr(self.record, "_dray_collection", None)
        if collection is None:
            raise missing

        theirs = self._theirs()
        found = theirs.select_first(
            f"select {theirs.columns} from {theirs.table}"
            f" where {theirs.id} = %s and {theirs.parent_type} = %s"
            f" and {theirs.parent_id} = %s",
            [wanted, collection.table, key_of(self.record)],
        )
        if found is None:
            raise missing
        return found

    def find(
        self, *, equals: dict[str, Any] | None = None
    ) -> list[_Child]:
        """
        This parent's children matching these values, and whatever is queued
        that matches too, so a set reads the same before and after its parent's
        save. Naming nothing means all of them, in the order the class declared.

            person.notes.find(equals={"kind": "call"})

        The conditions go into the statement rather than over a full read, which
        is what lets `(parent_type, parent_id)` do its job: a filter applied in
        Python has already paid for every row it is about to discard. Queued
        children have no row to ask about and are matched in hand.

        A filter lives in `equals` here as it does on a collection, though a
        child set has no options of its own to collide with it. That is the
        point: a filter must not be spelled one way through a parent and another
        way through the store, or the same question asked twice reads as two.
        """
        return [*self._stored(equals), *self._queued(equals)]

    def find_first(
        self, *, equals: dict[str, Any] | None = None
    ) -> _Child | None:
        """
        The first matching child, or None. The order is the one the class
        declared, and a queued child comes after a stored one.

        This builds the matching set and takes the head, where a collection's
        `find_first` asks the database for a single row. The difference is what
        a child set is: bounded by its parent, and half of it possibly in memory
        — a `limit 1` could not see the queued half at all.
        """
        found = self.find(equals=equals)
        return found[0] if found else None

    def count(self, *, equals: dict[str, Any] | None = None) -> int:
        """
        How many, asked of the database rather than measured over every one.

        Reading every note to return a number is what a list page asking once
        per row pays for two thousand times. Queued children are
        added on, because they are what the next save will write and `find`
        counts them.
        """
        collection = getattr(self.record, "_dray_collection", None)
        queued = len(self._queued(equals))
        if collection is None:
            return queued
        scoped = {**self._scope(collection), **(equals or {})}
        return self._theirs().count(equals=scoped) + queued

    def _scope(self, collection: Any) -> dict[str, Any]:
        """The parent, as conditions. Every read of a child carries these, which
        is both the guard and the reason one index serves all of them."""
        return {
            self.cls.__dray_parent_type__: collection.table,
            self.cls.__dray_parent_id__: key_of(self.record),
        }

    def _queued(self, equals: dict[str, Any] | None = None) -> list[Any]:
        """
        The half of this set that has no rows yet, matched in memory.

        `normalised` first, which is the same checking and converting the
        stored half gets on its way into a statement — the two answer one
        question and must not answer it differently depending on whether
        anybody has saved.

        What is left is the comparing, and that is where they legitimately
        differ: a statement compares in SQL and this compares in Python. `any_of`
        is the one place the difference shows. `= any(%s)` is an equality test
        against each member, and nothing equals `NULL` in SQL — so a `None` in
        there matches no row at all, where Python's `in` would happily match a
        child holding `None`. The rows are right and this follows them. A field
        with nothing in it is asked for by `find(equals={"x": None})`, which is
        the only spelling that means it on either side.

        `none_of` needs no such care, because it means what it reads as: a child
        holding nothing matches, which is what the null half of the statement
        says too, and no member of one can be `None` for the two to fall out
        over.
        """
        wanted = normalised(self.cls, equals or {})

        def holds(item: Any, name: str, value: Any) -> bool:
            held = getattr(item, name, None)
            if isinstance(value, NoneOf):
                return held is None or all(held != each for each in value)
            if isinstance(value, AnyOf):
                return any(held == each for each in value if each is not None)
            return held == value

        return [
            item
            for item in self._adding
            if all(holds(item, name, value) for name, value in wanted.items())
        ]

    def _stored(self, equals: dict[str, Any] | None = None) -> list[Any]:
        collection = getattr(self.record, "_dray_collection", None)
        if collection is None:
            return []
        theirs = self._theirs()
        where, params = theirs._conditions(
            {**self._scope(collection), **(equals or {})}
        )
        return theirs.select_many(
            f"select {theirs.columns} from {theirs.table}"
            f" where {' and '.join(where)}"
            f" order by {self.cls.__dray_order__}",
            params,
        )

    #
    # What the parent's save uses
    #

    def _pending(self) -> list[Any]:
        return self._queued()

    def _settled(self) -> None:
        self._adding.clear()

    def _requeue(self, items: Sequence[Any]) -> None:
        """
        Put back what `_settled` emptied, because the write it was emptied for
        has rolled back.

        Merged into the queue rather than assigned over it. A block can save,
        queue another child and save again, so by the time this runs the queue
        may hold children this snapshot has never seen — and replacing the list
        would discard exactly the kind of unwritten child the undo exists to
        protect. Ahead of what is there, because a snapshot taken earlier holds
        what was queued earlier, and the undos run backwards.

        Compared by identity and not by value: two notes reading the same words
        are two notes, and a dataclass says they are equal.
        """
        held = {id(item) for item in self._adding}
        self._adding[:] = [
            *(item for item in items if id(item) not in held),
            *self._adding,
        ]

    def __iter__(self) -> Iterator[_Child]:
        return iter(self.find())

    def __getitem__(self, index: Any) -> _Child:
        return self.find()[index]

    def __len__(self) -> int:
        # The count query, not the read. `len(person.notes)` beside a heading is
        # the commonest way to ask this and has no business building objects.
        return self.count()

    def __repr__(self) -> str:
        return f"<{self.cls.__name__} of {key_of(self.record)}>"


def _prepare(
    records: Sequence[Any],
    given: dict[str, Any],
    parent_table: str,
    seen: set[int] | None = None,
) -> list[tuple]:
    """
    Everything the queued children of these records need, worked out before the
    statements go out.

    Apart from the insert because the insert is replayed when DSQL refuses a
    commit and this is not — so a field that named an `on_add` is filled once
    per save rather than once per attempt.

    Which holds for what the caller queued and cannot hold for a child a
    `@before_save` queued. That call is inside the transaction and so inside
    the replay, because the child does not exist before the rule that built it
    runs — and the rule builds a new one on every attempt, so there is nothing
    of the refused attempt left for a handler to have been run once against. A
    handler deriving its value from what the child holds answers the same thing
    each time; one counting its own calls counts attempts.

    `given` is what the write was told — from the store early, or from the save
    late. A child takes a value from it when a key matches a field it declared
    and nothing more specific already filled that field in.

    Nothing is validated here. `_validate_queued` has already run every field
    rule in the tree, and the rules a child wrote about itself run once every
    child in the write has been filled, which is later than this — before the
    first transaction for what the caller queued, and inside it for a child a
    rule queued, since that is where the child arrived.

    All the way down, because a child is a record and may have queued children
    of its own — an attachment on a note, a reply to it. Each level names the
    level above as its parent, which is what makes the walk a walk rather than
    anything cleverer: nothing here needs to know how deep it is. What lets the
    whole tree be worked out before a single row exists is that every object
    already has its id, so a grandchild can point at a parent that has never
    been written.

    `seen` is how a second pass over the same tree tells a child it has already
    done from one a `@before_save` queued while it ran. It has to be told rather
    than work it out, because a field that named an `on_add` is filled here and
    filling it again on the second pass would run the handler twice for one
    save. Walked into either way — a child that has been prepared may have
    grown a queued child of its own since — and by identity, because two
    children holding the same values are two children and a dataclass says they
    are equal.
    """
    from dray.collection import _filled_by_write

    prepared = []
    done = set() if seen is None else seen

    def walk(record: Any, table: str) -> None:
        for items in (getattr(record, "_dray_sets", None) or {}).values():
            cls = items.cls
            for item in items._adding:
                if id(item) not in done:
                    done.add(id(item))
                    _told(item, cls, given)
                    object.__setattr__(item, cls.__dray_parent_type__, table)
                    object.__setattr__(
                        item, cls.__dray_parent_id__, key_of(record)
                    )
                    # `True` at every level, because a queued child is a row
                    # that does not exist yet however deep it hangs — which is
                    # what a rule reading `write.adding` on one is told.
                    filled, write = _filled_by_write(cls, item, True, given)
                    prepared.append((cls, item, filled, write))
                walk(item, cls.__dray_table__)

    for record in records:
        walk(record, parent_table)
    return prepared


def _told(item: Any, cls: type, given: dict[str, Any]) -> None:
    """
    What the write was told, onto a record that did not say otherwise.

    A value said on the record itself is the narrowest and wins; everything else
    the write was told is available to whatever the field declared. Converted on
    the way in, because this is another door a value arrives at from outside —
    `save(given={"whom": me})` hands over whatever the application calls a
    person, and the field is where it says what to make of one.

    Which fields were said comes off the object, put there at construction and
    added to by every assignment since. That is what lets a note built anywhere
    at all be attached later and still be filled in correctly — `add` no longer
    has to have watched it being made — and it is the same fact that lets this
    run on a record rather than only on its children.

    Around `__setattr__` deliberately. A write filling `imported_from` is not a
    change somebody made, and firing `on_change` here would queue a child in the
    middle of a write, after the tree has already been walked, where it would be
    silently dropped. The value is checked instead, by the `_dray_validate`
    that follows every call of this.

    Doing it twice lands the same values, which is what lets `_validate_queued`
    run before the chunking and `_prepare` apply it again inside it.
    """
    said = getattr(item, "_dray_said", None) or ()
    for key, value in given.items():
        if key in cls.__dray_fields__ and key not in said:
            object.__setattr__(
                item, key, convert(key, value, cls.__dray_fields__[key])
            )


def _validate_queued(
    records: Sequence[Any],
    given: dict[str, Any],
    seen: set[int] | None = None,
) -> None:
    """
    Every field rule on every child queued against these records, run before the
    first transaction opens.

    `add_all` and `save_all` validate every record up front so that a bad value
    cannot leave a set half written — a bad record at position 4,000 must not
    leave the first 2,000 committed. What rides with a record is held to the
    same promise: a bad note left to the chunk carrying it would be found on
    the third transaction with the first two already durable.

    What the write was told is applied first, exactly as `_prepare` will apply it
    again. Checking before that would reject a child whose field the write was
    always going to fill — the store's `defaults` carrying an author the class
    declares no usable default for is the ordinary case, and refusing it here
    would break a write that works. What a child's own `@check` reads is filled
    later still, so those are run by the write once the whole tree has been
    prepared.

    The whole tree, for the same reason `_prepare` walks it: an attachment queued
    on a queued note is written by the same save, so it is checked by the same
    pass.

    `seen` names the children this pass has already judged, and is what a child
    a `@before_save` queued is told apart by — the same set `_prepare` reads,
    and left alone here, because which children have been prepared is the
    question that set answers.
    """
    done = () if seen is None else seen

    def walk(record: Any) -> None:
        for items in (getattr(record, "_dray_sets", None) or {}).values():
            for item in items._adding:
                if id(item) not in done:
                    _told(item, items.cls, given)
                    item._dray_validate()
                walk(item)

    for record in records:
        walk(record)


def _insert_all(batch: Any, prepared: Sequence[tuple]) -> list[Any]:
    """Queue the prepared children on the batch their parent is already
    filling, so a note and the change it explains land in one transaction —
    and, now, in one round trip. Hands back the statements, because what each
    one filled in is not known until the batch comes back."""
    from dray.collection import _written_for

    sent = []
    for cls, item, filled, _ in prepared:
        names, holders, params = _written_for(cls, item, filled)
        computed = [name for name, value in filled.items() if isinstance(value, Sql)]
        returning = f" returning {', '.join(computed)}" if computed else ""

        # Bound as defaults rather than closed over, because every child in
        # this loop makes one of these and a closure would hand them all the
        # last child's row.
        def landed(
            cur: psycopg.Cursor,
            item: Any = item,
            computed: list[str] = computed,
        ) -> None:
            # Anything the database worked out for itself comes straight back,
            # the same as it does for a record. It is a child that needs this
            # most: `written_at = field(on_add=clock)` is how a note is
            # timestamped, and without it the note in hand says `None` about a
            # row that has the time.
            if computed:
                for name, value in zip(computed, cur.fetchone()):
                    object.__setattr__(item, name, value)

        sent.append(
            batch.send(
                f"insert into {cls.__dray_table__} ({', '.join(names)})"
                f" values ({', '.join(holders)}){returning}",
                params,
                landed=landed,
                # The sentence a collection's insert raises, because this is
                # the same event: a queued child is on the way in, and the line
                # the manual draws has `DuplicateRecord` on that side of it.
                # A caller cannot see from `except` which side of that line
                # a clash landed on, and batching is the way the name goes
                # missing — a hundred statements sent before any result is read
                # is a hundred ways for an error to arrive detached from its
                # row — so it is named here and matched back by position.
                clash=(cls, item),
            )
        )
    return sent


def _attach_all(store: Any, prepared: Sequence[tuple]) -> None:
    """
    Give each written child its way back to storage.

    A child queued with `notes.add(...)` is an ordinary row once its parent's
    save has written it, and the object handed back by `add` should be able to
    say so — `note.body = ...; note.save()` without going and reading it again
    through the parent it just rode in with.

    After the commit, beside where a record is attached and for the same
    reason: a child pointed at a transaction that rolled back would offer a
    save against a row that is not there.
    """
    from dray.collection import _collection_for

    collections: dict[type, Any] = {}
    for cls, item, *_ in prepared:
        if cls not in collections:
            collections[cls] = _collection_for(store, cls)
        collections[cls]._attached(item)
