"""
What a record asks dray to call, and how dray finds it.

Everything dray calls through a field is asked for where the field is declared:
`converter` and `validator` on the way in, `on_change` on assignment, `on_add`
and `on_save` at the write. What is about the record rather than about one of its
values has no field to sit on — a rule spanning two of them, or a step that has
to happen at a moment no field knows anything about — so it is written as a
method, and a method is reached for by a name, which is the whole difficulty this
module exists to answer.

**A hook is found by the marker a decorator leaves, never by the method's
name.** dray calling `check()` because it is spelled `check` would be dray
taking that word from every domain that has one, without a word said: a
`Parcel` whose `check()` means the customs check would start running before
every write. The decorator says the intent instead, so the spelling stays the
domain's — and a rule reads better named after what it is about than after who
calls it, since `ends_after_it_starts` is what somebody sees in the traceback.

**A marked method is handed what it is about.** Three of these are about the
record alone and take nothing but `self`: `@check` runs at `parse`, where there
is no write to speak of; `@before_delete` is about a removal `delete()` was told
nothing about; `@after_commit` is about rows that have landed. `@before_save` is
the one that runs inside a write a caller parameterised, so it is about two
things — this record and this write — and is handed both.
"""

import inspect
from collections.abc import Callable
from typing import Any

# What a decorator leaves behind, and the whole of how a hook is found. Under
# the prefix everything else dray puts on a class wears, because a method
# carrying this is one dray will call and a domain that happens to define
# `_dray_hook` on a function of its own has already been told the prefix is not
# theirs.
MARKER = "_dray_hook"

# The hooks there are, as the marker spells each.
CHECK = "check"
BEFORE_SAVE = "before_save"
BEFORE_DELETE = "before_delete"
AFTER_COMMIT = "after_commit"

# What each hook hands a marked method beyond the record itself, named as a
# signature would spell it. Here rather than at each call site because this is
# what a class is refused over, and the refusal and the call have to agree —
# a table saying less than `run` passes would turn a correct class away.
HANDED: dict[str, tuple[str, ...]] = {
    CHECK: (),
    BEFORE_SAVE: ("write",),
    BEFORE_DELETE: (),
    AFTER_COMMIT: (),
}


def check(method: Callable) -> Callable:
    """
    A rule about the whole record, run at every door the record comes in by.

        @record(table="event", collection="events")
        class Event:
            starts_at: datetime
            ends_at: datetime | None = field(default=None)

            @dray.check
            def ends_after_it_starts(self):
                if self.ends_at and self.ends_at <= self.starts_at:
                    raise ValueError("an event cannot end before it starts")

    Where a validator is handed one value, this is handed the record, which is
    the whole of why it exists: a rule spanning two fields has nowhere else to
    live.

    **It runs at `parse`, and once again on the way to storage** — so a record
    `parse` accepts is one the write will not refuse for anything `parse` could
    already see, which is what makes it safe to hand the result of a form
    straight to `add`. Never on assignment, since a record part-way through
    being built would pass or fail on the order its fields happen to be written
    in, and never on loading a row. Raise to reject and say nothing to accept,
    as a validator does. What you raise is what your caller catches: dray hands
    it on untouched, so raise `ValidationError` to have dray's name on it and a
    plain `ValueError` if you would rather it did not.

    **A rule sees what is set when it runs, and that differs between the two.**
    At `parse` the write has filled in nothing, so a field the store's
    `defaults` carry or an `on_add` supplies is not there yet; on the way to
    storage it is, because the pass runs once per write after the filling. A
    rule reading such a field says nothing about it while it is absent — the
    `and` in the example above is that guard — and is judged at the write, which
    is the first moment there is anything to judge. A field filled with `Sql`
    for the database to work out is never readable at all: there is no Python
    value until the row comes back.

    A rule that has to read *another* record reaches the store through
    `self.store`, and there is one to reach only where the record has already
    been in a store — not at `parse`, and not at the `add` that first writes it,
    since the write attaches a record after these have run.

    Every field rule runs first at either door, so a rule comparing two dates is
    never what reports a string where a date belonged — and a record that fails
    both hears about the fields, because the pass stops there.

    Because the write's pass runs after the filling, an `on_add` has already
    fired by the time a rule refuses the record. That costs nothing for a
    handler that returns a value, which is what one is for; a handler that also
    does something in the world will have done it for a write that never
    happened.

    Several are allowed on one record and all of them run, in the order they are
    written, a base class's first. Name the method after the rule; dray finds it
    by this decorator, and a method called `check` without one is never called
    by dray at all.
    """
    return marked(method, CHECK)


def before_save(method: Callable) -> Callable:
    """
    What has to happen before this record is written, inside the write's own
    transaction.

        @record(table="parcel", collection="parcels")
        class Parcel:
            depot: str = field(default="")

            @dray.before_save
            def keep_what_it_said(self, write):
                self.store.moves.add(Move(parcel=self.id, to=self.depot))

        parcel.save()                       # the rule runs
        store.parcels.save_all(batch)       # it runs, once for each of them
        store.parcels.add(parcel)           # and on the write that creates it

    Every door, which is what separates it from a `save` method of your own —
    that one stands in front of the call and `save_all` walks past it. Where
    `@check` is a rule about the *values* and runs before any transaction is
    open, this is a rule about the *write* and runs inside it: what it writes
    lands with the row or not at all, and what it refuses leaves the row where
    it was. Reach for `@check` first, and for this only where the rule has to
    write, or has to read another record with the transaction already open.

    **The second parameter is the write this is running inside**, the same
    `Write` an `on_add` or an `on_save` handler is given. `write.given` is what
    the write was told — the store's `defaults` under the `given=` of the call —
    which is how a rule asks *who is asking* without trusting every call site to
    have put it on the record. `write.adding` is true on the write that creates
    the record and false on a save of one that exists, which is the only honest
    way to tell those apart: the etag is minted at construction, so a record has
    one before its first row. `write.record` is this same object. A rule wanting
    none of it takes the parameter and ignores it, and a method that does not
    take it is refused where the class is written rather than at the first save.

    **`write.was` is what the record held before this write**, for the fields
    that have moved and for no others. It matters because `self` is the record
    as it *will be*: a rule judging a field the same write can assign — an
    owner, a status — compares the caller against the caller and passes. Written
    `write.was.get("owner", self.owner)` it holds either way, since a field
    nobody touched still holds what the row does. What it does not see is on the
    manual page under *Before a record is written*, and the sharp one is a blob
    container edited in place: nothing was assigned, so nothing was remembered.

    `given` is a plain dict and the same one for every record in the write. dray
    has finished reading it by the time this runs — every chunk is prepared
    before the first is sent — so writing into it changes nothing. `was` is
    read-only rather than merely ignored, because it is the one thing here that
    is read again: this method runs once per attempt, and a commit DSQL refuses
    is replayed against the same `Write`.

    Raise to refuse, and nothing in that transaction happened — not the record,
    not the records beside it in the same chunk, not what an earlier
    `@before_save` on the same record wrote. A set too big for one transaction
    is several of them, so the chunks already committed stay committed, which
    is `written` on a `DrayError` and is why a rule that could have been a
    `@check` should be one.

    **It runs once per attempt and not once per save**, exactly as
    `@before_delete` does. DSQL refuses a commit that raced another writer and
    dray replays the whole transaction, this included — right for what it
    writes, since the first attempt's rows went with the rollback, and wrong
    for a counter in memory, an email, or a call to another service. Those
    belong in `@after_commit`, which runs when the rows are durable and runs
    once.

    **It is a rule and not a filler.** What the write is about to send was
    worked out before this ran, so assigning a field in here does not reliably
    reach the row — a value that is filled at every write is what `on_save` is
    for. What it must not do is save itself: that is this same write again, and
    it recurses until Python stops it.

    The record is whole and attached while it runs, so it can read its own
    children, save other records into the same transaction, and reach whatever
    its fields point at. `self.store` is the store the write is going through,
    which is how the example above reaches a collection that is not hanging off
    this record.

    **It may queue a child on the record it is on** — `self.notes.add(...)` —
    and this write carries it. Which is also what keeps `records_change`
    working under a rule: that handler queues a line, so a field carrying one
    and moved in here writes its line with the row.

    A child arriving that way is judged where it arrives. Everything the caller
    queued has its field rules and its own `@check` run before the first
    transaction opens, so a bad value cannot leave half a set written; a child
    that did not exist at that moment cannot have been in that pass, and its
    rules run inside the transaction instead. What they refuse takes that
    transaction and leaves the chunks already committed committed. It runs its
    own `@before_save` too, and may queue in turn.

    The one thing it cannot do is push the transaction past the row ceiling.
    How many rows a write is was worked out before any rule ran, so a rule that
    takes a chunk over is refused by dray rather than by the cluster.

    **What it costs a bulk write is a Python call per record, inside the
    transaction, on every attempt.** A record that declares none pays a
    dictionary lookup and nothing else. A handler that only refuses or only
    writes in memory is nothing beside the statements; a handler that reads is
    a round trip per record with the transaction open, so four hundred of them
    is four hundred round trips inside one transaction and is the wrong shape
    for `save_all`.

    A queued child runs its own when the write carrying it lands — its parent's
    save, since it has none of its own. That is the opposite of the delete
    side's answer about a cascade, and for a reason that does not carry over: a
    cascade loads no rows and has nothing to run a hook on, where a queued
    child is in memory and whole.

    Several are allowed on one record and all of them run, in the order they
    are written, a base class's first. Name the method after the rule; dray
    finds it by this decorator, and a method called `before_save` without one
    is never called by dray at all.
    """
    return marked(method, BEFORE_SAVE)


def before_delete(method: Callable) -> Callable:
    """
    What has to happen before this record goes, inside the delete's own
    transaction.

        @child(of=Person, name="notes", table="note")
        class Note:
            body: str = field()

            @dray.before_delete
            def keep_what_it_said(self):
                person = self.store.people.by_id(self.parent_id)
                person.logs.add(f"note removed: {self.body}")
                person.save()

        note.delete()      # the rule runs
        person.delete()    # the notes go with them, and the rule does not

    `delete` opens a transaction of its own, so no caller can wrap it, and
    "remove this and write down what it said" has nowhere else to be atomic.
    Anything written in here lands with the removal or not at all.

    Raise to refuse, and the row is still there — which gives a record its
    domain says is never deleted one place to say so rather than a check at
    every call site. What you raise is what your caller catches: dray hands it
    on untouched.

    **It runs once per attempt and not once per delete.** DSQL refuses a commit
    that raced another writer and dray replays the whole transaction, this
    included — which is right for what it writes, since the first attempt's rows
    went with the rollback, and wrong for anything the rollback cannot undo. A
    counter in memory, an email, a call to another service: those belong in
    `store.after_commit`, which runs when the delete is durable and runs once.

    The record is whole and still attached while this runs, so it can read its
    own children, save other records into the same transaction, and reach
    anything its own fields point at. `self.store` is the store the delete is
    going through, which is how the example above reaches a record that is not
    hanging off this one — the class is written long before any store exists.
    Nothing else is handed over, because there is nothing to hand: `delete()`
    takes no arguments, so a removal was told nothing a rule about it could
    read. That is what separates this from `@before_save`, which runs inside a
    write a caller parameterised and is given it. What it must not do is delete
    itself: that is this same delete again, and it recurses until Python stops
    it.

    **A cascade does not run them.** `delete` takes a record's descendants out
    with one statement per generation and never loads a row of them, so only the
    record `delete` was called on runs its own hooks. Reaching the descendants'
    would mean reading every one of them first, which is the cost that design
    exists to avoid.
    """
    return marked(method, BEFORE_DELETE)


def after_commit(method: Callable) -> Callable:
    """
    What happens once this record's rows are durable, and not at all if they
    are not.

        @record(table="parcel", collection="parcels")
        class Parcel:
            status: str = field(default="held")

            @dray.after_commit
            def tell_the_depot(self):
                enqueue("parcel-accepted", self.id)

    The queued job above all. A worker is another process on another
    connection and cannot see a row that has not committed, so a job enqueued
    from inside the write is a race with something that is not waiting — it
    looks the record up and finds it absent, or finds it as it was.

    Inside a block somebody opened it waits for that block and runs when the
    outermost one commits; with no block open the save has committed by the
    time it returns, so it runs straight after. A block that rolls back never
    runs it, and one that is run again starts with nothing queued from the
    attempt that failed.

    **It runs once per save, not once per attempt.** dray replays a write DSQL
    refuses, and this is registered outside the replayed part for the reason
    `on_add` and `on_save` are filled outside it — which is the opposite of
    where a `before_delete` belongs, because that one is work a rollback
    destroyed and has to redo.

    It runs after the transaction has closed, so the record is whole and still
    attached and can be read, and anything it writes is a transaction of its
    own rather than part of the one that just landed. `self.store` is the store
    the write went through, and a read through it sees the committed rows — so a
    rule that has to look something else up before it announces anything can be
    written on the class rather than in a service function. If it raises,
    `AfterCommitFailed` — the rows are committed, so running the work again
    writes it twice. Every handler runs whatever the ones before it did, and
    `.failures` carries all of them.

    Two saves are two runs: it is about the write rather than about the record,
    and two writes happened. It does not run on a delete, which is
    `before_delete`'s side — nothing here could tell the two apart, and a
    handler holding a record whose row has gone can do very little with it.

    Several are allowed on one record and all of them run, in the order they
    are written, a base class's first. `store.after_commit` is the same moment
    reached from a service function rather than from the class.
    """
    return marked(method, AFTER_COMMIT)


def marked(method: Callable, hook: str) -> Callable:
    """
    Mark a method as one dray calls, and hand it back unchanged.

    Unchanged, rather than wrapped, so that the class keeps an ordinary method:
    it is still callable by hand, still overridable, still what a traceback and
    a debugger say it is. The marker is a note for `_declare` to read at
    declaration and nothing at all afterwards.

    Refused here if it is not callable, because `@check` above a `field(...)` is
    a mistake worth hearing about while the class is being written rather than
    as a `'Field' object is not callable` at somebody's first save.
    """
    if not callable(method):
        raise TypeError(
            f"@{hook} marks a method, and {method!r} cannot be called. It goes "
            "on a def in the class body, above the rule it names."
        )
    setattr(method, MARKER, hook)
    return method


def declared_on(cls: type) -> dict[str, tuple[str, ...]]:
    """
    Which methods this class marked, by hook and in the order they were
    written.

    Names rather than the functions themselves, so a subclass overriding a
    marked method gets its own called instead of the one it replaced — ordinary
    Python dispatch, which is what somebody reading the subclass expects. It
    also means an override need not repeat the decorator, though repeating it
    does no harm. What it cannot be allowed to mean is a name taken over by a
    class that marked nothing, which `check_reached` refuses below.

    The bases first, because a rule a base class carries is a rule about every
    record built on it, and a method overriding one keeps the place the base
    gave it. Order matters only in that a reader has to be able to predict it,
    and *declaration order, bases first* is the only answer anybody would guess.

    A class the decorator was never used on gets an empty mapping, which is the
    ordinary case and costs a record nothing.
    """
    found: dict[str, list[str]] = {}
    for base in reversed(cls.__mro__):
        for name, value in vars(base).items():
            hook = getattr(value, MARKER, None)
            if not isinstance(hook, str):
                continue
            names = found.setdefault(hook, [])
            if name not in names:
                names.append(name)
    for hook, names in found.items():
        for name in names:
            check_reached(cls, hook, name)
            check_signature(cls, hook, name)
    return {hook: tuple(names) for hook, names in found.items()}


def check_reached(cls: type, hook: str, name: str) -> None:
    """
    Refuse a marked name that some other class's method has taken over.

    Collecting names rather than functions is what lets a subclass override a
    marked method without repeating the decorator, and that is wanted. It has a
    hole under multiple inheritance, because the method `run` reaches is
    whichever the MRO puts first and the MRO knows nothing about markers: a
    record built on a mixin that marked `tidy` and on an unrelated class that
    also happens to define `tidy` calls the unrelated one, which nobody offered
    dray. That is the one promise the decorator exists to keep, so it is refused
    where the class is written rather than found in the traceback of whatever it
    ran.

    The distinction is who wrote the method that wins. A class overriding a rule
    it inherited is saying something about that rule, whether or not it repeats
    the decorator. A class that never heard of the rule and shares a word with it
    is a collision, and there is nothing to read off the two classes to say which
    of them was meant.
    """
    # The first class in the order holding the name is the one `getattr` will
    # reach at the call, so it is the only one there is anything to decide
    # about — and it carries the marker itself in every ordinary case, either
    # because it is the class that marked the method or because the override
    # repeated the decorator.
    provider = next(base for base in cls.__mro__ if name in vars(base))
    if getattr(vars(provider)[name], MARKER, None) == hook:
        return
    marking = [
        base
        for base in cls.__mro__
        if getattr(vars(base).get(name), MARKER, None) == hook
    ]
    if any(issubclass(provider, one) for one in marking):
        return
    raise TypeError(
        f"{marking[0].__name__} marks {name} with @{hook}, and "
        f"{cls.__name__} resolves {name} to {provider.__name__}.{name}, which "
        "nothing marked — so dray would call a method it was never shown. "
        f"{provider.__name__} knows nothing about the rule and shares a word "
        "with it, and there is nothing to say which of the two was meant. "
        "Rename one of them, or mark the one that is going to run."
    )


def check_signature(cls: type, hook: str, name: str) -> None:
    """
    Refuse a marked method that cannot be called with what its hook hands over.

    Where the class is written rather than at somebody's first save, which is
    the whole point: a `@before_save` spelled `(self)` is a `TypeError` from
    inside a transaction, on a write that had already opened one, and the
    traceback names dray's call rather than the line that is wrong.

    The method the *resolution* reaches, not the one that carries the marker, so
    an override that dropped the parameter is caught too — an override need not
    repeat the decorator, which is what makes it easy to forget the signature
    that came with it.

    Bound rather than counted, so a handler written `(self, *args)` or with a
    parameter it has defaulted is allowed: what matters is whether the call dray
    is going to make will work, and Python already knows how to answer that.
    Anything `inspect` cannot describe is let through — a callable that is not a
    function has its own reasons, and refusing what cannot be read would be dray
    guessing.
    """
    provider = next(base for base in cls.__mro__ if name in vars(base))
    method = vars(provider)[name]
    handed = HANDED[hook]
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return
    try:
        signature.bind(*(None,) * (1 + len(handed)))
    except TypeError:
        pass
    else:
        return
    takes = ", ".join(("self", *handed))
    if handed:
        raise TypeError(
            f"@{hook} calls {provider.__name__}.{name}({takes}), and "
            f"{name}{signature} cannot be called that way. A rule about a "
            "write is about two things — this record and this write — so it "
            "is handed both, where the other three are about the record "
            "alone. Take the parameter and ignore it if the rule has no use "
            "for it."
        )
    raise TypeError(
        f"@{hook} calls {provider.__name__}.{name}({takes}), and "
        f"{name}{signature} cannot be called that way. It is handed nothing "
        "but the record: "
        f"@{BEFORE_SAVE} is the only one given more, because it alone runs "
        "inside a write a caller parameterised."
    )


def declares(record: Any, hook: str) -> bool:
    """
    Whether this record marked anything for a hook.

    Asked where the answer decides whether to *keep* something rather than
    whether to call it: a write queues one piece of work per record whose
    `after_commit` will have to run, and a bulk write of three thousand records
    that declared none should queue nothing at all rather than three thousand
    calls to `run` with an empty tuple to walk.
    """
    return bool(record.__dray_hooks__.get(hook))


def run(record: Any, hook: str, *handed: Any) -> None:
    """
    Call what this record marked for a hook, in order, with whatever the caller
    hands over.

    Through `getattr` on the instance rather than on anything kept from the
    declaration, which is what makes an override work and what keeps a bound
    method out of dray's bookkeeping.

    Nothing is inspected here. `HANDED` says what each hook gives a method and
    `check_signature` refused any class that could not take it, so by the time
    this runs there is nothing left to decide — the call site passes what its
    moment has and this hands it straight on.

    Nothing is caught either. What a hook raises is the caller's own exception,
    and wrapping it would put dray's name on a message the domain wrote.
    """
    for name in record.__dray_hooks__.get(hook, ()):
        getattr(record, name)(*handed)
