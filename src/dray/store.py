"""
The connection, and what to do when DSQL says no.

A store holds one connection and hands out collections. It is the only thing in
dray that knows a database exists at all.
"""

import functools
import random
import re
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from dray.caching import Cache, Caches
from dray.model import RECORDS, DrayError, key_of
from dray.watching import KINDS, UNWATCHED, Seen, Span, Watch


def _connector() -> Any:
    """AWS's connector, or a refusal that names the thing to install.

    Imported here rather than at the top of the file because dray does not
    need it: a store handed a connection somebody else opened never reaches
    this, and making it a hard dependency would mean every user of that shape
    installing an AWS SDK to not use it.

    The cost of that is the error, and left alone it is a bad one.
    `ModuleNotFoundError: No module named 'aurora_dsql_psycopg'` names a
    package nobody typed, that appears in no documentation, and that cannot be
    guessed back to the extra it lives in. So it is caught and said again with
    the command to run — and chained rather than replaced, so the traceback
    still shows the import that failed.
    """
    try:
        import aurora_dsql_psycopg
    except ModuleNotFoundError as missing:
        # Only when the connector itself is the module that is missing. It
        # imports boto3 and its own core on the way in, and any of those going
        # wrong is a different problem with a different answer: telling somebody
        # to install an extra they already have sends them to look in the one
        # place that is fine. Everything else goes up as itself, including
        # psycopg failing to find a libpq — which cannot reach here anyway,
        # since `import dray` imports psycopg long before this runs.
        if missing.name != "aurora_dsql_psycopg":
            raise
        raise DrayError(
            "connecting to a cluster needs AWS's own connector, which dray "
            "does not install by default — a store handed a connection you "
            "already have does not need it. Install the extra:\n\n"
            '    uv add "dray[dsql]"        # or: pip install "dray[dsql]"'
        ) from missing
    return aurora_dsql_psycopg

# DSQL rejects a commit that conflicted rather than blocking to avoid one, so a
# write path is expected to be replayed. How long to wait between attempts, and
# why the wait doubles, is `waiting` below — one place, so the two cannot say
# different things. What is decided here is only how many: four waits capped at
# 50, 100, 200 and 400 ms, so a write that loses all five attempts spends at
# most 750 ms waiting.
ATTEMPTS = 5
BACKOFF = 0.05
# The longest any one of those waits gets. dray's own five reach it on the last
# of them and never exceed it, so this changes nothing about a save — it is
# there for `@replaying`, where the count is the caller's and twenty attempts of
# unchecked doubling would be waiting for hours. Past half a second the doubling
# has done whatever breaking up a crowd it is going to do.
CEILING = 0.4


class RecordNotFound(DrayError, LookupError):
    """Asked for by id, and not there."""


class RecordHasChanged(DrayError, RuntimeError):
    """
    Somebody else wrote this record between it being read and being saved.

    Raised only when a save was given an etag to check. Without one, the last
    write wins and nothing says so — which is what makes the guard worth passing
    from any page a person can sit on.

    `ids` is which records moved, `records` is each of those as the table now
    has it, and `written` is which had already landed before the write stopped.
    An id in `ids` with no record among `records` is one that has gone: a
    guarded save is refused the same way whether somebody wrote the row or
    removed it, and this is how a caller tells the two apart without going back
    to the store. All three matter for a set: a bulk write above the row ceiling
    is several transactions, so the one holding the conflict rolls back and the
    ones before it do not. Reading a private attribute to find that out is the
    alternative.

    All three are empty where a save was refused before it asked the database
    anything — the comparison `save(etag=...)` makes against the record in hand.
    Nothing was read there, so there is nothing to carry, and the record and its
    id are both already in the caller's hand.
    """

    def __init__(
        self,
        message: str,
        *,
        ids: Sequence[Any] = (),
        records: Sequence[Any] = (),
    ) -> None:
        super().__init__(message)
        self.ids = tuple(ids)
        self.records = tuple(records)


class DuplicateRecord(DrayError, ValueError):
    """
    A unique constraint refused an insert.

    `columns` is the ones that clashed and `constraint` is the index that said
    so, both taken off the database's own report of it. They are here because
    the key is the wrong thing to name on an insert: it is minted here, so it
    is the one value certain not to be the clash. A caller
    catching this to say *that slot is taken, pick another* needs to know which
    slot, and parsing it back out of a sentence is the alternative.

    Either can be `None`: a database that reported neither, or a clash on the
    key itself, where the key is the whole of what went wrong and the message
    has always said so.
    """

    def __init__(
        self,
        message: str,
        *,
        columns: tuple[str, ...] | None = None,
        constraint: str | None = None,
    ) -> None:
        super().__init__(message)
        self.columns = columns
        self.constraint = constraint


def _clash(cls: type, record: Any, error: Any) -> "DuplicateRecord":
    """What the database refused, said in the caller's terms.

    `key_of` was the whole of this message and is the wrong thing to name on an
    insert: the key is minted here, so it is the one value that certainly does
    not already exist. psycopg carries the real answer on `diag` — the
    constraint, and the columns inside its detail line — so the message names
    those and the exception carries them for a caller who has to act on them
    rather than log them.

    **The values are left out of the message**, and it is worth being exact
    about what that buys. A unique column can hold an email or a medical record
    number, so `str(error)` is safe to put in a response or a structured log
    field. The traceback is not: the `UniqueViolation` this is raised *from*
    carries `Key (mrn)=(A1234567) already exists` and anything printing a chain
    prints it. Dropping the chain would cost more than it saves.

    Which columns clashed is read out of a sentence PostgreSQL writes, so it is
    parsed defensively: an index over an expression reads `Key (lower(email))=`
    and a server not running in English says it in another language, and both
    come back with nothing. The fallback names the constraint instead, because
    the one thing that must never happen here is naming the key again — that is
    the false sentence this whole function exists to stop.
    """
    diag = getattr(error, "diag", None)
    constraint = getattr(diag, "constraint_name", None)
    detail = getattr(diag, "message_detail", None) or ""
    # `Key (table_id, slot_at)=(4, 19:00) already exists.` — the names before
    # the `=` are worth having and the values after it are not. An expression
    # index puts brackets inside the brackets and matches nothing, deliberately.
    found = re.match(r"Key \(([a-z_][a-z0-9_]*(?:, [a-z_][a-z0-9_]*)*)\)=", detail, re.I)
    columns = tuple(name.strip() for name in found.group(1).split(",")) if found else None

    # A clash on the key is the one case where the value is the answer rather
    # than a liability: an import carrying its own keys wants to know which id
    # collided, and a key is dray's rather than anybody's data.
    if columns == (cls.__dray_key__,):
        return DuplicateRecord(
            f"{cls.__name__} {key_of(record)!r} exists",
            columns=columns,
            constraint=constraint,
        )
    # No article, because `a Event` is wrong and working out which one to use
    # from a class name is not this function's job.
    if columns:
        message = f"{cls.__name__} with that {', '.join(columns)} exists"
    elif constraint:
        message = f"{cls.__name__} refused by the unique index {constraint}"
    else:
        message = f"{cls.__name__} refused by a unique index"
    return DuplicateRecord(message, columns=columns, constraint=constraint)


class ConcurrencyExhausted(DrayError, RuntimeError):
    """Replayed until the attempts ran out and still conflicted."""


class CommitRefused(DrayError, RuntimeError):
    """
    DSQL refused the commit of a transaction the caller opened.

    Its own name rather than `ConcurrencyExhausted` because nothing was
    exhausted — nothing was even attempted twice. dray replays a write DSQL
    refuses, and that replay belongs to the transaction dray opened; a block
    somebody else opened is theirs to run again, because only they know what
    else is inside it and whether any of it may happen twice.

    So the two names divide the same event by who is expected to act. An
    ordinary save that reaches five attempts and still conflicts is
    `ConcurrencyExhausted` and there is nothing left to try. This is the first
    refusal of the first attempt, and running the work again is very likely to
    land.
    """


class AfterCommitFailed(DrayError, RuntimeError):
    """
    One or more `after_commit` handlers raised. **The rows are committed.**

    Which is the whole reason this has a name of its own. Everything else that
    comes out of a `with store.transaction()` means the work did not land, so a
    caller catching broadly and treating failure as "it did not happen" is right
    about all of them and wrong about this one — and running the work again here
    writes it twice.

    The handlers are independent of each other, so one raising does not stop the
    rest: every one is run, and `failures` holds what they raised, in the order
    they were queued. `__cause__` is the first of them, because a traceback
    showing one real error beats a summary showing none.

    What it does not tell you is which handler went with which failure. A
    handler is usually a lambda and prints as one, so a list of them would be a
    list of `<lambda>`; if that matters, the thing to do is give the handler a
    name and let its own exception say so.
    """

    def __init__(
        self, message: str, *, failures: Sequence[BaseException] = ()
    ) -> None:
        super().__init__(message)
        self.failures = tuple(failures)


class ConnectionLost(DrayError, psycopg.OperationalError):
    """
    The connection this store was working on was closed underneath it.

    A `psycopg.OperationalError` still, because that is what it is and what
    anybody already handling a dead connection catches — this is a better
    sentence rather than a new thing to write an `except` for. The name is here
    so that dray can recognise its own: a write meets a closed connection at
    `begin` and can meet it again at the statement, and without a name the
    second telling would bury the first.
    """


def firing(work: Sequence[Callable[[], None]]) -> None:
    """
    Run every one of these now that the rows are durable, and say what they
    raised.

    All of them, whatever the ones before did. They are independent by
    construction — three closures are three reporters — so stopping at the
    first failure would invent a dependency between them that nobody wrote.
    What comes out is one `AfterCommitFailed` carrying the lot, because the
    thing a caller has to be told is that the write landed regardless.
    """
    failed = []
    for one in work:
        try:
            one()
        # Every failure is collected and raised together below, so a broad
        # catch here is the point rather than a slip.
        except Exception as error:
            failed.append(error)
    if failed:
        raise AfterCommitFailed(
            f"the rows committed, and then {len(failed)} of {len(work)} "
            f"after_commit handlers raised. The write landed — do not run the "
            f"work again. See `.failures` for all of them.",
            failures=failed,
        ) from failed[0]


def enlisted(owner: Any) -> bool:
    """Whether this write is inside a transaction somebody else opened.

    Taken off the store, whether it was handed one directly — `Store._ddl` — or
    reached through a collection. A write that owns its transaction end to end
    is the only kind that may be replayed."""
    store = getattr(owner, "store", owner)
    return bool(getattr(store, "in_transaction", False))


def waiting(turn: int) -> None:
    """
    Wait before the next attempt at something DSQL refused.

    One function for dray's own replay and the caller's, because there is one
    right answer to *how long* and two of them would be two answers a reader has
    to compare. The wait doubles with the turn: the first is short, since a
    conflict is resolved by somebody else committing and that has already
    happened by the time we hear, and the later ones are long enough to back off
    a crowd rather than reshuffle it.

    Full jitter, so each of those numbers is a ceiling and not a pause everybody
    takes together. It is the later turns that need it: 400 ms is long enough
    that two writers coming off it at the same instant is a conflict of their
    own making, which is not true of two coming off 50 ms.
    """
    time.sleep(random.uniform(0, min(CEILING, BACKOFF * 2**turn)))


def retrying(work: Callable) -> Callable:
    """
    Replay the whole thing when DSQL refuses the commit.

    Only ever wrapped around something that owns its transaction end to end,
    because a replay must redo all of it. Safe for the usual reason: a rejected
    commit made nothing durable, so the work lands exactly once.

    Which is a precondition rather than a description, so it is checked. Inside
    a block the caller opened, the same function owns nothing: it enlists, the
    commit is somebody else's, and a refusal leaves that transaction aborted
    with every statement in it already spent. Replaying there redoes the
    statements against a transaction PostgreSQL has stopped accepting, so the
    caller gets `InFailedSqlTransaction` from the second attempt instead of the
    refusal from the first — and the backoff sleeps inside a transaction that is
    ageing against DSQL's five minutes while it waits.

    So an enlisted write does not retry. The refusal travels up to the block
    that owns the commit, where `Store.transaction` turns it into
    `CommitRefused` and the caller runs the work again — which is the whole of
    what *Running it again* on the page is about.

    The wait before each replay doubles rather than stepping, because growth is
    what clears contention where an even step only spreads the same crowd
    thinner — eight writers on one row cost a quarter fewer attempts under a
    doubling wait and stopped running out of them altogether. What it costs is
    wall clock: five attempts is at most three quarters of a second of waiting,
    and that is dray's whole contribution to a timeout somebody is choosing.
    """

    @functools.wraps(work)
    def attempt(*args: Any, **kwargs: Any) -> Any:
        # Which turn this is, left where the transaction span can find it. The
        # count belongs to the replay and the replay is here, but the thing a
        # reader wants it on is the transaction — so it is written onto the
        # store rather than passed down through three signatures that have no
        # other use for it. A store is one thread, so no two writes are counting
        # at once, and it is put back on the way out so that the next
        # transaction does not inherit somebody else's second attempt.
        store = getattr(args[0], "store", args[0]) if args else None
        counting = isinstance(store, Store)
        try:
            for turn in range(ATTEMPTS):
                if counting:
                    store._attempt = turn + 1
                try:
                    return work(*args, **kwargs)
                except psycopg.errors.SerializationFailure:
                    # Raw, so the block that owns the commit can name it.
                    # Turning it into `ConcurrencyExhausted` here would say five
                    # attempts were made where none were.
                    if enlisted(args[0]):
                        raise
                    if turn == ATTEMPTS - 1:
                        raise ConcurrencyExhausted(
                            f"{work.__name__} conflicted {ATTEMPTS} times"
                        ) from None
                    waiting(turn)
            return None
        finally:
            if counting:
                store._attempt = 1

    return attempt


def replaying(attempts: Any = ATTEMPTS) -> Callable:
    """
    Run this function of yours again when DSQL refuses what it wrote.

        @replaying
        def close(store, ticket_id: str) -> None:
            ticket = store.tickets.by_id(ticket_id)
            ticket.status = "closed"
            ticket.notes.add("Closed by the reporter.")
            with store.transaction():
                ticket.save()

    The loop from *Running it again*, written once. It goes on the function
    rather than around the block, for the reason that section gives: what you
    queue on a record before the block is not inside the block, so re-running
    the block alone writes a queued line twice in one spelling and loses it in
    the other. And it cannot be a `with` block of dray's either — a context
    manager may swallow what its body raised and has no way to run that body
    again.

    A count and nothing else, dray's own five by default: `@replaying(20)` for
    a job nobody is waiting on, where the wait costs nothing and giving up
    costs a run. When they run out, what comes out is `ConcurrencyExhausted`
    naming your function, and the last refusal is on it as `__cause__`. The
    wait between attempts is dray's own and is not yours to choose — it is the
    same one an ordinary save takes.

    Both of the ways a refusal reaches you count as one: `CommitRefused` from
    the commit of a block your function opened, and `ConcurrencyExhausted` from
    an ordinary save inside it that dray had already replayed five times on its
    own account. The attempts you set are the depth those five do not have.

    **What it wraps has to be safe to run twice.** Everything the function does
    is inside it, nothing is queued on a record before it, and no side effect
    it has is one a rollback cannot reach — a job enqueued, a mail sent, a line
    appended to a list in memory. Those belong in `store.after_commit`, which
    runs once the rows are durable and so once however many attempts it took.

    Inside a block somebody else opened, this does nothing and the function
    runs exactly once. It has to: the refusal leaves that transaction aborted
    with every statement in it already spent, so running the work again would
    send statements to a transaction PostgreSQL has stopped accepting — and the
    wait before it would sleep inside a block ageing against DSQL's five
    minutes. The refusal goes up to whoever opened the block, which is the only
    level that can run the whole of it again.

    `retrying` is dray's own version of this and is not the one you want: it
    replays the writes dray owns, catching a refusal a caller never sees,
    and it is applied to dray's three write paths rather than to yours.
    """
    # `@replaying` and `@replaying(20)` are one name rather than two, so the
    # bare form arrives here with the function sitting where the count goes.
    # Nothing this legitimately takes is callable, so the two are told apart
    # without ambiguity.
    if callable(attempts):
        return _replays(attempts, ATTEMPTS)
    if not isinstance(attempts, int) or attempts < 1:
        raise ValueError(
            f"@replaying takes a number of attempts and {attempts!r} is not "
            "one. `@replaying` for dray's own five, or `@replaying(20)` for a "
            "job nobody is waiting on."
        )
    return lambda work: _replays(work, attempts)


def _replays(work: Callable, attempts: int) -> Callable:
    """The wrapper `replaying` builds, once the count is settled."""

    @functools.wraps(work)
    def again(*args: Any, **kwargs: Any) -> Any:
        for turn in range(attempts):
            try:
                return work(*args, **kwargs)
            # Both, because both mean the same thing to whoever wrote the
            # function: DSQL refused this and nothing landed. `CommitRefused`
            # is a block the function opened, `ConcurrencyExhausted` an
            # ordinary save inside it that dray had already replayed five
            # times — and the second is exactly the case for a caller wanting
            # more attempts than dray takes on its own account.
            except (CommitRefused, ConcurrencyExhausted) as refused:
                # Asked of the refusal rather than of the arguments, which is
                # the only way to be sure: a function that reaches its store
                # through a closure or an attribute hands this wrapper nothing
                # to inspect, and the refusal knows regardless.
                if getattr(refused, "_enlisted", False):
                    raise
                if turn == attempts - 1:
                    # Kept as the cause, where `retrying` drops it: every
                    # refusal here was swallowed by a replay the caller asked
                    # for, so without this the last one is the only evidence
                    # that anything was refused at all.
                    raise ConcurrencyExhausted(
                        f"{work.__name__} conflicted {attempts} times"
                    ) from refused
                waiting(turn)
        return None

    return again


@contextmanager
def explaining(conn: psycopg.Connection) -> Iterator[None]:
    """
    Say what a connection closed underneath dray means, where psycopg says only
    that it is closed.

    DSQL closes every connection after about an hour, busy or idle, and dray
    reconnects for nobody — a store holds the one connection it was handed. From
    above, that is a statement failing with `the connection is closed` hours
    after a deploy, on a store a warm container kept between requests: a message
    that names neither the hour nor the shape that avoids it, and that cost
    somebody an afternoon to place. `@retrying` is no help either, since that is
    for a commit DSQL refused and this connection is not there to refuse
    anything.

    Only a connection that died on its own, though. `broken` is psycopg's own
    word for the difference — closed, but not by whoever was holding it — and
    the difference is the whole of the care here. `store.close()` and then a
    statement is a store used after it was finished with, a mistake of another
    kind entirely, and psycopg's `the connection is closed` is already the truth
    about it. Telling somebody their connection aged out when they had closed it
    themselves would send them looking for an hour that never passed.

    Around the transaction as well as the cursor, because a write meets a closed
    connection at `begin` and never reaches a statement. One or the other would
    be a message that covers `find` and misses `save`.
    """
    try:
        yield
    except ConnectionLost:
        # Said already, one level down: a write that got its `begin` in and then
        # lost the connection would otherwise be told the same thing twice.
        raise
    except psycopg.OperationalError as error:
        if not conn.broken:
            raise
        raise ConnectionLost(
            "this store's connection was closed underneath it, and dray does "
            "not reconnect. DSQL closes every connection after about an hour, "
            "busy or idle, which is usually what this is: a store built by hand "
            "and kept — a warm container, a long-running job — holding a "
            "connection that was closed while nothing was happening. A store is "
            "short-lived by design, so build one per request or per job; "
            "anything longer-lived wants a `dray.Pool`, which retires its "
            "connections before DSQL closes them."
        ) from error


@contextmanager
def cursor(
    conn: psycopg.Connection,
    rows: Any = tuple_row,
    *,
    watch: Any = UNWATCHED,
    cls: type | None = None,
) -> Iterator[Any]:
    """
    A cursor that hands rows back as tuples, whatever the connection prefers.

    Every cursor dray opens says what it wants and inherits nothing. A
    connection handed over is one an application already built, and it may carry
    a row factory of its own — `dict_row` is the usual choice — which dray was
    then reading by position. `count()` became `KeyError: 0`, and worse,
    `zip(computed, cur.fetchone())` walked a dict's *keys*, so a field filled by
    the write was assigned its own column name as a string: `created_at` holding
    `'created_at'`, set through `object.__setattr__` and so past every check the
    field declared.

    Said here rather than switched on the connection, the way autocommit is.
    That one dray genuinely needs and documents; this one is only ever about the
    statements dray writes, and `store.conn` is still the caller's to query.
    Changing what their own statements hand back would be a strange thing to do
    to them on the way past.

    `rows` is for the one place that wants something else: `select_many`
    hydrates a record from a row read by name, so it asks for dicts. Asked for
    here rather than opened by hand, so every statement dray runs goes the one
    door — which is what makes the door worth having, since `explaining` sits on
    it and a cursor opened around it is a read that fails in psycopg's words
    rather than dray's. `watch` is the other thing the one door buys: an
    observer is told about a statement here, once, instead of at eleven call
    sites that would each have to remember.

    `watch` and `cls` are dray's own and default to nothing. A store nobody is
    watching hands back the driver's cursor untouched, so there is not even a
    wrapper between the statement and the socket.
    """
    with explaining(conn), conn.cursor(row_factory=rows) as cur:
        if not watch:
            yield cur
            return
        watched = watch.cursor(cur, cls)
        try:
            yield watched
        finally:
            # The last statement is still open at this point — nothing closed it
            # because nothing followed it — and the cursor going away is the end
            # of it.
            watched.finish()


class _Sent:
    """
    One statement in a batch, and what came back for it.

    `landed()` is where a batched statement turns back into an ordinary one:
    the result is matched to it by position, and whatever the caller wanted
    done with that result — a returned value onto the record, a rowcount read,
    a clash named — happens there and not before. Whatever `landed=` returned
    is what `landed()` hands back, and calling it twice does the work once.
    """

    __slots__ = (
        "batch",
        "cur",
        "statement",
        "params",
        "on_landing",
        "clash",
        "_read",
        "_answer",
    )

    def __init__(
        self,
        batch: "_Batch",
        cur: psycopg.Cursor,
        statement: str,
        params: Any,
        on_landing: Callable[[psycopg.Cursor], Any] | None,
        clash: tuple[type, Any] | None,
    ) -> None:
        self.batch = batch
        self.cur = cur
        self.statement = statement
        self.params = params
        self.on_landing = on_landing
        self.clash = clash
        self._read = False
        self._answer: Any = None

    @property
    def rowcount(self) -> int:
        """How many rows this statement touched, once it has landed."""
        return self.cur.rowcount

    def landed(self) -> Any:
        if not self._read:
            self._read = True
            self._answer = self.batch._land(self)
        return self._answer


class _Batch:
    """
    Statements queued together and read back in the order they were sent.

    A set used to be one round trip per record, which on local PostgreSQL is
    microseconds and against a cluster is a trip to another city each — inside
    the transaction that is also the conflict window. Everything queued here
    goes out together and comes back together, and a hundred waits become one.

    What that costs is the thing this class exists to pay for. Sending a
    hundred statements before reading any result would detach a failure from
    the row that caused it, so every statement gets a cursor of its own:
    psycopg hands each result to the cursor that asked for it, in order, and a
    statement that failed is the first one left without one. That is what keeps
    `DuplicateRecord` naming the record it always named.
    """

    def __init__(
        self, conn: psycopg.Connection, *, watch: Any, cls: type | None
    ) -> None:
        self._conn = conn
        self._watch = watch
        self._cls = cls
        self._sent: list[_Sent] = []
        self._pipe: Any = None
        self._flushed = False
        self._failed: psycopg.Error | None = None

    def send(
        self,
        statement: str,
        params: Any = None,
        *,
        landed: Callable[[psycopg.Cursor], Any] | None = None,
        clash: tuple[type, Any] | None = None,
    ) -> _Sent:
        """
        Queue a statement, and say what to do with its result when it comes.

        Nothing is waited for here. `landed` is handed this statement's cursor
        once the batch has come back, and `clash` is the class and record a
        unique violation on this statement is about — without it a refused
        insert comes back as the driver's error rather than as dray's.
        """
        cur = self._conn.cursor(row_factory=tuple_row)
        sent = _Sent(self, cur, statement, params, landed, clash)
        self._sent.append(sent)
        # A statement queued after something has already been read back needs
        # its own trip; the results in hand cannot contain a result for it.
        self._flushed = False
        try:
            cur.execute(statement, params)
        except psycopg.Error as error:
            # Held rather than raised. In pipeline mode psycopg sends when its
            # buffer fills, so a failure surfaces at whichever `execute`
            # happened to trigger the send rather than at the one that caused
            # it — and which row it belongs to is decided by walking the
            # results in order, which cannot happen until everything is queued.
            if self._failed is None:
                self._failed = error
        return sent

    def settle(self) -> None:
        """Land whatever has not been landed yet, in the order it was sent, so
        no statement in the batch goes unchecked."""
        for sent in self._sent:
            sent.landed()
        if self._failed is not None:
            # Every result accounted for and a failure nobody claimed, which
            # should not happen and must not be swallowed if it does.
            raise self._failed

    def _land(self, sent: _Sent) -> Any:
        watch = self._watch
        span = watch.opened("statement", cls=self._cls)
        span.sql = sent.statement
        span.params = sent.params
        try:
            # The wait, and it belongs to whichever statement asked for a
            # result first — that is genuinely where the batch stops and waits.
            # Every statement behind it reads a result already in hand, so a
            # trace of a batched write is one long `execute` and a tail of
            # short ones, which is what a batched write actually is.
            with watch.span("execute", cls=self._cls):
                self._flush()
            if sent.cur.pgresult is None:
                self._blame(sent)
            span.rowcount = sent.cur.rowcount
            return sent.on_landing(sent.cur) if sent.on_landing else None
        except BaseException as error:
            span.error = error
            raise
        finally:
            watch.closed(span)

    def _flush(self) -> None:
        if self._flushed:
            return
        self._flushed = True
        if self._pipe is None:
            return
        try:
            self._pipe.sync()
        except psycopg.Error as error:
            if self._failed is None:
                self._failed = error

    def _blame(self, sent: _Sent) -> None:
        """A statement with no result of its own is the one the batch died on:
        psycopg stops at the first failure and abandons everything behind it,
        so walking in order lands on the culprit and never past it."""
        failed = self._failed
        if failed is None:
            raise RuntimeError(
                f"a statement in a batch came back with no result and no "
                f"error to explain it: {sent.statement!r}"
            )
        if isinstance(failed, psycopg.errors.UniqueViolation) and sent.clash:
            cls, record = sent.clash
            raise _clash(cls, record, failed) from failed
        raise failed

    def _close(self) -> None:
        for sent in self._sent:
            sent.cur.close()


@contextmanager
def batching(
    conn: psycopg.Connection,
    *,
    watch: Any = UNWATCHED,
    cls: type | None = None,
) -> Iterator[_Batch]:
    """
    A door like `cursor`, for the writes that send a set rather than a row.

    Pipeline mode rather than `executemany`, because a write is not one
    statement shape repeated: an insert and its `returning`, a parent and the
    children riding with it, a delete and the generations under it all go in
    the same batch. `executemany` covers one shape at a time and cannot carry
    the mixed set.

    Falls back to sending each statement as it is queued where libpq is too old
    for pipelining. Everything above this reads the same either way — the same
    statements, the same order, the same results matched back the same way —
    and only the number of round trips differs.
    """
    batch = _Batch(conn, watch=watch, cls=cls)
    try:
        with explaining(conn):
            if not psycopg.Pipeline.is_supported():
                yield batch
                batch.settle()
                return
            with conn.pipeline() as pipe:
                batch._pipe = pipe
                yield batch
                batch.settle()
    finally:
        batch._close()


def on_dsql(conn: psycopg.Connection) -> bool:
    """
    Whether this connection is to DSQL or to something PostgreSQL-shaped.

    Asked of the server rather than read off the hostname, because a hostname is
    what a proxy or a tunnel changes and this has to be right for a connection
    somebody handed over. `sys.jobs` is the view DSQL builds an asynchronous
    index through, so it is both specific and exactly the thing being asked
    about. `to_regclass` answers `None` rather than raising, which is what makes
    the question cheap enough to ask.

    Almost nothing dray writes needs this. The statements are ordinary
    PostgreSQL by design — that is what lets a test suite run locally — and
    the one exception is `create index async`, which DSQL alone takes and
    everything else refuses.
    """
    with cursor(conn) as cur:
        cur.execute("select to_regclass('sys.jobs') is not null")
        return bool(cur.fetchone()[0])


def autocommitting(conn: psycopg.Connection) -> psycopg.Connection:
    """
    A connection dray holds is an autocommit connection, and it is switched
    rather than asked for.

    Not a preference. `transaction()` joins rather than nests when the
    connection reports it is already in one, and under autocommit that can only
    mean a block somebody opened deliberately — which is exactly when joining is
    right. With autocommit off, psycopg opens a transaction on the first read
    and leaves it open, so that same check reads an implicit transaction as a
    deliberate one and enlists in it. Nothing commits for the rest of the
    connection's life: every write after the first read is lost at close, and
    every rollback rolls back nothing.

    `Store.connect` builds one this way already. This is for the connection you
    hand over yourself, which `psycopg.connect` gives you with autocommit off.

    DSQL settles it regardless — a connection idling inside a transaction
    between requests is a transaction ageing against the five-minute ceiling.
    """
    if conn.autocommit:
        return conn
    # The two states that mean a block is open, rather than `!= IDLE`, which is
    # also true of a connection that has already died — that one reports
    # `UNKNOWN`, and answering it with a sentence about transactions sends
    # somebody looking for a block nobody opened.
    if conn.info.transaction_status in (
        TransactionStatus.INTRANS,
        TransactionStatus.INERROR,
    ):
        raise RuntimeError(
            "this connection is inside a transaction, so it cannot be switched "
            "to autocommit, which dray needs. Commit or roll back before "
            "handing it over."
        )
    conn.autocommit = True
    return conn


def working_in(conn: psycopg.Connection, namespace: str) -> psycopg.Connection:
    """
    Point a connection at a schema, by setting `search_path` and nothing else.

    Every statement dray writes names its table bare, so it already resolves
    through whatever `search_path` the connection carries — which is why this is
    a `set` and not a change to how statements are built. Qualifying names
    internally would send everything dray generates to `orders.event` while
    everything a collection wrote by hand went wherever the connection was
    pointing, and `select_many` is the escape hatch precisely so that SQL can be
    the caller's. One `search_path` puts both in the same place.

    The schema is not created here. Making one is an admin operation and this is
    not a migration runner — `schema.create_namespace` writes the statement to
    put in a migration.

    Refused rather than interpolated blindly: this is the one identifier dray
    takes from a caller, `set search_path` cannot take a parameter, and a name
    arriving from configuration is still a name somebody typed.
    """
    if not namespace.isidentifier():
        raise ValueError(
            f"{namespace!r} is not a name a schema can have. Letters, digits "
            "and underscores, not starting with a digit."
        )
    with conn.cursor() as cur:
        cur.execute(f"set search_path to {namespace}")
    return conn


class Store:
    """
    One connection, and a collection for every record that declared one.

    Short-lived by design. Build one per request or per job and let it go —
    which is also why anything it carries in `defaults` should be true for
    everything it writes.

    And used by one thread at a time. A store is dray's unit of concurrency:
    everything it does goes down one connection, and a connection has one
    session and one transaction. Two threads sharing one lost writes silently —
    the second saw the first's open transaction, took it for a block somebody
    had opened deliberately, joined it, and had its work rolled back by a
    failure it was not part of, `add()` having returned as though all was well.
    Concurrency is a store each, which is what `dray.Pool` makes cheap.

    `observer` is called with every span this store opens and closes: the
    statements, the transactions around them, and dray's own time in between.
    Off by default, and a store nobody is watching times nothing.
    """

    def __init__(
        self,
        conn: psycopg.Connection,
        *,
        defaults: dict[str, Any] | None = None,
        records: Sequence[type] = (),
        namespace: str | None = None,
        pool: "Pool | None" = None,
        observer: Callable[[Span], None] | None = None,
        timer: Callable[[], float] | None = None,
        watch: Any = None,
    ) -> None:
        # `observer` is what a caller says and `watch` is dray handing over a
        # stack that is already in use — `Pool.store` opens the checkout span
        # before there is a store to open it on, and the store's own spans have
        # to nest inside that one. Never both: the pool takes the caller's
        # observer and builds the watch from it.
        self._watch = watch if watch is not None else Watch.of(observer)
        # Which attempt the replay is on, for the transaction span to record.
        # One unless `@retrying` has said otherwise.
        self._attempt = 1
        # The pool this store's connection came from, or nothing for one built
        # by hand. Kept because a pool outlives the request and a store does
        # not, so anything shared between requests — a short-lived read cache
        # above all of them — has to live there and be reachable from here.
        self.pool = pool
        # Where the cached rows are, which is decided by where the connection
        # came from. A pool's are shared by every store it lends to, or the
        # phase that warms one would be filling something the phase that reads
        # cannot see. A store built by hand has no neighbours to share with, so
        # it keeps its own and they go when it does — which is right for a
        # script or a job, and is what makes `cached_for=` mean the same thing
        # whichever way the store was built.
        self._caches = pool._caches if pool is not None else Caches(timer)
        # How deep this store is inside an `uncached()` block. A store is one
        # thread, so a count is enough and needs no lock of its own.
        self._bypassed = 0
        self._conn = autocommitting(conn)
        # How deep this store is inside its own transaction, and whose. Kept
        # here rather than read off the connection, which cannot tell dray's
        # block from anybody else's and cannot tell two threads apart at all.
        self._depth = 0
        self._owner: int | None = None
        self._guard = threading.Lock()
        # Three queues, and a block runs the commit side or the rollback side
        # and never both. A block that commits runs dray's own bookkeeping for
        # the rows that landed and then what the caller asked for; a block that
        # raises runs what was waiting to be undone. All three are emptied on
        # the way out either way, so the next block on this store starts with
        # none of them.
        self._after_commit: list[Callable[[], None]] = []
        self._on_commit: list[Callable[[], None]] = []
        self._undos: list[Callable[[], None]] = []
        # Whether the transaction this store owns has committed. Read by
        # `transaction` to tell a refused commit from something that went wrong
        # after a good one, which are the same exception and opposite advice.
        self._committed = False
        # How many rows `thin`'s passes have taken since the outermost block
        # opened. Outside a block it means nothing and is never read, because
        # every pass is its own transaction there; inside one the passes are all
        # the same transaction, and this is the only count of it anybody has.
        self._thinned = 0
        self.defaults = dict(defaults or {})
        self.namespace = namespace
        if namespace:
            working_in(self._conn, namespace)
        self._collections: dict[str, Any] = {}
        # A store's own registry, consulted before the global one. Two records
        # can share a collection name — a test suite does it constantly — and
        # resolving that by import order picks one of them silently. A store
        # told which records it serves cannot be wrong about it.
        self._records: dict[str, type] = {}
        self._dsql: bool | None = None
        self.serves(*records)

    @property
    def conn(self) -> psycopg.Connection:
        """
        The connection, if this thread is the one entitled to it.

        Checked here because everything goes through here — a read opens a
        cursor on it, a write opens a transaction on it — so one gate covers
        both. And a read matters as much: a statement run while another thread
        holds a block runs *inside* that block, seeing what it has not committed
        and ageing against the five minutes it is spending.

        Only while a block is open. Outside one, statements on a connection are
        each their own transaction and take their turn, so passing a store from
        one thread to another is fine — what is refused is two threads in it at
        once, which is the thing that silently loses work.
        """
        owner = self._owner
        if owner is not None and owner != threading.get_ident():
            raise RuntimeError(
                f"this {type(self).__name__} is in use by thread {owner} and "
                f"was reached from thread {threading.get_ident()}. A store is "
                "one connection, so it belongs to one thread at a time — a "
                "second thread joins the first's transaction and loses its "
                "work when that one rolls back. Give each thread its own: "
                "`with pool.store() as store:`."
            )
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        """
        Two saves that have to agree, in one transaction.

            with store.transaction():
                ticket.save()
                store.alerts.save_all(raised)

        Everything written through this store joins it rather than opening its
        own, so it all lands or none of it does. See *Two collections in one
        transaction* on the page for what that costs — five minutes, no
        chunking, and no replay.

        That last one is why this is a different door from `_transacting`
        below rather than the same one. dray replays a write DSQL refuses, and
        that replay belongs to the transaction dray opened; the commit of a
        block you opened is nobody's to replay but yours. So the refusal
        arrives here as `CommitRefused` — a dray name for a caller who never
        asked psycopg anything — where inside dray it stays a
        `SerializationFailure` for `@retrying` to catch and redo.
        """
        try:
            with self._transacting() as conn:
                yield conn
        except psycopg.errors.SerializationFailure as refused:
            # Only a refusal of the commit itself. `_transacting` runs the
            # `after_commit` handlers on its way out, which is inside this
            # `try` and after the rows are durable — so a handler that reaches
            # past dray to the connection and conflicts there would otherwise
            # be reported as a refused commit, and this exception tells the
            # caller to run the work again. That would write it twice.
            if self._committed:
                raise
            telling = CommitRefused(
                "DSQL refused the commit of a transaction you opened, because "
                "something else wrote the same rows first. dray did not replay "
                "it: the block is yours and only you know what is safe to run "
                "twice. Read the records again and run the work from the top, "
                "which is what `@dray.replaying` on that function does for you."
            )
            # Whether there is still a block open around the one that was
            # refused. `_transacting` has already given its depth back by now,
            # so what is left is somebody else's. `@replaying` reads it and
            # runs nothing again while it is true — the outer transaction is
            # aborted, so a second attempt would send statements PostgreSQL has
            # stopped accepting. Private because it is dray talking to itself:
            # whoever owns that outer block is the one who has to run the work
            # again, and they can see their own `with` from where they stand.
            telling._enlisted = self.in_transaction
            raise telling from refused

    @contextmanager
    def _transacting(self) -> Iterator[psycopg.Connection]:
        """
        One transaction, owned by this store and by the thread that opened it.

        Nesting on one thread joins, as it always did: an inner block is not a
        second transaction, because DSQL has no `SAVEPOINT` for it to be one
        with. A second *thread* is refused instead, which is the whole
        difference — the same condition, and only the store knows which of the
        two it is looking at.

        The outermost block is also where deferred work runs. Anything a write
        left on `_after_commit` because it had only enlisted happens once the
        real commit has returned, and is dropped without running if the block
        raises — which is what makes a rollback leave the records as they were
        rather than tidied up from a write that never landed.
        """
        thread = threading.get_ident()
        with self._guard:
            if self._depth and self._owner != thread:
                raise RuntimeError(
                    f"this {type(self).__name__} is inside a transaction on "
                    f"thread {self._owner} and was written from thread "
                    f"{thread}. Give each thread its own store."
                )
            # Named rather than `!= IDLE`, which is also true of a connection
            # that has died: a closed one reports `UNKNOWN`, and answering that
            # with a sentence about transactions sends somebody looking for a
            # block nobody opened instead of at the connection `explaining`
            # is about to tell them the truth about.
            open_already = self._conn.info.transaction_status in (
                TransactionStatus.INTRANS,
                TransactionStatus.INERROR,
            )
            # A block on the connection that dray did not open. Joining it
            # is what makes a write unsafe: dray can see that its tidying up
            # must wait and never learns whether the commit happened, so the
            # etag is not put back, the queued children are gone, and
            # `after_commit` fires for rows that rolled back. Refused rather
            # than half-supported — `store.transaction()` is the one way to
            # open a block dray can finish.
            if open_already and not self._depth:
                raise RuntimeError(
                    "this store's connection is already inside a transaction "
                    "that dray did not open, so dray cannot tell whether it "
                    "commits — which means a write in here cannot be undone "
                    "when it does not. Open the block with "
                    "`with store.transaction():` instead, which is the same "
                    "transaction with the records looked after."
                )
            joining = bool(self._depth)
            self._depth += 1
            self._owner = thread

        raised = False
        if not joining:
            self._committed = False
            # A count of one transaction, so it starts again with each of them.
            # An inner block is not a second transaction and must not reset it,
            # which is the whole reason this hangs off `joining`.
            self._thinned = 0
        try:
            # `explaining` around the whole block rather than around the
            # statements inside it: a write on a connection DSQL has closed
            # fails at `begin`, before any collection has opened a cursor.
            with explaining(self._conn):
                if joining:
                    # No span. An inner block is not a second transaction, so
                    # calling it one would put a `transaction` in the tree that
                    # nothing ever commits, and the statements inside it belong
                    # under the block that does.
                    yield self._conn
                else:
                    with self._watch.span("transaction") as span:
                        span.attempt = self._attempt
                        with self._conn.transaction():
                            yield self._conn
                        # The `with` above sends the COMMIT on its way out, so
                        # reaching here is the rows being durable. Anything that
                        # raises from this point on — a handler, the tidying —
                        # is not a refused commit however much it looks like one.
                        self._committed = True
        except BaseException:
            raised = True
            raise
        finally:
            with self._guard:
                self._depth -= 1
                if not self._depth:
                    self._owner = None
            # After the depth is back down, so anything run here opens a
            # transaction of its own rather than enlisting in the one that has
            # just ended. And only for the block that owned it: an inner one
            # joined, so the commit it would be waiting on has not happened.
            if not joining:
                # Undone backwards, done forwards. Two saves of one record in a
                # block leave two undos, the first holding the etag the record
                # arrived with and the second the one the first save minted, so
                # running them in order finishes on the intermediate value.
                # Unwinding is the reverse of doing, and only one of these is
                # unwinding — `after_commit` is a queue of things the caller
                # asked for and they happen in the order they were asked.
                waiting = (
                    self._undos[::-1] if raised else self._after_commit
                )
                # dray's own for the commit side, which runs before the
                # caller's handlers and only where the block committed. What it
                # holds is bookkeeping a rollback wants left alone — the prior
                # values a rule judges against, for work the caller is about to
                # run again.
                settled = [] if raised else self._on_commit
                # All three emptied whichever way it went, and taken before any
                # of it runs — a handler that writes must not find itself
                # replaying the queue it is being run from.
                self._after_commit, self._on_commit, self._undos = [], [], []
                if raised:
                    # Undos are dray's own two lines of bookkeeping and cannot
                    # realistically fail. If one ever does, it goes straight up
                    # rather than being collected: we are already unwinding an
                    # exception here, and a summary that replaced the caller's
                    # own error with dray's would be the worse trade.
                    for work in waiting:
                        work()
                else:
                    for work in settled:
                        work()
                    firing(waiting)

    @property
    def in_transaction(self) -> bool:
        """Whether a block is open on this store — so a write can tell whether
        it is committing on its own account or enlisting in somebody else's."""
        return bool(self._depth)

    def span(self, label: str) -> Any:
        """
        Put a name of your own around whatever happens in here.

            with store.span("render the ticket page"):
                page = render(store)

        A `caller` span, and the only kind dray does not open for itself.
        Everything the store does inside the block nests under it, so an
        observer can say what the page cost as well as what each statement in
        it cost — and where the four seconds went when the six statements only
        account for forty milliseconds.

        Free when nobody is watching: the block runs and nothing is timed.
        """
        return self._watch.span("caller", label=label)

    @contextmanager
    def watching(self, *, kind: str | None = "statement") -> Iterator[Seen]:
        """
        Collect what dray does while this block runs.

            with store.watching() as seen:
                render_the_page()

            assert len(seen) == 1

        For the test that says *this page does one read and not six*, which is
        the question a callback cannot answer without everybody writing the same
        closure over a list — and the list needs a lock the moment the page fans
        out, which is exactly when the question is worth asking.

        Statements by default. `kind=None` collects every span, and any other
        name in `dray.watching.KINDS` collects that one — `"transaction"` for
        how long a block was open, `"hydrate"` for dray's own time, `"cache"`
        for the reads that were answered out of memory and never happened.

        It catches this store, and every store checked out of this store's pool
        while the block is open. That is what makes it answer for a fan-out,
        where a page is several stores on several threads: the workers take
        theirs from the pool inside the block and are caught by it. It does not
        reach back to a store that was already checked out somewhere else — a
        store decides whether it is watched when it is made, which is what keeps
        an unwatched one free — and on a store with no pool it collects that
        store alone.

        A window rather than a subscription: what it caught is a plain sequence
        the moment the block ends, and nothing goes on collecting afterwards.
        """
        if kind is not None and kind not in KINDS:
            raise ValueError(
                f"{kind!r} is not a kind of span. One of: {', '.join(KINDS)} — "
                "or None for all of them."
            )
        seen = Seen(kind)
        # A store that was not being watched has no `Watch` at all, so one is
        # made for the block and taken away again afterwards. Nested blocks see
        # the outer one's and leave it where it is.
        was = self._watch
        if not isinstance(was, Watch):
            self._watch = Watch()
        watch = self._watch
        watch.add(seen)
        if self.pool is not None:
            self.pool._watch_with(seen)
        try:
            yield seen
        finally:
            if self.pool is not None:
                self.pool._stop_watching(seen)
            watch.drop(seen)
            if not isinstance(was, Watch):
                self._watch = was

    @contextmanager
    def uncached(self) -> Iterator[None]:
        """
        The read that has to be true, whatever is in memory.

            with store.uncached():
                balance = store.accounts.by_id(account_id)

        Everything read inside the block goes to the database and nothing read
        inside it is remembered, so a decision that turns on the current value
        of a row is not made against one that was current ten seconds ago. It
        does not empty anything: what was cached before is still cached after,
        because the block is about this read rather than about the cache.

        Blocks nest, and a write inside one still drops the keys it wrote —
        that eviction is about every other store sharing the cache, and is not
        this store's to skip.
        """
        self._bypassed += 1
        try:
            yield
        finally:
            self._bypassed -= 1

    def forget_all(self) -> None:
        """Empty every cache this store can see — the pool's where its
        connection came from one, its own where it did not.

        For the process that has just written through something other than
        dray, which is the one case dray cannot see for itself. A migration
        run, a bulk load through `store.conn`, an admin script: the rows moved
        and nothing here knows which."""
        self._caches.forget_all()

    def _cache_for(self, cls: type) -> Cache | None:
        """This class's cache, or nothing, for a read about to happen.

        Nothing inside a transaction, and that is the rule rather than an
        optimisation. A block is where a read sees this store's own uncommitted
        writes, so filling the cache from one would publish rows that a
        rollback then takes away — and serving from it would answer a read that
        follows a write in the same block with the row from before it. Outside
        a block neither can happen: every statement is its own transaction, and
        a write has dropped its keys by the time the next read asks.
        """
        if self._bypassed or self._depth:
            return None
        return self._caches.of(cls)

    def _asking(
        self, cls: type, question: Any, ttl: float, maxsize: int
    ) -> Any:
        """Where a `@cached_for` collection method's answers are kept, or
        nothing where this call must not be answered out of memory.

        The same two gates a read by key goes through, and for the same
        reasons: a method called inside a block may have read this store's own
        uncommitted rows, and a block that said `uncached()` said it about
        everything in it rather than about `by_id` alone."""
        if self._bypassed or self._depth:
            return None
        return self._caches.asked(cls, question, ttl=ttl, maxsize=maxsize)

    def after_commit(self, work: Callable[[], None]) -> None:
        """
        Do this once the write has committed, and not at all if it does not.

        For the side effect that must not happen unless the rows are durable —
        the queued job above all, and with it the confirmation email and the
        cache key dropped.

        The job is the case worth building for. A worker is another process on
        another connection, so it cannot see rows that have not committed:
        enqueue inside a block and it may look the record up and find it absent,
        or find it as it was before. That is a race with something that is not
        waiting for you, rather than merely an announcement made too early.

        A caller who owns the block does not need this — a refused commit
        raises, so a line after the `with` is only reached when the rows landed.
        What needs it is code that cannot see whether a block is open above it:

            def close(ticket) -> None:
                ticket.status = "closed"
                ticket.save()
                store.after_commit(lambda: enqueue("closed", ticket.id))

        Called on its own that save owns its transaction and has committed by
        the time the job is queued. Called inside somebody's block it enlisted
        and committed nothing, so the worker would be asked about a ticket that
        is not there. `close` cannot tell those apart. The store can, because
        it is the thing holding the depth.

        Written last in the function, by convention: outside a block this runs
        where it stands and inside one it runs at the end, so anything after it
        happens in a different order in the two cases.

        Inside a block it waits for that block. Outside one there is nothing
        left to wait for and it runs immediately, which is the same promise
        either way rather than two behaviours to remember.

        It runs after the transaction has closed, so a handler may write —
        that write is a transaction of its own, and it is not covered by the
        one that has just committed. If it raises, what comes out is
        `AfterCommitFailed`: from the `with` where there was a block to wait
        for and from this call where there was not, and either way the rows
        are durable and running the work again writes them twice.
        """
        if self._depth:
            self._after_commit.append(work)
        else:
            # Through the same firing as a queued set, rather than called here,
            # so that what a failing handler raises does not turn on whether
            # somebody had a block open. A caller who cannot tell the two apart
            # is the whole reason this method exists, and one that came back as
            # the domain's own exception here and as `AfterCommitFailed` from
            # the `with` would be two behaviours to remember.
            firing([work])

    def _after_commit_all(self, work: Sequence[Callable[[], None]]) -> None:
        """
        The same promise as `after_commit`, for a set of handlers that belong
        to one write. dray's own door: a `@dray.after_commit` on a record is
        registered here when the write that carried it is done.

        Together rather than one at a time, because outside a block there is
        nothing left to wait for and they run where they stand — and a set run
        one call at a time would stop at the first that raised, where the same
        set inside a block runs all of them and reports every failure. The
        handlers are independent of each other either way, so the promise
        should not turn on whether somebody had a block open.
        """
        if self._depth:
            self._after_commit.extend(work)
        else:
            firing(work)

    def _when_committed(self, work: Callable[[], None]) -> None:
        """
        Do this once the rows are durable. dray's own, and the mirror of
        `_undo_on_rollback` — hence the underscore, which `after_commit` does
        not have.

        Not the queue beside it, though it waits on the same moment. That one
        is the caller's: what goes in it is counted and reported by an
        `AfterCommitFailed`, and a store telling somebody that one of four
        handlers raised when they wrote three is a worse answer than a second
        list. This one runs first, so a handler somebody wrote finds the record
        as the write left it, bookkeeping included.

        Outside a block it runs where it stands, because the rows are already
        durable by then and there is nothing to wait for.
        """
        if self._depth:
            self._on_commit.append(work)
        else:
            work()

    def _undo_on_rollback(self, work: Callable[[], None]) -> None:
        """
        Undo this if the block rolls back. dray's own, and deliberately not a
        hook — hence the underscore, which `after_commit` does not have.

        `after_commit` has an answer for a caller: the thing you wanted to
        happen once the rows were durable. The other direction does not. Undoing
        a side effect is compensation, which needs to know which half of it
        happened and is a service's job — and the page says as much, so a public
        `on_rollback` would be a name on the store contradicting it.

        What this is for is narrower than a hook and not a caller's business at
        all: the bookkeeping a write does to the objects in hand, which dray is
        the only one that knows about and the only one that can put back. It
        only ever gets called while a block is open, because that is the only
        time a write records anything to undo.
        """
        self._undos.append(work)

    def serves(self, *records: type) -> None:
        """Bind these records to this store, by the collection each declared."""
        for cls in records:
            if cls.__dray_collection__:
                self._records[cls.__dray_collection__] = cls

    @classmethod
    def connect(
        cls,
        *,
        host: str,
        region: str | None = None,
        user: str = "admin",
        database: str = "postgres",
        defaults: dict[str, Any] | None = None,
        namespace: str | None = None,
        observer: Callable[[Span], None] | None = None,
        timer: Callable[[], float] | None = None,
        records: Sequence[type] = (),
        **options: Any,
    ) -> "Store":
        """
        Connect to a DSQL cluster.

        There is no password. DSQL authenticates with an IAM token, minted by
        AWS's own connector — which is why it is imported at the point of use
        rather than at the top, since a store built on a connection you already
        have should not need it installed at all.

        `records` is the same list `Store(conn, records=[...])` takes, and it
        is here because a store built by connecting needs it as much as one
        handed a connection: two records can share a collection name, and
        resolving that by import order picks one of them silently.

        The connector rather than a token minted here, for a reason worth
        stating: it picks the token type from the user. `admin` gets an admin
        token and every other name gets `DbConnect`, which is what a scoped
        role needs. Minting one kind by hand meant `user=` was a parameter that
        could not work — anything but `admin` was handed an admin token and
        refused, saying `Wrong user to action mapping`.

        `namespace` is the schema this store works in, and `None` means touch
        nothing. The schema has to exist already — `schema.create_namespace`
        writes the statement for a migration to carry.

        `observer` is called with every span this store opens and closes, the
        connection it is being made on included: the IAM handshake is a round
        trip to another service and is the one part of a store's life nothing
        else can see. Off by default, and off costs nothing.

        `timer` is the clock a cached row's lifetime is measured on, as it is
        on a `Pool`. A store built this way keeps its own cached rows and they
        go when it does, since there is no pool for them to outlive it in.

        Anything else goes to the driver, `sslmode` included — and the default
        for that one is dray's opinion rather than anybody else's. `verify-full`
        checks the certificate against a CA and the hostname against the
        certificate; `require` encrypts and verifies neither, so anything able
        to put itself in the path goes unnoticed. Neither libpq nor the
        connector asks for more than `require`, so it is said here.

        Verifying needs a CA to verify against, and `sslrootcert=system` is not
        it: libpq reads that through OpenSSL, which does not consult the macOS
        keychain, so a machine that trusts Amazon's CA perfectly well fails to
        connect. The bundle botocore ships is the one to use — it is already
        installed, because minting the token needs it, and it is the same set of
        roots the SDK trusted to fetch the token in the first place.

        Say anything `ssl`-shaped yourself and dray says nothing: the pair is
        replaced rather than merged, because half of somebody else's opinion is
        no opinion at all. libpq refuses `sslmode=require` alongside an
        `sslrootcert`, so merging would turn overriding one into an error about
        the other.
        """
        dsql = _connector()

        # Built before the connection, so the store carries the same stack the
        # `connect` span was opened on rather than starting a second one.
        watch = Watch.of(observer)
        with watch.span("connect"):
            conn = dsql.connect(
                host=host,
                region=region or region_of(host),
                user=user,
                dbname=database,
                autocommit=True,
                **(
                    verified()
                    if not any(k.startswith("ssl") for k in options)
                    else {}
                ),
                **options,
            )
        return cls(
            conn,
            defaults=defaults,
            namespace=namespace,
            records=records,
            timer=timer,
            watch=watch,
        )

    def __getattr__(self, name: str) -> Any:
        # Only called for names that are not real attributes, so `conn` and
        # `defaults` never reach here.
        collections = self.__dict__.get("_collections")
        known = {**RECORDS, **self.__dict__.get("_records", {})}
        if collections is None or name not in known:
            raise AttributeError(
                f"{type(self).__name__} has no collection {name!r}. "
                f"Known: {', '.join(sorted(known)) or 'none'}"
            )
        if name not in collections:
            from dray.collection import _collection_for

            collections[name] = _collection_for(self, known[name])
        return collections[name]

    def create(self, *records: type) -> None:
        """
        Make the tables these records imply.

        For a test suite and local PostgreSQL. A real schema change is a
        statement you have read, taken from `dray.schema.statements` and put in
        a migration — this is not a migration runner and does not pretend to be
        one.

        The indexes come out in whichever form the connection will take, asked
        once and remembered. `create index async` is DSQL's and nothing else
        accepts it; plain `create index` is everything else's and DSQL does not.
        Same schema either way, which is what lets the same test run against
        both — a compatibility run against a cluster is the same file that ran
        in development, rather than a second one written to match.
        """
        from dray import schema

        self.serves(*records)
        for cls in records:
            # One statement at a time, and no commit: the connection is
            # autocommit, so each lands on its own — which is also the only way
            # DSQL will take DDL.
            for statement in schema.statements(cls, asynchronous=self.dsql):
                self._ddl(statement)

    @retrying
    def _ddl(self, statement: str) -> None:
        """
        One statement of schema, replayed when it conflicts with another.

        DSQL refuses a commit whose schema moved underneath it — `OC001`, a
        schema conflict, arriving as an ordinary serialization failure — and two
        deployments coming up at once do exactly that to each other. It is the
        same transient as a write conflict and wants the same answer, and this
        was the one write path with no replay on it: a second instance calling
        `create` at the same moment as the first failed outright.

        Per statement rather than around the loop, so a conflict on the fourth
        does not redo the first three. Safe to replay for the reason every
        statement here is written `if not exists` — a migration has to survive
        being run twice whatever happens, because DSQL cannot commit the
        statement and the row recording it together.
        """
        with cursor(self.conn, watch=self._watch) as cur:
            cur.execute(statement)

    @property
    def dsql(self) -> bool:
        """Whether this store is talking to DSQL, asked once and kept.

        Lazily, because a store is built per request and almost nothing needs
        the answer — a round trip on every one of them to settle a question only
        `create` asks would be a poor trade."""
        if self._dsql is None:
            self._dsql = on_dsql(self.conn)
        return self._dsql

    def close(self) -> None:
        """
        Done with this store.

        What that means depends on where the connection came from, which the
        caller should not have to know: one this store made is closed, and one a
        pool lent is given back. Leaving a `with pool.store()` block does the
        same thing, so this is the spelling for a store you built by hand.
        """
        if self.pool is not None:
            return
        self._conn.close()


class Pool:
    """
    Connections to hand stores, so that having one each is cheap.

        pool = dray.Pool(host="ab12cd.dsql.ap-southeast-2.on.aws")

        with pool.store() as store:
            store.people.add(...)

    A store is one connection and one thread, which is the shape DSQL wants:
    ten thousand connections to a cluster, no shared buffers to contend on, and
    a connection closed after an hour whatever it is doing. What made that
    advice hard to follow was the cost — a fresh handshake and a fresh token per
    request — and a pool is the answer to the cost rather than a change to the
    shape. Nothing about a store is different for having come from one.

    Above the store rather than inside it, deliberately. A store reaches for its
    connection several times in one unit of work — `with self.store.transaction()
    as conn, cursor(conn)` is two — so a connection checked out per statement
    would put the cursor outside the transaction. The only honest scope for a
    checkout is the store's own life, which is what this is.

    It also settles the hour. `max_lifetime` retires a connection before DSQL
    does, so nothing above has to know that DSQL closes them, and no part of
    dray has to check whether the one it holds is still alive.
    """

    def __init__(
        self,
        source: Any = None,
        *,
        host: str | None = None,
        region: str | None = None,
        user: str = "admin",
        database: str = "postgres",
        min_size: int = 1,
        max_size: int = 8,
        max_lifetime: float = 3300.0,
        defaults: dict[str, Any] | None = None,
        records: Sequence[type] = (),
        namespace: str | None = None,
        observer: Callable[[Span], None] | None = None,
        timer: Callable[[], float] | None = None,
        **options: Any,
    ) -> None:
        """
        Built from a host, or handed a pool you already have.

            Pool(host="ab12cd.dsql.ap-southeast-2.on.aws")
            Pool(psycopg_pool.ConnectionPool(...))

        Both, for the same reason `Store` takes a connection as well as making
        one: an application that already pools has its own opinion about sizes
        and timeouts, and a test suite has no cluster to reach.

        `max_lifetime` is short of DSQL's hour rather than at it. A connection
        retired early costs one handshake; a connection retired by DSQL costs a
        failed statement somebody has to handle.

        `min_size` is one, not psycopg's four. A pool that opens four
        connections before anybody asks for one is four handshakes on a cold
        start, which on Lambda is the whole of the request.

        `observer` goes to every store this pool makes, which means it is called
        from every thread that has one. Making the handler safe to call
        concurrently is yours, exactly as it is for an `after_commit` handler.

        `timer` is the clock a cached row's lifetime is measured on, and is
        `time.monotonic` unless you say otherwise. It is here so that a test can
        watch an entry expire without sleeping for it.
        """
        self.observer = observer
        # The cached rows, made here and shared by every store this pool lends
        # to. A store per checkout is what makes a fan-out concurrent, and a
        # cache per store would mean the threads that warmed it were the only
        # ones that ever saw it.
        self._caches = Caches(timer)
        # Collectors a `store.watching()` block opened, so that a store checked
        # out inside the block reports to it however many threads away it is.
        # Replaced rather than mutated, so a reader takes one immutable tuple
        # and needs no lock of its own.
        self._watchers: tuple[Callable[[Span], None], ...] = ()
        self._watching = threading.Lock()
        self.defaults = dict(defaults or {})
        self.namespace = namespace
        self.records = tuple(records)
        # Kept so a caller can build a one-off store the same way this pool
        # would, without restating the host it already told us about.
        self.host = host
        self._opened = False

        if source is not None:
            self._pool = source
            self._mine = False
            return

        if host is None:
            raise TypeError("a Pool needs a host to connect to, or a pool to use")

        dsql = _connector()

        # Two kinds of argument, and the pool keeps them apart: how to shape the
        # pool is its own, and how to reach the database goes to every
        # connection it makes. `autocommit` is not among them — it belongs in
        # `configure`, which runs once per connection rather than being a thing
        # to remember about each.
        self._pool = dsql.create_pool(
            kwargs={
                "host": host,
                "region": region or region_of(host),
                "user": user,
                "dbname": database,
                **(
                    verified()
                    if not any(k.startswith("ssl") for k in options)
                    else {}
                ),
                **options,
            },
            min_size=min_size,
            max_size=max_size,
            max_lifetime=max_lifetime,
            configure=ready,
        )
        self._mine = True

    def opened(self) -> Any:
        """
        The pool, connected.

        Opened when it is first asked for rather than when it is built, because
        a pool is the sort of thing built once at module scope — and connecting
        as a side effect of an import is a poor way to find out your credentials
        are wrong.
        """
        if not self._opened:
            self._pool.open()
            self._opened = True
        return self._pool

    @contextmanager
    def store(self, **overrides: Any) -> Iterator["Store"]:
        """
        A store on a connection from this pool, given back at the end.

            with pool.store() as store:
                store.people.add(...)

        `defaults` merge with the pool's rather than replacing them, which is
        what makes a per-request value — a request id, whoever is signed in —
        say itself once without restating what the job already knows.
        """
        defaults = {**self.defaults, **(overrides.pop("defaults", None) or {})}
        # Opened before the connection is asked for, because waiting for a free
        # one is most of what a slow checkout is — and it stays open for the
        # store's whole life, so everything the store does is inside it.
        watch = Watch.of(
            overrides.pop("observer", None) or self.observer, self._watchers
        )
        with watch.span("checkout"):
            with self.opened().connection() as conn:
                yield Store(
                    conn,
                    defaults=defaults,
                    records=overrides.pop("records", None) or self.records,
                    namespace=overrides.pop("namespace", self.namespace),
                    pool=self,
                    watch=watch,
                    **overrides,
                )

    def _watch_with(self, collector: Callable[[Span], None]) -> None:
        with self._watching:
            self._watchers = (*self._watchers, collector)

    def _stop_watching(self, collector: Callable[[Span], None]) -> None:
        with self._watching:
            self._watchers = tuple(
                one for one in self._watchers if one is not collector
            )

    def forget_all(self) -> None:
        """Empty every cache this pool holds, for every record and every store
        it has lent to.

        The whole of it rather than one collection, because what this answers
        is *something outside dray has written* — a migration, a bulk load, an
        admin script — and that is the case where nothing here knows which rows
        moved. One collection's is `store.people.forget_all()`."""
        self._caches.forget_all()

    def close(self) -> None:
        """Close every connection. A pool handed over is somebody else's to
        close, and closing it here would take out whatever else is using it."""
        if self._mine:
            self._pool.close()


def ready(conn: psycopg.Connection) -> None:
    """
    A connection on its way into the pool, before anybody is handed it.

    Autocommit is what dray needs and what `Store` would otherwise switch on
    every checkout; here it happens once, when the connection is made.

    The namespace is not set here and cannot be: it belongs to the store rather
    than to the connection, and two stores drawn from one pool may want
    different ones. `Store.__init__` applies it per checkout for that reason,
    so a pool configured with this still gets whatever `namespace=` each store
    asked for.
    """
    conn.autocommit = True


def verified() -> dict[str, str]:
    """
    What it takes to check who answered, rather than only encrypting the way
    there.

    The CA bundle botocore ships, which is present whenever `Store.connect` is
    usable at all — the connector mints the token through boto3, and this is the
    same set of roots it trusted to do that. Why `sslrootcert=system` is not
    used instead is in `Store.connect` above, where a caller meets the question
    first.
    """
    from botocore.httpsession import DEFAULT_CA_BUNDLE

    return {"sslmode": "verify-full", "sslrootcert": DEFAULT_CA_BUNDLE}


def region_of(host: str) -> str:
    """
    The region out of a DSQL hostname — `ab12cd.dsql.ap-southeast-2.on.aws`.

    Derived rather than asked for, because a host and a region that disagree is
    a confusing failure and the host already carries the answer.
    """
    parts = host.split(".")
    if len(parts) < 3 or parts[1] != "dsql":
        raise ValueError(f"cannot read a region from {host!r}; pass region=")
    return parts[2]
