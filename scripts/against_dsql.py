#!/usr/bin/env python
"""
The checks that need a real cluster, run concurrently.

Everything in `tests/` runs against local PostgreSQL, which proves the SQL is
right and proves nothing about DSQL — the refused commits, the row ceiling, the
statement whose syntax differs, and the IAM handshake nothing local exercises at
all. This is that other half, and it is a script rather than a test file for one
reason: every check is a round trip to another city, so they should happen at
once rather than one after another.

Which is only safe because a store is one connection and one thread. Each check
takes its own store from the pool and its own table, so nothing here shares
anything with anything.

    scripts/against_dsql.py ab12cd.dsql.ap-southeast-2.on.aws
    scripts/against_dsql.py --host ... --only occ,ceiling --keep

Exit code is the number of checks that failed, so it reads as a build step.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg

import dray
from dray import (
    CommitRefused,
    ConcurrencyExhausted,
    DuplicateRecord,
    Pool,
    after_commit,
    before_delete,
    child,
    clock,
    field,
    index,
    record,
    schema,
)
from dray.collection import MAX_ROWS
from dray.model import UNINDEXABLE

CHECKS: list[tuple[str, Callable]] = []
RUN = uuid.uuid4().hex[:8]


def check(name: str) -> Callable:
    """One thing worth knowing about a cluster. Each gets a table of its own,
    named for this run, so several can be in flight without arguing."""

    def wrap(fn: Callable) -> Callable:
        CHECKS.append((name, fn))
        return fn

    return wrap


SCHEMA = threading.Lock()


def _ran(conn: Any, statement: str, tolerate: tuple = ()) -> Any | None:
    """
    A schema change, retried, because that is what a schema change is here.

    DDL runs in a transaction under the same optimistic control as everything
    else, so two of these checks changing the schema at the same moment is an
    `OC001` about the moment rather than an answer about the statement — and
    every check in this file runs at once by design. Anything in `tolerate` is
    the answer being looked for and comes back as `None` rather than raising.
    """
    for attempt in range(5):
        try:
            # One at a time. Concurrency here buys nothing — a schema change is
            # not the round trip this script parallelises for — and it costs the
            # checks that are only trying to make their own table, which retry
            # inside dray and can exhaust.
            with SCHEMA:
                return conn.execute(statement)
        except tolerate:
            return None
        except (psycopg.errors.SerializationFailure, psycopg.errors.DuplicateTable):
            if attempt == 4:
                raise
            time.sleep(0.3 * (attempt + 1))
    return None


def ddl(conn: Any, statement: str, tolerate: tuple = ()) -> bool:
    """A schema change, and whether the cluster took it. What most checks want,
    and the reasoning is on `_ran` above."""
    return _ran(conn, statement, tolerate) is not None


def submitted(conn: Any, statement: str) -> str:
    """
    The id of the build a `create index async` started.

    There is no second way to get one. `sys.jobs` carries a job's object name
    only while the job is still going, so a check that means to wait on a build
    has to keep what the statement handed back.
    """
    return _ran(conn, statement).fetchone()[0]


#
# What only a cluster can answer
#


@check("connect")
def _connect(pool: Pool, table: str) -> str:
    """The IAM handshake, and that `verify-full` reaches this cluster with the
    CA bundle dray picks. Nothing local touches either."""
    store = dray.Store.connect(host=pool.host)
    try:
        with store.conn.cursor() as cur:
            cur.execute("select version()")
            version = cur.fetchone()[0]
        return version.split(" on ")[0]
    finally:
        store.close()


@check("scoped-role")
def _scoped(pool: Pool, table: str) -> str:
    """`user=` picks the token type. Anything but `admin` needs a `DbConnect`
    token, and nothing local can tell the two apart — a wrong token type
    connects perfectly against PostgreSQL. A role that does not exist still
    proves the point: the failure has to be about the role, not about the token
    type."""
    try:
        dray.Store.connect(host=pool.host, user=f"nobody_{RUN}").close()
    except psycopg.OperationalError as error:
        text = str(error)
        if "Wrong user to action mapping" in text:
            raise AssertionError("still minting an admin token for a named role")
        return "refused as an unknown role, not as the wrong token type"
    raise AssertionError("a role that does not exist connected")


@check("occ")
def _occ(pool: Pool, table: str) -> str:
    """A refused commit, which local PostgreSQL raises differently and dray has
    to recognise. Two transactions on the same row; the second to commit loses.

    It arrives as `CommitRefused` rather than the driver's own error, because
    the block belongs to the caller and dray will not replay it on their
    behalf. The driver's exception is the `__cause__`, which is what proves
    dray renamed a real serialization failure rather than inventing one.
    """
    with pool.store() as a, pool.store() as b:
        ddl(a.conn, f"create table if not exists {table} (id int primary key, n int)")
        a.conn.execute(f"insert into {table} values (1, 0)")
        try:
            with a.transaction():
                a.conn.execute(f"update {table} set n = n + 1 where id = 1")
                with b.transaction():
                    b.conn.execute(f"update {table} set n = n + 100 where id = 1")
        except CommitRefused as refused:
            underneath = refused.__cause__
            if not isinstance(underneath, psycopg.errors.SerializationFailure):
                raise AssertionError(
                    f"refused, but not over a conflict: {underneath!r}"
                ) from refused
            return (
                f"{type(refused).__name__}"
                f" over {type(underneath).__name__}"
                f" sqlstate={underneath.sqlstate}"
            )
    raise AssertionError("no conflict was raised")


@check("for-update-reaches-what-it-returned")
def _for_update_reaches(pool: Pool, table: str) -> str:
    """What `select … for update` flags, which the manual states as a fact
    about DSQL and has no other way to keep honest.

    The page once carried a measured table saying the clause was refused on
    anything but an equality predicate on the key, and quoted DSQL's own error
    under it. That stopped being true, under a sentence nobody was re-running,
    and nothing failed — local PostgreSQL takes real locks and answers every
    shape, so the suite could not have noticed and did not. Both halves are
    asserted here, because acceptance without enforcement would be worse than
    a refusal: any predicate is taken, and the rows it returned really are in
    the conflict set.
    """
    with pool.store() as a, pool.store() as b:
        ddl(
            a.conn,
            f"create table if not exists {table}"
            f" (id uuid primary key, team text, on_call boolean)",
        )
        shapes = {
            "key": f"select * from {table} where id = '{uuid.uuid4()}' for update",
            "non-key": f"select * from {table} where on_call = true for update",
            "range": f"select * from {table} where team > 'a' for update",
            "no where": f"select * from {table} for update",
        }
        for name, statement in shapes.items():
            try:
                with a.transaction():
                    a.conn.execute(statement).fetchall()
            except psycopg.errors.FeatureNotSupported as refused:
                raise AssertionError(
                    f"`for update` refused for {name}: {refused}"
                ) from refused

        # Write skew: both read the same set on a non-key predicate, then each
        # changes a different row in it. Neither writes what the other wrote,
        # so nothing but the clause can make them collide.
        team = uuid.uuid4().hex[:8]
        one, two = uuid.uuid4(), uuid.uuid4()
        for who in (one, two):
            a.conn.execute(
                f"insert into {table} (id, team, on_call) values (%s, %s, true)",
                (who, team),
            )
        reads = (
            f"select id from {table} where team = %s and on_call = true for update"
        )
        try:
            with a.transaction():
                a.conn.execute(reads, (team,)).fetchall()
                a.conn.execute(
                    f"update {table} set on_call = false where id = %s", (one,)
                )
                with b.transaction():
                    b.conn.execute(reads, (team,)).fetchall()
                    b.conn.execute(
                        f"update {table} set on_call = false where id = %s", (two,)
                    )
        except CommitRefused:
            left = a.conn.execute(
                f"select count(*) from {table} where team = %s and on_call = true",
                (team,),
            ).fetchone()[0]
            if left != 1:
                raise AssertionError(
                    f"refused, but {left} left on call rather than 1"
                ) from None
            return f"{len(shapes)} shapes accepted, and write skew refused"
    raise AssertionError(
        "two writers of a set read `for update` both committed — the clause "
        "was accepted and did nothing"
    )


@check("for-update-cannot-flag-an-absence")
def _for_update_absence(pool: Pool, table: str) -> str:
    """And the other half of the same sentence: a select that returns nothing
    puts nothing in the conflict set.

    This is the leg the page's conclusion actually rests on — why *is there
    anything overlapping this hour* cannot be asked, and why the slot pattern
    exists. If it ever stops being true the argument for that whole section
    goes with it, so it is worth a check of its own rather than a clause in
    the one above.
    """
    with pool.store() as a, pool.store() as b:
        ddl(
            a.conn,
            f"create table if not exists {table}"
            f" (id uuid primary key, room text, hour int)",
        )
        room = uuid.uuid4().hex[:8]
        asks = f"select id from {table} where room = %s and hour = 9 for update"
        with a.transaction():
            a.conn.execute(asks, (room,)).fetchall()
            a.conn.execute(
                f"insert into {table} (id, room, hour) values (%s, %s, 9)",
                (uuid.uuid4(), room),
            )
            with b.transaction():
                b.conn.execute(asks, (room,)).fetchall()
                b.conn.execute(
                    f"insert into {table} (id, room, hour) values (%s, %s, 9)",
                    (uuid.uuid4(), room),
                )
        booked = a.conn.execute(
            f"select count(*) from {table} where room = %s and hour = 9", (room,)
        ).fetchone()[0]
        if booked != 2:
            raise AssertionError(
                f"an empty `for update` flagged something: {booked} booked, not 2"
            )
        return "both callers found the hour free and both booked it"


@check("occ-replayed")
def _occ_replayed(pool: Pool, table: str) -> str:
    """And that dray replays it rather than passing it on — the README's claim
    that nothing above `save` ever sees a refused write."""

    @record(table=table, collection=f"c_{RUN}_replay")
    class Tally:
        n: int = field(default=0)

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        collection = getattr(store, f"c_{RUN}_replay")
        tally = collection.add(Tally(n=0))

        # Two threads saving the same record, so at least one commit is refused
        # and has to be replayed under the covers.
        errors: list[BaseException] = []

        def bump() -> None:
            try:
                with pool.store(records=[Tally]) as mine:
                    theirs = getattr(mine, f"c_{RUN}_replay").by_id(tally.id)
                    theirs.n += 1
                    theirs.save()
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        threads = [threading.Thread(target=bump) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        if errors:
            raise AssertionError(f"a save was not replayed: {errors[0]!r}")
        return "six concurrent saves, none surfaced a conflict"


@check("before-delete-replayed")
def _before_delete_replayed(pool: Pool, table: str) -> str:
    """A `before_delete` runs inside the transaction the delete opens, so a
    refused commit replays the handler along with the statements. Nothing local
    can produce that — the suite has to raise the refusal by hand — and what the
    page promises is the part that survives it: the handler runs more than once
    and what it wrote lands exactly once per delete.

    Six deletes whose handlers all write to the same row, which is the cheapest
    way to make DSQL refuse a commit that a delete owns.
    """
    ran: list[str] = []
    counter: list[Any] = []

    @record(table=table, collection=f"c_{RUN}_before_delete")
    class Tally:
        n: int = field(default=0)
        role: str = field(default="victim")

        @before_delete
        def bump_the_counter(self) -> None:
            ran.append(self.role)
            # The collection this record was loaded through, which is this
            # thread's own store — so the write enlists in the delete's
            # transaction rather than opening one of its own.
            mine = self._dray_collection
            held = mine.by_id(counter[0])
            held.n += 1
            held.save()

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        collection = getattr(store, f"c_{RUN}_before_delete")
        counter.append(collection.add(Tally(role="counter")).id)
        victims = collection.add_all([Tally() for _ in range(6)])

        errors: list[BaseException] = []

        def remove(victim_id: Any) -> None:
            try:
                with pool.store(records=[Tally]) as mine:
                    getattr(mine, f"c_{RUN}_before_delete").by_id(victim_id).delete()
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        threads = [
            threading.Thread(target=remove, args=(victim.id,)) for victim in victims
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        if errors:
            raise AssertionError(f"a delete was not replayed: {errors[0]!r}")
        written = collection.by_id(counter[0]).n
        if written != len(victims):
            raise AssertionError(
                f"{len(victims)} deletes and {written} lines written"
            )
        left = collection.count(equals={"role": "victim"})
        if left:
            raise AssertionError(f"{left} rows the delete did not remove")
        return (
            f"{len(ran)} handler runs for {len(victims)} deletes,"
            f" each written once"
        )


@check("attempt-observed")
def _attempt_observed(pool: Pool, table: str) -> str:
    """`Span.attempt` is the one field nothing but dray needs, and it is the
    one nothing local can prove: a real refused commit needs a cluster, and the
    suite has to raise its own by hand. What is being asked is whether the
    number a replay reaches ever lands on the transaction span a caller sees —
    a write refused twice reading as three unrelated transactions is exactly
    what the field exists to prevent.

    The same six threads on one row that `occ-replayed` uses, since that is the
    cheapest way to make DSQL refuse a commit dray owns."""
    seen: list[Any] = []
    keeping = threading.Lock()

    def watch(span: Any) -> None:
        if span.phase == "close" and span.kind == "transaction":
            with keeping:
                seen.append(span)

    watching = Pool(pool._pool, observer=watch)

    @record(table=table, collection=f"c_{RUN}_attempt")
    class Tally:
        n: int = field(default=0)

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        tally = getattr(store, f"c_{RUN}_attempt").add(Tally(n=0))

        errors: list[BaseException] = []

        def bump() -> None:
            try:
                with watching.store(records=[Tally]) as mine:
                    theirs = getattr(mine, f"c_{RUN}_attempt").by_id(tally.id)
                    theirs.n += 1
                    theirs.save()
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        threads = [threading.Thread(target=bump) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        if errors:
            raise AssertionError(f"a save was not replayed: {errors[0]!r}")
        highest = max((span.attempt or 0) for span in seen) if seen else 0
        if highest < 2:
            raise AssertionError(
                "six concurrent saves and no transaction span said it was a "
                f"replay; attempts seen: {sorted(s.attempt for s in seen)}"
            )
        return (
            f"{len(seen)} transactions watched,"
            f" the deepest replay at attempt {highest}"
        )


@check("backoff-clears-contention")
def _backoff_clears_contention(pool: Pool, table: str) -> str:
    """The wait between replays doubles, and this is the only place the reason
    can be seen. Local PostgreSQL takes row locks, so a second writer blocks
    instead of being refused, no commit is ever rejected and the backoff is
    dead code under test — the suite can prove the waits grow and nothing about
    what growing buys.

    What it buys is measured here: writers going at one row over and over,
    every increment its own read-then-write transaction that dray replays on
    refusal. Under a wait that went up by the same 50 ms each turn, eighty such
    increments cost about three attempts each; doubling took that to about two,
    at roughly a third more wall clock. Fewer attempts for the same work is the
    whole of what is claimed and the whole of what is asserted.

    **Some writers will still give up, and that is not this check failing.**
    The measurement that chose doubling let each increment take up to twelve
    attempts, which is a caller replaying its own function. `retrying` gets
    `ATTEMPTS`, and at this much contention on one row that runs out — which is
    what a caller-facing replay is for rather than something a longer wait
    fixes. So what is asserted here is that the row holds exactly what landed:
    contention costs attempts and refusals, and it must never cost an
    increment that was reported as written.

    Six writers rather than the eight that were measured, because eight stores
    is the whole of this script's pool and the other checks are running at the
    same time. Six is enough to keep the row contended.
    """
    writers, each = 6, 10
    seen: list[Any] = []
    keeping = threading.Lock()

    def watch(span: Any) -> None:
        if span.phase == "close" and span.kind == "transaction":
            with keeping:
                seen.append(span)

    watching = Pool(pool._pool, observer=watch)

    @record(table=table, collection=f"c_{RUN}_backoff")
    class Tally:
        n: int = field(default=0)

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        tally = getattr(store, f"c_{RUN}_backoff").add(Tally(n=0))

        landed, gave_up = [], []
        counting = threading.Lock()

        def bump() -> None:
            # One store for the ten increments rather than one each: the
            # checkout is not what is being measured, and sixty of them would
            # hold the pool against every other check in flight.
            with watching.store(records=[Tally]) as mine:
                collection = getattr(mine, f"c_{RUN}_backoff")
                for _ in range(each):
                    try:
                        theirs = collection.by_id(tally.id)
                        theirs.n += 1
                        theirs.save()
                    except ConcurrencyExhausted:
                        # Expected at this much contention, and counted rather
                        # than raised: `retrying` gets `ATTEMPTS` and a caller
                        # who needs more replays its own function.
                        with counting:
                            gave_up.append(1)
                        continue
                    with counting:
                        landed.append(1)

        threads = [threading.Thread(target=bump) for _ in range(writers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(300)

        units = writers * each
        # Reported rather than asserted, and worth reading: these are unguarded
        # saves, so two writers reading the same `n` both write the same `n + 1`
        # and one of them is lost with nothing said. That is what the etag is
        # for and it is not what this check is about — the number is here
        # because a reader of this output should see how far a counter drifts
        # under contention nobody guarded against.
        held = getattr(store, f"c_{RUN}_backoff").by_id(tally.id).n

        # A run where nothing was ever refused proves nothing about the wait,
        # so the absence of contention is a failure of the check rather than a
        # pass.
        deepest = max((span.attempt or 1) for span in seen) if seen else 1
        if deepest < 2:
            raise AssertionError(
                f"{units} increments on one row and not one was refused"
            )
        return (
            f"{units} increments attempted, {len(landed)} written,"
            f" {len(gave_up)} ran out of attempts, unguarded so the row holds"
            f" {held},"
            f" {len(seen) / units:.2f} attempts each,"
            f" deepest replay at attempt {deepest}"
        )


@check("read-inside-the-block")
def _read_inside_the_block(pool: Pool, table: str) -> str:
    """What a read costs when it is inside a block the caller opened, which is
    the one advice on the page that local PostgreSQL contradicts by saying
    nothing. A block is the window a conflict has to find you in, so a round
    trip inside it is a wider window — and locally there is no window at all,
    because PostgreSQL takes a row lock and the second writer waits its turn
    instead of being refused.

    The same unit of work both ways, against one hot row with a writer
    contending with it throughout: read the record inside the block, or read it
    before opening the block and write only inside. Thirty of each. What the
    page claims is that the window is wider, which is what this holds — both
    rates are extreme by construction, and uncontended both shapes are
    nought.

    Asserted weakly on purpose. The measurement that put the numbers on the page
    had the inside shape refused about 1.6 times as often, which is a margin
    thirty rounds hold comfortably; but it is a rate rather than a rule, and a
    check that fails a build on a coin landing the other way is a check nobody
    trusts. So what fails here is an inversion, and the numbers are reported for
    a person to read.
    """
    rounds = 30

    @record(table=table, collection=f"c_{RUN}_window")
    class Slot:
        n: int = field(default=0)

    with pool.store(records=[Slot]) as store:
        store.create(Slot)
        hot = getattr(store, f"c_{RUN}_window").add(Slot(n=0))

        stop = threading.Event()
        errors: list[BaseException] = []

        def contend() -> None:
            # A writer that never stops, so both shapes are measured against
            # the same weather. Its own refusals are the point rather than a
            # failure — this thread exists to cause them.
            try:
                with pool.store(records=[Slot]) as mine:
                    collection = getattr(mine, f"c_{RUN}_window")
                    while not stop.is_set():
                        try:
                            theirs = collection.by_id(hot.id)
                            theirs.n += 1
                            theirs.save()
                        except ConcurrencyExhausted:
                            continue
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        def measured(inside: bool) -> tuple[int, float]:
            """How often this shape was refused, and how long its block was
            open. The clock starts at the `with` either way, so a hoisted read
            is time this shape spends and not time the transaction does."""
            refused, widths = 0, []
            with pool.store(records=[Slot]) as mine:
                collection = getattr(mine, f"c_{RUN}_window")
                theirs = None
                for _ in range(rounds):
                    if not inside:
                        theirs = collection.by_id(hot.id)
                    started = time.perf_counter()
                    try:
                        with mine.transaction():
                            if inside:
                                theirs = collection.by_id(hot.id)
                            theirs.n += 1
                            theirs.save()
                    except CommitRefused:
                        refused += 1
                    widths.append(time.perf_counter() - started)
            return refused, sorted(widths)[len(widths) // 2] * 1000

        contender = threading.Thread(target=contend)
        contender.start()
        try:
            refused_inside, wide = measured(inside=True)
            refused_hoisted, narrow = measured(inside=False)
        finally:
            stop.set()
            contender.join(60)

        if errors:
            raise AssertionError(f"the contending writer stopped: {errors[0]!r}")
        # A run where nothing was refused was a run with no contention in it,
        # and it says nothing either way about where the read should go.
        if not refused_inside and not refused_hoisted:
            raise AssertionError(
                f"{rounds * 2} blocks against a contended row and not one of "
                "them was refused"
            )
        # The width is what is asserted, because the width is what reproduces:
        # one round trip more of the block open, run after run. The refusal
        # counts are reported and not asserted, and that is a finding rather
        # than caution — measured in one harness the read inside was refused
        # half again as often, and in this one the two counts have come back
        # level and once the wrong way round. What a wider window costs depends
        # on how many writers are meeting on the row, so a check that asserted
        # a ratio would be asserting somebody else's contention.
        if wide <= narrow:
            raise AssertionError(
                f"the block with the read inside was not wider — {wide:.0f}ms "
                f"against {narrow:.0f}ms"
            )
        return (
            f"read inside: {refused_inside}/{rounds} refused,"
            f" block open {wide:.0f}ms;"
            f" hoisted out: {refused_hoisted}/{rounds} refused,"
            f" block open {narrow:.0f}ms"
        )


@check("after-commit-replayed")
def _after_commit_replayed(pool: Pool, table: str) -> str:
    """The mirror of the check above, and the discipline this hook needs the
    other way round. A `@dray.after_commit` is registered outside the part dray
    replays, so a save DSQL refuses runs the handler once however many attempts
    it took — where the `before_delete` above runs once per attempt by design.

    Nothing local can produce a real refusal; the suite raises one by hand. And
    this is the promise a caller cannot check for themselves, because by the
    time a job has gone out five times it is somebody else's process holding
    the evidence.

    Six threads saving the same row, which is the shape `occ-replayed` uses to
    make DSQL refuse a commit.
    """
    ran: list[Any] = []

    @record(table=table, collection=f"c_{RUN}_after_commit")
    class Tally:
        n: int = field(default=0)

        @after_commit
        def say_it_landed(self) -> None:
            ran.append(self.n)

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        collection = getattr(store, f"c_{RUN}_after_commit")
        tally = collection.add(Tally(n=0))
        # The row this check is about is the six below; the one that made it
        # was a save like any other and ran the handler like any other.
        ran.clear()

        errors: list[BaseException] = []

        def bump() -> None:
            try:
                with pool.store(records=[Tally]) as mine:
                    theirs = getattr(mine, f"c_{RUN}_after_commit").by_id(tally.id)
                    theirs.n += 1
                    theirs.save()
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        threads = [threading.Thread(target=bump) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        if errors:
            raise AssertionError(f"a save was not replayed: {errors[0]!r}")
        if len(ran) != len(threads):
            raise AssertionError(
                f"{len(threads)} saves and {len(ran)} handler runs"
            )
        return f"{len(threads)} concurrent saves under replay, {len(ran)} runs"


@check("check-replayed")
def _check_replayed(pool: Pool, table: str) -> str:
    """A `@dray.check` is a rule and not a step, so it runs once for a save
    however many attempts DSQL takes to accept it. It sits outside the replayed
    part of the write for the reason `after_commit` does, and the number a
    caller would notice is the count: a rule that ran per attempt would be one
    an author could see reading its own record more than once.

    The same six threads on one row that `occ-replayed` uses, and the same
    reason nothing local can stand in for it.
    """
    ran: list[Any] = []

    @record(table=table, collection=f"c_{RUN}_rule")
    class Tally:
        n: int = field(default=0)

        @dray.check
        def counted(self) -> None:
            ran.append(self.n)

    with pool.store(records=[Tally]) as store:
        store.create(Tally)
        collection = getattr(store, f"c_{RUN}_rule")
        tally = collection.add(Tally(n=0))
        # The row this check is about is the six saves below; making it was a
        # write like any other and ran the rule like any other.
        ran.clear()

        errors: list[BaseException] = []

        def bump() -> None:
            try:
                with pool.store(records=[Tally]) as mine:
                    theirs = getattr(mine, f"c_{RUN}_rule").by_id(tally.id)
                    theirs.n += 1
                    theirs.save()
            except BaseException as error:  # every failure is reported below
                errors.append(error)

        threads = [threading.Thread(target=bump) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        if errors:
            raise AssertionError(f"a save was not replayed: {errors[0]!r}")
        if len(ran) != len(threads):
            raise AssertionError(f"{len(threads)} saves and {len(ran)} rule runs")
        return f"{len(threads)} concurrent saves under replay, {len(ran)} runs"


@check("ceiling")
def _ceiling(pool: Pool, table: str) -> str:
    """The 3,000-row transaction ceiling, which is the reason `_write_all`
    splits. Local PostgreSQL takes any number, so nothing else proves the split
    is needed or that it works."""

    @record(table=table, collection=f"c_{RUN}_ceiling")
    class Row:
        n: int = field(default=0)

    count = MAX_ROWS + 500
    with pool.store(records=[Row]) as store:
        store.create(Row)
        collection = getattr(store, f"c_{RUN}_ceiling")
        collection.add_all([Row(n=n) for n in range(count)])
        written = collection.count()
    if written != count:
        raise AssertionError(f"wrote {written} of {count}")
    return f"{count} rows, split into transactions that fit"


@check("index-async")
def _index_async(pool: Pool, table: str) -> str:
    """`create index async` is the one statement DSQL takes and PostgreSQL
    refuses, so the local suite can only check that dray decides to write it."""

    @record(table=table, collection=f"c_{RUN}_index", indexes=[index("email")])
    class Row:
        email: str = field(default="")

    with pool.store(records=[Row]) as store:
        store.create(Row)
        with store.conn.cursor() as cur:
            cur.execute(
                "select count(*) from pg_indexes where tablename = %s", [table]
            )
            made = cur.fetchone()[0]
    if made < 1:
        raise AssertionError("no index was made")
    return f"{made} index(es) present"


@check("declared-indexes")
def _declared_indexes(pool: Pool, table: str) -> str:
    """Where a declared index's DDL goes, which is dray's decision rather than
    the caller's: on a table being created a unique index is a constraint in the
    `create table`, and the same declaration against a table that is already
    there is `create unique index async`.

    Local PostgreSQL takes both forms and enforces both at once, so it cannot
    say any of what matters here — that the cluster accepts a named table-level
    constraint at all, that its backing index carries the name `drift` goes
    looking for, and that running the index form afterwards finds it rather than
    building a second copy of it.
    """

    @record(
        table=table,
        collection=f"c_{RUN}_indexes",
        indexes=[
            index("on_date", "sitting", unique=True),
            index("on_date", "service"),
        ],
    )
    class Service:
        on_date: date | None = field()
        sitting: str = field(default="")
        service: str = field(default="")

    def indexes(store: Any) -> list[str]:
        with store.conn.cursor() as cur:
            cur.execute(
                "select indexname from pg_indexes where tablename = %s", [table]
            )
            return sorted(row[0] for row in cur.fetchall())

    with pool.store(records=[Service]) as store:
        store.create(Service)
        drifted = schema.drift(store.conn, Service)
        made = indexes(store)

        collection = getattr(store, f"c_{RUN}_indexes")
        collection.add(Service(on_date=date(2026, 3, 14), sitting="lunch"))
        try:
            collection.add(Service(on_date=date(2026, 3, 14), sitting="lunch"))
            enforced = False
        except DuplicateRecord:
            enforced = True

        # The other path, against the table that now exists. Same declaration,
        # same name, so a migration running this finds the constraint's index
        # and has nothing to do.
        for statement in schema.create_indexes(Service):
            ddl(store.conn, statement)
        after = indexes(store)

    wanted = {f"{table}_on_date_sitting", f"{table}_on_date_service"}
    if not wanted <= set(made):
        raise AssertionError(f"wanted {sorted(wanted)}, the table has {made}")
    if drifted:
        raise AssertionError(f"drift on a table just created: {drifted}")
    if not enforced:
        raise AssertionError("a duplicate pair was taken")
    if after != made:
        raise AssertionError(f"the index form built something: {made} then {after}")
    return f"{len(made)} indexes, unique enforcing at once, async form a no-op"


@check("long-index-name")
def _long_index_name(pool: Pool, table: str) -> str:
    """
    What a cluster does with an index name past the 63 bytes an identifier
    holds, which is the assumption `_index_name` now shortens against.

    Local PostgreSQL agrees with DSQL on every part of this and that is exactly
    why it is here: the suite proves dray generates the short name, and only a
    cluster can say that the short name is the one DSQL would have arrived at
    on its own. If it ever stops being, `drift` starts reporting an index the
    table is carrying as missing, and nothing else in the library notices.

    Two failures rather than one. A name too long is stored cut and works
    perfectly, so the first is `drift` alone. The second is a name that collides
    only because both were cut: `create index async if not exists` matches the
    index already carrying that name, builds nothing, and says so in the shape
    of a statement that succeeded — which is what dray now refuses at
    declaration, and what is provoked here with SQL of its own because a class
    can no longer say it.
    """

    @record(
        table=table,
        collection=f"c_{RUN}_long_name",
        indexes=[
            index("effective_from", "contribution_level", "renewed_by_volunteer")
        ],
    )
    class Membership:
        effective_from: date | None = field()
        contribution_level: str = field(default="")
        renewed_by_volunteer: str = field(default="")

    asked = "_".join((table, "effective_from", "contribution_level",
                      "renewed_by_volunteer"))
    cut = schema.create_indexes(Membership)[0].split(" if not exists ")[1]
    cut = cut.split(" on ")[0]

    def indexes(store: Any) -> list[str]:
        with store.conn.cursor() as cur:
            cur.execute(
                "select indexname from pg_indexes where tablename = %s", [table]
            )
            return sorted(row[0] for row in cur.fetchall())

    with pool.store(records=[Membership]) as store:
        store.create(Membership)
        made = indexes(store)
        drifted = schema.drift(store.conn, Membership)

        # A second name sharing those first 63 bytes, which is every collision
        # this is about: the cluster cuts it to the name already on the table.
        quiet = ddl(
            store.conn,
            f"create index async if not exists {cut}_and_then_some"
            f" on {table} (contribution_level)",
        )
        after = indexes(store)

        # And the same name said without `if not exists`, which is the cluster
        # saying out loud what the form above swallows.
        loud = not ddl(
            store.conn,
            f"create index async {cut}_and_then_some"
            f" on {table} (contribution_level)",
            tolerate=(psycopg.errors.DuplicateTable,),
        )

    if len(asked.encode()) <= 63:
        raise AssertionError(f"{asked!r} is not long enough to prove anything")
    if cut not in made:
        raise AssertionError(f"wanted {cut!r}, the table has {made}")
    if drifted:
        raise AssertionError(f"drift on a table just created: {drifted}")
    if not quiet:
        raise AssertionError("the colliding name was refused, not swallowed")
    if after != made:
        raise AssertionError(f"a second index was built: {made} then {after}")
    if not loud:
        raise AssertionError("the same name without `if not exists` was taken")
    return (
        f"{len(asked.encode())} bytes stored at {len(cut.encode())},"
        " and a name colliding with it built nothing"
    )


@check("index-shapes")
def _index_shapes(pool: Pool, table: str) -> str:
    """
    What DSQL will and will not have in an index, which the manual now states
    and dray has no part in either way.

    None of it can be looked up. AWS enumerate what *is* supported and say the
    list is not exhaustive; no page says a partial index is refused, and no page
    is going to. So a cluster is the only thing that knows, and a paragraph
    written from one afternoon's probing rots unless something asks again.

    The two placements are here because that pair is the whole reason a class
    may say `asc(name, nulls=...)` on an index and may not say `desc`. A key
    takes `nulls first` and `nulls last` and refuses a direction, which is a
    line dray now draws at declaration — and it is drawn there because local
    PostgreSQL takes `(n desc)` without a word, so this file is the only thing
    in the repository that can tell the two apart.

    Three more the page quotes. The 8-column ceiling, which is documented. The
    24-index one, which is documented as a number rather than as a budget:
    `pg_indexes` at the refusal holds 24 rows *including the primary key*, so
    what a table has left to spend is 23. And that a unique index built `async`
    does not enforce while it is still building — a real window rather than a
    theoretical one, and the argument for a rule a record cannot be without
    going in the `create table` instead.

    The last two are here because a name search is the shape everybody reaches
    for and the local answer to it does not exist here. A plain btree cannot
    serve `like 'rob%'` in any collation but `C`, so on PostgreSQL you reach for
    `text_pattern_ops`, and for a misspelling you reach for a trigram index —
    and this cluster refuses the operator class outright and refuses `using`
    altogether, so btree is the only method there is. **Neither is a gap in
    dray.** DSQL runs in `C`, which the check below pins, and in `C` the plain
    index a class can already declare answers the prefix match. What has no
    answer here at all is the substring one, and nothing dray adds would give it
    one.

    The most expensive check in this file, and nearly all of that is the
    ceiling: one schema change per index up to the refusal, through the lock
    every other check is waiting on. There is no cheaper way to ask.
    """
    # The four shapes arrive as `FeatureNotSupported` and the two ceilings as
    # neither one thing nor the other — `more than 8 column keys` is a
    # `TooManyColumns` and `more than 24 indexes` a `ProgramLimitExceeded`, and
    # psycopg makes those siblings rather than one a kind of the other.
    closed = (
        psycopg.errors.FeatureNotSupported,
        psycopg.errors.TooManyColumns,
        psycopg.errors.ProgramLimitExceeded,
    )
    eight = ", ".join(f"c{n}" for n in range(1, 9))
    nine = ", ".join(f"c{n}" for n in range(1, 10))
    shapes = {
        "a predicate": f"create index async {table}_where on {table} (a) where flag",
        "a sort order": f"create index async {table}_desc on {table} (n desc)",
        "nulls first": f"create index async {table}_nf on {table} (n nulls first)",
        "nulls last": f"create index async {table}_nl on {table} (n nulls last)",
        "no async": f"create index {table}_sync on {table} (a)",
        "unique, no async": f"create unique index {table}_usync on {table} (e, f)",
        "unique as a constraint": (
            f"alter table {table} add constraint {table}_uc unique (e, f)"
        ),
        "nine key columns": f"create index async {table}_nine on {table} ({nine})",
        "a composite": f"create index async {table}_comp on {table} (a, c, b)",
        "the same one again": (
            f"create index async if not exists {table}_comp on {table} (a, c, b)"
        ),
        "an expression": f"create index async {table}_lower on {table} (lower(a))",
        "an operator class": (
            f"create index async {table}_pat on {table} (a text_pattern_ops)"
        ),
        "another index method": (
            f"create index async {table}_gin on {table} using gin (a)"
        ),
        "eight key columns": f"create index async {table}_eight on {table} ({eight})",
    }
    door_shut = {
        "a predicate",
        "a sort order",
        "no async",
        "unique, no async",
        "unique as a constraint",
        "nine key columns",
        "an operator class",
        "another index method",
    }
    refused, taken = [], []
    spare = ", ".join(f"c{n} text" for n in range(1, 31))
    with pool.store() as store:
        ddl(
            store.conn,
            f"create table {table} (id uuid primary key, a text, b text,"
            f" c text, e text, f text, g text, h text, flag boolean, n int,"
            f" {spare})",
        )
        for what, statement in shapes.items():
            ok = ddl(store.conn, statement, tolerate=closed)
            (taken if ok else refused).append(what)
        if sorted(refused) != sorted(door_shut):
            raise AssertionError(
                "the page and the cluster disagree about what may be in an "
                f"index — the page says {sorted(door_shut)} are refused; the "
                f"cluster refused {sorted(refused)} and took {sorted(taken)}"
            )

        # The window. The duplicate goes in and comes straight back out, so the
        # build finishes on clean data — an index whose build *failed* is a
        # different state, described differently by AWS, and the page keeps the
        # two apart rather than measuring one and quoting the other.
        ddl(store.conn, f"create unique index async {table}_win on {table} (g, h)")
        pair = f"insert into {table} (id, g, h) values (gen_random_uuid(), 'x', 'y')"
        store.conn.execute(pair)
        try:
            store.conn.execute(pair)
            window = True
        except psycopg.errors.UniqueViolation:
            window = False
        store.conn.execute(f"delete from {table} where g = 'x'")
        ddl(store.conn, f"drop index {table}_win")
        if not window:
            raise AssertionError(
                "a duplicate written while the build was still running was "
                "refused — the page says that window is open"
            )

        # And that it does enforce once the build has finished, which is the
        # half that makes the window worth warning about.
        job = submitted(
            store.conn, f"create unique index async {table}_pair on {table} (e, f)"
        )
        # Not through the lock. It is a wait rather than a schema change, and
        # holding the lock for a build would stop every other check for as long
        # as the build takes.
        store.conn.execute("call sys.wait_for_job(%s)", [job])
        state = store.conn.execute(
            "select status, details from sys.jobs where job_id = %s", [job]
        ).fetchone()
        if not state or state[0] != "completed":
            raise AssertionError(f"the index build did not complete: {state}")
        held = f"insert into {table} (id, e, f) values (gen_random_uuid(), 'p', %s)"
        store.conn.execute(held, ["q"])
        enforced = False
        try:
            store.conn.execute(held, ["q"])
        except psycopg.errors.UniqueViolation:
            enforced = True
        if not enforced:
            raise AssertionError("a finished unique index took a duplicate pair")
        # The pair rather than either column, which is what it is for.
        store.conn.execute(held, ["r"])

        # Last, because it fills the table's budget up. The number is
        # documented; that the primary key is one of the 24 is not, and this is
        # the only way to ask.
        for n in range(1, 31):
            if not ddl(
                store.conn,
                f"create index async {table}_fill{n} on {table} (c{n})",
                tolerate=(psycopg.errors.ProgramLimitExceeded,),
            ):
                break
        else:
            raise AssertionError("thirty indexes on one table and no refusal")
        names = [
            row[0]
            for row in store.conn.execute(
                "select indexname from pg_indexes where tablename = %s", [table]
            ).fetchall()
        ]

    if len(names) != 24:
        raise AssertionError(f"the ceiling fell at {len(names)} indexes, not 24")
    if not any(name.endswith("_pkey") for name in names):
        raise AssertionError(f"24 indexes and the primary key not among them: {names}")
    return (
        f"refused {', '.join(sorted(refused))};"
        f" took {', '.join(sorted(taken))};"
        f" {len(names)} indexes at the ceiling, the primary key one of them;"
        " a unique index enforced only once its build had finished"
    )


@check("collation")
def _collation(pool: Pool, table: str) -> str:
    """
    What this cluster sorts by, which is not what local PostgreSQL sorts by.

    DSQL runs in `C`, and `tests/` runs against whatever `initdb` picked, which
    on a developer's machine is a language collation. So `'Z' < 'a'` is true
    here and false there, and every `order_by` on a text column is a green test
    proving an order the cluster does not use. That is the ordinary shape of
    this whole file — local passes for a reason that does not carry — and it is
    worth pinning because two things rest on it.

    A read: in `C` a plain btree answers `like 'rob%'`, which is why the
    operator class the shapes check finds refused is not a gap. And a sort: an
    index is built in the collation of its column, so measuring either in
    development is measuring a different database.

    Nothing dray does depends on the answer. It is here so that the day it
    changes, something fails rather than a paragraph quietly going wrong.
    """
    with pool.store() as store:
        with store.conn.cursor() as cur:
            cur.execute(
                "select datcollate from pg_database"
                " where datname = current_database()"
            )
            collation = cur.fetchone()[0]
            cur.execute("select 'Z' < 'a'")
            bytewise = cur.fetchone()[0]
    if collation != "C" or not bytewise:
        raise AssertionError(
            f"this cluster collates as {collation!r} and sorts 'Z' before 'a' "
            f"{bytewise} — the suite is configured to match `C`, and no longer "
            "does"
        )
    return "C, so 'Z' sorts before 'a' and a plain btree serves a prefix match"


@check("key-types")
def _key_types(pool: Pool, table: str) -> str:
    """Which types may be in a key at all, which is what a declared index, a
    declared unique index and a declared `id` are refused for.

    The refusal is about keys and not about indexes — the cluster says
    `datatype X is not supported in a key` to a unique constraint, a unique
    index and a plain index alike — so the class refuses all three uses of the
    same three types. Both doors are tried here: an index a class asked for, and
    the primary key nobody asked for, which is what `id: bytes` becomes.
    `date` rides along as the control: without it a failure here reads as a
    broken statement rather than a rejected type.
    """
    # Read off the class rather than repeated here, so this is an assertion
    # about what dray claims rather than a second copy of it: a type added to
    # `UNINDEXABLE` is covered without anybody remembering, and one taken out
    # wrongly fails here rather than agreeing with a stale list. `date` rides
    # along as the control — without it a failure reads as a broken statement
    # rather than a rejected type.
    unindexable = sorted(set(UNINDEXABLE.values()))
    kinds = (*unindexable, "date")
    indexed_refused, indexed_taken = [], []
    keyed_refused, keyed_taken = [], []
    with pool.store() as store:
        # One table with a column of each, and an index attempted per column,
        # rather than a table per type. The refusal is identical either way —
        # it is about the key — and this is five schema changes rather than
        # twelve, which matters because they contend with every other check.
        ddl(
            store.conn,
            f"create table {table} (id uuid primary key, "
            + ", ".join(f"v_{k} {k}" for k in kinds)
            + ")",
        )
        for kind in kinds:
            ok = ddl(
                store.conn,
                f"create index async {table}_{kind} on {table} (v_{kind})",
                tolerate=(psycopg.errors.FeatureNotSupported,),
            )
            (indexed_taken if ok else indexed_refused).append(kind)

        # The primary key has to be a table apiece, because a table has one of
        # them, and it is the statement `store.create` writes for a record that
        # declares its own `id` — no index is asked for anywhere in it. The one
        # the cluster takes is dropped where it was made: the sweep at the end
        # knows about this check's own table and nothing else.
        for kind in kinds:
            keyed = f"{table}_k_{kind}"
            ok = ddl(
                store.conn,
                f"create table {keyed} (id {kind} primary key)",
                tolerate=(psycopg.errors.FeatureNotSupported,),
            )
            (keyed_taken if ok else keyed_refused).append(kind)
            if ok:
                ddl(store.conn, f"drop table if exists {keyed}")

    for door, refused, taken in (
        ("an index", indexed_refused, indexed_taken),
        ("a primary key", keyed_refused, keyed_taken),
    ):
        if sorted(refused) != unindexable or taken != ["date"]:
            raise AssertionError(
                f"the class and the cluster disagree about {door} — the class says "
                f"{unindexable} cannot be in a key; the cluster refused "
                f"{sorted(refused)} and took {taken}"
            )
    return (
        f"refused as an index and as a primary key: {', '.join(unindexable)}; "
        "date taken as both"
    )


@check("numeric-size")
def _numeric_size(pool: Pool, table: str) -> str:
    """That a `numeric` column is the size dray wrote and rounds where it says.

    The reason `SQL_TYPES` spells `numeric(18,6)` out: a bare `numeric` is
    unbounded on local PostgreSQL and this on a cluster, which applies it on
    `INSERT` rather than at `create table`, so a rate carried to eight places
    was stored whole by the suite and rounded in production with nothing raised
    at either end. Local PostgreSQL now rounds identically — what it cannot say
    is that eighteen and six are still DSQL's default, or that a declared size
    reaches the cluster at all.
    """

    @record(table=table, collection=f"c_{RUN}_numeric")
    class Rate:
        default_size: Decimal | None = field()
        declared: Decimal | None = field(precision=12, scale=8)

    with pool.store(records=[Rate]) as store:
        store.create(Rate)
        collection = getattr(store, f"c_{RUN}_numeric")
        written = collection.add(
            Rate(
                default_size=Decimal("0.00012345"),
                declared=Decimal("0.00012345"),
            )
        )
        back = collection.by_id(written.id)
        with store.conn.cursor() as cur:
            cur.execute(
                "select column_name, numeric_precision, numeric_scale"
                " from information_schema.columns"
                " where table_name = %s and table_schema = current_schema()"
                " and data_type = 'numeric'",
                [table],
            )
            sizes = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    wanted = {
        "default_size": (schema.DECIMAL_PRECISION, schema.DECIMAL_SCALE),
        "declared": (12, 8),
    }
    if sizes != wanted:
        raise AssertionError(f"the cluster made {sizes}, not {wanted}")
    if back.default_size != Decimal("0.000123"):
        raise AssertionError(
            f"a value too precise for the default came back as {back.default_size}, "
            "not rounded to six places"
        )
    if back.declared != Decimal("0.00012345"):
        raise AssertionError(
            f"a column declared to eight places rounded anyway: {back.declared}"
        )
    return "numeric(18,6) rounded at six places, numeric(12,8) kept eight"


@check("drift")
def _drift(pool: Pool, table: str) -> str:
    """Asked of `information_schema` on a real cluster, and scoped to the schema
    in use rather than every schema on it."""

    @record(table=table, collection=f"c_{RUN}_drift")
    class Row:
        name: str = field(default="")

    @record(table=table, collection=f"c_{RUN}_drift_more")
    class Wider:
        name: str = field(default="")
        added_later: str | None = field()

    with pool.store(records=[Row]) as store:
        store.create(Row)
        clean = schema.drift(store.conn, Row)
        moved = schema.drift(store.conn, Wider)
    if clean:
        raise AssertionError(f"a table it just made reads as drifted: {clean}")
    if not any("added_later" in line for line in moved):
        raise AssertionError(f"a missing column was not noticed: {moved}")
    return "clean when it agrees, and names the column when it does not"


@check("pool-threads")
def _pool_threads(pool: Pool, table: str) -> str:
    """A store each, from the pool, at the same time — the shape the whole
    design points at and the one `Pool(host=...)` exists for."""

    @record(table=table, collection=f"c_{RUN}_threads")
    class Row:
        name: str = field(default="")

    with pool.store(records=[Row]) as store:
        store.create(Row)

    failed: list[BaseException] = []

    def work(n: int) -> None:
        try:
            with pool.store(records=[Row]) as mine:
                getattr(mine, f"c_{RUN}_threads").add(Row(name=f"t{n}"))
        except BaseException as error:  # every failure is reported below
            failed.append(error)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)

    if failed:
        raise AssertionError(f"{len(failed)} failed, first: {failed[0]!r}")
    with pool.store(records=[Row]) as store:
        written = getattr(store, f"c_{RUN}_threads").count()
    if written != 8:
        raise AssertionError(f"{written} of 8 rows landed")
    return "8 threads, 8 stores, 8 rows"


@check("batched-clash")
def _batched_clash(pool: Pool, table: str) -> str:
    """
    A set goes out as one round trip, and a failure inside it has to come back
    still naming the record that caused it.

    Which is the whole of what the batching rests on, and the one part of it local
    PostgreSQL cannot settle. psycopg hands each result to the cursor that asked
    for it and abandons everything behind the first failure, so dray reads the
    answers in the order it sent them and the statement with no result of its
    own is the culprit. Whether DSQL's protocol keeps that ordering under a
    pipeline is not dray's to assume: if it ever stops, a clash in the middle
    of an `add_all` starts naming the wrong record rather than failing, which
    is the kind of wrong that gets believed.

    A clash on the key deliberately, because that is the one message that names
    a *record* and not a column — blame the wrong statement and it says the
    wrong id out loud. In the middle of the set rather than at its head, since
    a head that happens to be right proves nothing about matching by position.

    And that a field the database worked out arrives on every record of a
    batched set, not just the first. That is the half of a write `executemany`
    could not have carried and the reason pipelining is what this uses.
    """

    @record(table=table, collection=f"c_{RUN}_batched")
    class Slot:
        code: str = field(default="")
        seen_at: datetime | None = field(on_add=clock)

    with pool.store(records=[Slot]) as store:
        store.create(Slot)
        collection = getattr(store, f"c_{RUN}_batched")

        first = collection.add_all([Slot(code=f"a{n}") for n in range(10)])
        unfilled = [one.code for one in first if one.seen_at is None]

        # An id somebody else chose, which is how a key clash happens for real:
        # an import carrying its own keys, a record rebuilt from a backup.
        taken = first[6].id
        clashing = [Slot(code=f"b{n}") for n in range(10)]
        clashing[6] = Slot(code="b6", id=taken)
        try:
            collection.add_all(clashing)
            said = None
        except DuplicateRecord as named:
            said = str(named)

        left = collection.count()

    if unfilled:
        raise AssertionError(
            f"a batched write filled nothing in for {unfilled}"
        )
    if said is None:
        raise AssertionError("a clash in the middle of a batch went through")
    if str(taken) not in said:
        raise AssertionError(
            f"the clash named a record that did not clash: {said}"
        )
    if left != 10:
        raise AssertionError(f"the refused set left {left - 10} rows behind")
    return (
        "a clash at position 6 of 10 named position 6,"
        " and none of the set landed"
    )


@check("delete-over-the-ceiling")
def _delete_over_the_ceiling(pool: Pool, table: str) -> str:
    """
    A delete too big for one transaction, which the page now describes as a
    clean no.

    It rests on two things local PostgreSQL has no opinion about, since local
    PostgreSQL has no ceiling to cross. That the refusal leaves the tree
    exactly as it was — the page tells somebody the failure they are accepting
    is safe, and a half-deleted tree would make that untrue. And that the
    ceiling counts a transaction rather than a statement, which is the whole
    reason a delete cannot buy room by sending more of them: it is what says
    counting each generation first would predict the refusal without avoiding
    it.
    """
    with pool.store() as store:
        conn = store.conn
        ddl(
            conn,
            f"create table if not exists {table}"
            " (id uuid primary key, n int not null)",
        )

        rows = 4000
        for start in range(0, rows, 500):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(
                        f"insert into {table} (id, n)"
                        " values (gen_random_uuid(), %s)",
                        [(n,) for n in range(start, start + 500)],
                    )

        def held() -> int:
            (n,) = conn.execute(f"select count(*) from {table}").fetchone()
            return n

        if held() != rows:
            raise AssertionError(f"{held()} rows to delete rather than {rows}")

        try:
            with conn.transaction():
                conn.execute(f"delete from {table}")
        except psycopg.errors.ProgramLimitExceeded:
            pass
        else:
            raise AssertionError(
                f"a delete of {rows} rows was taken, so the ceiling the page "
                "describes is not this cluster's"
            )
        if held() != rows:
            raise AssertionError(
                f"the refusal left {rows - held()} rows deleted, so a delete "
                "over the ceiling is not the clean no the page promises"
            )

        # Two statements, each under the ceiling, in one transaction.
        try:
            with conn.transaction():
                conn.execute(f"delete from {table} where n < 1800")
                conn.execute(f"delete from {table} where n >= 1800")
        except psycopg.errors.ProgramLimitExceeded:
            pass
        else:
            raise AssertionError(
                "two statements under the ceiling were taken together, so it "
                "counts a statement rather than a transaction and a delete "
                "could buy room by sending more of them"
            )
        if held() != rows:
            raise AssertionError(
                f"{rows - held()} rows went in the second try"
            )

    return (
        f"{rows} refused whole, twice, and the ceiling is the transaction's"
    )


@check("thinning-past-the-ceiling")
def _thinning_past_the_ceiling(pool: Pool, table: str) -> str:
    """
    `thin`, on a tree whose total is over the ceiling — and the trap the page
    warns about beside it.

    Two claims local PostgreSQL cannot make, because it has no ceiling to cross
    and would take the whole tree in one statement. That a loop of passes gets a
    set off that `clear` is refused for, no pass ever near the limit. And that
    the same loop inside a block the caller opened is the single transaction
    `thin` exists to escape, rebuilt — refused whole, with every row still
    there.

    The refusal is dray's rather than the database's, and that is the half only
    a cluster can settle: dray counts the rows its passes took in the block and
    stops at `MAX_ROWS`, which is short enough of DSQL's 3,000 that its answer
    has to arrive first. A `ProgramLimitExceeded` here would mean it does not.

    The refused half runs first deliberately: it leaves the tree exactly as it
    was, so the half that works thins the same rows rather than a second seeding
    of them.
    """
    mid_table, low_table = f"{table}_mid", f"{table}_low"

    @record(table=table, collection=f"c_{RUN}_thinning")
    class Top:
        name: str = field(default="")

    @child(of=Top, name="mids", table=mid_table,
           collection=f"c_{RUN}_thinning_mid")
    class Mid:
        label: str = field(default="")

    @child(of=Mid, name="lows", table=low_table,
           collection=f"c_{RUN}_thinning_low")
    class Low:
        label: str = field(default="")

    fanout, wide = 20, 160
    total = fanout + fanout * wide
    with pool.store(records=[Top, Mid, Low]) as store:
        store.create(Top, Mid, Low)
        tops = getattr(store, f"c_{RUN}_thinning")
        mids = getattr(store, f"c_{RUN}_thinning_mid")
        lows = getattr(store, f"c_{RUN}_thinning_low")

        top = tops.add(Top(name="one"))
        for mid in mids.add_all(
            [Mid(label=f"m{n}") for n in range(fanout)], parent=top
        ):
            lows.add_all(
                [Low(label=f"l{n}") for n in range(wide)], parent=mid
            )
        held = mids.count() + lows.count()
        if held != total:
            raise AssertionError(f"seeded {held} rows rather than {total}")

        def drain() -> list[int]:
            took = []
            while True:
                pass_took = top.mids.thin(at_a_time=500)
                if not pass_took:
                    return took
                took.append(pass_took)

        try:
            with store.transaction():
                drain()
        except psycopg.errors.ProgramLimitExceeded:
            raise AssertionError(
                "the cluster refused the block before dray's own count reached "
                "the ceiling, so the caller sees `transaction row limit "
                "exceeded` from the middle of the loop after all"
            )
        except ValueError as refused:
            if "passes in this block" not in str(refused):
                raise
        else:
            raise AssertionError(
                f"a loop of passes totalling {total} rows was taken inside a "
                "block, so joining one costs nothing and the page's warning is "
                "about a trap this cluster does not have"
            )
        if mids.count() + lows.count() != total:
            raise AssertionError(
                "the refused block left rows removed, so a thinning loop "
                "inside one is not the clean no the page describes"
            )

        took = drain()
        if mids.count() + lows.count() != 0:
            raise AssertionError(
                f"{mids.count() + lows.count()} rows left after the loop"
            )
        if tops.count() != 1:
            raise AssertionError("the loop took the parent as well")

        for gone in (low_table, mid_table):
            ddl(store.conn, f"drop table if exists {gone}")

    return (
        f"{total} rows: refused whole inside a block, gone in {len(took)} "
        f"passes outside one, none over {max(took)}"
    )


@check("thinning-with-a-rule")
def _thinning_with_a_rule(pool: Pool, table: str) -> str:
    """
    The statements `thin` sends that nothing else does, and the row they would
    lose without the cascade under them.

    A generation whose class declares a `@before_delete` is read before its pass
    takes it and then removed *by id*, so `delete ... where id = any(%s)` and
    the cascade beneath those same ids are the one statement shape `clear` never
    builds. The check above takes the other path — neither of its classes
    declares a rule — so without this one the shape never leaves local
    PostgreSQL.

    And the row: a pass reaches the children only once every generation below
    them has answered empty, so a rule that writes under the child it is losing
    writes past the statement that would have taken it. Nothing conflicts on a
    row nobody else has touched, so a snapshot is the version of this where the
    commit is *most* likely to be taken and the row most likely to be left —
    which is why it is measured here rather than reasoned about.
    """
    mid_table, low_table = f"{table}_mid", f"{table}_low"
    ran: list[str] = []

    @record(table=table, collection=f"c_{RUN}_thinrule")
    class Top:
        name: str = field(default="")

    @child(of=Top, name="mids", table=mid_table,
           collection=f"c_{RUN}_thinrule_mid")
    class Mid:
        label: str = field(default="")

        @before_delete
        def file_what_it_said(self):
            ran.append(self.label)
            self.lows.add(f"{self.label}-filed")
            self.save()

    @child(of=Mid, name="lows", table=low_table,
           collection=f"c_{RUN}_thinrule_low")
    class Low:
        label: str = field(default="")

    fanout, per_pass = 12, 5
    with pool.store(records=[Top, Mid, Low]) as store:
        store.create(Top, Mid, Low)
        tops = getattr(store, f"c_{RUN}_thinrule")
        mids = getattr(store, f"c_{RUN}_thinrule_mid")
        lows = getattr(store, f"c_{RUN}_thinrule_low")

        top = tops.add(Top(name="one"))
        mids.add_all(
            [Mid(label=f"m{n}") for n in range(fanout)], parent=top
        )

        took = []
        while True:
            this = top.mids.thin(at_a_time=per_pass)
            if not this:
                break
            took.append(this)

        if mids.count():
            raise AssertionError(
                f"{mids.count()} rows left, so the delete by id took fewer "
                "than the read said it would"
            )
        # Every row's rule ran, and the count is reported rather than asserted.
        # A refused commit replays the whole pass, so a run under contention
        # runs the rule again over rows whose first attempt rolled back — which
        # is what `_delete_batch` has said about itself since it was written and
        # is not this call's to fix. What must hold is that no row was taken
        # without its rule, which is what the set says.
        if set(ran) != {f"m{n}" for n in range(fanout)}:
            raise AssertionError(
                f"the rule ran for {len(set(ran))} of {fanout} rows, so a pass "
                "took one it never read"
            )
        left = lows.count()
        if left:
            raise AssertionError(
                f"{left} rows the rule wrote under a child it was losing are "
                "still there, with the row that named them gone"
            )
        if tops.count() != 1:
            raise AssertionError("the loop took the parent as well")

        for gone in (low_table, mid_table):
            ddl(store.conn, f"drop table if exists {gone}")

    return (
        f"{fanout} taken by id in {len(took)} passes, the rule ran {len(ran)} "
        f"times over all {fanout}, and what it wrote went with them"
    )


@check("write-skew")
def _write_skew(pool: Pool, table: str) -> str:
    """
    Two writers, one rule, and rows that do not overlap — and what
    `for update` does about it.

    The page tells somebody to read the thing a rule is about `for update` and
    promises that a second writer of that row is then refused. It rests on two
    claims local PostgreSQL cannot make: that DSQL lets write skew through in
    the first place, since snapshot isolation is repeatable read and two
    transactions writing different rows conflict on nothing; and that the
    clause adds the rows it read to the conflict set rather than doing nothing
    at all.

    Both halves, so a tightening at either end shows up here rather than as a
    page that has quietly stopped being true. Two people on call and the rule
    is at least one — each checks that somebody else is on, then takes only
    itself off, so the writes are disjoint and the reads are common.
    """
    with pool.store() as a, pool.store() as b:
        ddl(
            a.conn,
            f"create table if not exists {table}"
            " (who text primary key, on_call bool)",
        )

        def reset() -> None:
            a.conn.execute(f"delete from {table}")
            a.conn.execute(f"insert into {table} values ('alice', true)")
            a.conn.execute(f"insert into {table} values ('bob', true)")

        def race(guarded: bool) -> bool:
            """Both transactions interleaved by hand, so the gap is certain
            rather than hoped for. True if one of them was refused."""
            reset()
            for conn in (a.conn, b.conn):
                conn.execute("begin")
            for conn, me, them in (
                (a.conn, "alice", "bob"),
                (b.conn, "bob", "alice"),
            ):
                if guarded:
                    conn.execute(
                        f"select on_call from {table} where who = %s"
                        " for update",
                        [them],
                    ).fetchall()
                (others,) = conn.execute(
                    f"select count(*) from {table}"
                    " where on_call and who <> %s",
                    [me],
                ).fetchone()
                if others < 1:
                    raise AssertionError(f"{me} saw nobody else on call")
            for conn, me in ((a.conn, "alice"), (b.conn, "bob")):
                conn.execute(
                    f"update {table} set on_call = false where who = %s", [me]
                )
            try:
                for conn in (a.conn, b.conn):
                    conn.execute("commit")
                return False
            except psycopg.errors.SerializationFailure:
                return True
            finally:
                for conn in (a.conn, b.conn):
                    try:
                        conn.execute("rollback")
                    except psycopg.Error:
                        pass

        if race(guarded=False):
            raise AssertionError(
                "DSQL refused a plain write skew, so the page is describing a "
                "problem this cluster no longer has"
            )
        (left,) = a.conn.execute(
            f"select count(*) from {table} where on_call"
        ).fetchone()
        if left != 0:
            raise AssertionError(
                f"unguarded, {left} left on call rather than 0"
            )

        if not race(guarded=True):
            raise AssertionError(
                "`for update` did not make the two conflict, so the "
                "pattern on the page buys nothing"
            )
        (left,) = a.conn.execute(
            f"select count(*) from {table} where on_call"
        ).fetchone()
        if left != 1:
            raise AssertionError(f"guarded, {left} left on call rather than 1")

    return "skew lands unguarded, and `for update` refuses one of the two"


@check("shared-store-refused")
def _shared_store(pool: Pool, table: str) -> str:
    """And the other half: sharing one store across threads is refused rather
    than silently losing the second thread's work."""
    with pool.store() as store:
        opened = threading.Event()
        tried = threading.Event()
        seen: list[BaseException | None] = []

        def holds() -> None:
            with store.transaction():
                opened.set()
                tried.wait(30)

        def intrudes() -> None:
            opened.wait(30)
            try:
                store.conn.execute("select 1")
                seen.append(None)
            except RuntimeError as error:
                seen.append(error)
            finally:
                tried.set()

        threads = [threading.Thread(target=t) for t in (holds, intrudes)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

    if not seen or seen[0] is None:
        raise AssertionError("a second thread was let in")
    return "refused, naming both threads"


#
# Running them
#


def run(name: str, fn: Callable, pool: Pool) -> tuple[str, bool, str, float]:
    table = f"dray_{RUN}_{name.replace('-', '_')}"
    started = time.monotonic()
    try:
        note = fn(pool, table) or "ok"
        return name, True, note, time.monotonic() - started
    except Exception:
        lines = traceback.format_exc().strip().splitlines()
        return name, False, lines[-1], time.monotonic() - started


def sweep(pool: Pool, names: list[str]) -> None:
    with pool.store() as store:
        for name in names:
            table = f"dray_{RUN}_{name.replace('-', '_')}"
            try:
                store.conn.execute(f"drop table if exists {table}")
            except Exception as error:  # best effort; the failure is reported
                print(f"  could not drop {table}: {error}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", nargs="?", default=os.environ.get("DRAY_DSQL_HOST"))
    parser.add_argument("--host", dest="host_flag")
    parser.add_argument("--only", help="comma-separated check names")
    parser.add_argument("--at-once", type=int, default=8)
    parser.add_argument("--keep", action="store_true", help="leave the tables")
    args = parser.parse_args()

    host = args.host_flag or args.host
    if not host:
        parser.error("a cluster hostname, or DRAY_DSQL_HOST")

    wanted = set(args.only.split(",")) if args.only else None
    chosen = [(n, f) for n, f in CHECKS if wanted is None or n in wanted]
    if not chosen:
        parser.error(f"no check matched; known: {', '.join(n for n, _ in CHECKS)}")

    print(f"\n{host}")
    print(f"run {RUN}, {len(chosen)} checks, {args.at_once} at once\n")

    pool = Pool(host=host, min_size=2, max_size=max(4, args.at_once))

    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(args.at_once) as pool_of_threads:
            results = list(
                pool_of_threads.map(lambda job: run(job[0], job[1], pool), chosen)
            )
    finally:
        if not args.keep:
            sweep(pool, [n for n, _ in chosen])
        pool.close()

    failed = 0
    for name, ok, note, took in sorted(results, key=lambda r: r[0]):
        mark = " ok " if ok else "FAIL"
        print(f"  [{mark}] {name:22} {took:5.1f}s  {note}")
        failed += not ok

    total = time.monotonic() - started
    print(f"\n{len(results) - failed}/{len(results)} in {total:.1f}s wall\n")
    return failed


if __name__ == "__main__":
    sys.exit(main())
