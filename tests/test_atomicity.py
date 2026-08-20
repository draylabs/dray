"""
Is a write actually one transaction?

Every atomicity claim dray makes rests on this, and none of it was true against
a cluster: `Store.connect` sets `autocommit=True`, and committing by hand groups
nothing under autocommit — no `BEGIN` is ever sent and `commit()` does nothing.
The tests all passed because pytest-postgresql hands over a connection with
autocommit off, so local PostgreSQL was configured differently from what
`connect` builds.

So these run the same write both ways. A test that only ran one of them is what
let it through.
"""

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from dray import (
    AfterCommitFailed,
    CommitRefused,
    RecordHasChanged,
    Store,
    after_commit,
    child,
    field,
    record,
    replaying,
)
from dray.store import ATTEMPTS, ConcurrencyExhausted, retrying


@record(table="probe", collection="probes")
class Probe:
    name: str = field(default="")


@child(of=Probe, name="marks", table="mark")
class Mark:
    body: str = field(default="")


@pytest.fixture(params=[False, True], ids=["autocommit-off", "autocommit-on"])
def probes(postgresql, request):
    """The same store handed a connection both ways round. `connect` produces
    the second; `psycopg.connect` gives you the first, and a store takes it to
    the second itself."""
    postgresql.autocommit = request.param
    store = Store(postgresql)
    store.create(Probe, Mark)
    return store


def onlooker(store):
    """A second connection to the same database.

    Reading back through the connection that did the writing proves nothing
    about whether they committed — it sees its own uncommitted work either way,
    which is exactly how a whole suite of atomicity tests missed a write path
    that committed nothing at all.
    """
    info = store.conn.info
    return psycopg.connect(
        host=info.host, port=info.port, user=info.user, dbname=info.dbname
    )


def test_a_failed_write_leaves_nothing_behind(probes):
    with pytest.raises(RuntimeError):
        with probes.transaction() as conn, conn.cursor() as cur:
            cur.execute("insert into probe (id, etag, name)"
                    " values (gen_random_uuid(), 'e', 'x')")
            raise RuntimeError("boom")

    assert probes.conn.execute("select count(*) from probe").fetchone()[0] == 0


def test_the_connection_says_it_is_in_one(probes):
    with probes.transaction():
        assert probes.conn.info.transaction_status != TransactionStatus.IDLE


def test_a_record_and_its_children_are_one_write(probes):
    probe = Probe(name="Hemingway")
    probe.marks.add("Written with it.")
    probes.probes.add(probe)

    assert probes.conn.execute("select count(*) from mark").fetchone()[0] == 1


def test_a_delete_takes_its_children_in_the_same_transaction(probes):
    probe = probes.probes.add(Probe(name="Hemingway"))
    probe.marks.add("Goes with it.")
    probe.save()

    probe.delete()
    assert probes.conn.execute("select count(*) from mark").fetchone()[0] == 0


def test_a_delete_inside_a_block_joins_it_rather_than_opening_its_own(probes):
    """The claim the manual had backwards, pinned where somebody checking it
    would look. It said a delete could not be wrapped from outside, which is
    the sort of claim somebody designs around — a flag column rather than
    deleting rows at all. A delete enlists in a caller's block the way every
    other write does, so the rollback puts the row and its children back."""
    probe = probes.probes.add(Probe(name="Hemingway"))
    probe.marks.add("Goes with it.")
    probe.save()

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.delete()
            # Gone inside the block, which is what makes the rollback the point.
            assert probes.probes.count() == 0
            raise RuntimeError("thought better of it")

    assert probes.probes.by_id(probe.id).name == "Hemingway"
    assert probes.conn.execute("select count(*) from mark").fetchone()[0] == 1


def test_the_store_takes_the_connection_to_autocommit(probes):
    """However it arrived. `transaction()` decides whether to open one by asking
    the connection whether it is already in a transaction, and that question
    only has a truthful answer under autocommit."""
    assert probes.conn.autocommit is True


def test_a_connection_already_in_a_transaction_is_refused(postgresql):
    postgresql.autocommit = False
    postgresql.execute("select 1")  # psycopg opens one, implicitly
    with pytest.raises(RuntimeError) as raised:
        Store(postgresql)
    assert "commit or roll back" in str(raised.value).lower()


def test_writes_after_a_read_are_committed(probes):
    """The one that was missing. A read leaves psycopg holding an implicit
    transaction when autocommit is off; joining that and never committing loses
    every write that follows, and only another connection can tell."""
    probes.probes.add(Probe(name="one"))
    probes.probes.count()  # any read at all
    probes.probes.add(Probe(name="two"))
    probes.probes.add(Probe(name="three"))

    with onlooker(probes) as other:
        seen = sorted(r[0] for r in other.execute("select name from probe"))
    assert seen == ["one", "three", "two"]


def test_a_rollback_after_a_read_still_rolls_back(probes):
    probe = probes.probes.add(Probe(name="before"))
    probes.probes.count()  # any read at all

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.name = "after"
            probe.save()
            raise RuntimeError("boom")

    with onlooker(probes) as other:
        stored = other.execute("select name from probe").fetchone()[0]
    assert stored == "before"


def test_an_inner_transaction_joins_rather_than_nests(probes):
    # DSQL has no SAVEPOINT, so an inner one could only lie about what a
    # rollback would undo. It enlists in the outer instead.
    with pytest.raises(RuntimeError):
        with probes.transaction():
            with probes.transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    "insert into probe (id, etag, name)"
                    " values (gen_random_uuid(), 'e', 'x')"
                )
            # The inner block ended without committing anything.
            raise RuntimeError("boom")

    assert probes.conn.execute("select count(*) from probe").fetchone()[0] == 0


#
# What a rollback leaves in your hands
#


def test_a_rollback_leaves_the_queued_children_where_they_were(probes):
    """The loss this section exists for, and the one re-reading cannot fix. A
    queued child has no row to be fetched again from — it only ever existed on
    the object — so a write that tidied up and then rolled back took the note
    with it, and the caller who caught the exception and ran the work again
    wrote the parent with the note silently missing."""
    probe = probes.probes.add(Probe(name="before"))
    probe.marks.add("Explains the change.")

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.save()
            raise RuntimeError("something later in the block failed")

    assert [mark.body for mark in probe.marks] == ["Explains the change."]
    assert probes.probes.by_id(probe.id).marks.count() == 0


def test_a_rollback_leaves_the_etag_the_row_still_carries(probes):
    """The other half. A record stamped with a token that never committed
    refuses its own next save, against a row nobody else has touched."""
    probe = probes.probes.add(Probe(name="before"))
    was = probe.etag

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.name = "after"
            probe.save()
            raise RuntimeError("boom")

    assert probe.etag == was
    # And so the next save is an ordinary one rather than RecordHasChanged.
    probe.save()
    assert probes.probes.by_id(probe.id).name == "after"


def test_the_work_can_simply_be_run_again(probes):
    """Which is the point of all of it: catch, re-read, run the same function a
    second time, and the second run lands whole."""
    probe = probes.probes.add(Probe(name="before"))

    def promote(record, fail):
        record.name = "after"
        record.marks.add("Promoted.")
        with probes.transaction():
            record.save()
            if fail:
                raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        promote(probe, fail=True)
    promote(probes.probes.by_id(probe.id), fail=False)

    again = probes.probes.by_id(probe.id)
    assert again.name == "after"
    assert [mark.body for mark in again.marks] == ["Promoted."]


def test_a_child_queued_inside_the_block_is_not_written_twice(probes):
    """The other shape of the same story. Putting the children back must not
    hand a second save the same child again — `_settle` runs for a write that
    committed, and only a rollback puts anything back."""
    probe = probes.probes.add(Probe(name="before"))

    with probes.transaction():
        probe.marks.add("Queued inside.")
        probe.save()
        probe.name = "changed again"
        probe.save()

    assert probes.probes.by_id(probe.id).marks.count() == 1


#
# after_commit
#


def test_after_commit_waits_for_the_block(probes):
    """The confirmation email. It cannot go inside the write, which dray
    replays, and it cannot go after the `with`, because only dray knows whether
    the commit happened."""
    sent = []

    with probes.transaction():
        probes.probes.add(Probe(name="one"))
        probes.after_commit(lambda: sent.append("mail"))
        assert sent == []

    assert sent == ["mail"]


def test_after_commit_does_not_happen_when_the_block_rolls_back(probes):
    sent = []

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probes.probes.add(Probe(name="one"))
            probes.after_commit(lambda: sent.append("mail"))
            raise RuntimeError("boom")

    assert sent == []


def test_after_commit_outside_a_block_has_nothing_to_wait_for(probes):
    """One promise rather than two behaviours: this runs when the rows are
    durable, and outside a block they already are."""
    sent = []
    probes.after_commit(lambda: sent.append("mail"))
    assert sent == ["mail"]


def test_a_handler_may_write_because_the_transaction_has_closed(probes):
    """It runs after the block has ended, so its own write is a transaction of
    its own rather than an enlistment in one that has already committed."""
    with probes.transaction():
        probes.probes.add(Probe(name="one"))
        probes.after_commit(
            lambda: probes.probes.add(Probe(name="written by the handler"))
        )

    assert sorted(p.name for p in probes.probes.find()) == [
        "one",
        "written by the handler",
    ]


def test_neither_queue_survives_into_the_next_block(probes):
    """Both are emptied on the way out whichever way it went, or the next block
    on this store inherits the tidying up from the last one."""
    sent = []

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probes.after_commit(lambda: sent.append("first"))
            raise RuntimeError("boom")

    with probes.transaction():
        probes.after_commit(lambda: sent.append("second"))

    assert sent == ["second"]


def test_two_saves_in_one_block_leave_the_etag_the_record_arrived_with(probes):
    """Unwinding is the reverse of doing. Two saves leave two undos, the first
    holding the etag the record came in with and the second holding the one the
    first save minted — so running them forwards finishes on the intermediate
    value, which is a token no row ever carried either."""
    probe = probes.probes.add(Probe(name="before"))
    arrived = probe.etag

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.name = "one"
            probe.save()
            probe.name = "two"
            probe.save()
            raise RuntimeError("boom")

    assert probe.etag == arrived
    probe.save()
    assert probes.probes.by_id(probe.id).name == "two"


def test_a_child_queued_after_a_save_in_the_block_survives_the_rollback(probes):
    """The undo holds what was queued when the save ran, and by the time it
    runs the queue may have grown. Putting the snapshot back over the top of
    that discards exactly the kind of unwritten child it exists to protect."""
    probe = probes.probes.add(Probe(name="before"))
    probe.marks.add("Written before the block.")
    probe.save()

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.save()
            probe.marks.add("Queued after the save.")
            raise RuntimeError("boom")

    assert [mark.body for mark in probe.marks._pending()] == [
        "Queued after the save."
    ]


def test_a_child_queued_between_two_saves_in_a_block_comes_back_too(probes):
    """Two saves, two snapshots, and the answer is everything that was queued
    and never landed — in the order it was queued, which is why the undos run
    backwards and each merges ahead of what it finds."""
    probe = probes.probes.add(Probe(name="before"))
    probe.marks.add("Queued before the block.")

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probe.save()
            probe.marks.add("Queued between the saves.")
            probe.save()
            raise RuntimeError("boom")

    assert [mark.body for mark in probe.marks._pending()] == [
        "Queued before the block.",
        "Queued between the saves.",
    ]
    # And running the work again writes both, once each.
    probe.save()
    assert probes.probes.by_id(probe.id).marks.count() == 2


#
# A commit DSQL refuses, in a block the caller opened
#


def test_a_conflict_after_the_commit_is_not_called_a_refused_commit(
    probes, monkeypatch
):
    """`CommitRefused` tells the caller to run the work again, so it must only
    ever mean the commit was refused. The handlers run after the rows are
    durable and inside the same block, so one of them conflicting — reaching
    past dray to the connection, where nothing retries — would otherwise be
    reported as a refusal and the work would be written a second time."""
    from contextlib import contextmanager

    from dray.store import Store

    real = Store._transacting

    @contextmanager
    def failing_late(self):
        with real(self) as conn:
            yield conn
        # After the block and only for the outermost one — the depth is back to
        # nought by here, where an inner enlistment still leaves it at one. A
        # write inside the block enlists through this same function, and firing
        # there would be a failure before the commit rather than after it.
        if not self._depth:
            raise psycopg.errors.SerializationFailure("could not serialize")

    monkeypatch.setattr(Store, "_transacting", failing_late)

    with pytest.raises(psycopg.errors.SerializationFailure):
        with probes.transaction():
            probes.probes.add(Probe(name="one"))

    # And it committed, which is the whole reason it must not be replayed.
    assert probes.probes.count() == 1


def test_the_records_are_put_back_when_the_commit_is_what_failed(probes):
    """The path nothing else here reaches. Every other rollback test raises
    from inside the body; this one lets the body succeed and takes the
    transaction away at the commit, with a constraint that only fires there.
    What arrives is psycopg's — a deferred unique violation is nobody's to
    replay — and the undo has to run just the same."""
    probes.conn.execute(
        "alter table probe add constraint probe_name_once unique (name)"
        " deferrable initially deferred"
    )
    probes.probes.add(Probe(name="taken"))

    probe = probes.probes.add(Probe(name="fine"))
    was = probe.etag
    probe.marks.add("Explains the change.")

    with pytest.raises(psycopg.errors.UniqueViolation):
        with probes.transaction():
            probe.name = "taken"
            probe.save()

    assert probe.etag == was
    assert [mark.body for mark in probe.marks._pending()] == [
        "Explains the change."
    ]


def test_a_failed_commit_runs_no_after_commit_handler(probes):
    probes.conn.execute(
        "alter table probe add constraint probe_name_once unique (name)"
        " deferrable initially deferred"
    )
    probes.probes.add(Probe(name="taken"))
    sent = []

    with pytest.raises(psycopg.errors.UniqueViolation):
        with probes.transaction():
            probes.probes.add(Probe(name="taken"))
            probes.after_commit(lambda: sent.append("mail"))

    assert sent == []


#
# A block dray did not open
#


def test_a_block_opened_on_the_connection_itself_is_refused(probes):
    """It used to be joined, which is what made a write in one unsafe: dray can
    see that its tidying must wait and never learns whether the commit
    happened, so the etag is not put back, the queued children are gone, and
    `after_commit` fires for rows that rolled back."""
    probe = probes.probes.add(Probe(name="before"))

    with probes.conn.transaction():
        with pytest.raises(RuntimeError, match="dray did not open"):
            probe.save()


def test_the_store_still_works_after_the_raw_block_ends(probes):
    """Refused rather than poisoned — the next write through the store is
    ordinary."""
    with probes.conn.transaction():
        pass

    probes.probes.add(Probe(name="after"))
    assert probes.probes.count() == 1


def test_a_service_function_cannot_tell_whether_it_was_wrapped(probes):
    """Which is the whole reason `after_commit` exists. The same function, run
    both ways: on its own the save has committed by the time the mail would go,
    and inside a block it has committed nothing at all."""
    sent = []

    def cancel(probe):
        probe.name = "cancelled"
        probe.save()
        probes.after_commit(lambda: sent.append(probe.name))

    on_its_own = probes.probes.add(Probe(name="one"))
    cancel(on_its_own)
    assert sent == ["cancelled"], "nothing was holding it, so it runs now"

    wrapped = probes.probes.add(Probe(name="two"))
    with pytest.raises(RuntimeError):
        with probes.transaction():
            cancel(wrapped)
            assert sent == ["cancelled"], "still waiting on the block"
            raise RuntimeError("boom")

    assert sent == ["cancelled"], "and the block rolled back, so it never runs"


def test_the_page_is_right_that_you_can_just_put_it_after_the_with(probes):
    """The other half of the same story, and the one the page used to deny: a
    refused commit raises, so a caller who owns the block needs nothing from
    dray to know whether the rows landed."""
    reached = []

    with pytest.raises(RuntimeError):
        with probes.transaction():
            probes.probes.add(Probe(name="one"))
            raise RuntimeError("boom")
        reached.append("after the with")

    assert reached == []

    with probes.transaction():
        probes.probes.add(Probe(name="two"))
    reached.append("after the with")

    assert reached == ["after the with"]


def test_a_commit_refused_inside_a_block_puts_the_records_back(probes, monkeypatch):
    """The refusal raised where DSQL raises it — as the transaction the block
    owns commits — rather than after it, which is where the translation test
    above injects one and so cannot see any of this. It has to arrive as
    `CommitRefused`, and the record has to be re-runnable afterwards."""
    from contextlib import contextmanager

    from dray.store import Store

    real = Store._transacting
    armed = []

    @contextmanager
    def refusing(self):
        with real(self) as conn:
            yield conn
            # Inside, so the transaction really rolls back — and only for the
            # outermost block, which is where a commit happens. Raising after
            # the block would commit the rows and then claim it had not.
            # `armed` so that setting the test up does not trip it: an ordinary
            # `add` is also a block at depth one, being its own.
            if armed and self._depth == 1:
                raise psycopg.errors.SerializationFailure("could not serialize")

    monkeypatch.setattr(Store, "_transacting", refusing)

    probe = probes.probes.add(Probe(name="before"))
    was = probe.etag
    probe.marks.add("Explains the change.")
    armed.append(True)

    with pytest.raises(CommitRefused):
        with probes.transaction():
            probe.name = "after"
            probe.save()

    assert probe.etag == was
    assert [mark.body for mark in probe.marks._pending()] == [
        "Explains the change."
    ]


def test_an_enlisted_write_is_not_replayed(probes, monkeypatch):
    """`@retrying` replays a write that owns its transaction end to end. Inside
    a caller's block it owns nothing, and replaying redoes the statements
    against a transaction the refusal has already aborted — so the caller would
    get `InFailedSqlTransaction` from the second attempt rather than the
    refusal from the first."""
    from dray.collection import Collection

    tries = []

    def counting(self, *args, **kwargs):
        # Refused before it writes anything, so a replay has nothing to trip
        # over — what is being counted is how many times it is attempted.
        tries.append(1)
        raise psycopg.errors.SerializationFailure("could not serialize")

    monkeypatch.setattr(
        Collection, "_commit_batch", retrying(counting)
    )

    # `CommitRefused` rather than psycopg's, because the refusal travelled up
    # to the block that owns the commit and was named there. That is the whole
    # point of not replaying it here.
    with pytest.raises(CommitRefused):
        with probes.transaction():
            probes.probes.add(Probe(name="one"))

    assert tries == [1], "enlisted, so it must not be replayed"

    tries.clear()
    with pytest.raises(ConcurrencyExhausted):
        probes.probes.add(Probe(name="two"))

    assert len(tries) == 5, "on its own account it owns the transaction"


def test_a_wait_between_replays_grows_rather_than_stepping(probes, monkeypatch):
    """The wait used to go up by the same 50 ms every turn, which backs a
    losing writer off by an amount that never catches up with the crowd it is
    losing to. Eight writers on one row spent about three attempts on every
    unit of work and four or five in eighty gave up altogether — a
    `ConcurrencyExhausted` out of a service function that had done nothing
    wrong. Doubling the wait took the give-ups to none.

    Local PostgreSQL cannot show any of that: it takes row locks, so a second
    writer waits rather than being refused, no commit is ever rejected and the
    backoff is never reached. So the refusal is raised by hand and the wait is
    read off `time.sleep` — and the numbers themselves are not asserted, so
    tuning them later is not a test change.
    """
    from types import SimpleNamespace

    from dray import store as dray_store
    from dray.collection import Collection

    waits: list[float] = []

    def always_refuses(self, *args, **kwargs):
        raise psycopg.errors.SerializationFailure("as DSQL says no")

    # The ceiling of each wait rather than a draw from under it. The wait is
    # jittered on purpose, so two samples say nothing about which turn they
    # came from; pinning the random part to the top of its range is what leaves
    # the growth there to be read.
    monkeypatch.setattr(
        dray_store, "random", SimpleNamespace(uniform=lambda low, high: high)
    )
    monkeypatch.setattr(
        dray_store, "time", SimpleNamespace(sleep=waits.append)
    )
    monkeypatch.setattr(Collection, "_commit_batch", retrying(always_refuses))

    with pytest.raises(ConcurrencyExhausted):
        probes.probes.add(Probe(name="one"))

    assert len(waits) == ATTEMPTS - 1, "a wait between attempts, not after one"
    assert all(b > a for a, b in zip(waits, waits[1:])), waits

    # Growth rather than a step, which is the whole of the change: an even step
    # rises too, and it is the gaps that tell the two apart.
    gaps = [b - a for a, b in zip(waits, waits[1:])]
    assert all(b > a for a, b in zip(gaps, gaps[1:])), waits


def refusing(times, monkeypatch):
    """A write path that says no the first `times` it is asked and then does
    the work.

    Nothing local can produce a real refusal: PostgreSQL takes a row lock, so a
    second writer waits its turn and every commit succeeds. So the refusal is
    raised where DSQL raises one — inside the write, as `SerializationFailure`
    — and what is being tested is everything that happens after it.
    """
    from dray.collection import Collection

    real = Collection._commit_batch
    refused = iter([True] * times)

    def refusing_at_first(self, *args, **kwargs):
        if next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(
        Collection, "_commit_batch", retrying(refusing_at_first)
    )


def test_a_refused_block_runs_the_callers_function_again(probes, monkeypatch):
    """dray replays the writes it owns and stops at a block somebody else
    opened, which left every service writing the same loop by hand — and a
    service that wrote it around the block rather than around the function
    re-ran a `with` whose queued children had already been written or already
    been discarded, so a note went in twice or went missing with nothing said.
    `@replaying` is that loop, on the function, where the record is read again
    and the work built again."""
    runs = []
    refusing(2, monkeypatch)

    @replaying
    def rename(store, name):
        runs.append(name)
        with store.transaction():
            store.probes.add(Probe(name=name))

    rename(probes, "settled")

    assert len(runs) == 3, "twice refused, and the third landed"
    assert [probe.name for probe in probes.probes.find()] == ["settled"]


def test_a_function_that_keeps_conflicting_gives_up_by_its_own_name(
    probes, monkeypatch
):
    """`ConcurrencyExhausted` says *replayed until the attempts ran out*, and it
    is the same name dray's own replay ends at — a caller who set the count is
    still told the same thing. The function is named because the loop is no
    longer written at the call site, so nothing else in the traceback says which
    piece of work gave up.

    The last refusal is kept as the cause. Every one before it was swallowed by
    a replay the caller asked for, so without it there is nothing in the chain
    saying a commit was ever refused."""
    runs = []
    refusing(99, monkeypatch)

    @replaying(3)
    def rename(store, name):
        runs.append(name)
        with store.transaction():
            store.probes.add(Probe(name=name))

    with pytest.raises(ConcurrencyExhausted) as raised:
        rename(probes, "never lands")

    assert "rename conflicted 3 times" in str(raised.value)
    assert isinstance(raised.value.__cause__, CommitRefused)
    assert len(runs) == 3, "the count is the caller's, not dray's five"
    assert probes.probes.count() == 0


def test_a_function_inside_somebody_elses_block_is_not_replayed(
    probes, monkeypatch
):
    """The same reason `@retrying` refuses an enlisted write, one level up.
    Inside a block the caller opened, the function owns nothing: the refusal has
    already aborted that transaction with every statement in it spent, so
    running the work again sends statements to a transaction PostgreSQL has
    stopped accepting — and the wait before it sleeps inside a block ageing
    against DSQL's five minutes.

    So the refusal goes up to whoever opened the block, which is the only level
    that can run the whole of it again."""
    runs = []
    refusing(99, monkeypatch)

    @replaying
    def rename(store, name):
        runs.append(name)
        with store.transaction():
            store.probes.add(Probe(name=name))

    with pytest.raises(CommitRefused):
        with probes.transaction():
            rename(probes, "one")

    assert runs == ["one"], "enlisted, so it must not be replayed"


def test_a_save_that_ran_out_of_drays_own_attempts_is_run_again_too(
    probes, monkeypatch
):
    """A function whose writes are ordinary saves never opens a block, so what
    reaches the caller is `ConcurrencyExhausted` rather than `CommitRefused` —
    dray having already replayed the save five times on its own account. Both
    mean the same thing to whoever wrote the function, and the depth is what a
    caller is choosing here: eight writers on one row exhaust `ATTEMPTS`
    regularly, and the answer to that is the caller's own attempts rather than
    a number in dray nobody asked for."""
    from types import SimpleNamespace

    from dray import store as dray_store

    runs = []
    refusing(ATTEMPTS, monkeypatch)
    # dray's four waits and the caller's one, which is about a second of
    # sleeping to establish that two counts multiply rather than that either
    # of them waits.
    monkeypatch.setattr(
        dray_store, "time", SimpleNamespace(sleep=lambda seconds: None)
    )

    @replaying
    def rename(store, name):
        runs.append(name)
        store.probes.add(Probe(name=name))

    rename(probes, "settled")

    assert len(runs) == 2, "dray's five, and then the caller's second run"
    assert [probe.name for probe in probes.probes.find()] == ["settled"]


def test_a_count_that_would_never_run_the_function_is_refused():
    """`@replaying(0)` is a decorator that quietly does nothing and hands back
    `None`, which is the shape of a bug nobody would look for in a decorator
    that says it replays."""
    with pytest.raises(ValueError):
        replaying(0)


def test_one_failing_handler_does_not_stop_the_others(probes):
    """The handlers are independent by construction — three cancellations are
    three guests — so one raising says nothing about the rest. Stopping there
    would invent a dependency between them that the caller never wrote."""
    ran = []

    with pytest.raises(AfterCommitFailed) as raised:
        with probes.transaction():
            probes.probes.add(Probe(name="one"))
            probes.after_commit(lambda: ran.append("first"))
            probes.after_commit(lambda: 1 / 0)
            probes.after_commit(lambda: ran.append("third"))

    assert ran == ["first", "third"]
    assert [type(f) for f in raised.value.failures] == [ZeroDivisionError]
    assert isinstance(raised.value.__cause__, ZeroDivisionError)


def test_a_failing_handler_does_not_mean_the_write_was_lost(probes):
    """The reason it has a name of its own. Everything else out of this block
    means the work did not land; this one means it did, and running it again
    would write it twice."""
    with pytest.raises(AfterCommitFailed, match="do not run the work again"):
        with probes.transaction():
            probes.probes.add(Probe(name="one"))
            probes.after_commit(lambda: 1 / 0)

    assert probes.probes.count() == 1


def test_every_failure_is_carried_rather_than_the_first(probes):
    ran = []

    with pytest.raises(AfterCommitFailed) as raised:
        with probes.transaction():
            probes.probes.add(Probe(name="one"))
            probes.after_commit(lambda: 1 / 0)
            probes.after_commit(lambda: ran.append("between"))
            probes.after_commit(lambda: {}["missing"])

    assert ran == ["between"]
    assert [type(f) for f in raised.value.failures] == [
        ZeroDivisionError,
        KeyError,
    ]


def test_a_handler_raising_with_no_block_open_is_named_the_same_way(probes):
    """With nothing to wait for the handler ran where it stood and its own
    exception came out raw, so one lambda meant `AfterCommitFailed` from a
    `with` and a `ZeroDivisionError` from a bare save. Which is two behaviours
    to remember, in the one call that exists because its caller cannot see
    whether a block is open above it."""
    with pytest.raises(
        AfterCommitFailed, match="do not run the work again"
    ) as raised:
        probes.after_commit(lambda: 1 / 0)

    assert [type(f) for f in raised.value.failures] == [ZeroDivisionError]
    assert isinstance(raised.value.__cause__, ZeroDivisionError)


#
# A record that says what happens once it has landed
#
# The same moment as the queue above, reached from the class rather than from a
# service function. Everything it promises is the store's promise; what has to
# be got right here is where the write registers it.
#

SENT: list[str] = []


@record(table="booking", collection="bookings")
class Booking:
    status: str = field(default="held")

    @after_commit
    def tell_the_kitchen(self) -> None:
        SENT.append(self.status)


@pytest.fixture
def kitchen(probes):
    """A store with the booking table, and nothing left over from the last
    test's handlers."""
    SENT.clear()
    probes.create(Booking)
    return probes


def test_a_record_is_told_when_another_connection_can_see_its_row(probes):
    """The whole reason it waits at all. A job queued from inside the write is
    a race with a worker that is not waiting for you — it looks the record up
    on its own connection and finds it absent — so the handler has to run late
    enough that somebody else can already see the row."""
    seen = []

    @record(table="landing", collection="landings")
    class Landing:
        name: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            with onlooker(probes) as other:
                seen.append(
                    other.execute("select count(*) from landing").fetchone()[0]
                )

    probes.create(Landing)
    probes.landings.add(Landing(name="one"))

    assert seen == [1]


def test_it_waits_for_the_outermost_block_rather_than_the_save(kitchen):
    """A save inside somebody's block has committed nothing, and an inner block
    is not a second transaction — there is one commit and the handler belongs
    to it."""
    with kitchen.transaction():
        kitchen.bookings.add(Booking())
        assert SENT == [], "nothing is durable until the block ends"

    assert SENT == ["held"]

    with kitchen.transaction():
        with kitchen.transaction():
            kitchen.bookings.add(Booking(status="second"))
        assert SENT == ["held"], "leaving the inner block commits nothing"

    assert SENT == ["held", "second"]


def test_a_block_that_rolls_back_never_tells_it_and_can_be_run_again(kitchen):
    """The queue is dropped rather than kept, and both queues are emptied on the
    way out whichever way it went — so the second run of the work registers the
    handler against its own commit rather than inheriting the failed attempt's
    and sending the job twice."""
    booking = Booking()

    with pytest.raises(RuntimeError):
        with kitchen.transaction():
            kitchen.bookings.add(booking)
            raise RuntimeError("the second half of the work failed")

    assert SENT == []

    with kitchen.transaction():
        kitchen.bookings.add(booking)

    assert SENT == ["held"]


def test_a_replayed_write_tells_it_once(kitchen, monkeypatch):
    """The discipline this hook needs, and the opposite of `before_delete`'s.
    DSQL refuses a commit that raced another writer and dray replays the whole
    write; registered inside that replay, a handler would enqueue its job once
    per attempt and a confirmation would go out five times for one save."""
    from dray.collection import Collection

    booking = kitchen.bookings.add(Booking())
    SENT.clear()

    real = Collection._commit_batch
    refused = iter([True])

    def refusing_once(self, *args, **kwargs):
        real(self, *args, **kwargs)
        if next(refused, False):
            raise psycopg.errors.SerializationFailure("as DSQL says no")

    monkeypatch.setattr(Collection, "_commit_batch", retrying(refusing_once))
    booking.status = "confirmed"
    booking.save()

    assert SENT == ["confirmed"]
    assert kitchen.bookings.by_id(booking.id).status == "confirmed"


def test_two_saves_are_two_runs_and_both_read_the_record_as_it_is(kitchen):
    """Decided rather than left to fall out of the implementation: this is about
    the write rather than about the record, and two saves are two writes. What
    a deferred pair sees is the record now — not what it held when the save
    happened — because the record is the same object and nothing snapshots it."""
    booking = kitchen.bookings.add(Booking())
    assert SENT == ["held"], "outside a block there is nothing left to wait for"

    booking.status = "confirmed"
    booking.save()
    assert SENT == ["held", "confirmed"]

    with kitchen.transaction():
        booking.status = "seated"
        booking.save()
        booking.status = "paid"
        booking.save()

    assert SENT == ["held", "confirmed", "paid", "paid"]


def test_an_edit_that_lost_to_another_is_never_announced(kitchen):
    """Two people edit one booking and the second save is refused by the guard
    at the statement — after the write path has done everything but land it. A
    handler is the announcement of a durable write, so the kitchen must hear
    about the edit that won and nothing about the one that did not; a job
    queued for the refused save would send a worker to act on a status the row
    never held."""
    booking = kitchen.bookings.add(Booking())

    mine = kitchen.bookings.by_id(booking.id)
    shown = mine.etag

    theirs = kitchen.bookings.by_id(booking.id)
    theirs.status = "confirmed"
    theirs.save()

    SENT.clear()
    # The record in hand still matches what this process read, so the in-hand
    # comparison passes and the refusal comes from the update statement itself
    # — the write got as far as a write can and still not land.
    mine.status = "cancelled"
    with pytest.raises(RecordHasChanged):
        mine.save(etag=shown)

    assert SENT == []
    assert kitchen.bookings.by_id(booking.id).status == "confirmed"


def test_a_handler_that_raises_says_the_rows_are_committed(kitchen):
    """`AfterCommitFailed` is the name for "it landed, do not run it again", and
    this needs it more than a lambda a service function wrote does: whoever
    calls `add` may never have read the class, so the domain's own exception
    coming out of it would read as a write that failed."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        name: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            raise RuntimeError("the kitchen is not answering")

    kitchen.create(Ticket)

    with pytest.raises(
        AfterCommitFailed, match="do not run the work again"
    ) as raised:
        kitchen.tickets.add(Ticket(name="one"))

    assert [type(f) for f in raised.value.failures] == [RuntimeError]
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert kitchen.tickets.count() == 1

    # And from the `with`, when there was a block to wait for. Same name, same
    # thing to do about it.
    with pytest.raises(AfterCommitFailed):
        with kitchen.transaction():
            kitchen.tickets.add(Ticket(name="two"))

    assert kitchen.tickets.count() == 2


def test_a_hand_registered_handler_fails_the_way_a_record_hook_does(kitchen):
    """The page says a record's handler failing is the same name and the same
    advice as one you registered yourself, and for a while it was not: the same
    body raised `RuntimeError` by hand outside a block and `AfterCommitFailed`
    through the decorator. Whichever way it was written, and whoever owns the
    transaction, the thing a caller has to be told is that the rows landed."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        name: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            raise RuntimeError("the kitchen is not answering")

    def tell_the_kitchen() -> None:
        raise RuntimeError("the kitchen is not answering")

    kitchen.create(Ticket)

    with pytest.raises(AfterCommitFailed):
        kitchen.after_commit(tell_the_kitchen)

    with pytest.raises(AfterCommitFailed):
        with kitchen.transaction():
            kitchen.after_commit(tell_the_kitchen)

    with pytest.raises(AfterCommitFailed):
        kitchen.tickets.add(Ticket(name="one"))

    with pytest.raises(AfterCommitFailed):
        with kitchen.transaction():
            kitchen.tickets.add(Ticket(name="two"))

    assert kitchen.tickets.count() == 2


def test_one_record_failing_does_not_stop_the_records_behind_it(kitchen):
    """They are independent of each other — three bookings are three tables —
    so all of them run and every failure comes back. Outside a block the
    handlers run where they stand, and registering them one at a time would
    have stopped at the first, which is a different promise from the one the
    same set gets inside a block."""

    @record(table="ticket", collection="tickets")
    class Ticket:
        name: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            if self.name == "bad":
                raise RuntimeError(self.name)
            SENT.append(self.name)

    kitchen.create(Ticket)

    with pytest.raises(AfterCommitFailed) as raised:
        kitchen.tickets.add_all(
            [Ticket(name="one"), Ticket(name="bad"), Ticket(name="two")]
        )

    assert SENT == ["one", "two"]
    assert [type(f) for f in raised.value.failures] == [RuntimeError]
    assert kitchen.tickets.count() == 3


def test_what_a_handler_writes_is_a_transaction_of_its_own(kitchen):
    """It runs once the transaction has closed, so the record is still attached
    and readable and anything it writes is a fresh write. Which means that write
    can fail on its own account — and it arrives as `AfterCommitFailed`, because
    by then the save that ran the handler has landed."""

    # Keyed by the name, so queuing the same job twice is a clash the table
    # refuses — which is the cheapest way to have the handler's own write fail.
    @record(table="job", collection="jobs", key="name")
    class Job:
        name: str = field(default="")

    @record(table="ticket", collection="tickets")
    class Ticket:
        name: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            kitchen.jobs.add(Job(name=self.name))

    kitchen.create(Job, Ticket)

    with kitchen.transaction():
        kitchen.tickets.add(Ticket(name="one"))

    assert [job.name for job in kitchen.jobs.find()] == ["one"]

    # The same job again, which the key refuses. The ticket is written either
    # way, which is the whole difference this name carries.
    with pytest.raises(AfterCommitFailed) as raised:
        kitchen.tickets.add(Ticket(name="one"))

    assert [type(f).__name__ for f in raised.value.failures] == ["DuplicateRecord"]
    assert kitchen.tickets.count() == 2


def test_a_set_that_splits_is_told_once_every_row_has_landed(kitchen, monkeypatch):
    """A set above the row ceiling is several transactions. Handled per
    transaction, a handler that raised would take the chunks behind it with it
    — records nobody wrote is a worse thing to go wrong than handlers that ran
    late — so they run together once the last chunk is durable."""
    import sys

    seen = []

    @record(table="ledger", collection="ledgers")
    class Ledger:
        name: str = field(default="")

        @after_commit
        def count_what_landed(self) -> None:
            with onlooker(kitchen) as other:
                seen.append(
                    other.execute("select count(*) from ledger").fetchone()[0]
                )

    kitchen.create(Ledger)
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    kitchen.ledgers.add_all([Ledger(name=f"n{n}") for n in range(5)])

    assert seen == [5, 5, 5, 5, 5]


def test_a_handler_reaches_the_store_the_write_went_through(kitchen):
    """A handler is called with `self` and nothing else, so the only store a
    rule on the class could reach was one it closed over when the module was
    written — which a service handing out a store per request has not got. So a
    rule that had to read another record once the write was durable went back
    into a service function that any call site can walk past."""
    seen = []

    @record(table="cover", collection="covers", key="name")
    class Cover:
        name: str = field(default="")
        party: int = field(default=0)

    @record(table="sitting", collection="sittings")
    class Sitting:
        cover: str = field(default="")

        @after_commit
        def tell_the_kitchen(self) -> None:
            store = self.store
            # Another record, which is the whole point — and this table, which
            # says the rows the handler is announcing are the committed ones
            # rather than something the handler can only see from inside.
            seen.append(store.covers.by_id(self.cover).party)
            seen.append(store.sittings.count())
            seen.append(store is kitchen)

    kitchen.create(Cover, Sitting)
    kitchen.covers.add(Cover(name="table four", party=6))

    kitchen.sittings.add(Sitting(cover="table four"))
    assert seen == [6, 1, True]

    # And where somebody owns the block, which is the case where the store the
    # handler reaches has just left a transaction rather than never been in one.
    seen.clear()
    with kitchen.transaction():
        kitchen.sittings.add(Sitting(cover="table four"))
        assert seen == []

    assert seen == [6, 2, True]


def test_a_queued_child_reaches_the_store_its_parents_write_went_through(kitchen):
    """A child has no save of its own — it is told when the write that carried
    it lands — and it is attached by that write like anything else, so the store
    is there for it too. It was worth checking rather than assuming: a queued
    child is the one record on the page that never went through `add`."""
    seen = []

    @record(table="rota", collection="rotas")
    class Rota:
        name: str = field(default="")

    @child(of=Rota, name="tallies", table="tally")
    class Tally:
        body: str = field(default="")

        @after_commit
        def count_what_landed(self) -> None:
            seen.append(self.store.rotas.count())

    kitchen.create(Rota, Tally)
    rota = Rota(name="Hemingway")
    rota.tallies.add("Written with it.")
    kitchen.rotas.add(rota)

    assert seen == [1]
