"""
A store is dray's unit of concurrency, and belongs to one thread at a time.

Everything a store does goes down one connection, and a connection has one
session and one transaction. Two threads sharing one did not fail — the second
saw the first's open transaction, took it for a block somebody had opened
deliberately, joined it, and had its work rolled back by a failure it was not
part of. `add()` returned normally and the row was never there.

What is refused here is two threads *at once*. Handing a store from one thread
to another is fine, and is what a job queue does.
"""

import threading

import pytest

from dray import child, field, record
from dray.store import retrying


@record(table="runner", collection="runners")
class Runner:
    family_name: str = field()


@child(of=Runner, name="notes", table="runner_note")
class RunnerNote:
    body: str = field()


@pytest.fixture
def runners(store):
    store.create(Runner, RunnerNote)
    return store


def run(*work):
    """Every callable at once, giving back whatever each raised."""
    raised: dict[int, BaseException | None] = {}

    def wrap(n, job):
        try:
            job()
            raised[n] = None
        except BaseException as error:  # reported, not swallowed
            raised[n] = error

    threads = [
        threading.Thread(target=wrap, args=(n, job)) for n, job in enumerate(work)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    return [raised.get(n) for n in range(len(work))]


#
# The failure this exists to stop
#


def test_a_write_from_a_second_thread_is_refused_rather_than_lost(runners):
    """The whole point. Thread A holds a transaction and rolls back; thread B
    saves in the meantime. B used to return normally with nothing written —
    its row swallowed by a rollback it had no part in."""
    opened = threading.Event()
    tried = threading.Event()

    def holds_it_open():
        try:
            with runners.transaction():
                runners.runners.add(Runner(family_name="A"))
                opened.set()
                tried.wait(5)
                raise RuntimeError("and then fails, so it rolls back")
        except RuntimeError:
            pass

    def saves_meanwhile():
        opened.wait(5)
        try:
            runners.runners.add(Runner(family_name="B"))
        finally:
            tried.set()

    _, refused = run(holds_it_open, saves_meanwhile)

    assert isinstance(refused, RuntimeError)
    assert "thread" in str(refused)
    # And nothing pretended otherwise: A rolled back, B never wrote.
    assert runners.runners.count() == 0


def test_a_read_from_a_second_thread_is_refused_too(runners):
    """A read matters as much as a write. A statement run while another thread
    holds a block runs inside that block — seeing what it has not committed,
    and ageing against the five minutes it is spending."""
    opened = threading.Event()
    tried = threading.Event()

    def holds_it_open():
        with runners.transaction():
            opened.set()
            tried.wait(5)

    def reads_meanwhile():
        opened.wait(5)
        try:
            runners.runners.find(equals={"family_name": "anybody"})
        finally:
            tried.set()

    _, refused = run(holds_it_open, reads_meanwhile)
    assert isinstance(refused, RuntimeError)


def test_the_message_names_both_threads(runners):
    opened = threading.Event()
    tried = threading.Event()
    seen = {}

    def holds_it_open():
        with runners.transaction():
            seen["holder"] = threading.get_ident()
            opened.set()
            tried.wait(5)

    def saves_meanwhile():
        opened.wait(5)
        seen["other"] = threading.get_ident()
        try:
            runners.runners.add(Runner(family_name="B"))
        finally:
            tried.set()

    _, refused = run(holds_it_open, saves_meanwhile)
    assert str(seen["holder"]) in str(refused)
    assert str(seen["other"]) in str(refused)


#
# What stays allowed
#


def test_one_thread_nesting_still_joins(runners):
    """An inner block is not a second transaction — DSQL has no `SAVEPOINT` for
    it to be one with. The same condition as the refusal above, and only the
    store can tell which of the two it is looking at."""
    with runners.transaction():
        runner = runners.runners.add(Runner(family_name="Hemingway"))
        runner.notes.add("Written inside the caller's own block.")
        runner.save()

    assert runners.runners.count() == 1
    assert len(runners.runners.by_id(runner.id).notes) == 1


def test_a_store_can_be_handed_to_another_thread(runners):
    """Passing one along is fine, and is what a job queue does. What is refused
    is two threads in it at once, not a store that has seen two threads."""
    runner = runners.runners.add(Runner(family_name="Hemingway"))

    def in_a_worker():
        runners.runners.by_id(runner.id).save()

    (raised,) = run(in_a_worker)
    assert raised is None


def test_threads_with_a_store_each_do_not_interfere(postgresql_proc, runners):
    """The supported shape, and the one a pool makes cheap. Two stores, two
    connections, no ownership to argue about."""
    import psycopg

    from dray import Store

    made = []

    def own_store(name):
        def work():
            conn = psycopg.connect(
                host=postgresql_proc.host,
                port=postgresql_proc.port,
                user=postgresql_proc.user,
                dbname=runners.conn.info.dbname,
            )
            mine = Store(conn, records=[Runner, RunnerNote])
            made.append(mine.runners.add(Runner(family_name=name)).id)
            mine.close()

        return work

    raised = run(own_store("A"), own_store("B"), own_store("C"))
    assert raised == [None, None, None]
    assert runners.runners.count() == 3


def test_nothing_is_refused_when_no_block_is_open(runners):
    """Outside a transaction each statement is its own, so threads take their
    turn on the connection rather than corrupting one another. Refusing here
    would make a store useless for the hand-off above."""
    runners.runners.add(Runner(family_name="Hemingway"))

    raised = run(
        lambda: runners.runners.count(),
        lambda: runners.runners.count(),
        lambda: runners.runners.count(),
    )
    assert raised == [None, None, None]


#
# Watching, which nests within a thread and never across one
#


def test_a_span_never_has_a_parent_another_thread_opened(
    postgresql_proc, runners
):
    """The invariant that lets the parent stack be a plain list with no lock
    and no contextvars: a store is one connection and one thread, so nesting
    happens inside a thread and a `parent_id` never points out of it.

    What that gives up is deliberate. In a fan-out each worker's spans are a
    separate root, laid beside the others on one monotonic clock rather than
    hanging under the caller that started them — four roots and not one tree.
    The alternative is dray copying a context into threads it did not create,
    which is dray taking a position on how work fans out, and it is exactly the
    position a record layer should not have."""
    import psycopg

    from dray import Store

    seen = []
    keeping = threading.Lock()

    def watch(span):
        with keeping:
            seen.append(span)

    def own_store(name):
        def work():
            conn = psycopg.connect(
                host=postgresql_proc.host,
                port=postgresql_proc.port,
                user=postgresql_proc.user,
                dbname=runners.conn.info.dbname,
            )
            mine = Store(conn, records=[Runner, RunnerNote], observer=watch)
            with mine.span(f"worker {name}"):
                mine.runners.add(Runner(family_name=name))
            mine.close()

        return work

    assert run(own_store("A"), own_store("B"), own_store("C")) == [None] * 3

    whose = {span.id: span.thread_ident for span in seen}
    for span in seen:
        if span.parent_id is not None:
            assert whose[span.parent_id] == span.thread_ident

    roots = [
        span for span in seen if span.parent_id is None and span.phase == "open"
    ]
    assert {span.label for span in roots} == {"worker A", "worker B", "worker C"}


def test_threads_sharing_one_store_do_not_share_a_parent(runners):
    """The shape above is the one to write, and this is the one dray still
    allows: outside a block, statements on a connection each take their turn, so
    three threads may read through one store. A single parent stack would have
    handed one thread's statement the other's parent, and the tree would say a
    read happened inside a read it had nothing to do with."""
    from dray.watching import Watch

    seen = []
    keeping = threading.Lock()

    def watch(span):
        with keeping:
            seen.append(span)

    runners._watch = Watch(watch)
    runners.runners.add(Runner(family_name="Hemingway"))

    assert run(
        lambda: runners.runners.count(),
        lambda: runners.runners.count(),
        lambda: runners.runners.count(),
    ) == [None, None, None]

    whose = {span.id: span.thread_ident for span in seen}
    for span in seen:
        if span.parent_id is not None:
            assert whose[span.parent_id] == span.thread_ident


def test_watching_does_not_weaken_the_one_thread_rule(runners):
    """Cheap insurance. A handler is called from inside dray, part-way through
    a statement, so it is worth pinning that watching a store changes nothing
    about who is allowed to be in it."""
    from dray.watching import Watch

    runners._watch = Watch(lambda span: None)

    opened = threading.Event()
    tried = threading.Event()

    def holds_it_open():
        with runners.transaction():
            opened.set()
            tried.wait(5)

    def saves_meanwhile():
        opened.wait(5)
        try:
            runners.runners.add(Runner(family_name="B"))
        finally:
            tried.set()

    _, refused = run(holds_it_open, saves_meanwhile)
    assert isinstance(refused, RuntimeError)
    assert "thread" in str(refused)


#
# Schema, which is a write path too
#


def test_making_tables_is_replayed_when_the_schema_conflicts(store, monkeypatch):
    """DSQL refuses a commit whose schema moved underneath it — `OC001`, a
    schema conflict, arriving as an ordinary serialization failure — and two
    deployments coming up at once do that to each other. `create` was the one
    write path with no replay on it, so the second instance failed outright.

    Found by running the cluster checks at once: three of nine failed for this
    and the same nine passed one at a time."""
    import psycopg

    from dray.store import Store

    refused = iter([True, True])
    real = Store._ddl.__wrapped__

    def refusing_twice(self, statement):
        if next(refused, False):
            raise psycopg.errors.SerializationFailure(
                "schema has been updated by another transaction (OC001)"
            )
        return real(self, statement)

    monkeypatch.setattr(Store, "_ddl", retrying(refusing_twice))

    store.create(Runner)
    assert store.runners.count() == 0


def test_a_schema_that_will_not_settle_is_reported_rather_than_hidden(
    store, monkeypatch
):
    import psycopg

    from dray import ConcurrencyExhausted
    from dray.store import Store

    def always_refuses(self, statement):
        raise psycopg.errors.SerializationFailure("(OC001)")

    monkeypatch.setattr(Store, "_ddl", retrying(always_refuses))

    with pytest.raises(ConcurrencyExhausted):
        store.create(Runner)
