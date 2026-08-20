"""
Connections to hand stores, so that having one each is cheap.

A store is one connection and one thread — which is what DSQL wants, and what
nobody follows when every request pays for a fresh handshake and a fresh token.
The pool is the answer to the cost rather than a change to the shape: nothing
about a store is different for having come from one.

Tested through `Pool(an_existing_pool)`, because the other spelling builds an
`AuroraDSQLPool` and there is no cluster here. That is the same reason `Store`
takes a connection as well as making one.
"""

import threading

import pytest
from psycopg_pool import ConnectionPool

from dray import DrayError, Pool, after_commit, child, field, record
from dray.store import _connector, ready


@record(table="rider", collection="riders")
class Rider:
    family_name: str = field()
    whom: str = field(default="System")


@child(of=Rider, name="notes", table="rider_note")
class RiderNote:
    body: str = field(default="")


@pytest.fixture
def dsn(postgresql):
    """Off a live connection rather than off the process: `postgresql_proc`
    starts a server, and it is the connection fixture that makes the database
    in it. A pool pointed at one that does not exist does not fail — it retries
    until its timeout, which reads exactly like a hang."""
    info = postgresql.info
    return (
        f"host={info.host} port={info.port}"
        f" user={info.user} dbname={info.dbname}"
    )


@pytest.fixture
def pool(dsn):
    """A pool dray was handed, configured the way dray configures its own."""
    made = ConnectionPool(
        dsn, min_size=1, max_size=4, configure=ready, open=False, timeout=5
    )
    pool = Pool(made, records=[Rider, RiderNote])
    with pool.store() as store:
        store.create(Rider, RiderNote)
    yield pool
    made.close()


def test_a_store_from_the_pool_works_like_any_other(pool):
    with pool.store() as store:
        rider = store.riders.add(Rider(family_name="Hemingway"))
        assert store.riders.by_id(rider.id).family_name == "Hemingway"


def test_the_connection_goes_back_rather_than_being_closed(pool):
    """The pool owns it. A store that closed one on the way out would empty the
    pool one request at a time."""
    with pool.store() as store:
        conn = store.conn
        store.close()
        assert not conn.closed

    # And it is usable again afterwards, which is the whole point.
    with pool.store() as store:
        assert store.riders.count() == 0


def test_connections_are_reused_rather_than_made_per_store(pool):
    """Which is the whole point: a store per request was always the advice, and
    a handshake and a token per request was the reason nobody took it. The pool
    still grows to meet demand, so what matters is that it makes fewer
    connections than it hands out stores, not that it makes exactly one."""
    for _ in range(6):
        with pool.store() as store:
            store.riders.count()

    assert pool.opened().get_stats()["connections_num"] < 6


def test_the_connection_arrives_autocommit(pool):
    """dray needs it, and the pool's `configure` is where it happens once —
    when the connection is made, rather than on every checkout."""
    with pool.store() as store:
        assert store.conn.autocommit


def test_defaults_merge_rather_than_replace(pool):
    """What the job knows is said once on the pool; what the request knows is
    said on the store. Replacing would mean restating the first to add the
    second."""
    pool.defaults["whom"] = "System import"

    with pool.store() as store:
        rider = store.riders.add(Rider(family_name="Hemingway"))
        assert rider.whom == "System import"

    with pool.store(defaults={"whom": "rod"}) as store:
        rider = store.riders.add(Rider(family_name="Shelley"))
        assert rider.whom == "rod"

    # And the pool is unchanged by the request that overrode it.
    with pool.store() as store:
        rider = store.riders.add(Rider(family_name="Byron"))
        assert rider.whom == "System import"


def test_a_pool_handed_over_is_not_closed_by_dray(dsn):
    """Somebody else's to close — closing it here would take out whatever else
    is using it."""
    made = ConnectionPool(
        dsn, min_size=1, max_size=2, configure=ready, open=False, timeout=5
    )
    mine = Pool(made)
    mine.opened()
    mine.close()

    # `closed` on a psycopg pool means "not open", so this only says anything
    # because dray opened it above.
    assert not made.closed
    made.close()


def test_a_pool_needs_a_host_or_a_pool():
    with pytest.raises(TypeError, match="host"):
        Pool()


#
# The reason it exists
#


def test_threads_with_a_store_each_do_not_argue(pool):
    """The shape the guard refuses to fake. Every thread takes its own store
    and its own connection, so there is no ownership to argue about — and this
    is the line that used to be expensive."""
    made = []
    failed = []

    def work(name):
        def run():
            try:
                with pool.store() as store:
                    made.append(store.riders.add(Rider(family_name=name)).id)
            except BaseException as error:  # every failure is reported below
                failed.append(error)

        return run

    threads = [threading.Thread(target=work(f"R{n}")) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert failed == []
    assert len(made) == 4
    with pool.store() as store:
        assert store.riders.count() == 4


def test_a_transaction_in_one_thread_does_not_reach_another(pool):
    """Two stores, two connections: one rolling back takes nothing from the
    other. On a shared store this was the silent loss."""
    opened = threading.Event()
    written = threading.Event()

    def rolls_back():
        try:
            with pool.store() as store:
                with store.transaction():
                    store.riders.add(Rider(family_name="rolled back"))
                    opened.set()
                    written.wait(5)
                    raise RuntimeError("fails")
        except RuntimeError:
            pass

    def writes_meanwhile():
        opened.wait(5)
        try:
            with pool.store() as store:
                store.riders.add(Rider(family_name="kept"))
        finally:
            written.set()

    threads = [threading.Thread(target=t) for t in (rolls_back, writes_meanwhile)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    with pool.store() as store:
        assert [r.family_name for r in store.riders.find()] == ["kept"]


def test_a_store_knows_the_pool_it_came_from(pool):
    """A pool outlives a request and a store does not, so anything shared
    between requests — a short-lived read cache above all of them — has to live
    on the pool. A store with no way back to it could never reach one."""
    with pool.store() as store:
        assert store.pool is pool


def test_a_store_built_by_hand_has_no_pool(postgresql):
    from dray import Store

    store = Store(postgresql)
    assert store.pool is None


#
# Watching, which is where a pool is the only useful scope
#


def test_an_observer_on_the_pool_hears_from_every_thread(pool):
    """Every store the pool makes shares the handler, so it is called
    concurrently and dray says so rather than serialising it: making the handler
    safe to call from several threads is the caller's, exactly as it is for an
    `after_commit` handler. What matters here is that nothing is lost — four
    writers, four inserts, all four reported."""
    seen = []
    keeping = threading.Lock()

    def watch(span):
        with keeping:
            seen.append(span)

    watched = Pool(pool._pool, records=[Rider, RiderNote], observer=watch)

    def work(name):
        def run():
            with watched.store() as store:
                store.riders.add(Rider(family_name=name))

        return run

    threads = [threading.Thread(target=work(f"R{n}")) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    inserts = [
        span
        for span in seen
        if span.phase == "close"
        and span.kind == "statement"
        and span.sql.startswith("insert into rider ")
    ]
    assert len(inserts) == 4
    assert len({span.thread_ident for span in inserts}) == 4


def test_a_checkout_is_a_span_and_the_stores_whole_life_is_inside_it(pool):
    """Opened before the connection is asked for, because waiting for a free
    one is most of what a slow checkout is — and held for as long as the store,
    so a checkout that cost more than everything done through it is visible as
    such."""
    seen = []
    watched = Pool(pool._pool, records=[Rider], observer=seen.append)

    with watched.store() as store:
        store.riders.count()

    (checkout,) = [
        span
        for span in seen
        if span.phase == "close" and span.kind == "checkout"
    ]
    assert checkout.parent_id is None
    assert all(
        span.parent_id is not None
        for span in seen
        if span.kind != "checkout"
    )
    assert seen[-1].id == checkout.id


def test_a_collector_reaches_the_stores_a_fan_out_checked_out(pool):
    """The case that makes a per-store collector answer the wrong question. The
    shape dray is designed for is half a dozen questions, each on its own thread
    and its own store, so a page costs the longest rather than the sum — which
    means one request is several stores, and *how many round trips did this page
    make* is exactly that case. A collector scoped to the store the request
    started on would see its own statement and none of the ones it fanned out
    to, and would say so quietly."""
    with pool.store() as store:
        store.riders.add(Rider(family_name="Hemingway"))

    with pool.store() as store, store.watching() as seen:

        # All three inside their stores at once, held there until the last one
        # arrives. Started and joined without it, the first can finish before
        # the third begins — and CPython hands a finished thread's identity to
        # the next one, so the count below came up short on a slow machine
        # while every span it is really about was there.
        together = threading.Barrier(3)

        def ask():
            with pool.store() as mine:
                together.wait(10)
                mine.riders.count()

        threads = [threading.Thread(target=ask) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        store.riders.count()

    assert len(seen) == 4
    assert len({span.thread_ident for span in seen}) == 4


def test_a_collector_stops_at_the_end_of_the_block_for_later_checkouts_too(pool):
    """A window rather than a subscription, and the pool has to forget it as
    well — a collector left on the pool would go on catching every request the
    process served afterwards, and hold every span it caught."""
    with pool.store() as store, store.watching() as seen:
        store.riders.count()

    was = len(seen)
    with pool.store() as later:
        later.riders.count()

    assert len(seen) == was
    assert pool._watchers == ()


def test_a_store_from_a_pool_nobody_is_watching_is_not_watched(pool):
    """Off by default all the way down. A pool with no observer and no collector
    hands out stores that cost nothing to have made."""
    from dray import watching

    with pool.store() as store:
        assert store._watch is watching.UNWATCHED


#
# Transactions, which are per store and so per connection
#


def test_a_second_store_is_not_in_the_first_ones_transaction(pool):
    """A transaction belongs to a connection, and `pool.store()` hands out a
    fresh one. So work done through a second store inside somebody's block
    commits on its own account and survives their rollback — which is worth a
    test rather than a sentence, because the two reads identically at the call
    site."""
    with pool.store() as store:
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.riders.add(Rider(family_name="rolled back"))
                with pool.store() as other:
                    other.riders.add(Rider(family_name="not in the block"))
                raise RuntimeError("boom")

    with pool.store() as store:
        assert [r.family_name for r in store.riders.find()] == ["not in the block"]


def test_a_rollback_puts_the_queued_children_back_on_a_pooled_store(pool):
    """The same guarantee, through the pool. A store from a checkout keeps its
    own two queues, so this cannot be answered by the plain-store test."""
    with pool.store() as store:
        rider = store.riders.add(Rider(family_name="Hemingway"))
        was = rider.etag
        rider.notes.add("Explains the change.")

        with pytest.raises(RuntimeError):
            with store.transaction():
                rider.save()
                raise RuntimeError("boom")

        assert [n.body for n in rider.notes] == ["Explains the change."]
        assert rider.etag == was


def test_a_handler_reaches_a_checkouts_store_while_it_is_still_open(pool):
    """The case a rule on the class could not be written for at all. A handler
    is called with `self` and nothing else, so the only store it could reach was
    one it closed over when the module was written — and on a pool there is no
    such store, because a store is per request and the class was written long
    before the request. What it reaches is the checkout's own, still open: the
    hooks fire in the transaction block's `finally`, and that block is always
    the inner of the two however the `with`s are written."""
    seen = []

    @record(table="fare", collection="fares")
    class Fare:
        name: str = field(default="")

        @after_commit
        def count_what_landed(self) -> None:
            seen.append((self.store.fares.count(), self.store))

    with pool.store() as store:
        store.create(Fare)
        store.fares.add(Fare(name="one"))

    assert seen == [(1, store)]

    seen.clear()
    with pool.store() as store:
        with store.transaction():
            store.fares.add(Fare(name="two"))

    assert seen == [(2, store)]

    # And with the two on one line, which is the same nesting written the way a
    # request handler usually writes it.
    seen.clear()
    with pool.store() as store, store.transaction():
        store.fares.add(Fare(name="three"))

    assert seen == [(3, store)]


def test_the_next_checkout_does_not_inherit_a_block(pool):
    """A connection handed back mid-transaction would make the next checkout
    compute `joining` from somebody else's open block, and then commit nothing
    at all — for the whole of that request, silently."""
    with pytest.raises(RuntimeError):
        with pool.store() as store:
            with store.transaction():
                store.riders.add(Rider(family_name="rolled back"))
                raise RuntimeError("boom")

    with pool.store() as store:
        store.riders.add(Rider(family_name="written"))

    with pool.store() as store:
        assert [r.family_name for r in store.riders.find()] == ["written"]


def test_connecting_without_the_extra_names_the_extra(monkeypatch):
    """Without AWS's connector installed, `Store.connect` said
    `No module named 'aurora_dsql_psycopg'` — a package nobody typed, in no
    documentation, and not guessable back to `dray[dsql]`.

    It is the first wall somebody hits after `pip install dray`, and the whole
    of the fix is that the message says what to run. Both doors go through one
    helper so neither can drift from the other.
    """
    import builtins

    real = builtins.__import__

    def refuse(name, *rest):
        if name == "aurora_dsql_psycopg":
            # What Python raises, spelled the way Python spells it: the `name`
            # is what tells the helper which module was the missing one.
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return real(name, *rest)

    monkeypatch.setattr(builtins, "__import__", refuse)

    with pytest.raises(DrayError) as caught:
        _connector()

    said = str(caught.value)
    assert 'dray[dsql]' in said
    assert "uv add" in said and "pip install" in said
    # Chained, so the original still says which import it was.
    assert isinstance(caught.value.__cause__, ImportError)


def test_a_broken_connector_is_not_reported_as_a_missing_extra(monkeypatch):
    """The connector imports boto3 and its own core on the way in, and any of
    those failing is somebody else's problem with somebody else's answer.

    Answering it with "install `dray[dsql]`" sends a reader to check the one
    thing that is already right, and buries the name of the thing that is not.
    """
    import builtins

    real = builtins.__import__

    def refuse(name, *rest):
        if name == "aurora_dsql_psycopg":
            # The connector imports boto3 on the way in, so a missing boto3
            # surfaces here as a failure of *this* import carrying boto3's name.
            # Raised rather than provoked, because refusing boto3 itself only
            # works while nothing has imported the connector yet — and under
            # `-n auto` whether anything has is a matter of which worker got
            # which test.
            raise ModuleNotFoundError("No module named 'boto3'", name="boto3")
        return real(name, *rest)

    monkeypatch.setattr(builtins, "__import__", refuse)

    with pytest.raises(ModuleNotFoundError) as caught:
        _connector()

    assert caught.value.name == "boto3"
