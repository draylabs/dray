"""
Rows kept for a moment, so the same record read twice costs one round trip.

Tested through `Pool(an_existing_pool)` where the cache has to be shared and
through a plain `Store` where it does not, because those are the two places it
can live and they are the two answers `cached_for=` has to give the same way.
Round trips are counted with `store.watching()` rather than by patching psycopg,
since that is the door the manual tells a reader to count them through.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from psycopg_pool import ConnectionPool

from dray import (
    Collection,
    Pool,
    Store,
    any_of,
    cached_for,
    child,
    collection,
    field,
    index,
    record,
)
from dray.store import ready


@record(
    table="ward",
    collection="wards",
    cached_for=30,
    indexes=[index("code", unique=True), index("region", "bed_count")],
)
class Ward:
    name: str = field()
    # Nullable, so the wards a test does not care about the code of can all
    # leave it empty: a unique index takes as many nulls as it likes.
    code: str | None = field(default=None)
    region: str = field(default="")
    bed_count: int = field(default=0)
    tags: list = field(default_factory=list, stored_in="blob")


@record(table="visit", collection="visits")
class Visit:
    purpose: str = field(default="")


@child(of=Ward, name="rounds", table="ward_round", collection="ward_rounds",
       cached_for=30)
class Round:
    body: str = field(default="")


@record(table="region", collection="regions", cached_for=30,
        indexes=[index("code", unique=True)])
class Region:
    name: str = field(default="")
    code: str | None = field(default=None)


@record(table="area", collection="areas")
class Area:
    """A record with no cache of its own, on a collection that keeps an
    answer — which is the pair that tells the two caches apart."""

    name: str = field(default="")


class Tallying:
    """A question written once and mixed into two collections, which is how a
    cache keyed by the method alone would answer for the wrong table."""

    @cached_for(300)
    def how_many(self) -> int:
        return self.count()


@collection(of=Region)
class Regions(Tallying):
    @cached_for(1800)
    def by_code(self, code: str) -> Region | None:
        return self.find_first(equals={"code": code})

    @cached_for(60)
    def named(self, names: tuple) -> list:
        return self.find(equals={"name": any_of(*names)})


@collection(of=Area)
class Areas(Tallying):
    pass


class Counting:
    """A clock a test moves by hand, so an entry can be watched expiring
    without anybody sleeping for it."""

    def __init__(self) -> None:
        self.at = 0.0

    def __call__(self) -> float:
        return self.at


@pytest.fixture
def clock():
    return Counting()


@pytest.fixture
def dsn(postgresql):
    info = postgresql.info
    return (
        f"host={info.host} port={info.port}"
        f" user={info.user} dbname={info.dbname}"
    )


@pytest.fixture
def pool(dsn, clock):
    made = ConnectionPool(
        dsn, min_size=1, max_size=6, configure=ready, open=False, timeout=5
    )
    pool = Pool(
        made, records=[Ward, Visit, Round, Region, Area], timer=clock
    )
    with pool.store() as store:
        store.create(Ward, Visit, Round, Region, Area)
    yield pool
    made.close()


@pytest.fixture
def cached(store):
    """A store built by hand, with the tables made. The README's own shape, and
    the one with no pool for a cache to live on."""
    store.create(Ward, Visit, Round, Region, Area)
    return store


def statements(seen):
    return [span for span in seen if span.phase == "close"]


def test_a_record_read_twice_is_read_from_the_database_once(pool):
    """The whole of it. `by_id` was a round trip every time it was called, so a
    page that asked three questions of one record paid for three — and a warming
    phase that fanned out across threads had nothing to hand the sequential
    phase that followed it."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Katoomba", code="KAT"))

    with pool.store() as store, store.watching() as seen:
        first = store.wards.by_id(ward.id)
        second = store.wards.by_id(ward.id)

    assert first.name == second.name == "Katoomba"
    assert len(statements(seen)) == 1


def test_a_record_that_asked_for_no_cache_is_read_every_time(pool):
    """Off unless the class says otherwise, because a cache nobody asked for is
    a staleness nobody agreed to — and the record that must not be stale is the
    one whose author never thought about it."""
    with pool.store() as store:
        visit = store.visits.add(Visit(purpose="assessment"))

    with pool.store() as store, store.watching() as seen:
        store.visits.by_id(visit.id)
        store.visits.by_id(visit.id)

    assert len(statements(seen)) == 2
    assert store.visits.cache_info() is None


def test_the_cache_a_pool_holds_is_the_same_one_every_store_reads(pool):
    """The constraint the whole design turns on. A cache per store would be
    filled by the threads that warmed it and empty for the one that read, which
    is the shape — fan out, then read straight — this exists for."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Leura"))

    with pool.store() as warming:
        warming.wards.by_id(ward.id)

    with pool.store() as reading, reading.watching() as seen:
        assert reading.wards.by_id(ward.id).name == "Leura"

    assert statements(seen) == []


def test_a_store_built_by_hand_caches_for_its_own_life(cached):
    """`Store.connect` and `Store(conn)` have no pool, and the first shape the
    README shows is one of them. A record saying `cached_for=` and silently
    getting nothing would make the declaration mean two different things
    depending on how the store was built, which is invisible at the call site."""
    ward = cached.wards.add(Ward(name="Wentworth Falls"))

    with cached.watching() as seen:
        cached.wards.by_id(ward.id)
        cached.wards.by_id(ward.id)

    assert len(statements(seen)) == 1


def test_a_save_drops_its_own_key_so_the_next_read_is_the_row_it_wrote(pool):
    """This is the half a caller cannot write for themselves: dray sees its own
    writes, so a save drops exactly the key it wrote rather than leaving a
    lifetime in which the process that changed a row reads back what it
    replaced."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Katoomba"))
        store.wards.by_id(ward.id)

        ward.name = "Katoomba North"
        store.wards.save(ward)

        assert store.wards.by_id(ward.id).name == "Katoomba North"


def test_a_write_that_rolls_back_leaves_the_row_that_is_still_there(pool):
    """An eviction that fired before the commit would empty the cache and let
    the next read fill it from a transaction that then rolled back — which is
    worse than no cache at all, because the wrong row would then be believed for
    the whole of a lifetime."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Blackheath"))
        store.wards.by_id(ward.id)

        with pytest.raises(RuntimeError, match="no"):
            with store.transaction():
                ward.name = "Mount Victoria"
                store.wards.save(ward)
                raise RuntimeError("no")

        assert store.wards.by_id(ward.id).name == "Blackheath"


def test_a_delete_drops_the_record_it_removed(pool):
    """A removal is a write. Without this, `by_id` would go on answering with a
    row the same process had just taken away — and `RecordNotFound` is the whole
    of what a caller has to tell them apart."""
    from dray import RecordNotFound

    with pool.store() as store:
        ward = store.wards.add(Ward(name="Springwood"))
        store.wards.by_id(ward.id)

        store.wards.delete(ward)

        with pytest.raises(RecordNotFound):
            store.wards.by_id(ward.id)


def test_a_child_the_cascade_took_is_not_read_back_out_of_memory(pool):
    """A delete takes everything under the record with it, and takes it without
    loading a row — one statement per generation, so there are no keys to drop
    one at a time and the whole of that generation's cache goes instead. The
    delete above has no child cached when it runs, so the branch that does this
    is exercised with nothing to show for it. Without the branch, a request that
    removes a record and still holds a child's id is served the deleted child
    for a full lifetime."""
    from dray import RecordNotFound

    with pool.store() as store:
        ward = store.wards.add(Ward(name="Bell"))
        ward.rounds.add("Morning round.")
        store.wards.save(ward)
        (one,) = store.ward_rounds.find()
        store.ward_rounds.by_id(one.id)

        store.wards.delete(ward)

        with pytest.raises(RecordNotFound):
            store.ward_rounds.by_id(one.id)


def test_clearing_a_child_set_is_not_read_back_out_of_memory(pool):
    """A set removal names a parent and not the rows it takes, so there are no
    keys to drop one at a time. Everything cached about that generation goes
    instead, because the only honest thing to say about it afterwards is that
    all of it may be wrong."""
    from dray import RecordNotFound

    with pool.store() as store:
        ward = store.wards.add(Ward(name="Hazelbrook"))
        ward.rounds.add("Morning round.")
        store.wards.save(ward)
        (one,) = store.ward_rounds.find()
        store.ward_rounds.by_id(one.id)

        ward.rounds.clear()

        with pytest.raises(RecordNotFound):
            store.ward_rounds.by_id(one.id)


def test_a_child_written_by_its_parent_s_save_drops_its_own_key(pool):
    """Children come free with their parent: a queued child is written by the
    parent's save, so the same eviction reaches it. Without that, a note edited
    through the record that carries it would read back as it was."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Woodford"))
        ward.rounds.add("Morning round.")
        store.wards.save(ward)
        (one,) = store.ward_rounds.find()
        store.ward_rounds.by_id(one.id)

        one.body = "Morning round, two beds free."
        store.ward_rounds.save(one)

        assert store.ward_rounds.by_id(one.id).body.endswith("two beds free.")


def test_a_find_fills_the_cache_and_is_not_itself_answered_from_it(pool):
    """Most of the win, and it costs no invalidation that a write does not
    already do. The other half is the promise `find` cannot make: a write to any
    record can change what a statement matches, and dray cannot know which
    statements it touched, so the set itself is read every time."""
    with pool.store() as store:
        store.wards.add_all([Ward(name="Leura"), Ward(name="Katoomba")])

    with pool.store() as store, store.watching() as seen:
        found = store.wards.find()
        for ward in found:
            store.wards.by_id(ward.id)
        store.wards.find()

    assert len(statements(seen)) == 2


def test_a_read_past_what_the_cache_may_be_filled_with_seeds_nothing(dsn, clock):
    """A `find` bringing back three thousand rows would evict everything a
    fan-out had just warmed, and the caller who wrote the `find` is not the one
    who wrote the warming — that is the whole shape this is for. So a read past
    a share of the cache seeds nothing rather than a prefix of itself, which
    would make which records came free unpredictable.

    Said here with a cache small enough to prove it: three rows against a
    `cache_most` of four is over the quarter one read may cost the set already
    in there, so the `find` fills nothing and the `by_id` after it is a second
    statement."""

    @record(table="tiny_ward", collection="tiny_wards", cached_for=30,
            cache_most=4)
    class TinyWard:
        name: str = field()

    made = ConnectionPool(
        dsn, min_size=1, max_size=3, configure=ready, open=False, timeout=5
    )
    try:
        pool = Pool(made, records=[TinyWard], timer=clock)
        with pool.store() as store:
            store.create(TinyWard)
            wards = store.tiny_wards.add_all(
                [TinyWard(name=f"Ward {n}") for n in range(3)]
            )

        with pool.store() as store, store.watching() as seen:
            store.tiny_wards.find()
            store.tiny_wards.by_id(wards[0].id)

        assert len(statements(seen)) == 2
    finally:
        made.close()


def slowly(collection, seconds):
    """A read of every row of a collection that takes its time in the database.

    The sleep is a scalar subquery rather than a term of the `where`, because
    it has to survive the planner: `pg_sleep(1) or true` is folded away and
    waits for nothing. What it buys is the interleaving the cache's ordering
    turns on — the statement's snapshot is taken when the sleep starts, so a
    write that commits inside it lands after these rows were read and before
    they are put anywhere.
    """
    return (
        f"select {collection.columns} from {collection.table}"
        f" where (select count(*) from (select pg_sleep({seconds})) as _s) = 1"
    )


def test_a_write_landing_during_a_read_is_not_put_back_by_what_it_fills(pool):
    """A read fills the cache when it finishes and a write empties it when it
    commits, and nothing ordered those two against each other. So a `find`
    that fetched its rows before a save could put them back after it, and from
    then on the process answered `by_id` with the row it had itself committed
    over — for the whole of a lifetime, which is the very thing the eviction
    exists to prevent.

    Made deterministic by a statement that waits in the database rather than by
    a seam cut into dray for the test: the read's snapshot is taken as the
    sleep begins, the save commits inside it, and the filling happens on the
    far side. The assertion about what the read brought back is this test
    checking its own premise — a save that landed before the snapshot would
    leave nothing here to catch, and would do it silently."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Leura"))

    reading = threading.Event()

    def slow_read():
        with pool.store() as store:
            reading.set()
            return store.wards.select_many(slowly(store.wards, 1))

    with ThreadPoolExecutor(max_workers=1) as threads:
        fetched = threads.submit(slow_read)
        reading.wait()
        # Long enough for the statement to be away and holding its snapshot,
        # and short enough to be well inside the sleep it is holding it for.
        time.sleep(0.2)
        with pool.store() as store:
            moved = store.wards.by_id(ward.id)
            moved.name = "Katoomba"
            moved.save()
        found = fetched.result()

    assert [one.name for one in found] == ["Leura"]

    with pool.store() as store, store.watching() as seen:
        assert store.wards.by_id(ward.id).name == "Katoomba"
    assert len(statements(seen)) == 1


def test_a_read_with_nothing_written_under_it_fills_as_it_always_did(pool):
    """The other half of that rule and the reason it is worth its own test: a
    read that drops its filling whatever happened underneath would close the
    race by turning the cache off. The same slow read with nobody writing
    during it fills, and the `by_id` after it is no statement at all."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Wentworth Falls"))

    with pool.store() as store:
        found = store.wards.select_many(slowly(store.wards, 0.2))
    assert [one.name for one in found] == ["Wentworth Falls"]

    with pool.store() as store, store.watching() as seen:
        assert store.wards.by_id(ward.id).name == "Wentworth Falls"
    assert not statements(seen)


def test_a_write_to_a_row_a_read_never_saw_throws_its_filling_away_too(pool):
    """What the coarse answer costs, and it is accepted rather than
    overlooked. The count is one number per collection cache and cannot say
    which key an eviction took, so a save of a record this read never returned
    discards the whole of its filling — here an `add` of a row that did not
    exist when the read started, whose key nothing had cached at all. A
    discarded fill costs a round trip somebody was going to pay anyway, where
    a kept one costs a row the process has overwritten, so the asymmetry only
    points one way."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Blackheath"))

    reading = threading.Event()

    def slow_read():
        with pool.store() as store:
            reading.set()
            return store.wards.select_many(slowly(store.wards, 1))

    with ThreadPoolExecutor(max_workers=1) as threads:
        fetched = threads.submit(slow_read)
        reading.wait()
        time.sleep(0.2)
        with pool.store() as store:
            store.wards.add(Ward(name="Mount Victoria"))
        found = fetched.result()

    assert [one.name for one in found] == ["Blackheath"]

    with pool.store() as store, store.watching() as seen:
        assert store.wards.by_id(ward.id).name == "Blackheath"
    assert len(statements(seen)) == 1


def test_a_generation_a_cascade_took_is_not_put_back_by_a_read_in_flight(
    pool,
):
    """The other way a key leaves a cache, and the busier of the two. A save
    drops the keys it named; a cascade, a child set's `clear()` or `thin()`,
    and either `forget_all` name a parent instead and empty the whole map — so
    without that emptying counted as well, a read of the generation that was
    taken would put every row of it back after the delete had committed, and
    `by_id` would go on serving records that are not in the table at all.

    Read straight from the child collection rather than through the parent,
    because the parent is what the delete removes and the point is what
    happens to the rows the read is holding. Same arrangement as the two
    above: the statement's snapshot is taken as the sleep begins, the delete
    commits inside it, and the filling happens on the far side. The rounds
    coming back at all is this test checking its own premise."""
    from dray import RecordNotFound

    with pool.store() as store:
        ward = store.wards.add(Ward(name="Springwood"))
        ward.rounds.add("Morning round.")
        store.wards.save(ward)
        (round_,) = store.ward_rounds.find()

    reading = threading.Event()

    def slow_read():
        with pool.store() as store:
            reading.set()
            return store.ward_rounds.select_many(
                slowly(store.ward_rounds, 1)
            )

    with ThreadPoolExecutor(max_workers=1) as threads:
        fetched = threads.submit(slow_read)
        reading.wait()
        time.sleep(0.2)
        with pool.store() as store:
            store.wards.delete(store.wards.by_id(ward.id))
        found = fetched.result()

    assert [one.body for one in found] == ["Morning round."]

    with pool.store() as store:
        with pytest.raises(RecordNotFound):
            store.ward_rounds.by_id(round_.id)


def test_a_find_first_on_a_unique_index_is_answered_by_key_then_by_id(pool):
    """`index("code", unique=True)` has already told dray those columns identify
    one row, so the second lookup by student number, licence or code is a map in
    memory rather than a round trip. Key to id and never key to row: two entries
    for one record would be two things to drop on a write and two that can
    disagree."""
    with pool.store() as store:
        store.wards.add(Ward(name="Katoomba", code="KAT"))

    with pool.store() as store, store.watching() as seen:
        first = store.wards.find_first(equals={"code": "KAT"})
        second = store.wards.find_first(equals={"code": "KAT"})

    assert first.name == second.name == "Katoomba"
    assert len(statements(seen)) == 1


def test_a_leading_run_of_a_unique_index_is_not_a_natural_key(pool):
    """A unique index over two columns says nothing about how many rows share
    the first of them, so a filter naming only that one identifies no row. It
    was tempting to take a leading run, and the answer would have been the first
    of several — remembered as though it were the only one."""
    with pool.store() as store:
        store.wards.add_all([
            Ward(name="Leura", code="LEU", region="upper", bed_count=4),
            Ward(name="Katoomba", code="KAT", region="upper", bed_count=8),
        ])

    with pool.store() as store, store.watching() as seen:
        store.wards.find_first(equals={"region": "upper"})
        store.wards.find_first(equals={"region": "upper"})

    assert len(statements(seen)) == 2


def test_a_natural_key_naming_a_record_that_has_moved_is_not_believed(pool):
    """A key remembered a moment ago can still name the wrong row: the record it
    named may have been written since. So the row is checked against the filter
    it came back for, which costs nothing and is the alternative to a second
    index from ids back to the keys naming them."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Leura", code="LEU"))
        store.wards.find_first(equals={"code": "LEU"})

        ward.code = "LEU2"
        store.wards.save(ward)

        assert store.wards.find_first(equals={"code": "LEU"}) is None
        assert store.wards.find_first(equals={"code": "LEU2"}).name == "Leura"


def test_a_natural_key_naming_a_record_that_has_gone_is_not_believed(pool):
    """The same check, arrived at the other way. `by_id` raises where a natural
    key points at a row that has been removed, and a search answering with an
    exception would be the one reading rule dray does not have."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Bullaburra", code="BUL"))
        store.wards.find_first(equals={"code": "BUL"})

        store.wards.delete(ward)

        assert store.wards.find_first(equals={"code": "BUL"}) is None


def test_two_readers_of_one_record_never_hold_the_same_object(pool):
    """Records are mutable, so handing two callers one object lets one of them
    see the other's half-finished edit, and the caller behind reads changes that
    are in nobody's database. The copy has to be deep, or a blob field holding a
    list is one list shared by every record built from that row."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Medlow Bath", tags=["rural"]))

    with pool.store() as store:
        first = store.wards.by_id(ward.id)
        second = store.wards.by_id(ward.id)

        assert first is not second
        assert first.tags is not second.tags

        first.tags.append("scratch")
        assert store.wards.by_id(ward.id).tags == ["rural"]


def test_a_row_a_read_left_behind_is_the_cache_s_own_copy(pool):
    """The fill path is not the read path, and only the read path is pinned by
    the test above. A `find` hands back records built from the very rows it
    left behind, so a seed that stored those rows rather than copies of them
    would let a caller edit a blob field in place and change what every later
    `by_id` hydrates from — with nothing saved, and nothing said."""
    with pool.store() as store:
        store.wards.add(Ward(name="Mount Wilson", tags=["rural"]))

    with pool.store() as store:
        (found,) = store.wards.find()
        found.tags.append("scratch")

        assert store.wards.by_id(found.id).tags == ["rural"]


def test_concurrent_misses_on_one_key_make_one_round_trip(pool):
    """Three threads asking the same question at once should be one round trip
    and two waits, or a warming phase does the very thing it exists to avoid.
    The map's own lock does not buy this: guarding the map guards nothing about
    the call, and three threads missing together would each go and ask."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Blaxland"))

    together = threading.Barrier(3)

    def ask():
        with pool.store() as store:
            together.wait()
            return store.wards.by_id(ward.id).name

    with pool.store() as watcher, watcher.watching() as seen:
        with ThreadPoolExecutor(max_workers=3) as threads:
            asking = [threads.submit(ask) for _ in range(3)]
            names = [one.result() for one in asking]

    assert names == ["Blaxland"] * 3
    assert len(statements(seen)) == 1


def test_a_row_older_than_its_lifetime_is_read_again(pool, clock):
    """The lifetime is the whole of what bounds staleness, and it is
    process-local: another container that wrote cannot reach this one. The clock
    is a seam so that a test can watch an entry expire rather than sleeping for
    it, which is how a suite comes to take thirty seconds for one assertion."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Faulconbridge"))
        store.wards.by_id(ward.id)

    clock.at += 31

    with pool.store() as store, store.watching() as seen:
        store.wards.by_id(ward.id)

    assert len(statements(seen)) == 1


def test_forgetting_one_record_leaves_the_rest_where_they_are(pool):
    """For the row that moved some other way — a statement of your own through
    `store.conn`, a job in another language, a trigger. Everything dray wrote
    itself it has already dropped."""
    with pool.store() as store:
        leura, katoomba = store.wards.add_all(
            [Ward(name="Leura"), Ward(name="Katoomba")]
        )
        store.wards.by_id(leura.id)
        store.wards.by_id(katoomba.id)

        store.wards.forget(leura.id)

        with store.watching() as seen:
            store.wards.by_id(leura.id)
            store.wards.by_id(katoomba.id)

    assert len(statements(seen)) == 1


def test_forgetting_a_collection_and_forgetting_everything(pool):
    """Two doors for two questions. One collection's rows moved is one call on
    it; something outside dray has written is the pool's, because in that case
    nothing here knows which rows moved or of what kind."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Lawson"))
        store.wards.by_id(ward.id)
        assert store.wards.cache_info().size == 1

        store.wards.forget_all()
        assert store.wards.cache_info().size == 0

        store.wards.by_id(ward.id)
        assert store.wards.cache_info().size == 1

        store.pool.forget_all()
        assert store.wards.cache_info().size == 0


def test_a_store_with_no_pool_still_has_somewhere_to_forget(cached):
    """`store.pool.forget_all()` is the spelling for a pooled store and there is
    no pool to say it to on a store built by hand — which is the store the
    README opens with."""
    ward = cached.wards.add(Ward(name="Glenbrook"))
    cached.wards.by_id(ward.id)

    cached.forget_all()

    assert cached.wards.cache_info().size == 0


def test_a_read_that_must_be_true_asks_the_database_and_remembers_nothing(pool):
    """The caller who must not read a stale row wants to say so at the read
    rather than be told afterwards that they did — which is why there is no
    "this came from cache" flag on a record. The block does not empty anything:
    it is about this read, not about the cache."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Warrimoo"))
        store.wards.by_id(ward.id)

        with store.watching() as seen, store.uncached():
            store.wards.by_id(ward.id)
        assert len(statements(seen)) == 1

        # And what was cached before is still cached after.
        with store.watching() as seen:
            store.wards.by_id(ward.id)
        assert statements(seen) == []


def test_a_write_inside_an_uncached_block_still_drops_the_keys_it_wrote(pool):
    """The block is one store saying *do not answer my reads out of memory*, and
    the eviction a write does is not that store's to skip: what it drops from
    belongs to every store on the pool, and the next one to ask is somebody
    else. It is true today only because the write path never consults the gate
    the read path does — which is exactly the difference a later change tidies
    away for consistency, leaving every other store reading the old row for a
    lifetime after this one wrote it."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Mount Irvine"))

    with pool.store() as warming:
        warming.wards.by_id(ward.id)

    with pool.store() as writing, writing.uncached():
        held = writing.wards.by_id(ward.id)
        held.region = "upper"
        writing.wards.save(held)

    with pool.store() as reading:
        assert reading.wards.by_id(ward.id).region == "upper"


def test_a_read_inside_a_block_is_neither_answered_nor_remembered(pool):
    """A block is where a read sees this store's own uncommitted writes, so
    filling the cache from one would publish rows a rollback then takes away —
    and answering from it would hand back the row from before a write in the
    same block."""
    with pool.store() as store:
        ward, other = store.wards.add_all(
            [Ward(name="Valley Heights"), Ward(name="Emu Plains")]
        )
        store.wards.by_id(ward.id)
        assert store.wards.cache_info().size == 1

        with store.transaction():
            ward.name = "Valley Heights South"
            store.wards.save(ward)
            with store.watching() as seen:
                assert store.wards.by_id(ward.id).name == "Valley Heights South"
                store.wards.by_id(other.id)
            # Two statements for two reads that were both in the cache's reach,
            # and nothing left behind by either: the row a block can see is a
            # row only that block can see.
            assert len(statements(seen)) == 2
            assert store.wards.cache_info().size == 1

        # And the key the save wrote went once the rows were durable.
        assert store.wards.cache_info().size == 0


def test_cache_info_tells_a_cache_nobody_used_from_no_cache_at_all(pool):
    """Counters answer what a test and a dashboard actually ask, and `None` is
    the answer to a different question — this record named no `cached_for`.
    Reporting nought hits over nought entries for both would make the two
    indistinguishable."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Linden"))

        assert store.visits.cache_info() is None
        assert store.wards.cache_info().hits == 0

        store.wards.by_id(ward.id)
        store.wards.by_id(ward.id)

        info = store.wards.cache_info()
        assert (info.hits, info.misses, info.size) == (1, 1, 1)


def test_a_lifetime_of_nought_is_refused_where_the_class_is_written():
    """Nought reads as *cache it forever* to anybody skimming and would mean
    *cache it not at all*, and both readings are silent. The way to say a record
    is not cached is to leave the option off."""
    with pytest.raises(ValueError, match="not a cache that is off"):

        @record(table="nought", collection="noughts", cached_for=0)
        class Nought:
            name: str = field()


def test_a_lifetime_that_is_not_a_number_of_seconds_is_refused():
    """One knob, and it is seconds. A value object for it would be a second
    thing to import for the one number anybody tunes."""
    with pytest.raises(TypeError, match="number of seconds"):

        @record(table="worded", collection="wordeds", cached_for="ten")
        class Worded:
            name: str = field()


#
# Questions a collection was told to keep
#


def test_a_question_a_collection_keeps_is_asked_once(pool):
    """A method of yours is dray's only way of knowing what a page actually
    costs to answer, and the developer who wrote it is the only one who knows
    what staleness it tolerates. So the lifetime goes on the method, and the
    same question inside it does not reach the database twice."""
    with pool.store() as store:
        store.regions.add(Region(name="Blue Mountains", code="BM"))

    with pool.store() as store, store.watching() as seen:
        first = store.regions.by_code("BM")
        second = store.regions.by_code("BM")

    assert first.name == second.name == "Blue Mountains"
    assert len(statements(seen)) == 1


def test_a_question_is_kept_under_the_arguments_it_was_asked_with(pool):
    """Two questions and not one. Keyed by the method alone, the second call
    would answer with the first one's row and nothing would say so."""
    with pool.store() as store:
        store.regions.add_all([
            Region(name="Blue Mountains", code="BM"),
            Region(name="Hawkesbury", code="HAW"),
        ])

        assert store.regions.by_code("BM").name == "Blue Mountains"
        assert store.regions.by_code("HAW").name == "Hawkesbury"


def test_a_question_on_a_base_class_is_kept_per_collection(pool):
    """A method arrives on two collections through a base class and is one
    function object for both, so a cache keyed by the function alone would hand
    the count of one table back as the count of the other."""
    with pool.store() as store:
        store.regions.add(Region(name="Blue Mountains"))

        assert store.regions.how_many() == 1
        assert store.areas.how_many() == 0


def test_an_answer_of_nothing_is_kept_like_any_other(pool):
    """Unlike a row read by key, where nothing found means *not written yet* and
    remembering the absence would leave a new record missing for a lifetime.
    A question answering `None` has answered — that is what `Lookup | None`
    means — and asking the database again for it every time would leave the
    expensive half of a search uncached."""
    with pool.store() as store, store.watching() as seen:
        assert store.regions.by_code("NOPE") is None
        assert store.regions.by_code("NOPE") is None

    assert len(statements(seen)) == 1


def test_a_kept_answer_is_a_copy_for_every_caller(pool):
    """Same promise as a row read by key, for the same reason: two callers
    holding one record is one of them reading the other's unsaved edit. The
    copy has to leave dray's own way back to the store alone, or copying a
    record would mean copying a connection."""
    with pool.store() as store:
        store.regions.add(Region(name="Blue Mountains", code="BM"))

        first = store.regions.by_code("BM")
        second = store.regions.by_code("BM")
        assert first is not second

        first.name = "scratch"
        assert store.regions.by_code("BM").name == "Blue Mountains"

        # And the copy is a working record rather than a snapshot of one: its
        # way back to the store came through the copy intact.
        second.name = "Greater Blue Mountains"
        second.save()
        assert store.regions.by_id(second.id).name == "Greater Blue Mountains"


def test_a_kept_answer_goes_stale_and_the_lifetime_is_the_whole_bound(
    pool, clock
):
    """The honest half of it, and the half worth a test rather than a sentence.
    dray drops the keys its own writes touched because a key is a thing a write
    can be matched back to; a question is not, so a write leaves the answer
    where it was. Nothing here is a promise except the number the caller
    chose."""
    with pool.store() as store:
        region = store.regions.add(Region(name="Blue Mountains", code="BM"))
        assert store.regions.by_code("BM").name == "Blue Mountains"

        region.name = "Greater Blue Mountains"
        store.regions.save(region)

        # The row this write touched went; the question it may have changed did
        # not, because nothing can say whether it did.
        assert store.regions.by_id(region.id).name == "Greater Blue Mountains"
        assert store.regions.by_code("BM").name == "Blue Mountains"

        clock.at += 1801
        assert store.regions.by_code("BM").name == "Greater Blue Mountains"


def test_an_argument_that_cannot_be_a_key_is_refused_where_it_is_called(pool):
    """A method with a lifetime on it is one somebody found expensive, so a
    call that quietly opted out of the cache and went to the database every time
    is the kind of thing nobody finds for a year. The refusal names the argument
    and its type, and not its value — an argument is as likely to be somebody's
    data as anything else dray refuses to print."""
    with pool.store() as store:
        assert store.regions.named(("Leura", "Katoomba")) == []

        with pytest.raises(TypeError, match="names was given a list"):
            store.regions.named(["Leura"])


def test_a_question_asked_inside_a_block_is_neither_answered_nor_kept(pool):
    """The same two gates a read by key goes through. A method called inside a
    block may have read this store's own uncommitted rows, and keeping that
    answer would publish it to every other store when the block rolled back."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura", code="LEU"))
        store.regions.by_code("LEU")

        with store.transaction():
            with store.watching() as seen:
                store.regions.by_code("LEU")
                store.regions.by_code("LEU")
            assert len(statements(seen)) == 2


def test_uncached_reaches_a_kept_question_as_well_as_a_row(pool):
    """A block saying *this must be true* said it about everything inside it. A
    caller who had to know which of two caches a call went through would be
    reading dray's implementation to answer a question about their own data."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura", code="LEU"))
        store.regions.by_code("LEU")

        with store.watching() as seen, store.uncached():
            store.regions.by_code("LEU")
            store.regions.by_code("LEU")

        assert len(statements(seen)) == 2


def test_forgetting_a_collection_takes_its_kept_questions_with_it(pool):
    """`forget_all` on a collection means everything it remembers, or a caller
    who had just written past dray would empty half of what they meant to."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura", code="LEU"))
        store.regions.by_code("LEU")

        store.regions.forget_all()

        with store.watching() as seen:
            store.regions.by_code("LEU")
        assert len(statements(seen)) == 1


def test_the_pool_forgets_kept_questions_too(pool):
    """Which is what putting them on the pool buys: one call empties every
    cache in the process, rather than a module-level map somewhere that
    outlives every store and answers to nobody."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura", code="LEU"))
        store.regions.by_code("LEU")

        store.pool.forget_all()

        with store.watching() as seen:
            store.regions.by_code("LEU")
        assert len(statements(seen)) == 1


def test_concurrent_askers_of_one_question_ask_it_once(pool):
    """The same single flight a read by key gets, and it matters more here: the
    call this is on is the expensive one by construction."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura"))

    together = threading.Barrier(3)

    def ask():
        with pool.store() as store:
            together.wait()
            return store.regions.how_many()

    with pool.store() as watcher, watcher.watching() as seen:
        with ThreadPoolExecutor(max_workers=3) as threads:
            asking = [threads.submit(ask) for _ in range(3)]
            counts = [one.result() for one in asking]

    assert counts == [1, 1, 1]
    assert len(statements(seen)) == 1


def test_cache_info_counts_both_of_the_things_a_collection_keeps(pool):
    """One number for one question — *is anything here being served from
    memory*. A collection whose record is not cached but whose class keeps an
    answer is still a collection that remembers something, so `None` there
    would be wrong."""
    with pool.store() as store:
        store.regions.add(Region(name="Leura", code="LEU"))

        assert store.areas.cache_info().hits == 0

        store.regions.by_code("LEU")
        store.regions.by_code("LEU")
        store.regions.by_id(store.regions.find()[0].id)

        info = store.regions.cache_info()
        assert info.hits >= 2
        assert info.size >= 2


def test_a_question_on_a_record_s_own_method_is_refused(cached):
    """A method on a record is about one row, which is the thing `by_id`
    already keeps and the one thing a write can be matched back to — so putting
    a second cache in front of it under a different key would be two answers
    about one row with different lifetimes."""

    @record(table="misplaced", collection="misplaceds")
    class Misplaced:
        name: str = field(default="")

        @cached_for(60)
        def shouted(self) -> str:
            return self.name.upper()

    cached.create(Misplaced)
    one = cached.misplaceds.add(Misplaced(name="leura"))

    with pytest.raises(TypeError, match="not a method of a collection"):
        one.shouted()


def test_a_lifetime_is_required_on_the_decorator_rather_than_defaulted():
    """`@cached_for` without a number would be dray choosing how stale somebody
    else's answer may be, which is the one thing this decorator exists to hand
    over."""
    with pytest.raises(TypeError, match=r"rather than `@cached_for`"):

        @cached_for
        def by_name(self, name: str) -> None:
            return None


def closes(seen, kind, cls):
    """The finished spans of one kind, about one record class."""
    return [
        span
        for span in seen
        if span.phase == "close" and span.kind == kind and span.cls is cls
    ]


def test_a_read_by_id_that_went_to_the_database_traces_as_one_statement(pool):
    """`hydrate` belongs under the `statement` that fetched the row, so that
    keeping only the statements leaves each one's elapsed an honest total —
    which is what the page promises about the flat view. Building the record
    above the cursor moved it out to the root, and did so on a record that had
    asked for no cache at all, where the shape had changed for nothing."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Katoomba"))
        visit = store.visits.add(Visit(purpose="assessment"))

    with pool.store() as store, store.watching(kind=None) as seen:
        store.wards.by_id(ward.id)
        store.visits.by_id(visit.id)

    for cls in (Ward, Visit):
        (statement,) = closes(seen, "statement", cls)
        (hydrate,) = closes(seen, "hydrate", cls)
        assert hydrate.parent_id == statement.id
        # A read that went to the database is a miss, and a miss opens nothing:
        # the statement under it already says where it went.
        assert closes(seen, "cache", cls) == []


def test_a_read_answered_from_memory_traces_as_a_cache_span(pool):
    """The other half, and the reason the record is not built above the cursor
    for both: a row that came out of memory has no statement to hang under, and
    a `hydrate` under a `statement` that never ran would be the wrong picture of
    where the time went.

    It hangs under a `cache` instead. A hit emits no statement, so before there
    was a kind for it a page the cache answered and a page that never asked
    were the same picture — which is the one thing a trace has to be able to
    tell apart once a record has `cached_for=` on it."""
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Leura"))
        store.wards.by_id(ward.id)

        with store.watching(kind=None) as seen:
            store.wards.by_id(ward.id)

    assert closes(seen, "statement", Ward) == []
    (hit,) = closes(seen, "cache", Ward)
    (hydrate,) = closes(seen, "hydrate", Ward)
    assert hit.parent_id is None
    assert hit.rowcount == 1
    assert hydrate.parent_id == hit.id


def test_a_read_that_seeds_the_cache_is_one_statement_and_no_hits(pool):
    """Seeding is a side effect of one statement and not forty answers. A span
    per row would drown the tree in the cheap half of the page, and would be
    saying a hit happened where nobody had asked for anything."""
    with pool.store() as store:
        store.wards.add_all([Ward(name=f"Ward {n}") for n in range(40)])

        with store.watching(kind=None) as seen:
            found = store.wards.find()

        # The premise, said out loud: forty rows is under the ceiling and they
        # all went where `by_id` will find them. Without this the assertions
        # below would pass just as happily on a read that seeded nothing.
        assert store.wards.cache_info().size == 40

    assert len(found) == 40
    assert len(closes(seen, "statement", Ward)) == 1
    assert closes(seen, "cache", Ward) == []


def test_a_key_of_your_own_answered_from_memory_is_a_hit_and_so_is_the_row(
    pool
):
    """Two maps answer a `find_first` on a unique index — the key that resolves
    to an id, and the row that id names — so it is two hits, which is what
    `cache_info` has always counted it as. The row's hit hangs under the key's,
    so the tree says which of the two answered and the other one is visible
    where it did not."""
    with pool.store() as store:
        store.regions.add(Region(name="Blue Mountains", code="BM"))
        store.regions.find_first(equals={"code": "BM"})

        with store.watching(kind="cache") as hits:
            found = store.regions.find_first(equals={"code": "BM"})

    assert found.name == "Blue Mountains"
    assert len(hits) == 2
    key, row = sorted(hits, key=lambda span: span.id)
    assert row.parent_id == key.id


def test_a_key_that_outlived_the_row_it_names_is_one_hit_and_one_statement(
    pool
):
    """The other half of the pair above, and the one a reader will be counting
    against. A save drops the row and deliberately leaves the key naming it, so
    read, write, read again is one hit and not two — the key answered and the
    row had to be fetched. Which is why the count `cache_info` gives is per map
    answered rather than per call, and why the tree hangs the statement under
    the hit rather than beside it."""
    with pool.store() as store:
        region = store.regions.add(Region(name="Blue Mountains", code="BM"))
        store.regions.find_first(equals={"code": "BM"})

        region.name = "Greater Blue Mountains"
        store.regions.save(region)
        before = store.regions.cache_info()

        with store.watching(kind=None) as seen:
            found = store.regions.find_first(equals={"code": "BM"})

        after = store.regions.cache_info()

    assert found.name == "Greater Blue Mountains"
    (hit,) = closes(seen, "cache", Region)
    (statement,) = closes(seen, "statement", Region)
    assert statement.parent_id == hit.id
    assert after.hits - before.hits == 1
    assert after.misses - before.misses == 1


def test_a_kept_answer_served_from_memory_is_a_hit_like_any_other(pool):
    """One kind for both caches, for the reason `cache_info` reports one pair
    of counters: *is anything here being served from memory* is one question,
    and a second name for it would be two things to add up at the call site.

    The label is what tells them apart once they are drawn. `cls` cannot: the
    span carries the record class, so an answer served out of memory and a row
    served out of memory both read as the record on a collection that keeps
    both."""
    with pool.store() as store:
        store.areas.add(Area(name="Katoomba"))
        store.areas.how_many()

        with store.watching(kind=None) as seen:
            assert store.areas.how_many() == 1

    assert closes(seen, "statement", Area) == []
    (hit,) = closes(seen, "cache", Area)
    assert hit.parent_id is None
    # The method as declared — `__qualname__`, so a question written on a base
    # class and mixed into two collections is named where it was written.
    assert hit.label == "Tallying.how_many"


def test_the_hits_a_block_counts_are_the_ones_cache_info_counts(pool):
    """The two answers about one cache, and they have to agree or one of them
    is a lie. `cache_info` is the running total and the block is this page's
    share of it."""
    with pool.store() as store:
        region = store.regions.add(Region(name="Blue Mountains", code="BM"))
        store.regions.by_id(region.id)
        store.regions.find_first(equals={"code": "BM"})
        before = store.regions.cache_info().hits

        with store.watching(kind="cache") as hits:
            store.regions.by_id(region.id)
            store.regions.find_first(equals={"code": "BM"})

        after = store.regions.cache_info().hits

    assert len(hits) == after - before


def test_hits_are_counted_across_every_store_a_block_lends_out(pool):
    """The shape the cache exists for: warm on a fan-out, read straight
    afterwards. The workers take a store each from the pool inside the block,
    so a count of hits has to reach all of them or it answers for one thread of
    several — which is the assertion a test of the warming actually wants."""
    with pool.store() as warming:
        wards = warming.wards.add_all(
            [Ward(name=f"Ward {n}") for n in range(3)]
        )
        for ward in wards:
            warming.wards.by_id(ward.id)

    together = threading.Barrier(3)

    def read(ward_id):
        with pool.store() as store:
            together.wait()
            return store.wards.by_id(ward_id).name

    with pool.store() as watcher, watcher.watching(kind="cache") as hits:
        with ThreadPoolExecutor(max_workers=3) as threads:
            reading = [threads.submit(read, ward.id) for ward in wards]
            names = [one.result() for one in reading]

    assert sorted(names) == sorted(ward.name for ward in wards)
    assert len(hits) == 3
    assert len({span.thread_ident for span in hits}) == 3


def test_a_hit_that_waited_draws_a_span_saying_what_the_waiting_cost(
    pool, monkeypatch
):
    """Three threads on one cold key are one round trip and two waits, and the
    two that waited are counted as hits because that is what the read cost
    them. The span has to cost that too, or the fan-out this whole feature is
    sold on is drawn as two reads of a tenth of a millisecond sitting beside a
    statement that took four hundred — which reads as a cache working
    beautifully and is a queue.

    So the span is backdated by what the map spent answering: the wait, and the
    copy on the way out. It cannot simply be opened first, because until the
    map has answered there is no telling a hit from a miss, and a miss must
    open none."""
    slow = 0.2
    with pool.store() as store:
        ward = store.wards.add(Ward(name="Blackheath"))

    reading = Collection._row_for

    def dawdling(self, wanted):
        time.sleep(slow)
        return reading(self, wanted)

    monkeypatch.setattr(Collection, "_row_for", dawdling)
    together = threading.Barrier(3)

    def ask():
        with pool.store() as store:
            together.wait()
            return store.wards.by_id(ward.id).name

    with pool.store() as watcher, watcher.watching(kind=None) as seen:
        with ThreadPoolExecutor(max_workers=3) as threads:
            asking = [threads.submit(ask) for _ in range(3)]
            names = [one.result() for one in asking]

    assert names == ["Blackheath"] * 3
    # One thread read and did not hit; the two behind it waited on that read.
    assert len(closes(seen, "statement", Ward)) == 1
    hits = closes(seen, "cache", Ward)
    assert len(hits) == 2
    for hit in hits:
        assert hit.elapsed_ns > slow * 1e9 / 2


def test_a_kept_answer_is_handed_to_the_store_that_asked_for_it(pool):
    """A record carries the collection it was read through and a collection
    carries a connection, so an answer computed on one store and handed to a
    caller on another arrived holding the first store's connection — which by
    then had gone back to the pool and could be in another thread's hands. Two
    threads on one connection, in exactly the shape this exists for: warm on a
    fan-out, read straight afterwards."""
    with pool.store() as warming:
        warming.regions.add(Region(name="Blue Mountains", code="BM"))
        warming.regions.by_code("BM")

    with pool.store() as reading:
        served = reading.regions.by_code("BM")

        assert served.store is reading
        assert served.store.conn is reading.conn

        # And the record still works, which is the promise the page makes about
        # it: the binding is to this store rather than to none at all.
        served.name = "Greater Blue Mountains"
        served.save()
        assert reading.regions.by_id(served.id).name == "Greater Blue Mountains"


def test_records_inside_a_kept_answer_are_rebound_wherever_they_sit(pool):
    """An answer is as likely to be a list of records or a mapping of them as
    one record, and every one of them carries the same way back to a store."""
    with pool.store() as warming:
        warming.regions.add_all([
            Region(name="Blue Mountains"), Region(name="Hawkesbury")
        ])
        warming.regions.named(("Blue Mountains", "Hawkesbury"))

    with pool.store() as reading:
        served = reading.regions.named(("Blue Mountains", "Hawkesbury"))

        assert len(served) == 2
        assert all(one.store is reading for one in served)


def test_a_question_that_reaches_itself_is_a_runaway_and_not_a_hang(pool):
    """A method of somebody's that asks itself the same question — directly, or
    round a cycle of two — was a `RecursionError` a moment later and became a
    thread waiting on a lock it was holding itself, with no timeout on it,
    inside a transaction ageing against five minutes. It is still a mistake; it
    is a mistake that says so."""

    # Written out by hand rather than through `@collection`, which would bind
    # it to Area for every other test in this file as a side effect.
    class Circling(Collection):
        @cached_for(60)
        def round_and_round(self) -> int:
            return self.round_and_round()

    with pool.store() as store:
        circling = Circling(store, Area)
        with pytest.raises(RecursionError):
            circling.round_and_round()


def test_a_write_that_stopped_partway_forgets_the_chunks_that_landed(
    pool, monkeypatch
):
    """A set over the row ceiling is several transactions, so a refusal in the
    last chunk leaves the ones before it durable — and the raise went straight
    past the eviction at the foot of the write. So the rows this process had
    just changed went on reading back as they were, in the process that changed
    them, which is the one thing this is supposed never to do. It is a case a
    caller is told to expect and recover from, and recovering starts by reading
    records the write left in the table."""
    import sys

    from dray import RecordHasChanged

    # `dray.collection` is the decorator, so the module has to come from
    # `sys.modules` rather than off the package.
    monkeypatch.setattr(sys.modules["dray.collection"], "MAX_ROWS", 2)

    with pool.store() as store:
        store.wards.add_all([Ward(name=f"Ward {n}") for n in range(6)])
        mine = sorted(store.wards.find(), key=lambda one: one.id)
        for one in mine:
            store.wards.by_id(one.id)

        # Somebody gets to the last of them first, so the chunk holding it is
        # refused and the two chunks in front of it are not.
        theirs = store.wards.by_id(mine[5].id)
        theirs.name = "Moved"
        theirs.save()

        for one in mine:
            one.region = "upper"

        with pytest.raises(RecordHasChanged) as raised:
            store.wards.save_all(mine, guarded=True)

        assert raised.value.written == tuple(one.id for one in mine[:4])
        assert store.wards.by_id(mine[0].id).region == "upper"
        assert store.wards.by_id(mine[3].id).region == "upper"
        # And the chunk that rolled back is as it was, which it always was.
        assert store.wards.by_id(mine[5].id).region == ""


def test_thinning_a_child_set_is_not_read_back_out_of_memory(pool):
    """The last of the removal doors, and the one with the most moving parts: a
    bounded delete, a loop of passes, and a rule branch that reads the rows it
    is about to take. Like `clear` it names a parent rather than the rows it
    took, so there are no keys to drop one at a time and the whole generation
    goes."""
    from dray import RecordNotFound

    with pool.store() as store:
        ward = store.wards.add(Ward(name="Katoomba"))
        ward.rounds.add("Morning round.")
        ward.rounds.add("Evening round.")
        store.wards.save(ward)

        held = store.ward_rounds.find()
        for one in held:
            store.ward_rounds.by_id(one.id)
        assert store.ward_rounds.cache_info().size == 2

        assert ward.rounds.thin(at_a_time=1) == 1
        assert store.ward_rounds.cache_info().size == 0

        (left,) = store.ward_rounds.find()
        gone = next(one for one in held if one.id != left.id)
        with pytest.raises(RecordNotFound):
            store.ward_rounds.by_id(gone.id)
