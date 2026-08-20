"""
What dray keeps between reads: rows, and the answers to questions a collection
was told to keep.

It lives wherever the connection came from — on the pool where there is one,
because a store is built per checkout and a cache that died with the store
would be filled by the warming and empty by the time anything read it. That is
also why everything here is written to be used from several threads at once.

**Copies, both ways.** Records are mutable, so handing two callers one object
lets one of them see the other's half-finished edit, and the thread behind it
reads changes that are in nobody's database. What goes in is the cache's own
and what comes out is the caller's, and the copy is deep — a blob field holding
a list would otherwise be one list shared by every record built from that row.
"""

import copy
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

from cachetools import TTLCache


class CacheInfo(NamedTuple):
    """What a collection has been asked and what it is holding.

    `hits` and `misses` count lookups by key — a caller that waited for another
    thread's round trip counts as a hit, because that is what it cost. Rows a
    `find` left behind are neither: nobody asked for those by key.
    """

    hits: int
    misses: int
    size: int


# Nothing to keep. A loader answering this is telling the map to hand the value
# back and store none of it, which is what a `by_id` on a row that is not there
# needs: remembering the absence would leave a record missing for a whole
# lifetime after it was created. A plain `None` cannot carry that, because a
# question whose honest answer is `None` is an ordinary thing for a collection
# method to have.
NOTHING = object()


class Kept:
    """
    One map of things kept for a while, asked for once however many threads
    want the same one.

    The lifetime and the size limit are `cachetools`' — hand-rolling either is
    a day's work and a decade of corner cases. What is here is the waiting,
    which that library will not do and says so: the lock its own `cached` takes
    guards the map and not the call, so three threads missing together make
    three round trips. That is the very thing a warming phase exists to avoid.
    """

    def __init__(
        self, *, ttl: float, maxsize: int, timer: Callable[[], float]
    ) -> None:
        self._kept: TTLCache = TTLCache(maxsize=maxsize, ttl=ttl, timer=timer)
        # `TTLCache` is not thread-safe and a read of one mutates it — expiry
        # and the order it gives things up in are both bookkeeping done on the
        # way past — so every touch is under this. Reentrant because the cost
        # of being wrong about a nested one is a deadlock rather than a test
        # failure.
        self._lock = threading.RLock()
        # Who is already asking for a key: a lock the asker holds until it has
        # an answer, and the thread it is holding it on. Everybody arriving
        # behind waits on the lock rather than asking again — except the asker
        # itself, which is what the thread is recorded for.
        self._flights: dict[Any, tuple[threading.Lock, int]] = {}
        self._hits = 0
        self._misses = 0
        # How many times something here has been dropped. A read that is going
        # to fill this from rows it fetched a moment ago notes the number
        # first and hands it back with them, so that a write which landed in
        # between is something the fill can see rather than something it
        # overwrites — `fill` below, and `Cache.seed` above it.
        self._evictions = 0

    def get(
        self,
        key: Any,
        load: Callable[[], Any],
        *,
        copies: bool = False,
        took: list[int] | None = None,
    ) -> Any:
        """
        This key's value, from here or from `load`, and asked for once however
        many threads want it.

        `load` answers `NOTHING` to say the value is the caller's and not
        this map's. A thread whose leader raised becomes the leader itself,
        which is what keeps a failed read from being an answer everybody waits
        for.

        `took` is somewhere to put the nanoseconds a hit cost — the lock, the
        wait for whoever was already asking, and the copy on the way out —
        appended once, and only where the answer came from here. A caller that
        waited is counted as a hit because that is what it cost it, and the
        wait is the half of that only this method can see. Nothing is appended
        on a miss: what a miss cost is the statement the loader sent, which
        says so itself.

        A list rather than a function, because it is written where the lock is
        held and a caller's own code is the one thing that must never run
        there. Passing nothing is how a caller says it is not being timed, and
        no clock is read at all in that case.
        """
        # The lock this call took, or nothing where it is asking for a key it
        # is already asking for — see below.
        mine: threading.Lock | None = None
        began = time.perf_counter_ns() if took is not None else 0
        while True:
            with self._lock:
                try:
                    found = self._kept[key]
                except KeyError:
                    pass
                else:
                    self._hits += 1
                    answer = copy.deepcopy(found) if copies else found
                    if took is not None:
                        took.append(time.perf_counter_ns() - began)
                    return answer
                flight = self._flights.get(key)
                if flight is None:
                    mine = threading.Lock()
                    mine.acquire()
                    self._flights[key] = (mine, threading.get_ident())
                    self._misses += 1
                    break
                # This thread is already inside a `load` for this very key: a
                # question of somebody's that reaches itself, directly or round
                # a cycle of two. Waiting would be waiting for ourselves, with
                # no timeout, inside a transaction ageing against five minutes
                # — so fall through and let it be the runaway recursion it was
                # before there was anything here to wait on. It takes no flight
                # and stores nothing, because the call further out owns both.
                if flight[1] == threading.get_ident():
                    self._misses += 1
                    break
            # Somebody else is already asking. Wait for them outside the lock,
            # then go round again and read what they wrote.
            with flight[0]:
                pass

        try:
            value = load()
            if value is not NOTHING and mine is not None:
                self.put(key, value, copies=copies)
        finally:
            if mine is not None:
                with self._lock:
                    self._flights.pop(key, None)
                mine.release()
        return value

    def put(self, key: Any, value: Any, *, copies: bool = False) -> None:
        with self._lock:
            self._kept[key] = copy.deepcopy(value) if copies else value

    def fill(
        self,
        rows: Sequence[tuple[Any, Any]],
        *,
        since: int,
        copies: bool = False,
    ) -> None:
        """Put several at once, or none of them where something has been
        dropped since `since`.

        The count and the writing are one act under the lock: an eviction
        landing between the two would be an eviction this then undid, which is
        the whole of what the number is for.
        """
        with self._lock:
            if self._evictions != since:
                return
            for key, value in rows:
                self.put(key, value, copies=copies)

    def forget(self, key: Any) -> None:
        with self._lock:
            # Counted whether or not the key was here, because a write drops
            # the keys it dirtied without knowing which of them anything had
            # cached — and a read that fetched that row before the write is
            # holding exactly the row this call is trying to make unaskable.
            # A drop that found nothing is still one a fill has to lose to.
            self._evictions += 1
            self._kept.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._evictions += 1
            self._kept.clear()

    def evictions(self) -> int:
        """How many times something has been dropped from here."""
        with self._lock:
            return self._evictions

    def counts(self) -> tuple[int, int, int]:
        with self._lock:
            return self._hits, self._misses, len(self._kept)


class Cache:
    """
    One record class's rows, and the ids a natural key resolves to.

    Two maps rather than one, and the second holds ids rather than rows on
    purpose. A row cached under both its key and its student number is two
    entries to drop on a write and two that can disagree; a key that resolves
    to an id and then goes through the by-id map is one copy of the row under
    one key, invalidated by the eviction that already exists.
    """

    def __init__(self, *, ttl: float, maxsize: int, timer: Callable[[], float]):
        self._rows = Kept(ttl=ttl, maxsize=maxsize, timer=timer)
        self._ids = Kept(ttl=ttl, maxsize=maxsize, timer=timer)
        # How large a read may be and still fill this. A `find` bringing back
        # three thousand rows would evict everything a fan-out just warmed, so
        # past this it fills nothing at all rather than a prefix of itself —
        # the rows a list page returns are the ones least likely to be asked
        # for by id afterwards, and a quarter is what a seed may cost the set
        # already in here.
        self.seeds_at_most = max(1, maxsize // 4)

    def row(
        self,
        key: Any,
        load: Callable[[], Any],
        *,
        took: list[int] | None = None,
    ) -> Any:
        """The row behind this key, from here or from the database.

        `load` answers `None` where there is no such row, and nothing is
        remembered about that: a key nobody has written yet is a question the
        database has to keep being asked, or a record would be missing for the
        whole of a lifetime after it was created.

        `took` is `Kept.get`'s, and means the same thing here."""
        row = self._rows.get(
            key, lambda: _or_nothing(load()), copies=True, took=took
        )
        return None if row is NOTHING else row

    def identity(
        self,
        natural: Any,
        load: Callable[[], Any],
        *,
        took: list[int] | None = None,
    ) -> Any:
        """The id a natural key resolves to, from here or from the database."""
        found = self._ids.get(natural, lambda: _or_nothing(load()), took=took)
        return None if found is NOTHING else found

    def evictions(self) -> int:
        """How many keys this has dropped, for a read to note before its
        statement and hand back to `seed`.

        One number per collection cache rather than one for the store, because
        a write to some other kind of record cannot make these rows stale and
        counting it here would throw away warming for nothing. Per key would be
        finer still and is deliberately not what this is: knowing which key an
        eviction took means keeping a stamp that outlives the entry it dropped,
        which is a second thing to expire and to size.
        """
        return self._rows.evictions()

    def seed(self, rows: Sequence[tuple[Any, Any]], *, since: int) -> None:
        """Fill this from a read that was not asking by key.

        Every read of whole records comes through here, which is most of what
        the cache is worth and costs no invalidation that a write does not
        already do. A read too large to seed is dropped whole, for the reason
        `seeds_at_most` is written down.

        `since` is what `evictions()` said before the read ran its statement.
        A read is slow enough for a write to commit while its rows are in
        flight, and that write drops its keys and then finds them put back as
        they were before it — so a seed with any eviction under it fills
        nothing at all. **A discarded seed is the right outcome and not a
        failure**: a number cannot say which key an eviction took, and the
        reads it costs are round trips somebody was going to pay anyway, where
        keeping it costs a process serving a row it committed over itself.
        """
        if not rows or len(rows) > self.seeds_at_most:
            return
        self._rows.fill(rows, since=since, copies=True)

    def forget(self, key: Any) -> None:
        """Drop one record's row. The ids that resolve to it are left where
        they are: a natural key is checked against the record it produced, so a
        stale one costs a read and corrects itself, where dropping them all
        would mean a second index from ids back to the keys naming them."""
        self._rows.forget(key)

    def forget_identity(self, natural: Any) -> None:
        """Drop one natural key, having found that it no longer names that
        record. It counts against the keys and not the rows, so a read in
        flight keeps its seed: nothing seeds a natural key, and the row that
        key was wrong about was dropped by whatever moved it."""
        self._ids.forget(natural)

    def clear(self) -> None:
        self._rows.clear()
        self._ids.clear()

    def counts(self) -> tuple[int, int, int]:
        """Both maps added up, and the size is the rows. A natural key is a way
        through to a row rather than a thing kept in its own right, so counting
        it again would say a collection was holding twice what it has."""
        hits, misses, size = self._rows.counts()
        theirs = self._ids.counts()
        return hits + theirs[0], misses + theirs[1], size


def _or_nothing(value: Any) -> Any:
    """`None` from a read by key means *there is no such row*, which is not a
    thing to keep."""
    return NOTHING if value is None else value


class Caches:
    """
    A cache per record class that asked for one and per question a collection
    was told to keep, made when first wanted.

    Per class and per question rather than one map for everything, because the
    lifetime is per class and per question: a lookup table read constantly and
    written twice a year wants half an hour where a person wants ten seconds,
    and a single number would be wrong for one of them.
    """

    def __init__(self, timer: Callable[[], float] | None = None) -> None:
        # Monotonic rather than wall clock, so a machine correcting its time
        # does not expire everything at once or nothing for an hour.
        self._timer = timer or time.monotonic
        self._caches: dict[type, Cache] = {}
        self._asked: dict[tuple[type, Any], Kept] = {}
        self._lock = threading.Lock()

    def of(self, cls: type) -> Cache | None:
        """This class's cache, or nothing where the class did not ask for
        one."""
        ttl = getattr(cls, "__dray_cached_for__", None)
        if not ttl:
            return None
        found = self._caches.get(cls)
        if found is not None:
            return found
        with self._lock:
            # Asked again under the lock: two threads reaching a class for the
            # first time at once must not each build one, or half the fan-out
            # warms a cache the other half cannot see.
            if cls not in self._caches:
                self._caches[cls] = Cache(
                    ttl=ttl,
                    maxsize=cls.__dray_cache_most__,
                    timer=self._timer,
                )
            return self._caches[cls]

    def asked(
        self, cls: type, question: Any, *, ttl: float, maxsize: int
    ) -> Kept:
        """
        Where one collection method's answers are kept.

        Keyed by the collection class as well as the method, because a method
        can arrive on two collections through a base class and *the takings by
        section* is not one answer for two tables.
        """
        where = (cls, question)
        found = self._asked.get(where)
        if found is not None:
            return found
        with self._lock:
            if where not in self._asked:
                self._asked[where] = Kept(
                    ttl=ttl, maxsize=maxsize, timer=self._timer
                )
            return self._asked[where]

    def asked_of(self, cls: type) -> list[Kept]:
        """Every question this collection class has been asked and told to
        keep — the ones that have been called at least once, since that is when
        a map is made for one."""
        with self._lock:
            asked = list(self._asked.items())
        return [kept for (where, _), kept in asked if where is cls]

    def forget(self, pairs: Sequence[tuple[type, Any]]) -> None:
        """Drop these records' rows — a write's own keys, once the rows it
        wrote are durable."""
        for cls, key in pairs:
            cache = self.of(cls)
            if cache is not None:
                cache.forget(key)

    def forget_class(self, cls: type) -> None:
        """Drop everything cached about one kind of record. What a removal that
        never read the rows it took has to do: a cascade names a generation and
        not the keys in it, so there is nothing to drop one at a time."""
        cache = self.of(cls)
        if cache is not None:
            cache.clear()

    def forget_all(self) -> None:
        with self._lock:
            everything = [*self._caches.values(), *self._asked.values()]
        for one in everything:
            one.clear()
