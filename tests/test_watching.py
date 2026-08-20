"""
What dray is doing, handed over to whoever asked.

dray never showed you the statement it wrote. A collection builds SQL, sends it
and hands back records, and from above there was no way to see the statement,
count the round trips a page made, or tell four slow seconds of database from
four slow seconds of hydrating — the two look identical from the call site, and
they want opposite fixes.

What it hands over is spans rather than log lines, because the interesting
question on DSQL is usually not about one statement: how long a transaction was
open is what this database punishes you for getting wrong, and a statement-only
view cannot see it. dray owns no levels, no formatters and no destination; where
any of this goes is the caller's, which is also the only place the question of
whether a parameter is a medical note can be answered.
"""

import threading
import time

import psycopg
import pytest

from dray import Store, child, field, record, watching


@record(table="walker", collection="walkers")
class Walker:
    family_name: str = field()


@child(of=Walker, name="notes", table="walker_note")
class WalkerNote:
    body: str = field(default="")


@pytest.fixture
def seen():
    return []


@pytest.fixture
def watched(postgresql, seen):
    """A watched store with its tables already made. `create` is a dozen
    statements of DDL and none of them is what any of this is about, so the
    collected spans are emptied before the test starts."""
    store = Store(postgresql, records=[Walker, WalkerNote], observer=seen.append)
    store.create(Walker, WalkerNote)
    seen.clear()
    return store


def closes(seen, kind=None):
    """The finished spans, which is where everything measured lives."""
    return [
        span
        for span in seen
        if span.phase == "close" and (kind is None or span.kind == kind)
    ]


def opens(seen, kind=None):
    return [
        span
        for span in seen
        if span.phase == "open" and (kind is None or span.kind == kind)
    ]


#
# What it costs when nobody asked
#


def test_a_store_nobody_is_watching_reads_no_clock_and_builds_no_span(
    store, monkeypatch
):
    """The promise, and it is the easy half to lose. A library that sells
    itself on knowing what every call costs cannot charge three percent for an
    observability feature nobody switched on — so an unwatched store takes no
    timestamp, allocates no span and pushes nothing onto a stack, rather than
    building the object and throwing it away."""
    store.create(Walker)

    def counted() -> int:
        raise AssertionError("an unwatched store read a clock")

    def refused(**anything) -> None:
        raise AssertionError("an unwatched store built a span")

    monkeypatch.setattr(watching, "perf_counter_ns", counted)
    monkeypatch.setattr(watching, "Span", refused)

    assert store._watch is watching.UNWATCHED
    store.walkers.add(Walker(family_name="Hemingway"))
    store.walkers.find()
    store.walkers.count()


def test_an_observer_sees_the_statements_dray_wrote_and_not_your_own(watched, seen):
    """The boundary, and it is worth pinning rather than leaving to be
    discovered. Everything through a collection is dray's and is watched — a
    hand-written `select_many` body included, because dray sent it. A statement
    put down `store.conn` is the caller's, which is exactly what `cursor()`
    leaves alone deliberately, so somebody counting round trips knows which of
    their own calls are in the number."""
    watched.walkers.add(Walker(family_name="Hemingway"))
    seen.clear()

    watched.walkers.select_many("select id, family_name, etag, data from walker")
    assert len(closes(seen, "statement")) == 1

    with watched.conn.cursor() as cur:
        cur.execute("select count(*) from walker")
        assert cur.fetchone()[0] == 1
    assert len(closes(seen, "statement")) == 1


#
# The shape of what arrives
#


def test_a_span_arrives_twice_once_when_it_opens_and_once_when_it_closes(
    watched, seen
):
    """Parents open before their children, so a consumer can draw a tree
    top-down as it happens rather than assembling one after everything has
    finished. The cost is double the volume and a handler that ignores the half
    it does not want."""
    watched.walkers.count()

    assert [span.id for span in opens(seen)] == sorted(
        span.id for span in opens(seen)
    )
    assert {span.id for span in opens(seen)} == {
        span.id for span in closes(seen)
    }
    # Nothing measured on the way in: an open event knows only where it sits.
    assert all(span.elapsed_ns is None for span in opens(seen))
    assert all(span.sql is None for span in opens(seen))


def test_a_statement_carries_the_sql_and_the_parameters_as_they_were_sent(
    watched, seen
):
    """With `%s` still in it. dray hands over what it gave the driver and
    nothing further — it cannot know which column is a medical note, so
    interpolating for readability would be dray deciding what is safe to write
    down on behalf of somebody who has never told it."""
    watched.walkers.add(Walker(family_name="Hemingway"))
    seen.clear()

    watched.walkers.find(equals={"family_name": "Hemingway"})

    (statement,) = closes(seen, "statement")
    assert statement.sql == (
        "select id, family_name, etag, data from walker"
        " where family_name = %s order by id"
    )
    assert statement.params == ["Hemingway"]
    assert statement.rowcount == 1
    assert statement.error is None


def test_a_span_names_the_record_class_rather_than_leaving_it_in_the_sql(
    watched, seen
):
    """dray knows which record a statement is about, and a caller reduced to
    `sql.startswith("select id, family_name")` to find out is a bad outcome —
    *everything about walkers* should be `span.cls is Walker`."""
    watched.walkers.count()

    assert closes(seen, "statement")[0].cls is Walker


def test_a_read_separates_the_round_trip_from_building_the_records(watched, seen):
    """`hydrate` is the one number in the whole tree that is dray's own time
    rather than the database's, and it is the whole reason a statement is a
    span with children rather than a log line. A `find()` with no filter
    showing `execute 40ms, hydrate 3,900ms` is a completely different problem
    from a slow query, and from above the two used to look identical."""
    watched.walkers.add_all([Walker(family_name=f"W{n}") for n in range(20)])
    seen.clear()

    watched.walkers.find()

    (statement,) = closes(seen, "statement")
    (execute,) = closes(seen, "execute")
    (hydrate,) = closes(seen, "hydrate")
    assert execute.parent_id == statement.id
    assert hydrate.parent_id == statement.id
    assert hydrate.rowcount == 20
    # The statement is the honest total, so a consumer keeping only statements
    # gets the flat view without under-counting.
    assert statement.elapsed_ns >= execute.elapsed_ns + hydrate.elapsed_ns


def test_a_parent_stays_open_until_everything_under_it_has_closed(watched, seen):
    """Which is what makes the nesting free: nothing at a call site says a
    `hydrate` belongs to the statement above it — it belongs because the
    statement was still on the stack when it opened."""
    watched.walkers.add(Walker(family_name="Hemingway"))

    order = {span.id: n for n, span in enumerate(seen)}
    for span in closes(seen):
        if span.parent_id is None:
            continue
        parent = next(one for one in closes(seen) if one.id == span.parent_id)
        assert order[parent.id] > order[span.id], (
            f"a {parent.kind} closed before the {span.kind} inside it"
        )
        assert span.depth == parent.depth + 1


def test_the_two_clocks_are_not_used_for_each_others_job(watched, seen):
    """`at_ns` is monotonic and is the only one an elapsed is derived from —
    one clock across the process, which is what lets spans from several threads
    sit on a single axis. `wall` steps under NTP and can go backwards, so it is
    good for lining a span up against a request log and for nothing else."""
    before = time.time_ns()
    watched.walkers.count()
    after = time.time_ns()

    for span in closes(seen):
        opened = next(one for one in opens(seen) if one.id == span.id)
        assert span.elapsed_ns == span.at_ns - opened.at_ns
        assert span.elapsed_ns > 0
        assert before <= span.wall <= after


def test_a_statement_that_raised_is_reported_rather_than_dropped(watched, seen):
    """The one somebody debugging most wants to see, and the easiest to leave
    out by accident — an observer that only reports what worked is silent
    exactly when it is being read."""
    with pytest.raises(psycopg.errors.UndefinedTable):
        watched.walkers.select_many("select * from no_such_table")

    (statement,) = closes(seen, "statement")
    assert isinstance(statement.error, psycopg.errors.UndefinedTable)
    assert statement.sql == "select * from no_such_table"


#
# Spans a caller opens
#


def test_a_caller_span_puts_a_name_of_your_own_around_the_statements(
    watched, seen
):
    """The only kind dray does not open for itself, and the one that answers
    the question worth asking: not what each statement cost but what the page
    did, and where the rest of the time went when the statements only account
    for a tenth of it."""
    with watched.span("render the walker page"):
        watched.walkers.add(Walker(family_name="Hemingway"))
        watched.walkers.find()

    (page,) = closes(seen, "caller")
    assert page.label == "render the walker page"
    assert page.parent_id is None
    assert page.depth == 0
    assert all(
        span.parent_id is not None
        for span in closes(seen)
        if span.kind != "caller"
    )


def test_a_caller_span_costs_nothing_when_nobody_is_watching(store):
    """It has to be safe to leave in the code, which means an unwatched store
    runs the block and does not so much as build a context manager for it."""
    store.create(Walker)
    with store.span("render the walker page"):
        store.walkers.add(Walker(family_name="Hemingway"))
    assert store.walkers.count() == 1


#
# Transactions, which are the interesting thing on this database
#


def test_a_transaction_is_a_span_and_an_inner_block_is_not_a_second_one(
    watched, seen
):
    """*How long was this transaction open* is the question DSQL punishes you
    for getting wrong — five minutes and it is killed, and every millisecond is
    a wider window for the conflict that makes a replay fire. An observer that
    only saw statements could say the six of them took 40ms and not that the
    block around them was open for four seconds because a read waited on an
    HTTP call in the middle.

    An inner block joins rather than nesting, because DSQL has no `SAVEPOINT`
    for it to be a second transaction with — so calling it one here would put a
    span in the tree that nothing ever commits."""
    with watched.transaction():
        walker = watched.walkers.add(Walker(family_name="Hemingway"))
        walker.notes.add("inside the block")
        walker.save()

    (block,) = closes(seen, "transaction")
    assert {span.parent_id for span in closes(seen, "statement")} == {block.id}


def test_a_write_outside_a_block_opens_a_transaction_of_its_own(watched, seen):
    """dray's own, and it shows up as one. A save is a transaction whether or
    not anybody wrote `with`, which is what makes the replay safe and what makes
    a set above the row ceiling several of them."""
    watched.walkers.add(Walker(family_name="Hemingway"))
    watched.walkers.add(Walker(family_name="Shelley"))

    assert len(closes(seen, "transaction")) == 2


def test_a_batched_write_is_still_one_span_per_statement(watched, seen):
    """A set is sent as one round trip now, and the count must not follow it
    down. Counting these spans is how an N+1 is found, and a count that goes
    into somebody's suite is a number dray has to keep still: a write quietly
    reporting one span for twenty rows would break an assertion dray never made
    and take away the only view from which the rows are separately visible.

    The nesting goes with it, since a statement read back after its batch has
    landed could easily have ended up sitting somewhere else in the tree."""
    watched.walkers.add_all([Walker(family_name=f"W{n}") for n in range(20)])
    assert len(closes(seen, "statement")) == 20

    walker = watched.walkers.add(Walker(family_name="Hemingway"))
    for n in range(20):
        walker.notes.add(body=f"note {n}")
    seen.clear()
    walker.save()

    # The parent's update and one insert for each note that rode with it, all
    # of them inside the one transaction that carried them.
    (block,) = closes(seen, "transaction")
    assert len(closes(seen, "statement")) == 21
    assert {span.parent_id for span in closes(seen, "statement")} == {block.id}


def test_a_read_hoisted_out_of_a_block_is_one_less_statement_inside_it(
    watched, seen
):
    """The assertion the page hands a caller for keeping a read out of their own
    transaction, and the reason it is worth holding in a suite: against a
    cluster the same work with the read inside the block was refused about 1.6
    times as often, because the block stayed open for the round trip. Local
    PostgreSQL takes row locks and refuses nothing, so the rate is not something
    a test can see — the shape is, and the shape is what a person forgets.

    Every statement inside the block nests under its `transaction` span, so
    `depth > 0` is *inside the block* — which is what makes the assertion on the
    page a filter rather than anything dray had to add."""

    def reads_inside():
        return [
            span
            for span in closes(seen, "statement")
            if span.depth > 0 and span.sql.startswith("select")
        ]

    walker = watched.walkers.add(Walker(family_name="Hemingway"))

    seen.clear()
    with watched.transaction():
        inside = watched.walkers.by_id(walker.id)
        inside.family_name = "Shelley"
        inside.save()

    assert len(reads_inside()) == 1
    assert len([s for s in closes(seen, "statement") if s.depth > 0]) == 2

    seen.clear()
    hoisted = watched.walkers.by_id(walker.id)
    with watched.transaction():
        hoisted.family_name = "Woolf"
        hoisted.save()

    assert not reads_inside()
    assert [s.depth for s in closes(seen, "statement")] == [0, 1]


def test_a_replayed_transaction_says_which_attempt_it_was(
    watched, seen, monkeypatch
):
    """The field nothing else carries, because nothing else replays. DSQL
    refuses a commit that conflicted rather than blocking to avoid one, and dray
    runs the whole write again — so without this a write refused twice reads as
    three unrelated transactions and the thing that actually happened is
    invisible in the only place anybody would look for it.

    On the transaction rather than on a statement, because the replay owns the
    transaction and not any one statement inside it."""
    from dray.collection import Collection

    real = Collection._insert
    refused = iter([True])

    def refusing_once(self, cur, record, filled):
        if next(refused, False):
            raise psycopg.errors.SerializationFailure("conflicted (OC001)")
        return real(self, cur, record, filled)

    with monkeypatch.context() as patched:
        patched.setattr(Collection, "_insert", refusing_once)
        watched.walkers.add(Walker(family_name="Hemingway"))

    assert [span.attempt for span in closes(seen, "transaction")] == [1, 2]
    assert isinstance(
        closes(seen, "transaction")[0].error,
        psycopg.errors.SerializationFailure,
    )
    assert watched.walkers.count() == 1

    # And it is put back afterwards, or every write following a replayed one
    # would claim to have been replayed too.
    seen.clear()
    watched.walkers.add(Walker(family_name="Shelley"))
    assert [span.attempt for span in closes(seen, "transaction")] == [1]


#
# Preparing a write, which is dray's time too
#


def test_a_bulk_write_times_what_it_does_before_the_first_statement(
    watched, seen
):
    """Handlers filling fields in, every record's rules and every child's, and
    working out how many transactions this is. On a large set that is a real
    share of the wall clock, and it used to be indistinguishable from the
    database being slow."""
    watched.walkers.add_all([Walker(family_name=f"W{n}") for n in range(50)])

    (prepare,) = closes(seen, "prepare")
    assert prepare.rowcount == 50
    assert prepare.cls is Walker
    # Before the transaction, not inside it: the whole point of preparing up
    # front is that a bad value cannot leave half a set written.
    assert prepare.parent_id is None


#
# A handler that queries
#


def test_a_handler_that_queries_the_store_it_is_watching_raises(watched):
    """Unbounded otherwise: the query emits a span, which calls the handler,
    which queries. It would present as a hang rather than as an error, which is
    the worst way for a mistake this ordinary to show up. A `RuntimeError`
    rather than a `DrayError` — it is a programming mistake and not a refusal
    anybody writes an `except` for, so it has no place in the exception table."""

    def asks_the_store_it_is_watching(span):
        watched.walkers.count()

    watched._watch = watching.Watch(asks_the_store_it_is_watching)

    with pytest.raises(RuntimeError, match="already inside that handler"):
        watched.walkers.count()


def test_a_handler_that_does_not_query_is_left_alone(watched, seen):
    """The guard is per emission and put back afterwards, so one handler that
    reads its span and returns does not poison the next statement."""
    watched.walkers.count()
    watched.walkers.count()
    assert len(closes(seen, "statement")) == 2


#
# The collector
#


def test_the_collector_counts_the_statements_and_not_the_spans_around_them(
    watched
):
    """*Did this page do one read or six* is the question, and it is the one a
    callback cannot answer without everybody writing the same closure over a
    list. Statements by default for that reason: a tree of ten kinds is the
    right thing for a log and the wrong thing to assert a number about."""
    watched.walkers.add(Walker(family_name="Hemingway"))

    with watched.watching() as seen:
        watched.walkers.find()

    assert len(seen) == 1
    assert seen[0].kind == "statement"
    assert seen[0].sql.startswith("select")


def test_the_collector_can_be_asked_for_another_kind_or_for_all_of_them(
    watched
):
    """`kind="transaction"` is how long a block was open; `kind=None` is the
    whole tree, for a test about the shape rather than the count."""
    with watched.watching(kind="transaction") as blocks:
        with watched.transaction():
            watched.walkers.add(Walker(family_name="Hemingway"))

    assert len(blocks) == 1

    with watched.watching(kind=None) as everything:
        watched.walkers.count()

    assert {span.kind for span in everything} == {"statement", "execute"}


def test_a_misspelt_kind_is_refused_rather_than_collecting_nothing(watched):
    """A collector filtering on a word dray does not have comes back empty,
    which reads exactly like a page that did no reads — so the assertion passes
    and says nothing."""
    with pytest.raises(ValueError, match="statements"):
        with watched.watching(kind="statements"):
            pass


def test_a_collector_is_a_window_and_stops_when_the_block_does(watched):
    """A window rather than a subscription: what it caught is a plain sequence
    the moment the block ends, and nothing goes on collecting behind it."""
    with watched.watching() as seen:
        watched.walkers.count()

    was = len(seen)
    watched.walkers.count()
    assert len(seen) == was


def test_a_store_that_was_not_being_watched_goes_back_to_costing_nothing(
    store
):
    """The block turns one on and takes it away again, so a suite that measures
    one page does not leave every store after it paying for a clock."""
    store.create(Walker)
    with store.watching() as seen:
        store.walkers.count()

    assert len(seen) == 1
    assert store._watch is watching.UNWATCHED


def test_a_collector_on_one_store_does_not_see_another_stores_statements(
    postgresql_proc, store
):
    """Documented intent rather than a bug nobody got to. A store is one
    connection and one thread, so a collector with no pool behind it can only
    answer for its own — which is why the collector reaches through the pool
    when there is one, and why a fan-out over hand-built stores is several
    answers rather than one."""
    store.create(Walker)

    other = Store(
        psycopg.connect(
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            user=postgresql_proc.user,
            dbname=store.conn.info.dbname,
        ),
        records=[Walker],
    )

    with store.watching() as seen:
        store.walkers.count()
        other.walkers.count()

    assert len(seen) == 1
    other.close()


def test_a_collector_takes_spans_from_several_threads_without_losing_any():
    """The reason it ships rather than being five lines a caller writes. A page
    that fans out is several stores on several threads all reporting to the one
    collector, and the list everybody would write themselves is the thing that
    quietly drops one."""
    seen = watching.Seen()

    def one(n):
        return watching.Span(
            id=n,
            parent_id=None,
            depth=0,
            kind="statement",
            phase="close",
            at_ns=0,
            wall=0,
            thread_ident=threading.get_ident(),
            thread_name=threading.current_thread().name,
        )

    def report():
        for n in range(500):
            seen(one(n))

    threads = [threading.Thread(target=report) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert len(seen) == 2000
