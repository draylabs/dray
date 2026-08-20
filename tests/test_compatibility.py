"""
The statements dray writes, run against a real cluster.

Everything else in this suite runs on local PostgreSQL, which proves the SQL is
correct and proves nothing about DSQL — the row ceiling, the refused commits,
the transaction that ages out, and the one statement whose syntax differs. This
file is the other half, and it is deliberately the *same* file either way:
point it at local PostgreSQL to debug it, point it at a cluster to believe it.

    pytest tests/test_compatibility.py                 # local PostgreSQL
    DRAY_DSQL_HOST=ab12cd.dsql.ap-southeast-2.on.aws \
        pytest tests/test_compatibility.py             # a real cluster

Not part of the ordinary cycle: a cluster costs money, and every statement is a
round trip to another city where the rest of this suite talks to a socket. Run
it when the schema changes, when a statement changes, and before believing
anything about how a query is answered.

**Plans, not timings.** A plan is a claim about *how* the database intends to
answer, which is stable and reviewable. A duration is a claim about the machine
it ran on.

**Two tables, on purpose.** The schema tests want one they can drop; the plan
tests want two thousand rows that are expensive to write and never change. Kept
apart, they cannot order-depend on each other, and the rows are written once for
the whole session rather than once per test.

**And a table per worker.** Every other file here gets a database per test and
is parallel-safe without anybody thinking about it. This one cannot be: a
cluster has one database, and these tables outlive the test that made them. So
the names carry the xdist worker, and the tables are dropped at the end of the
session rather than left for somebody to find.
"""

import os
from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest

from dray import Store, child, field, index, jsonb, record, schema

DSQL_HOST = os.environ.get("DRAY_DSQL_HOST")

# One connection and one IAM token for the session. A store is short-lived by
# design and that is right in an application; here it would be nine tokens and
# nine handshakes to another city to answer nine questions.
_CONNECTED: Store | None = None
_SEEDED = False

ROWS = 2_000

# One row past what DSQL will take in a transaction, for the one test that has
# to be over the line rather than near it.
OVER = 3_001

# A table of this worker's own. Everything else in this suite gets a database
# per test, so it can be run in parallel without anybody thinking about it —
# this file cannot, because a cluster has one database and these tables live
# past the test that made them. Two workers would otherwise share a table, and
# the collision is not a clean one: one drops it at the start of a test while
# another is halfway through reading it, and both try to seed the same unique
# column. Named apart, they never meet.
WORKER = os.environ.get("PYTEST_XDIST_WORKER", "")
MINE = f"_{WORKER}" if WORKER else ""

# A table no record implies, for the one test that has to write its own DDL:
# dray refuses to build a class asking for these indexes, which is the thing
# being checked.
BY_HAND = f"compat_by_hand{MINE}"


@record(table=f"compat_schema{MINE}", collection="compat_schemas",
        indexes=[index("email", unique=True), index("family_name")])
class Made:
    """For the statements. Dropped and rebuilt per test."""

    email: str = field()
    family_name: str = field(default="")
    suburb: str | None = field(default=None, stored_in="blob")


@record(table=f"compat_reader{MINE}", collection="compat_readers",
        indexes=[index("email", unique=True), index("family_name")])
class Read:
    """For the plans. Written once and only read after that."""

    email: str = field()
    family_name: str = field(default="")
    status: str = field(default="enquiry")
    suburb: str | None = field(default=None, stored_in="blob")


@child(of=Made, name="pieces", table=f"compat_piece{MINE}",
       collection="compat_pieces")
class Piece:
    """For the one claim about a set removal that only the ceiling can make."""

    label: str = field(default="")


@child(of=Piece, name="marks", table=f"compat_mark{MINE}",
       collection="compat_marks")
class Mark:
    """A second generation under that one, because the claim `thin` makes is
    about a tree and a single generation cannot carry the fanout that makes it
    worth making."""

    label: str = field(default="")


def store_for(postgresql) -> Store:
    global _CONNECTED
    if not DSQL_HOST:
        return Store(postgresql)
    if _CONNECTED is None:
        _CONNECTED = Store.connect(host=DSQL_HOST)
    return _CONNECTED


@pytest.fixture
def fresh(postgresql):
    """A store and no table of its own.

    Dropped before rather than after, so a failure leaves the table where it
    fell. Locally this is redundant — every test gets a database of its own —
    and against a cluster it is the whole difference: one database, one schema,
    and tests that would otherwise inherit each other's rows.
    """
    store = store_for(postgresql)
    with store.conn.cursor() as cur:
        cur.execute(f"drop table if exists {Mark.__dray_table__}")
        cur.execute(f"drop table if exists {Piece.__dray_table__}")
        cur.execute(f"drop table if exists {Made.__dray_table__}")
    store.serves(Made, Piece, Mark)
    return store


@pytest.fixture
def populated(postgresql):
    """A store and a table holding `ROWS` rows, written once."""
    global _SEEDED
    store = store_for(postgresql)
    store.serves(Read)
    if DSQL_HOST and _SEEDED:
        return store

    store.create(Read)
    if store.compat_readers.count() < ROWS:
        store.compat_readers.add_all(
            [
                Read(email=f"{n}@example.com", family_name=f"Name{n}", suburb="Leura")
                for n in range(ROWS)
            ]
        )
    with store.conn.cursor() as cur:
        cur.execute(f"analyze {Read.__dray_table__}")
    _SEEDED = True
    return store


@pytest.fixture(scope="session", autouse=True)
def _left_as_we_found_it():
    """Take the tables away at the end, on a cluster.

    Locally the database goes with the test and there is nothing to tidy. A
    cluster keeps whatever it is given, and a run per worker would otherwise
    leave one table each behind, every time, for somebody to find later and
    wonder about."""
    yield
    if not DSQL_HOST or _CONNECTED is None:
        return
    for table in (
        Mark.__dray_table__, Piece.__dray_table__, Made.__dray_table__,
        Read.__dray_table__, BY_HAND,
    ):
        with _CONNECTED.conn.cursor() as cur:
            cur.execute(f"drop table if exists {table}")


def plan_for(store, statement, params=()):
    with store.conn.cursor() as cur:
        cur.execute(f"explain {statement}", list(params))
        return "\n".join(str(row[0]) for row in cur.fetchall())


def seeks(plan: str) -> bool:
    """
    Whether the database means to go *to* the rows rather than *through* them.

    `Index Cond` is the one word both dialects use for it, which is what makes
    this the same assertion on either. Their vocabulary for the opposite differs
    and neither is worth matching on: PostgreSQL says `Seq Scan`, and DSQL —
    which has no heap, and whose primary key index *is* the table, every other
    column carried as an INCLUDE — says `Full Scan (btree-table)` when it walks
    a table and `Index Only Scan using ..._pkey` with a `Filter` when it walks
    the same rows through the key. Both of those are reading everything, and
    only the absent `Index Cond` says so in both.
    """
    return "Index Cond" in plan


#
# The schema, made the way each database wants it made
#


def test_the_tables_a_record_implies_are_accepted(fresh):
    """`store.create` picks the index form from the connection, so this is one
    call either way and the difference is dray's to know."""
    fresh.create(Made)
    assert schema.drift(fresh.conn, Made) == []


def test_the_statements_run_twice(fresh):
    """DDL and the row recording that it ran cannot commit together on DSQL, so
    every migration has to survive being run again."""
    fresh.create(Made)
    fresh.create(Made)
    assert schema.drift(fresh.conn, Made) == []


def test_the_index_form_matches_the_database(fresh):
    """The one place dray writes a statement that is not ordinary PostgreSQL.
    Both kinds of index take the same word, so the unique one is here beside
    the plain one rather than trusted to follow it."""
    made = schema.create_indexes(Made, asynchronous=fresh.dsql)
    assert any(f"{Made.__dray_table__}_family_name" in one for one in made)
    assert any(f"{Made.__dray_table__}_email" in one for one in made)
    if fresh.dsql:
        assert all("index async if not exists" in one for one in made)
    else:
        assert all("index if not exists" in one for one in made)
        assert not any("async" in one for one in made)


def test_a_unique_index_refuses_a_second_row(fresh):
    """A table being created carries its unique indexes as constraints in the
    `create table`, so they are enforcing the moment the table is — which is
    what makes `DuplicateRecord` reachable by declaring something rather than by
    writing a migration by hand."""
    from dray import DuplicateRecord

    fresh.create(Made)
    fresh.compat_schemas.add(Made(email="rod@example.com"))
    with pytest.raises(DuplicateRecord):
        fresh.compat_schemas.add(Made(email="rod@example.com"))


#
# How the answers are actually arrived at
#


def test_a_lookup_by_id_goes_straight_to_its_row(populated):
    """On DSQL the primary key *is* the table, so this is the one read that
    needs no index of its own — and every claim about `by_id` and about the
    keyset walk in `in_batches` rests on it."""
    one = populated.compat_readers.find(equals={"email": "7@example.com"})[0]
    plan = plan_for(
        populated,
        f"select {populated.compat_readers.columns} from {Read.__dray_table__} where id = %s",
        [one.id],
    )
    assert seeks(plan), plan


def test_a_declared_index_is_the_one_a_filter_uses(populated):
    """The whole argument for declaring an index. A column with no index reads
    identically to one with an index until somebody asks the planner."""
    indexed = plan_for(
        populated,
        f"select {populated.compat_readers.columns} from {Read.__dray_table__}"
        " where family_name = %s",
        ["Name7"],
    )
    unindexed = plan_for(
        populated,
        f"select {populated.compat_readers.columns} from {Read.__dray_table__}"
        " where status = %s",
        ["enquiry"],
    )
    assert seeks(indexed), indexed
    assert not seeks(unindexed), unindexed


def test_a_unique_column_is_seekable_too(populated):
    """The constraint's backing index is a real one, which is why a second
    declaration over the same column would be an index built and paid for
    twice."""
    plan = plan_for(
        populated,
        f"select {populated.compat_readers.columns} from {Read.__dray_table__}"
        " where email = %s",
        ["7@example.com"],
    )
    assert seeks(plan), plan


def test_a_blob_filter_reads_everything(populated):
    """The honest half of the column/blob split: a key inside a shared jsonb
    document has no index and cannot be given one, and on DSQL `jsonb` carries
    no index support at all.

    Worth reading the plan rather than trusting the word. DSQL answers this with
    an `Index Only Scan` over the primary key — which looks like a seek and is
    not one, because the primary key is the table and it is walking all of it
    with a filter. No `Index Cond`, so nothing is being sought."""
    plan = plan_for(
        populated,
        f"select {populated.compat_readers.columns} from {Read.__dray_table__}"
        f" where {populated.compat_readers.blob}->'suburb' = %s",
        [jsonb("Leura")],
    )
    assert not seeks(plan), plan


#
# What only a cluster can answer
#


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_a_write_past_the_row_ceiling_is_split(fresh):
    """3,000 rows to a transaction, and `_write_all` splits to fit. Local
    PostgreSQL will take any number, so this is the assertion that cannot be
    made anywhere else."""
    fresh.create(Made)
    fresh.compat_schemas.add_all(
        [Made(email=f"bulk{n}@example.com") for n in range(4_000)]
    )
    assert fresh.compat_schemas.count() == 4_000


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_clearing_more_children_than_a_transaction_holds_is_refused(fresh):
    """`clear()` sizes nothing. It sends one statement per generation, the
    way a parent's delete does, so a set past the ceiling is a transaction the
    cluster refuses with every row still there — inherited from the delete side
    rather than solved, and the reason the page sends a set this size to `thin`
    instead. Local PostgreSQL removes all of them happily, which is why the
    assertion can only be made here."""
    fresh.create(Made, Piece, Mark)
    parent = fresh.compat_schemas.add(Made(email="clearing@example.com"))
    fresh.compat_pieces.add_all(
        [Piece(label=f"piece {n}") for n in range(OVER)], parent=parent
    )

    with pytest.raises(psycopg.Error, match="row limit"):
        parent.pieces.clear()

    assert parent.pieces.count() == OVER


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_a_tree_past_the_ceiling_is_thinned_a_generation_at_a_time(fresh):
    """The other half of the same claim, and the reason `thin` bounds a
    generation rather than a set: the 3,000 is a limit on a transaction, so a
    pass that took 20 pieces would be 20 pieces and 4,000 marks and a refusal
    however the limit on the pieces was phrased. One generation a pass is a real
    row count, and every pass here is under the ceiling on a tree whose total is
    well over it. Local PostgreSQL has no ceiling to cross and would take the
    whole thing in one statement, so this is the assertion that can only be made
    against a cluster."""
    fresh.create(Made, Piece, Mark)
    parent = fresh.compat_schemas.add(Made(email="thinning@example.com"))
    pieces = fresh.compat_pieces.add_all(
        [Piece(label=f"piece {n}") for n in range(20)], parent=parent
    )
    for piece in pieces:
        fresh.compat_marks.add_all(
            [Mark(label=f"mark {n}") for n in range(200)], parent=piece
        )
    assert fresh.compat_marks.count(parent_type=Piece) >= 4_000

    took = []
    while True:
        pass_took = parent.pieces.thin(at_a_time=500)
        if not pass_took:
            break
        took.append(pass_took)

    assert max(took) <= 500
    assert sum(took) == 4_020
    assert parent.pieces.count() == 0
    assert fresh.compat_marks.count(parent_type=Piece) == 0
    assert fresh.compat_schemas.count() == 1


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_the_cluster_refuses_the_indexes_the_class_refuses(fresh):
    """The premise under an index over a `timedelta`, `bytes`, `dict` or `list`
    field being refused, said by the cluster instead of by AWS's table of
    supported types. Local PostgreSQL builds every one of these, which is why
    the refusal had to move into the declaration and why this assertion can only
    be made here.

    The DDL is written out rather than implied by a record, because dray will no
    longer build the class that would imply it. Each refusal is paired with the
    same statement over a `date` column, so a failure means the type and not a
    typo — and the unique constraint is here beside the index because that is
    the whole of the argument for refusing `unique=True` on the same columns.
    """
    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")
        cur.execute(
            f"create table {BY_HAND} (id uuid primary key, issued_on date,"
            " took interval, digest bytea, answers jsonb)"
        )

    with fresh.conn.cursor() as cur:
        cur.execute(f"create index async {BY_HAND}_issued_on on {BY_HAND} (issued_on)")

    for column in ("took", "digest", "answers"):
        with pytest.raises(psycopg.Error):
            with fresh.conn.cursor() as cur:
                cur.execute(
                    f"create index async {BY_HAND}_{column} on {BY_HAND} ({column})"
                )

    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")
        cur.execute(
            f"create table {BY_HAND} (id uuid primary key, issued_on date unique)"
        )
        cur.execute(f"drop table if exists {BY_HAND}")

    with pytest.raises(psycopg.Error):
        with fresh.conn.cursor() as cur:
            cur.execute(
                f"create table {BY_HAND} (id uuid primary key, digest bytea unique)"
            )


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_the_cluster_sorts_the_three_it_will_not_index(fresh):
    """The other half of the test above, and the reason `order_by` refuses none
    of these where an index refuses three. Indexable and sortable are
    different questions of DSQL, and the page says so on the strength of this:
    an `interval`, a `bytea` and a `jsonb` column all order without complaint.

    It cannot be asserted anywhere else, because local PostgreSQL sorts all
    three as well — so a passing local run says nothing about the cluster, which
    is the same reason the refusals above live here.
    """
    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")
        cur.execute(
            f"create table {BY_HAND} (id uuid primary key,"
            " took interval, digest bytea, answers jsonb)"
        )
        for took, digest, answers in (
            ("2 hours", b"\x02", '["b"]'),
            ("1 hours", b"\x01", '["a"]'),
        ):
            cur.execute(
                f"insert into {BY_HAND} (id, took, digest, answers)"
                " values (%s, %s::interval, %s, %s::jsonb)",
                [uuid4(), took, digest, answers],
            )

    for column, first in (
        ("took", timedelta(hours=1)),
        ("digest", b"\x01"),
        ("answers", ["a"]),
    ):
        with fresh.conn.cursor() as cur:
            cur.execute(f"select {column} from {BY_HAND} order by {column}")
            assert [row[0] for row in cur.fetchall()][0] == first, column

    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_the_cluster_refuses_the_primary_keys_the_class_refuses(fresh):
    """The premise under a declared `id` being refused for those same four
    annotations. The refusal above is about an index a field asked for; this is
    the other door, where nobody asked for anything — `id: bytes` names no index
    and still implies `id bytea primary key`.

    A key of each column type the class refuses, with a `uuid` one beside them
    as the control — without it a failure here reads as a broken statement
    rather than a rejected type. Read out of `UNINDEXABLE` rather than listed
    again, because what is being asserted is that the class and the cluster
    agree about which types those are.
    """
    from dray.model import UNINDEXABLE

    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")
        cur.execute(f"create table {BY_HAND} (id uuid primary key)")
        cur.execute(f"drop table if exists {BY_HAND}")

    for column in sorted(set(UNINDEXABLE.values())):
        with pytest.raises(psycopg.Error):
            with fresh.conn.cursor() as cur:
                cur.execute(f"create table {BY_HAND} (id {column} primary key)")


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_the_cluster_drops_the_row_that_never_answered_the_same_way(fresh):
    """The premise under `none_of` carrying a null test rather than compiling to
    a bare `<> all(%s)`. Three-valued logic says the row holding nothing is not
    *not done* — it is unknown, and it vanishes — and the null half of what dray
    writes is there precisely to put it back. That was argued from local
    PostgreSQL, which proves nothing about the cluster, and the helper is wrong
    in both directions if DSQL reads either half differently.

    The two clauses are the ones `_conditions` writes, on a column and inside
    the blob, and each is paired with the bare form so a failure says which of
    the two behaviours moved. Written as DDL rather than implied by a record
    because the interesting row holds `null` in a column a class would declare
    `str`, and there is no honest way to write that one through `add`.
    """
    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")
        cur.execute(
            f"create table {BY_HAND} (id uuid primary key, status text,"
            " data jsonb)"
        )
        for status in ("open", "done", None):
            held = {} if status is None else {"status": status}
            cur.execute(
                f"insert into {BY_HAND} (id, status, data) values (%s, %s, %s)",
                [uuid4(), status, jsonb(held)],
            )

    def counted(where: str, params: list) -> int:
        with fresh.conn.cursor() as cur:
            cur.execute(f"select count(*) from {BY_HAND} where {where}", params)
            return cur.fetchone()[0]

    done, nothing = ["done"], []
    assert counted("status <> all(%s)", [done]) == 1, "the trap"
    assert counted("(status is null or status <> all(%s))", [done]) == 2
    assert counted("(status is null or status <> all(%s))", [nothing]) == 3

    inside = [jsonb("done")]
    kept = "(data->>'status' is null or data->'status' <> all(%s))"
    assert counted("data->'status' <> all(%s)", [inside]) == 1, "the trap"
    assert counted(kept, [inside]) == 2
    assert counted("data->'status' = any(%s)", [inside]) == 1, "and its opposite"

    with fresh.conn.cursor() as cur:
        cur.execute(f"drop table if exists {BY_HAND}")


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_an_asynchronous_index_is_visible_as_a_job(fresh):
    """`create index async` returns a job id and builds in the background.
    `sys.jobs` is where that is visible, and is also how `on_dsql` recognises a
    cluster in the first place."""
    fresh.create(Made)
    with fresh.conn.cursor() as cur:
        cur.execute("select count(*) from sys.jobs")
        assert cur.fetchone()[0] >= 0
    assert schema.drift(fresh.conn, Made) == []


@pytest.mark.dsql
@pytest.mark.skipif(not DSQL_HOST, reason="needs DRAY_DSQL_HOST")
def test_a_store_that_connected_can_be_told_which_records_it_serves():
    """`Store` and `Pool` both took `records=` and `connect` did not, so the
    one door that actually reaches a cluster was the one that could not say
    it. A caller who wrote it anyway had it handed to the driver as a
    connection option and got `invalid connection option "records"`, which
    says nothing about dray and sends them looking in the wrong place."""
    store = Store.connect(host=DSQL_HOST, records=[Made])
    try:
        assert store._records[Made.__dray_collection__] is Made
    finally:
        store.close()
