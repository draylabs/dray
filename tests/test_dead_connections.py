"""
A connection closed underneath a live store, and what dray says about it.

DSQL closes every connection after about an hour, busy or idle. A store built by
hand holds the one connection it was given, so one kept past that — a warm
Lambda container reusing a module-level store, a job left running — fails on its
next statement with psycopg's own words:

    psycopg.OperationalError: the connection is closed

Which names neither the hour that closed it nor the shape that avoids it. It
happened in production hours after a deploy and took an afternoon to place, and
`@retrying` was no help: that catches a commit DSQL refused, and this connection
was not there to refuse anything.

No cluster and no hour are needed to reproduce it. `pg_terminate_backend` from a
second connection closes one from the server's side, which is what DSQL does and
what psycopg sees.
"""

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from dray import ConnectionLost, Pool, field, record
from dray.store import ready


@record(table="courier", collection="couriers")
class Courier:
    family_name: str = field()


@pytest.fixture
def store(store):
    """The suite's store with this file's table already made."""
    store.create(Courier)
    return store


def dsn_of(conn):
    info = conn.info
    return f"host={info.host} port={info.port} user={info.user} dbname={info.dbname}"


def closed_by_the_server(conn):
    """Close a connection from the other side, the way DSQL does at the hour.

    From a second connection, because being closed by somebody else is the whole
    point: `conn.close()` is the other case entirely, and the one this must not
    be mistaken for."""
    backend = conn.info.backend_pid
    with psycopg.connect(dsn_of(conn), autocommit=True) as killer:
        killer.execute("select pg_terminate_backend(%s)", [backend])


def test_a_read_says_what_dsql_does_rather_than_that_a_connection_is_closed(store):
    """`the connection is closed` was true and useless: it named neither the
    hour that closed it nor the `Pool` that would have retired it first."""
    closed_by_the_server(store.conn)

    with pytest.raises(ConnectionLost) as failed:
        store.couriers.find()

    said = str(failed.value)
    assert "about an hour" in said
    assert "dray.Pool" in said
    assert "short-lived by design" in said


def test_a_write_says_it_too_rather_than_failing_at_begin(store):
    """The gap this had to avoid. A write opens its transaction before it opens
    a cursor, so a message sitting only on the cursor would have covered every
    read and left `add` and `save` failing in psycopg's words."""
    closed_by_the_server(store.conn)

    with pytest.raises(ConnectionLost) as failed:
        store.couriers.add(Courier(family_name="Hemingway"))

    assert "about an hour" in str(failed.value)


def test_a_count_says_it_as_well(store):
    """`count` opens its own cursor rather than reading through `select_many`,
    so it is the read most easily left behind."""
    closed_by_the_server(store.conn)

    with pytest.raises(ConnectionLost):
        store.couriers.count()


def test_the_psycopg_error_is_still_underneath_it(store):
    """Nothing is swallowed. Whoever wants to know what the driver actually saw
    — an admin shutdown, an EOF — still has it on the traceback."""
    closed_by_the_server(store.conn)

    with pytest.raises(ConnectionLost) as failed:
        store.couriers.find()

    assert isinstance(failed.value.__cause__, psycopg.OperationalError)


def test_it_is_still_an_operational_error(store):
    """A better sentence rather than a new thing to catch, so an application
    already handling a dead connection by its psycopg type keeps working."""
    closed_by_the_server(store.conn)

    with pytest.raises(psycopg.OperationalError):
        store.couriers.find()


def test_a_store_the_caller_closed_is_not_told_its_connection_aged_out(store):
    """The distinction worth being careful about. `store.close()` and then a
    statement is a store used after it was finished with — a mistake of another
    kind — and psycopg's `the connection is closed` is already the truth about
    it. Sending somebody to look for an hour that never passed would be worse
    than saying nothing. psycopg's `broken` is what tells the two apart: closed,
    but not by whoever was holding it."""
    store.close()

    with pytest.raises(psycopg.OperationalError) as failed:
        store.couriers.find()

    assert not isinstance(failed.value, ConnectionLost)
    assert "hour" not in str(failed.value)


def test_a_write_on_a_store_the_caller_closed_is_left_alone_too(store):
    """The same distinction at the other seam: a write meets a closed connection
    at the transaction rather than at the cursor."""
    store.close()

    with pytest.raises(psycopg.OperationalError) as failed:
        store.couriers.add(Courier(family_name="Hemingway"))

    assert not isinstance(failed.value, ConnectionLost)


def test_an_error_that_did_not_kill_the_connection_is_left_alone(store):
    """Which is why the connection is asked about rather than the exception.
    dray renames the failure that took the connection with it and nothing else;
    a statement that is simply wrong stays psycopg's to report."""
    with pytest.raises(psycopg.errors.UndefinedTable):
        store.couriers.select_many("select * from no_such_table")


def test_a_pooled_connection_that_is_dead_gets_the_same_message(postgresql):
    """A pool retires its connections before DSQL closes them, so this should
    not arise — but a connection can die for reasons that are not the hour, and
    when one does the message is still the right one to give."""
    made = ConnectionPool(
        dsn_of(postgresql), min_size=1, max_size=2, configure=ready, open=False
    )
    pool = Pool(made, records=[Courier])
    try:
        with pool.store() as store:
            store.create(Courier)
            closed_by_the_server(store.conn)

            with pytest.raises(ConnectionLost) as failed:
                store.couriers.find()

            assert "about an hour" in str(failed.value)
    finally:
        made.close()
