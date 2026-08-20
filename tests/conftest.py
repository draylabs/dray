"""
A real PostgreSQL, started by pytest and thrown away afterwards.

No fakes. A fake store can only tell you the fake works, and the SQL is the part
most likely to be wrong. Local PostgreSQL is not DSQL — serialization conflicts
and the row ceiling belong to a tier that runs against a real cluster — but the
schema and every statement are identical.

**And so is the sort order, which it is not by default.** DSQL collates as `C`
and `initdb` collates as whatever the machine says, so `'Z' < 'a'` is true on
the cluster and false in development — which makes every green test
about `order_by` on a text column a statement about an order the deployment does
not use. `scripts/against_dsql.py` pins the cluster's half of that. This file is
the other half: the database each test runs on is made to collate the same way.

It cannot be asked for. `pytest-postgresql` never passes `--locale` to `initdb`,
and the environment does not split by category — `LC_COLLATE=C` is overruled by
`LC_CTYPE` and the whole cluster comes out in one locale. So the cluster stays as
it was and the database is made from `template0`, which is the one template that
may be given a collation of its own.
"""

import uuid

import psycopg
import pytest

from dray import Store


def _dsn(proc, dbname: str) -> str:
    """Where the cluster pytest started is, and which database to open on it."""
    return (
        f"host={proc.host} port={proc.port} user={proc.user}"
        f" dbname={dbname}"
    )


@pytest.fixture
def postgresql(postgresql_proc):
    """
    A connection to a database of this test's own, collating as `C`.

    This shadows `pytest-postgresql`'s fixture of the same name deliberately, so
    that everything downstream — the `store` below, and the handful of test files
    that take the connection directly — gets the same collation without having to
    know that is what they wanted.
    """
    name = f"dray_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_dsn(postgresql_proc, "postgres"), autocommit=True) as admin:
        admin.execute(
            f'create database "{name}" template template0'
            " encoding 'UTF8' lc_collate 'C' lc_ctype 'C'"
        )
    conn = psycopg.connect(_dsn(postgresql_proc, name))
    try:
        yield conn
    finally:
        conn.close()
        with psycopg.connect(_dsn(postgresql_proc, "postgres"), autocommit=True) as a:
            # `with (force)` because a test may leave a second connection of
            # its own open — two of them do deliberately, to prove a store is
            # one connection — and a plain drop is refused while anybody is on
            # it. This is the same answer `pytest-postgresql`'s own janitor
            # gives, and the database is going in the bin either way.
            a.execute(f'drop database "{name}" with (force)')


@pytest.fixture
def store(postgresql):
    """A store on a fresh database, with nothing created yet. A test declares
    the records it needs and calls `store.create(...)`."""
    return Store(postgresql)
