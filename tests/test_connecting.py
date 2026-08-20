"""
What `Store.connect` hands the driver.

The one function in dray that needs a cluster, and so the one nothing could
check. That is how `user=` came to be a parameter that could not work: dray
minted an admin token whatever name it was given, so anything but `admin` was
refused by DSQL with `Wrong user to action mapping`. Nothing failed here,
because nothing here ran it.

The connector is stubbed rather than reached. What is worth pinning down is the
arguments — which is exactly what was wrong.
"""

import sys
import types

import pytest

from dray import Store
from dray.store import region_of


@pytest.fixture
def connecting(monkeypatch):
    """A stand-in for `aurora_dsql_psycopg`, recording what it was handed."""
    given = {}

    def connect(**kwargs):
        given.update(kwargs)
        # Enough of a connection for `Store.__init__`: it switches autocommit
        # and reads the transaction status on the way past.
        return types.SimpleNamespace(
            autocommit=True,
            closed=False,
            info=types.SimpleNamespace(transaction_status=0),
        )

    monkeypatch.setitem(
        sys.modules,
        "aurora_dsql_psycopg",
        types.SimpleNamespace(connect=connect),
    )
    return given


HOST = "ab12cd.dsql.ap-southeast-2.on.aws"


def test_the_user_is_passed_through_rather_than_assumed(connecting):
    """AWS's connector picks the token type from the name — `admin` gets an
    admin token and everything else gets `DbConnect`, which is what a scoped
    role needs. dray minting one kind by hand is what made this parameter a
    lie."""
    Store.connect(host=HOST, user="orders_app")
    assert connecting["user"] == "orders_app"


def test_admin_is_the_default(connecting):
    Store.connect(host=HOST)
    assert connecting["user"] == "admin"


def test_the_region_comes_out_of_the_hostname(connecting):
    Store.connect(host=HOST)
    assert connecting["region"] == "ap-southeast-2"


def test_a_region_given_outright_wins(connecting):
    Store.connect(host=HOST, region="us-east-1")
    assert connecting["region"] == "us-east-1"


def test_the_connection_is_verified_and_not_merely_encrypted(connecting):
    """`require` encrypts and checks nothing, so anything that can put itself
    in the path goes unnoticed. Neither libpq nor the connector defaults to
    more than that, so it is dray's to say."""
    import os

    Store.connect(host=HOST)
    assert connecting["sslmode"] == "verify-full"
    # A real bundle, and one that is already installed: minting the token needs
    # botocore, and these are the roots it trusted to do that. `system` reads
    # tidier and does not work — libpq goes through OpenSSL, which does not
    # consult the macOS keychain, so it fails against a cluster the machine
    # trusts perfectly well.
    assert os.path.exists(connecting["sslrootcert"])


def test_the_connection_is_autocommit_from_the_start(connecting):
    """dray opens its own transactions, and `transaction()` reads the
    connection's status to decide whether it is already inside one — which only
    means what it should under autocommit."""
    Store.connect(host=HOST)
    assert connecting["autocommit"] is True


def test_anything_else_reaches_the_driver(connecting):
    Store.connect(host=HOST, connect_timeout=5)
    assert connecting["connect_timeout"] == 5


def test_saying_anything_about_ssl_replaces_the_pair(connecting):
    """Replaced rather than merged, because half of somebody else's opinion is
    no opinion at all — and because libpq refuses `sslmode=require` alongside
    an `sslrootcert`, so merging turns overriding one into an error about the
    other. Verified against a cluster, where it failed."""
    Store.connect(host=HOST, sslmode="require")
    assert connecting["sslmode"] == "require"
    assert "sslrootcert" not in connecting


def test_a_deployment_with_its_own_bundle_says_so(connecting):
    Store.connect(
        host=HOST, sslmode="verify-full", sslrootcert="/etc/ssl/certs/amazon.pem"
    )
    assert connecting["sslrootcert"] == "/etc/ssl/certs/amazon.pem"


#
# The region a hostname carries
#


def test_a_region_is_read_from_the_hostname():
    assert region_of(HOST) == "ap-southeast-2"


def test_a_hostname_with_no_region_in_it_is_refused():
    """Rather than connecting somewhere unintended: a host and a region that
    disagree is a confusing way to fail."""
    with pytest.raises(ValueError, match="cannot read a region"):
        region_of("localhost")
