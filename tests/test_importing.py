"""
What `import dray` puts within reach.

Every other file here imports the names it wants and so proves only that they
exist for somebody who already knows to ask. This is the other direction: a
developer with the manual and an interpreter, typing `dir(dray)` to see what
there is.

The first check runs in a subprocess, because the question is what a fresh
interpreter sees. Importing a submodule binds it on the package for the rest of
the session, so a single `from dray import schema` anywhere else in this suite
would leave an in-process check passing with the export taken back out — which
is exactly the mistake the export exists to correct.
"""

import importlib.util
import pathlib
import subprocess
import sys
from datetime import date
from importlib.metadata import version as metadata_version
from typing import Any

import dray
from dray import Collection, collection, field, record, schema
from dray.child import ChildSet


def test_the_schema_module_is_in_the_package_namespace():
    """`from dray import schema` has always worked, so the module was there and
    nothing said so: three developers building from the manual alone started
    with `dir(dray)`, did not find it, and learned about `statements`, `drift`
    and `promote` only from the prose."""
    looked = subprocess.run(
        [sys.executable, "-c", "import dray; print('schema' in dir(dray))"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert looked.stdout.strip() == "True"


def test_the_package_counts_schema_among_what_it_offers():
    """`__all__` is the answer to what dray is handing out, and a module reached
    only by knowing its name is not being handed out at all."""
    assert "schema" in dray.__all__


@record(table="importing_event", collection="importing_events")
class Event:
    name: str
    starts_on: date
    suburb: str | None = field(default=None, stored_in="blob")


@collection(of=Event)
class Events:
    def on(self, day: date) -> list:
        return self.find(equals={"starts_on": day})

    def upcoming(self) -> list:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where starts_on >= current_date"
        )


class Reporting:
    """The shared half of two collections, of the sort that goes on a base
    class once the second one wants it. `count` is there because it is one of
    dray's own words and the question of who wins is a real one."""

    def summary(self) -> str:
        return "summary"

    def count(self, **kw: Any) -> str:
        return "the base class counted it"


@record(table="importing_report", collection="importing_reports")
class Report:
    name: str


@collection(of=Report)
class Reports(Reporting):
    pass


@record(table="importing_ledger", collection="importing_ledgers")
class Ledger:
    name: str


@collection(of=Ledger)
class Ledgers(Reports):
    pass


@record(table="importing_tally", collection="importing_tallies")
class Tally:
    name: str


@collection(of=Tally)
class Tallies(Collection):
    pass


def test_the_package_hands_out_the_names_a_record_class_answers_with():
    """Code holding record classes rather than collections reaches for
    `cls.__dray_table__`, which is past the documented surface and looks like
    an ordinary attribute while it does so. `names_of` is the door, and `Names`
    is what comes back for annotating, as `Collection` and `Record` are."""
    assert "names_of" in dray.__all__
    assert "Names" in dray.__all__


def test_the_package_hands_out_neither_retrying_nor_the_blob_s_default_name():
    """An export the page does not mention is worse than a missing one:
    `retrying` has a docstring of its own, so a reader who finds it has no way
    to tell whether the export or the page is the stale half, and the safe
    reading is to use neither. It is the export that is wrong. `@retrying`
    catches the `SerializationFailure`
    that `store.transaction()` has already turned into a `CommitRefused`, so on
    a caller's own function it replays nothing and looks like it works.
    `BLOB_COLUMN` is the other half — the word dray falls back to when a class
    says nothing, which is never the answer for a class that said something."""
    assert "retrying" not in dray.__all__
    assert "BLOB_COLUMN" not in dray.__all__
    assert "replaying" in dray.__all__


def test_the_blob_s_default_name_is_not_in_the_package_namespace_either():
    """`retrying` stays bound because the page sends a reader to `dir(dray)` to
    see both it and `replaying` and be told which is theirs. Nothing says
    anything about `BLOB_COLUMN`, and a class that moved its blob is exactly the
    one for which the constant is wrong — so it sits in `dray.model` with the
    four defaults it belongs to, and a caller who wants the name of a blob
    column asks the collection for `self.blob`."""
    assert not hasattr(dray, "BLOB_COLUMN")
    assert dray.model.BLOB_COLUMN == "data"


def reference_page() -> Any:
    """The maps `scripts/reference.py` draws the reference page from.

    Loaded by path because `scripts/` is not a package. Importing it builds the
    page and writes nothing — the write sits behind `__main__` so that this
    check cannot be the thing that rewrites the file it is checking.
    """
    path = (pathlib.Path(__file__).resolve().parent.parent
            / "scripts" / "reference.py")
    spec = importlib.util.spec_from_file_location("reference_page", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_reference_page_describes_every_function_the_package_hands_out():
    """The page tells a reader that every member on it came out of the library,
    and that was true of the five cards and of nothing else. The cards diff
    themselves against the class they drew; the panels of module-level
    functions were lists somebody typed, with nothing reading `__all__` and
    nothing comparing. Twelve exports were missing before anybody counted, and
    `names_of` is how they were found: it shipped in a pull request that edited
    `scripts/reference.py` in the same breath, and the page still came out a
    name short with nothing anywhere going red.

    Equality, not containment, because a name written on a panel and no longer
    exported is the same page lying in the other direction.
    """
    page = reference_page()
    written = ({page.bare(n) for n, *_ in page.DECORATORS}
               | {page.bare(n) for n, *_ in page.MARKERS}
               | {page.bare(n) for _, rows in page.CALLS for n, *_ in rows})
    assert written == page.module_functions()


def offered(thing: Any) -> list[str]:
    """What autocomplete shows somebody who has typed a dot, dunders dropped."""
    return sorted(n for n in dir(thing) if not n.startswith("_"))


def made_in(module: Any) -> list[str]:
    """The module's own names, rather than what it imported to build them."""
    return sorted(
        name
        for name, thing in vars(module).items()
        if not name.startswith("_")
        and getattr(thing, "__module__", None) == module.__name__
    )


def test_a_collection_offers_the_documented_surface_and_the_author_s_own():
    """`@collection(of=Event)` rebuilds the class on `Collection` rather than
    asking anybody to inherit it, so `Events.upcoming` and dray's own write path
    arrived in autocomplete looking exactly alike — thirty-three names, three of
    them the author's, and nothing in a name or a signature saying which was
    which. The seam is `docs/manual.md`: what the page tells a caller to reach
    for keeps its name, and everything else wears a leading underscore."""
    assert offered(Events) == [
        "add",
        "add_all",
        "blob",
        "by_id",
        "cache_info",
        "columns",
        "conn",
        "count",
        "counts_for",
        "delete",
        "etag",
        "find",
        "find_first",
        "forget",
        "forget_all",
        "id",
        "in_batches",
        "on",  # the author's
        "parent_id",
        "parent_type",
        "save",
        "save_all",
        "select_first",
        "select_many",
        "select_rows",
        "sql_for",
        "table",
        "upcoming",  # the author's
    ]


def test_a_base_class_on_a_collection_survives_the_rebuild(store):
    """Shared reporting on a base class, mixed into a collection, is the
    obvious way to write it and the way that used to vanish: if `@collection`
    built the new class on `Collection` alone, the base would go with the class
    it replaced — nothing raised at the declaration, and the first sign an
    `AttributeError` at a call site that could be anywhere. The record side has
    never had the problem: `@record` hands back the class you wrote."""
    assert Reports.__mro__ == (Reports, Reporting, Collection, object)
    assert store.importing_reports.summary() == "summary"


def test_a_name_a_base_class_spells_wins_over_dray_s_own(store):
    """`Collection` goes behind the bases rather than in front of them, so a
    base class that spells `count` is the one that runs — which is what the
    reader who wrote `class Reports(Reporting)` by hand asked for. dray has
    nothing to tell a name meant for it from a name that happens to match, so
    Python's method order is the answer, the same as for a `save` on a
    record's base class."""
    assert store.importing_reports.count() == "the base class counted it"


def test_a_collection_may_inherit_collection_without_naming_it_twice():
    """`class Events(Collection)` is the form *What your editor can see*
    teaches, so keeping the bases has to leave it standing: passing
    `Collection` to the rebuild alongside a base that is already it is
    `TypeError: duplicate base class`, raised where the class is written. The
    documented declaration would have stopped importing."""
    assert Tallies.__mro__ == (Tallies, Collection, object)


def test_a_collection_may_be_built_on_another_collection(store):
    """A base that has itself been through `@collection` is a `Collection`
    subclass without being `Collection`, so the check for one already in the
    bases asks `issubclass` rather than `is`. Everything up the chain is
    reachable — `summary` here is two classes above the one declared."""
    assert Ledgers.__mro__ == (Ledgers, Reports, Reporting, Collection, object)
    assert store.importing_ledgers.summary() == "summary"


def test_a_child_set_offers_only_the_calls_a_caller_makes():
    """The same list smaller. `mounted`, `theirs`, `pending`, `settled` and
    `requeue` are how a parent's save writes its queue and puts it back after a
    rollback, and every one of them read as something to call."""
    assert offered(ChildSet) == [
        "add", "by_id", "clear", "count", "find", "find_first", "thin"
    ]


def test_the_schema_module_offers_the_six_the_manual_sends_you_to():
    """`sql_type`, `columns`, `constraint` and `index_columns` are how those six
    are built rather than calls to make, so somebody reading `dir(dray.schema)`
    for the way to create a table had ten names to choose between."""
    assert made_in(schema) == [
        "create_indexes",
        "create_namespace",
        "create_table",
        "drift",
        "promote",
        "statements",
    ]


def test_the_modules_behind_a_collection_offer_only_the_decorators():
    """`from dray.collection import written_for` worked, and so did the same
    line for nineteen others across the two — every one of them a helper whose
    signature was free to move until somebody had written it down."""
    assert made_in(sys.modules["dray.collection"]) == [
        "Collection",
        "cached_for",
        "collection",
        "jsonb",
    ]
    assert made_in(sys.modules["dray.child"]) == ["ChildSet", "child"]


def test_the_package_can_say_which_version_it_is():
    """A bug report starts by asking for it, so a caller has to be able to
    answer without going to `pip show`.

    Read out of the installed metadata rather than written into the module,
    because two places to say a number is one place to get it wrong: a wheel
    whose constant disagrees with its own metadata is a report nobody can
    trust.
    """
    assert dray.__version__ == metadata_version("dray")
    assert dray.__version__ != "0+unknown"
