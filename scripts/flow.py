#!/usr/bin/env python
"""
What dray did, drawn as a tree, with DSQL's own plan under each read.

An observer is handed every span dray opens and closes. This collects them,
draws the tree, and then — afterwards, on a second store nobody is watching —
asks the cluster to `explain` each read and hangs the plan under the span that
ran it. The two halves answer different questions and are worth having side by
side: the span says *this took 38ms*, and the plan says *because it was a full
scan*.

    scripts/flow.py ab12cd.dsql.ap-southeast-2.on.aws
    DRAY_DSQL_HOST=... scripts/flow.py

The order is not a courtesy, it is the only order that works. A handler that
queried the store it is watching would emit a span, which would call the
handler, without end — so dray refuses the re-entry and raises. Collect first,
ask the database second.

Two tables are created, named for the run, and dropped at the end.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import uuid
from datetime import date
from typing import Any

import psycopg

import dray
from dray import Span, Store, child, field, record

RUN = uuid.uuid4().hex[:6]


# A lifetime on the record, so the page below can read one of the people it has
# already listed and the tree can show what that cost — which is a `cache` node
# and no statement at all.
@record(table=f"person_{RUN}", collection="people", cached_for=30)
class Person:
    family_name: str = field(default="")
    suburb: str | None = field(default=None)
    status: str = field(default="enquiry")


@child(of=Person, name="notes", table=f"note_{RUN}", collection="notes")
class Note:
    body: str = field(default="")
    written_on: date | None = field(default=None)


SPANS: list[Span] = []


def collect(span: Span) -> None:
    """Closes only.

    An open says where a span sits and nothing else, and the close carries the
    same `parent_id` — so keeping both would draw every branch twice.
    """
    if span.phase == "close":
        SPANS.append(span)


#
# The work being watched. An ordinary page: read some people, count the notes
# hanging off them without loading any, ask for one of them again, then write
# one of them back.
#


def render_a_page(store: Store) -> None:
    with store.span("the volunteer page"):
        people = store.people.find(
            equals={"status": "volunteer"}, order_by="family_name", limit=20
        )
        store.notes.counts_for(people)
        # The row the `find` left behind, asked for the way the second half of
        # a page asks for it — knowing only the id and not that anything has
        # read it already. Before the block, since a read inside one is neither
        # answered from memory nor kept.
        store.people.by_id(people[0].id)
        with store.transaction():
            people[0].suburb = "Leura"
            people[0].notes.add(body="Moved.", written_on=date(2026, 8, 15))
            people[0].save()


#
# Afterwards.
#


def plans_for(spans: list[Span], conn: Any) -> dict[int, list[str]]:
    """`explain analyze verbose` for every read, keyed by the span that ran it.

    **Verbose or nothing.** DSQL appends a per-statement DPU estimate — compute,
    read, write — and only to the verbose form. That is the number worth having:
    the planner's `cost=` is an abstract model with a fixed hundred-odd of
    startup for a storage round trip, and AWS say outright not to benchmark two
    queries by comparing them. DPUs are what the statement is billed.

    **Reads only, because `analyze` runs the statement.** That is fine for a
    select — it is read a second time, which is a second read's DPUs and worth
    knowing you are spending — and it is why no write goes near this.

    A refusal is kept rather than raised: a statement the planner will not take
    is itself worth seeing in the tree.

    **A `cache` span gets none, and that is the picture rather than a gap in
    it.** A read answered out of memory sent no SQL, so there is nothing for
    the planner to have done — a node with no plan hanging beneath it is one
    the cache answered.
    """
    plans: dict[int, list[str]] = {}
    for span in spans:
        if span.kind != "statement" or not span.sql:
            continue
        if not span.sql.lstrip().lower().startswith("select"):
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(f"explain analyze verbose {span.sql}", span.params)
                plans[span.id] = [row[0] for row in cur.fetchall()]
            conn.commit()
        except psycopg.Error as refused:
            conn.rollback()
            plans[span.id] = [f"(no plan: {str(refused).splitlines()[0]})"]
    return plans


# Eighths, so a span an eighth the width of the widest still draws something
# rather than rounding to nothing or to a whole block it has not earned.
EIGHTHS = " ▏▎▍▌▋▊▉█"

# Colour by what a kind means rather than to decorate: the two that cost a round
# trip are the ones worth finding, `cache` is worth finding for the opposite
# reason — a round trip that did not happen — and everything else is
# scaffolding.
HUE = {
    "connect": "35", "checkout": "35", "caller": "1;36", "transaction": "33",
    "statement": "1;37", "execute": "32", "hydrate": "34", "returning": "34",
    "prepare": "34", "cache": "1;32",
}

# What a plan says about how it read the table, which is the whole reason for
# fetching one. Nothing else in those lines needs finding by eye.
VERDICT = (("Full Scan", "1;31"), ("Seq Scan", "1;31"),
           ("Index Only Scan", "1;32"), ("Index Scan", "32"),
           ("Total:", "1;33"), ("Read:", "33"), ("Write:", "33"),
           ("Compute:", "33"))

COLOUR = sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def tint(text: str, hue: str) -> str:
    return f"\033[{hue}m{text}\033[0m" if COLOUR else text


def fit(text: str, room: int) -> str:
    """One line, cut to the room there is.

    A plan carries the whole `parent_id = ANY ('{…}')` array, which is four
    UUIDs here and two hundred on a real page — wrapping it would bury the tree
    it is annotating, so it is cut and said to be cut.
    """
    return text if len(text) <= room else text[: room - 1] + "…"


def bar(ms: float, widest: float, room: int = 24) -> str:
    """A bar to an eighth of a character.

    Whole blocks alone cannot show a 0.17ms hydrate beside a 61ms read without
    either rounding it away or giving it a block it has not earned, and the
    ratio between those two is the finding.
    """
    eighths = max(1, round(8 * room * ms / widest)) if widest else 1
    full, rest = divmod(min(eighths, room * 8), 8)
    return "█" * full + (EIGHTHS[rest] if rest else "")


def draw(spans: list[Span], plans: dict[int, list[str]]) -> None:
    """The tree, deepest-first from each root.

    Sorted by `at_ns` rather than by the order they arrived. A span closes
    after everything inside it, so arrival order is inside-out and the tree
    would come out upside down.

    Work that fanned out is several roots rather than one tree — a parent is
    always from the same thread as its child — so each thread's outermost span
    has `parent_id` of `None` and starts a run of its own here.
    """
    children: dict[int | None, list[Span]] = {}
    for span in spans:
        children.setdefault(span.parent_id, []).append(span)
    for kids in children.values():
        kids.sort(key=lambda s: s.at_ns)

    # Scaled to the slowest span that is *inside* the work, because `connect`
    # is a one-off handshake several times anything else and would flatten the
    # rest of the chart into one block each.
    inside = [s for s in spans if s.kind != "connect"]
    widest = max((s.elapsed_ns or 0 for s in inside or spans), default=1) / 1e6
    width = shutil.get_terminal_size((100, 24)).columns

    def walk(parent_id: int | None, prefix: str = "") -> None:
        kids = children.get(parent_id, ())
        for i, span in enumerate(kids):
            last = i == len(kids) - 1
            ms = (span.elapsed_ns or 0) / 1e6
            name = span.label or (span.cls.__name__ if span.cls else span.kind)
            rows = f"{span.rowcount} rows" if span.rowcount is not None else ""

            elbow = "╰─ " if last else "├─ "
            # What the *children* of this span hang from: nothing more of this
            # branch below the last one, a rail below any other.
            below = prefix + ("   " if last else "│  ")
            label = f"{span.kind} {name}" if name != span.kind else span.kind
            drawn = f"{tint(prefix + elbow, '90')}{tint(label, HUE.get(span.kind, '0'))}"
            print(
                f"  {drawn}{'·' * max(2, 40 - len(prefix + elbow + label))}"
                f"{ms:8.2f}ms {tint(f'{rows:<8}', '90')} "
                f"{tint(bar(ms, widest), '31' if ms > widest / 2 else '90')}"
            )

            plan = plans.get(span.id, ())
            for j, line in enumerate(plan):
                rail = "╰╴" if j == len(plan) - 1 and not children.get(span.id) else "│ "
                shown = fit(line.rstrip(), width - len(below) - 8)
                for phrase, hue in VERDICT:
                    if phrase in shown:
                        shown = shown.replace(phrase, tint(phrase, hue))
                        break
                print(f"  {tint(below + rail, '90')} {tint(shown, '90')}")
            walk(span.id, below)

    walk(None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", nargs="?", default=os.environ.get("DRAY_DSQL_HOST"))
    args = parser.parse_args()
    if not args.host:
        # Named rather than looked up, because a hostname is not written into
        # this repository — but somebody meeting this error has usually just
        # forgotten to export it rather than not having a cluster.
        parser.error(
            "a cluster hostname, or DRAY_DSQL_HOST.\n"
            "  aws dsql list-clusters --region <region>  lists them, and the\n"
            "  host is <identifier>.dsql.<region>.on.aws"
        )

    watched = Store.connect(host=args.host, observer=collect)

    # Setting up is not the experiment. A store made without an observer is one
    # dray never times, so none of this appears in the tree.
    plain = Store.connect(host=args.host)
    for statement in dray.schema.statements(Person) + dray.schema.statements(Note):
        plain.conn.execute(statement)
        plain.conn.commit()
    for i, name in enumerate(("Hemingway", "Woolf", "Orwell", "Austen")):
        person = plain.people.add(Person(family_name=name, status="volunteer"))
        for n in range(i):
            person.notes.add(body=f"note {n}")
        person.save()

    try:
        render_a_page(watched)
        plans = plans_for(SPANS, plain.conn)
        print(f"\n{len(SPANS)} spans, {len(plans)} plans\n")
        draw(SPANS, plans)
        print()
    finally:
        for table in (Note.__dray_table__, Person.__dray_table__):
            plain.conn.execute(f"drop table if exists {table}")
            plain.conn.commit()
        print(f"dropped {Person.__dray_table__} and {Note.__dray_table__}")
        watched.close()
        plain.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
