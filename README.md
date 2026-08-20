# dray

**A Python record layer for Amazon Aurora DSQL.**

dray is built out of what DSQL doesn't have. No foreign keys, no savepoints, no
locking, three thousand rows to a transaction. The limits are the design rather
than obstacles to route around.

You can already run SQLAlchemy or Django against DSQL — AWS ships adapters for
both — but you spend the time talking them out of foreign keys, pessimistic
locking and heap storage. dray assumes none of those to begin with.

## Install

```bash
uv add dray                   # or: pip install dray
```

Python 3.12 or newer. `Store.connect` authenticates against a cluster and so
needs AWS's own connector, which is an extra:

```bash
uv add "dray[dsql]"
```

A store handed a connection you already have needs none of that — psycopg is
the driver underneath either way, and the connector installs no driver of its
own.

**dray is not at 1.0.** 0.1 is the first release meant to be depended on, and
a version before 1.0 may still change what a call does. [The manual][manual] is
the promise; anything not on that page is not one yet.

The smallest thing that works:

```python
from dray import Store, record


@record(table="person", collection="people")
class Person:
    family_name: str


store = Store.connect(host="ab12cd.dsql.ap-southeast-2.on.aws")
store.create(Person)                  # the table it implies, once

person = store.people.add(Person(family_name="Hemingway"))
print(store.people.by_id(person.id).family_name)
# Hemingway
```

With dray you get:

- **DSQL's limits handled for you.** A transaction there holds 3,000 rows, so a
  big write is split to fit; a commit it refuses under contention is replayed
  rather than raised at you; and a delete reaches the records underneath.
- **Records that are plain dataclasses**, declared with a decorator. No base
  class, no metaclass, and no connection — you can build one, hand it to a
  function and test it with the database nowhere in sight.
- **A column or a JSON document, decided per field.** Give a field its own
  column, or keep it in a `jsonb` document beside the others — adding one of
  those is a write rather than a migration, and a filter is written the same
  way either side.
- **Somewhere for your queries to live.** A collection is a class you own, with
  `find` for matching on values and SQL you wrote for everything else. Both
  hand back records rather than rows.
- **A read cache, per record, off until you ask for it.** Put `cached_for=10`
  on a record and reading the same one again comes out of memory instead of
  the database. Other reads fill that cache too, a write drops what it
  changed, and nothing lives long enough to go far wrong.
- **Made to be used from many threads.** A store each out of a pool and
  one cache between them, so a page wanting six answers asks for all six at
  once and costs the longest rather than the sum — then reads on sequentially,
  out of the cache they filled.
- **Sub-records that travel with their parent.** Notes on a person, lines in an
  order: queued up, written in the same transaction as the change they explain,
  and deleted when it is. As DSQL does not have foreign keys, dray does it for
  you.
- **Business rules that live on the field, not at the call site.** What a value
  will accept, what it tidies on the way in, what to fill in on a write, and
  what to run when it changes. Said once on the class and applied at every door
  a value arrives through — a constructor, a form, an importer, a filter — so
  there is no route into your data that quietly goes around them, and nobody
  has to remember to call anything.
- **Optimistic concurrency, already wired up.** Two people open the same record
  and both save it: the first one lands, and the second is told rather than
  quietly painting over it. Every record carries an `etag` dray mints and
  checks on the way back in, so you get a `RecordHasChanged` to handle rather
  than a mystery change to account for weeks later.
- **The `create table` your classes imply**, as statements to paste into a
  migration — and a way to ask a live database where it has drifted from
  them.
- **Somewhere to look when it is slow.** Every trip dray made to DSQL and every
  one the cache saved you — the connection, the transaction, the statement, the
  hit — with what asked for it and what it cost, as a tree you can print or
  assert on. A test can pin *this page makes one read and not six*, and against
  a cluster you can hang DSQL's own query plan under each read.

## Documentation

**[The manual][manual]** is the whole of it, and is meant to be read from
the top rather than searched: a record, a store to reach it through, a collection
to ask, children hanging off it, and — throughout — what the database is doing
underneath and why the shape above it is what it is.

It is one page on purpose. The story builds, so it is read in order the first
time, and after that a browser's own search reaches every word of it at once.

**[The reference][reference]** is the other half of that, and is
meant to be looked at rather than read: what the pieces are and how you reach
one from another, what each of them offers, which door runs which rule, and
where the transaction is. All of it in one screen, which is the one thing a
page read from the top cannot do. Open it in a browser — it is generated by
`scripts/reference.py`, half of it read out of the library itself.

**Building with an agent?** Hand it [the manual][manual]. The page is one page
for this reason as much as for reading: the whole promise fits in a single
fetch, in order, with the reasoning attached — so an agent is not guessing at
what a call takes, or reaching for a foreign key, a savepoint or a lock that is
not there. Pointing at it from whatever file your agent reads for project
context is enough, and [the reference][reference] is worth adding where the
work is mostly about which door runs which rule.

The page is also written *against* agents building blind — handed a domain to
build and this page, with dray's source barred — and a good many of its
sentences are there because one of those builds got something wrong. The
nuance is on the page rather than waiting to be discovered, and yours will not
be the first agent to meet it.

## A fuller example

The manual is where all of this is actually taught, in order and with the
reasoning. What follows is only a feel for it — the same register with the
rules it needs, the history it keeps and the questions it gets asked:

```python
from datetime import datetime

import dray
from dray import (
    Store, child, clock, collection, field, index, record, records_change
)

STATUSES = ("enquiry", "volunteer", "lapsed")


def enqueue(what, who):               # yours — a queue, a mail, a webhook
    ...


def tidied(email):
    return email.strip().lower() if email else None


def whoever(write):
    """Whoever the save was told about, in `save(given={"whom": ...})`."""
    return write.given.get("whom")


# Define it

@record(
    table="person",                   # where the rows live
    collection="people",              # what the store calls them
    order_by="family_name",           # the order a read gets if nobody asks
    indexes=[index("status", "family_name")],
    cached_for=10,                    # seconds a row may be served from memory
)
class Person:
    family_name: str                  # a plain annotation is a text column
    given_names: str = ""

    status: str = field(
        default="enquiry",
        # Limit what this will accept, no matter how it is assigned.
        choices=STATUSES,
        # React when the value moves, wherever the edit came from, with any
        # function of yours. `records_change` is one dray ships: it queues a
        # line into the child named, filling that child's first field.
        on_change=records_change(into="logs"),
    )

    email: str | None = field(
        # Runs wherever a value reaches the field: the constructor, an
        # assignment, `parse`, and the value a filter is asked to match. So a
        # row stored tidied is found by any spelling of it.
        converter=tidied,
    )

    suburb: str | None = field(
        stored_in="blob",             # jsonb — no migration, no column spent
    )

    added_on: datetime | None = field(
        # Fill this on the insert, with any function of yours. `clock` is one
        # dray ships, and asks the database for the time rather than Python.
        on_add=clock,
    )

    changed_on: datetime | None = field(
        # Fill it again on every write after that, so a null here means
        # nobody has edited this record yet.
        on_save=clock,
    )

    # Ordinary Python. A record is your class, and dray has no opinion about
    # anything it did not put there — this is not a field, not stored, and
    # not in `as_dict()`.
    @property
    def full_name(self) -> str:
        return f"{self.given_names} {self.family_name}".strip()

    # Refuse a record that does not hang together. Where a rule needs the
    # whole record rather than one value, and it runs at every door one comes
    # in by, before any transaction is open.
    @dray.check
    def a_volunteer_is_somewhere(self):
        if self.status == "volunteer" and not self.suburb:
            raise ValueError("a volunteer is rostered out of a suburb")

    # Act inside the write's own transaction. For a rule that has to write, or
    # to read another record with it already open. `write.adding` is true only
    # on the write that creates the record, `write.was` is what it held before
    # this one, and `write.given` is who is asking.
    @dray.before_save
    def log_the_arrival(self, write):
        if write.adding:
            self.logs.add("enquiry received")

    # Act once the rows are durable, and not at all if they never are. Where
    # the work leaves the database — a job, a mail, a webhook.
    @dray.after_commit
    def tell_the_roster(self):
        enqueue("person-changed", self.id)


@child(of=Person, name="notes", table="note", order_by="at")
class Note:
    body: str                         # what `notes.add("...")` fills
    at: datetime | None = field(on_add=clock)


@child(of=Person, name="logs", table="person_log", order_by="at")
class Log:
    message: str                      # what `records_change` writes into
    at: datetime | None = field(on_add=clock)
    whom: str | None = field(on_add=whoever)


@collection(of=Person)
class People:
    """Questions of your own, in SQL, answered in records."""

    def changed_since(self, when: datetime) -> list[Person]:
        # `sql_for` is how a field is named in SQL: the column where it has
        # one, the expression that reads it out of the blob where it does
        # not. So this keeps working if the field ever moves, and a name the
        # class never declared is refused here rather than by the database.
        changed = self.sql_for("changed_on")
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {changed} >= %s order by {changed} desc",
            [when],
        )


# Use it

store = Store.connect(host="ab12cd.dsql.ap-southeast-2.on.aws")
store.create(Person, Note, Log)       # the three tables these imply

# An enquiry, typed in.
person = store.people.add(            # inserted, and handed back filled in
    Person(family_name="Hemingway", given_names="Ernest", suburb="Leura",
           email="  Ernest@Example.COM "),
    given={"whom": "rod"},            # who is asking, for the rules to use
)

# And one from outside — a form, a spreadsheet, an API. `parse` runs the
# converters and the rules, so what it accepts is what a write will take.
other = store.people.add(
    Person.parse({"family_name": "Stein", "given_names": "Gertrude",
                  "suburb": "Katoomba", "email": "GStein@Example.com"}),
    given={"whom": "rod"},
)

# Both of them signed up at the same open day. One transaction, so the
# register cannot end up holding half of it — and `after_commit` waits for
# the outermost block, so the roster hears once rather than twice.
with store.transaction():
    for one in (person, other):
        one.status = "volunteer"      # queues a line for `logs`
        one.notes.add("Signed up at the Katoomba open day.")
        one.save(given={"whom": "rod"})

# A name of your own in the tree, so a watcher can say what the whole page
# cost as well as what each read in it cost. Free when nobody is watching.
with store.span("the roster page"):
    store.people.find()               # both, by family_name — the class said
    store.people.find(equals={"status": "volunteer", "suburb": "Leura"})
    store.people.find(equals={"email": "ERNEST@example.com"})  # stored tidied
    store.people.changed_since(datetime(2026, 1, 1))
    store.people.by_id(person.id)     # seeded by the find — no round trip

person.full_name                      # 'Ernest Hemingway' — yours, not dray's

person.as_dict()                      # every field the class declares
# {'family_name': 'Hemingway', 'given_names': 'Ernest', 'status': 'volunteer',
#  'email': 'ernest@example.com', 'suburb': 'Leura', 'added_on': datetime(…),
#  'changed_on': datetime(…), 'id': UUID(…), 'etag': '…'}

# Two kinds of child, put in one order here rather than by the database.
# They are ordinary objects once they have arrived, so this is a `sorted`.
for one in sorted([*person.notes, *person.logs], key=lambda one: one.at):
    print(f"{one.at:%H:%M}", getattr(one, "message", None) or one.body)
# 15:25 enquiry received
# 15:25 status changed from 'enquiry' to 'volunteer'.
# 15:25 Signed up at the Katoomba open day.
```

Six of those fields get columns of their own and `suburb` lives in a jsonb
document, and the filter at the end reads the same across both. `status` takes
no word outside `STATUSES` and writes a line into `logs` every time it moves,
whether it was moved by your code, by a form or by an importer. The check runs
at every door a record comes in by, so a volunteer with nowhere to be rostered
is refused before a statement is written.

Each person and their notes and logs go in one transaction, and the block puts
both people in one more: all of it lands or none of it does, and
`tell_the_roster` waits for the outermost block rather than firing per save.
`changed_since` is the shape `find` will not do — equality is the whole of what
it takes, so a range is SQL you wrote, handed back as records rather than rows.
And the last read in the page costs nothing: the `find` above it left its rows
where `by_id` looks, for the ten seconds the class asked for.

And because every trip to DSQL is a span, that run draws itself. This is the
page above, against a real cluster:

```
├─ caller the roster page···············  191.74ms          █████████████████████▉
│  ├─ statement Person··················   61.38ms 2 rows   ███████
│  │  ├─ execute Person·················   60.58ms          ██████▉
│  │  ╰─ hydrate Person·················    0.13ms 2 rows   ▏
│  ├─ statement Person··················   33.90ms 1 rows   ███▉
│  │  ├─ execute Person·················   33.35ms          ███▊
│  │  ╰─ hydrate Person·················    0.08ms 1 rows   ▏
│  ├─ statement Person··················   35.36ms 1 rows   ████
│  │  ├─ execute Person·················   34.92ms          ████
│  │  ╰─ hydrate Person·················    0.05ms 1 rows   ▏
│  ├─ statement Person··················   60.02ms 2 rows   ██████▉
│  │  ├─ execute Person·················   59.17ms          ██████▊
│  │  ╰─ hydrate Person·················    0.14ms 2 rows   ▏
│  ╰─ cache Person······················    0.19ms 1 rows   ▏
│     ╰─ hydrate Person·················    0.04ms 1 rows   ▏
```

Four reads at thirty to sixty milliseconds each, and the fifth — the `by_id`
the `find` had already filled — at **0.19ms**. `just flow` hangs DSQL's own
query plan under each read as well, which is where you find out that the slow
one was a scan.

## Contributing

**Everything is in issues.** A finding becomes an issue, a decision becomes a
comment on one, and what a change did is in its pull request — there is no
design document and no notes file anywhere. Which means an obvious-sounding
proposal may be one that has already been argued down, so search the closed
issues before writing a new one.

The suite needs no configuration — `pytest-postgresql` starts a PostgreSQL and
throws it away — but it does want [`just`](https://just.systems) and
[`uv`](https://docs.astral.sh/uv/) on your machine:

```bash
just test
```

Run it that way rather than by hand. Both `--group test` and `--extra dsql`
have to be named, and a shorter command does not fail — `uv run pytest`
resolves a pytest that has never heard of dray and passes having run nothing.
`just --list` has the others: `just fresh` builds the environment from scratch
when a pass needs to be provably honest, and `just cluster` runs the half local
PostgreSQL cannot answer.

**Open an issue first, and wait for the decision in it to be settled.** Not as
a formality: `docs/manual.md` is written first, so a change to what a caller
can call is a change to that page and usually a decision as well. A pull
request that arrives without one is often work somebody has to turn down for a
reason that would have been two lines in a comment. An issue is welcome on its
own and obliges you to write nothing — a finding, reproduced, is worth as much
as a fix.

A pull request branches from `main` and is named for the change. Commit
subjects are a short declarative sentence in the present tense, and carry no
trailers — no `Co-Authored-By`, no tool or session links. The author field
already says, and this history will outlive every tool that touched it. Put
`Closes #N` in the body where the change finishes the issue.

The body carries the thinking, because the diff carries the change: before and
after as a handful of lines of real code, first and before any argument, then
what was wrong, the decisions you made that the issue did not settle, and what
you tested and deliberately did not. Tests come with the change rather than
after it, and are named for the behaviour rather than the function.

`docs/manual.md` is written first and the API follows it. If a change makes a
sentence on that page untrue, the sentence is part of the change.

[manual]: https://github.com/draylabs/dray/blob/main/docs/manual.md
[reference]: https://github.com/draylabs/dray/blob/main/docs/reference.html
