# The dray manual

dray is a record layer for Amazon Aurora DSQL, built out of what DSQL doesn't
have. No foreign keys, no savepoints, no locks to take, three thousand rows to
a transaction. The limits are the design rather than obstacles to route around.

You can already run SQLAlchemy or Django against DSQL — AWS ships adapters for
both — but you spend the time talking them out of foreign keys, pessimistic
locking and heap storage. dray assumes none of those to begin with.

This is the whole of it, and it is written to be read from the top: each section
uses what the one before it set up, and the parts that explain *why* the shape is
what it is are the paragraphs about what the database is doing underneath. The
[README](../README.md) has the short version if that is what you came for.

# The shape of it

Enough to build something: a record, a store to reach it through, a collection to
ask, children hanging off it, and a field that says when a change is worth
writing down.

## Defining a person

```python
from dray import record, field

@record(table="person", collection="people")
class Person:
    family_name: str
    given_names: str = ""
    status: str = "enquiry"
    suburb: str | None = field(default=None, stored_in="blob")
```

Three of those get columns and one does not. `family_name`, `given_names` and
`status` are what we filter, sort and count on; `suburb` lives in a jsonb blob
because we only ever read it back out. A plain annotation is a column, and
`field()` is the same thing when it needs saying more — a default, a validator,
something to call when it changes.

Adding an attribute is a write rather than a migration. Promoting one to a column
on the day it turns out to be worth indexing is deleting `stored_in="blob"`, and
nothing about the field or its callers changes — the rows already written need
moving across, and *Tables* below hands you the statements for that.

> **If you have a type checker on.** The class above is built by a decorator
> at import, so an editor is told the fields and the constructor and nothing
> about what dray attaches — `person.save()` reads as unknown until you say one
> more thing. It is one optional line, and *What your editor can see*, below,
> is the whole of it. Worth knowing now rather than concluding your setup is
> broken.

## A store

The class above needs no connection — it is a declaration and nothing more.
Everything that touches the database goes through a store, which holds one
connection and the collection named by each record.

```python
from dray import Store

store = Store.connect(host="ab12cd.dsql.ap-southeast-2.on.aws")
```

`store.people` is `Person`'s collection, from the `collection=` above. There is
no password and nothing else to configure — DSQL authenticates with an IAM token
that dray mints, and *Connections* below has the rest of it.

## Creating

```python
person = store.people.add(
    Person(family_name="Hemingway", given_names="Ernest", suburb="Leura")
)
```

A record is a dataclass, so a dict spreads straight into it, which is most of
what an importer needs:

```python
person = store.people.add(Person.parse(row))
```

`parse` is strict — an unknown key is a typo in somebody's spreadsheet, and a bad
value, whether the wrong type or one a rule refuses, fails here rather than at
the database. Rows already in the table load through a lenient path instead, so a
record written under an older rule stays readable.


## Reading

```python
person = store.people.by_id(person_id)

store.people.find(equals={"status": "enquiry", "suburb": "Leura"})
```

Both names in `equals` are filters, and both have to match. Underneath they are
not alike: `status` is compared as a column, while `suburb` is pulled out of the
jsonb document first. A column can be indexed and a key inside a shared jsonb
document cannot — on DSQL `jsonb` carries no index support at all — so which
side a field lives on decides whether asking about it can ever be cheap.
Identical at the call site, and which is which is a line in the class you can
change on the day the scan stops being free.

`find(equals={"suburb": None})` asks for the ones with nothing in that field, on
either side of the split.

## Updating

```python
person.status = "volunteer"   # a column
person.suburb = "Katoomba"    # jsonb
person.save()
```

Both are set the same way, though one has a column and the other lives in the
jsonb. Setting a field changes the record in hand and nothing else — nothing
reaches the database until `save`, which writes everything that moved in one
transaction: both together, or neither.

> **What DSQL is doing.** Nothing locks rows, so this save and somebody else's on
> the same person never wait for one another — each runs, and whichever reaches
> commit second is refused. dray replays the whole transaction when that happens,
> which is why a refused write is ordinary here rather than a fault, and why
> nothing above `save` ever sees one.

Anything that has to happen once a write is durable rather than once it is asked
for — the job a worker on another connection will pick up — belongs to the same
record and is said the same way: *After a record lands*.

## Deleting

```python
person.delete()
```

The person is gone, and saying it again raises `RecordNotFound` rather than
passing quietly. A delete asks by identity, the same as `by_id` and the same as
a save, and asking by identity for something that is not there is a broken
assumption rather than an answer — *Nothing found, and which way it says so*. A
cancellation arriving twice, from the guest and then from the host, is a thing
the record layer can tell you about rather than one your application has to
check for itself.

A record that has something to do before it goes, or a domain that says this
one never does, says so on the class — *Before a record goes*.

## Collections

Declaring `Person` came with a collection. `store.people` is where every question
about people in general lives, rather than about one of them, and it arrived with
the `collection="people"` on the class: `add` and `add_all` for new records,
`by_id`, `find`, `find_first` and `count` for asking, `save_all` for putting a
set back, and the `save` and `delete` that `person.save()` and `person.delete()`
have been calling on your behalf.

Those are what any record needs. When a kind of record has vocabulary of its own
— questions that mean something in your domain and nowhere else — write it down:

```python
from datetime import date

from dray import collection

@record(table="event", collection="events")
class Event:
    name: str
    starts_on: date
    status: str = "planned"
    suburb: str | None = field(default=None, stored_in="blob")


@collection(of=Event)
class Events:
    def on(self, day: date) -> list[Event]:
        return self.find(equals={"starts_on": day})

    def upcoming(self) -> list[Event]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where status = 'planned' and starts_on >= current_date"
            " order by starts_on"
        )

    def near(self, suburbs: list[str], after: date) -> list[Event]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.blob}->>'suburb' = any(%s) and starts_on >= %s"
            " order by starts_on",
            [suburbs, after],
        )
```

`find` covers what a row holds, and says what order to return it in and how
much of it to return — *The order, and how many* below. Anything with a range or
a join is SQL you write, and dray hydrates the rows into `Event` objects. There
is no query language to learn here, and none to fight when it can't say what you
mean.

**Writing that SQL is the ordinary case rather than the fallback**, and the two
methods above are the whole of what that looks like: a `where` clause and an
`order by`. What you do not write is everything around them — the table name,
the column list, which of your fields are columns and which are keys in a
document, the parameters going over safely, the rows coming back as `Event`
objects with their dates and decimals restored, the connection, the transaction.
That fabric is the work, it is the same in every application anybody writes, and
getting it wrong is quiet. A `select` naming what you want is neither.

The `select_` prefix is the whole of what those names mean: **you wrote the
statement**. `select_many` hands the rows back as records, `select_first` takes
the head of the same read, and `select_rows` skips the hydrating for an answer
that was never records. Everything without the prefix — `by_id`, `find`,
`find_first`, `count`, `in_batches` — is a statement dray wrote, and the
difference matters because a statement you wrote is yours to get right.

`{self.columns}` is not a house style. It is every column the class declares plus
the jsonb, and a statement that comes back without one of them is refused rather
than hydrated: a record built from half a select holds the class defaults for the
rest, and its next save writes them over the row. Leave the id out and it is
worse — the record is given a fresh one, so it belongs to no row at all. What
comes back that the class does *not* declare is still dropped without a word,
which is what lets a field be retired without a backfill.

> **On the f-strings.** What may be built into a statement this way is a name a
> class declared, and nothing else: `self.table`, `self.columns` and `self.blob`
> above, and `self.store.events.table` where a statement has to reach a second
> table. The columns dray owns are named the same way — `self.id`, `self.etag`,
> and `self.parent_type` and `self.parent_id` on a child — so a statement
> follows a class that moved one, which is what *The names dray owns* is about.
> None of them can come from a caller, which is what makes them safe to
> interpolate. Everything else is a parameter — `suburbs` and `after` go as `%s`
> in `near` above, and so does every value dray puts in a statement of its own.
> `find` interpolates a field *name*, for `order_by` as well as for a filter,
> and checks it against what the class declared before it does.
>
> Worth knowing because it will be raised. A scanner sees string-built SQL and
> flags it, AWS's DSQL guidance names f-strings specifically, and a reviewer who
> knows either is right to ask. The answer is the paragraph above. The case that
> guidance is really about is a sort column or a table name arriving from a query
> string, and there is nowhere in dray that one can reach.

Past those names a method has `self.store` for reaching another collection,
`self.cls` for the class this one serves, and `self.conn` for the connection the
store is on. Which is the whole of what a collection holds: the rest of what
`dir()` shows you is dray writing your statements for you, and wears a leading
underscore so the two are told apart at a glance. Nothing stops you calling one.
It is a signature that may move under you rather than a wall — where everything
on this page is a promise that will not.

A method that two collections want goes on a base class and both mix it in,
which is the ordinary Python answer and is the one here too:

```python
class Reporting:
    def summary(self) -> str:
        return f"{self.count()} of them"


@collection(of=Event)
class Events(Reporting):
    def upcoming(self) -> list[Event]:
        ...
```

`@collection` builds a new class rather than handing yours back — *What your
editor can see* is where that matters — and the bases you wrote come with it, in
the order you wrote them and with `Collection` behind them. Which is worth
saying because it decides a name spelled twice: a base class of yours that has
its own `count` is the one that runs, since Python reaches it before dray's.

Now a page asks the question it actually has, and gets `Event` objects back:

```python
for event in store.events.upcoming():
    print(f"{event.starts_on:%d %b} · {event.name} · {event.suburb}")

# 14 Sep · Blue Mountains working bee · Katoomba
# 02 Oct · Spring intake day · Leura
# 19 Oct · Volunteer training · Penrith
```

`starts_on` and `name` came from columns, `suburb` came out of the jsonb, and the
listing cannot tell. Nothing above the collection assembles a filter either, so
when `upcoming` has to change — a cancelled status, a date that means something
subtler — it changes once and every caller comes with it.

What comes back are records rather than rows, so they can be changed and put
back. The forecast turns bad:

```python
events_for_day = store.events.on(date(2026, 9, 14))

for event in events_for_day:
    event.status = "cancelled"

store.events.save_all(events_for_day)
```

They go back together, in one transaction.

> **What DSQL is doing.** A transaction takes 3,000 rows and no more, so a set
> past that cannot be one transaction however much anybody would prefer it. dray
> works out how many will fit and writes as many transactions as it takes — the
> arithmetic you would otherwise be doing at every call site. The implication is
> the part worth knowing: a set that fits is all-or-nothing, but a larger one is
> several writes, so a failure partway through leaves the earlier ones committed.

What that does not cover is a value one of the classes refuses. Every record in
the set is checked before the first transaction opens, and so is every child
queued against one, so a bad value stops the write rather than stopping it
halfway.

A set that points at itself is written the same way, because **a record has its
id before it is written**. `Person(...)` mints one and `add` does not change it,
so the references can be filled in before anything is saved:

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    membership_no: str
    introduced_by: UUID | None = None
```

```python
people = [Person.parse(row) for row in spreadsheet]
by_number = {person.membership_no: person.id for person in people}

for person, row in zip(people, spreadsheet):
    person.introduced_by = by_number.get(row["introduced by"])

store.people.add_all(people)
```

Nothing has been written by the time that loop runs — the ids exist because the
objects do. Without that you would write everyone, read them back to learn their
ids, and write them all again.

### Which record this is

That id is what a record *is*, so it is what `==` asks about:

```python
person = store.people.by_id(person_id)
again = store.people.by_id(person_id)

person.status = "volunteer"     # not saved, so `again` knows nothing about it
person == again                 # True
```

Two objects for the same row are the same record. Not the same *values* — one
may have been read hours ago and the other just now, one may be carrying edits
nobody has written down, and the question anybody comparing records is asking is
still whether they are about the same person. Records of two different kinds are
never equal, whatever their ids: two tables can hold the same one and mean
nothing by it.

Which also means a record goes in a set and works as a key, so the ordinary
things are ordinary:

```python
set(volunteers) - set(coordinators)
{person: person.notes.count() for person in people}
```

What that id is, where the class declared no field for one, is a version-4
uuid — `uuid4()`, minted in Python as the object is built, which is how the
loop above knew every id before a row existed. Random rather than climbing, and
that is the point of it rather than an accident of the call: on DSQL the table
*is* the primary-key B-tree, so keys arriving in order arrive at the same end of
it and a table taking writes in volume spends one partition's throughput on all
of them, where scattered ones spread across the whole of it. AWS ask for a key
that distributes for exactly that reason. It is a real cost paid for a real
thing, and where the bill arrives is *Asking for records* below — two ids
compare, and the comparison means nothing.

An id can be given when a record is built — an import carrying its own keys, a
record rebuilt from a backup — and cannot be moved afterwards:

```python
Person(family_name="Hemingway", id=chosen)       # fine
Person.parse({"family_name": "Hemingway", "id": chosen})   # also fine

person.id = somebody_elses_id
# AttributeError: 'id' is a Person's key, and a key cannot be changed once the
# record exists. Give it one when you build it — the constructor and `parse`
# both take it — or build the record it should be.
```

Which row a save writes to is whatever the object says its id is, so moving one
moves the write: assigning somebody else's and saving would overwrite their row
with these values and leave this one behind, with nothing raised. Pointing an
object at a different row is not editing a record, it is meaning another one.

A collection is everything you can ask or do about one kind of record. Work
spanning two of them is a service, and belongs above dray — and where two of
those writes have to agree, *Two collections in one transaction* below is how
they are made to.

## Children

Some records mostly make sense inside another. A note is about a person: read on
their page, written alongside the change it explains, and with no reason to
outlive them.

You could declare that as an ordinary `@record` with a `person_id` on it and do
the rest by hand — write it in the same transaction as the change it explains,
delete it when the person goes, and make sure a note id arriving from somebody's
form cannot reach a note belonging to a different person. None of that is
difficult. All of it is easy to get slightly wrong, and it is the same work every
time.

`@child` is that work declared rather than written. A child is a record that
belongs to another one: reached through its parent, written with it, and gone
when it goes.

Which is also the test for whether something wants to be one. A child is for a
record with no reason to outlive its parent and no reason to move, which a note
is on both counts. The second of those decides more than it looks like it will:
which parent a child belongs to is dray's to keep, settled when it is queued on
one, and nothing hands it to a different parent afterwards. So work broken out
of larger work — where the whole point is that a change of plan moves a piece
from under one thing to another — is not a child, however much the shape looks
like one. That is a record pointing at another record: an ordinary field with an
id in it, indexed where you ask questions by it.

```python
from dray import child

@child(of=(Person, Event), name="notes", table="note")
class Note:
    body: str
```

`name=` is what a parent calls them, so `person.notes` and `event.notes` both
read them and take new ones. A child can hang off more than one kind of record,
which is why adding a record type costs no migration.

> **What DSQL is doing.** There are no foreign keys, so a child has nothing to
> point at one parent row with. Each kind of child is one table instead, carrying
> its parent's type and its key in ordinary columns — which is how `note` serves
> people and events alike, and why the record type you add next week is already
> served by it. The second of those columns is typed as the key it holds, so the
> records sharing one child table have to be keyed alike — which every record
> here is, since each leaves its id to dray. *The names dray owns* is where that
> stops being automatic.

Gone when it goes is literal: `person.delete()` takes the notes with it, in the
same transaction.

> **What DSQL is doing.** There are no foreign keys, so nothing in the database
> removes a child when its parent goes, and there is no cascade to declare. Roll
> your own and you are remembering that at every place a person is deleted.
> Declare it as a child and the declaration remembers for you, in the same
> transaction as the parent.

Children are the only thing that goes with it. A field holding another record's
id is an ordinary field and dray reads nothing into it — an `organiser_id` on an
event is a `UUID` that happens to be an id, declared `UUID | None` like any other
field, and given `converter=as_uuid` if the ids reach you as text from a form or
a URL. `as_uuid` is supplied and takes a `UUID` or the text of one; *Converting
what arrives*, further down, is why it is dray's rather than yours. So nothing
checks that the field names a person when you write it, nothing stops that person
being deleted afterwards, and nothing goes looking for the event when they are.
Deleting a record leaves everything that mentioned it holding an id that no
longer resolves, and finding those is yours to do at the point you delete. There
is nowhere for dray to make it otherwise: with no foreign keys, a check before
the write would not survive a delete a moment later.

New children are queued on the parent and written by its save, so a note and the
change it explains land in one transaction:

```python
person.given_names = "Ernest Miller"
person.notes.add("Corrected from the paper enrolment form.")
person.save()
```

A new child writes nothing until `save`. The record accumulates what was done to
it — fields set, children added — and one transaction carries the lot or none of
it.

`add` also takes one you already have, which is what it means on a collection —
an importer that parses rows into notes should be able to attach one:

```python
person.notes.add(Note.parse(row))
```

Built anywhere at all and attached later. It knows which of its fields somebody
chose, so a write fills in the rest exactly as it would for a note queued by
keyword — *Early and late assignment* below is where that matters.

Both of those write the person as well, which is what `save` means and usually
what you want: the note explains a change, and the change lands with it. Where
nothing about the person has changed, the child's own collection writes the note
on its own:

```python
@child(of=(Person, Event), name="notes", table="note", collection="notes")
class Note:
    body: str


store.notes.add(Note(body="Called about the Katoomba weekend."), parent=person)
```

One row for the note and nothing at all for the person. Which door to take is a
question about the parent rather than about the child, and it starts to cost
something the moment the parent is shared: queue an item on a list and every
person adding one is writing the list's own row, which on DSQL is a conflict
rather than a queue. `parent=` takes the record and not its table name, exactly
as the reads do; `add_all` takes it for a whole set; and a note already naming a
different parent is refused rather than moved, because which parent a child
belongs to is settled when it is written and nothing hands it to another
afterwards. `collection=` is what the class gains there, since `store.notes` is
what this door hangs off — *Reading across parents*, below, is where a child's
own collection is worth more than one line.

**A child is written under somebody or it is not written.** Leave `parent=` off
and set neither column yourself and the write is refused, because the row it
would make is the one state `@child` rules out: reached by no read through a
parent, taken by no parent's delete, and not askable for afterwards either,
since `parent_type=None` in a filter means *unset* and filters on nothing.
Setting the two columns by hand still counts — an importer parsing rows holds
ids rather than the records they point at — and it is only the row naming
nobody that is turned away.

A child that already exists is a record like any other, and looks after itself:

```python
note.body = "Corrected from the paper enrolment form."
note.save()
note.delete()
```

Only a child with no row yet is queued on its parent, and an added note is
usually explaining the change being saved alongside it — which is what makes that
the door to reach for. Once it exists, none of that is true, and that includes
the note `add` handed back: the parent's save makes it a row like any other, and
the object already in hand is the one that can write it.

The parent's *id* is never the problem: it exists from the moment the object
does, which is what lets a note be queued against a person who has never been
saved. It is the row that is not there yet.

Deleting one needs a reference that outlives the request. A child has an id like
any other record, so a page can hand it out:

```python
for note in person.notes:
    print(f"{note.id} · {note.body}")

# 7f3a1e08-2c94-4d51-b6e3-0a8c5d9f4b27 · Corrected from the paper enrolment form.
```

and the request that follows hands it back:

```python
person = store.people.by_id(person_id)
person.notes.by_id(note_id).delete()
```

`by_id` puts the parent in the statement, so an id arriving from somebody else's
form finds nothing — `RecordNotFound`, the same as a collection's. That is the
whole of the guard: a child can only be acted on by whoever can reach it through
its parent, and reaching it is the read. `save` and `delete` need no scoping of
their own, because holding the object means having come through there.

Taking the whole set off is one call rather than one delete each, which is the
act a correction usually is — a generation replaced rather than edited:

```python
person.notes.clear()
```

It empties the set as `find` and `count` see it, the stored rows and whatever
is queued on it alike, and hands back how many children went. The rows go now
rather than at the parent's next save: a new child queues because it is usually
explaining a change being saved beside it, and a removal has nothing to ride
with. So clearing and then adding writes the new generation, and the other
order writes nothing — which reads the way it behaves. An empty set is not an
error, because a set carries no belief that anything is in it where an id is a
belief about one row.

What it sends is one statement per generation, the same walk `person.delete()`
does and stopping one short of the person — unless the child class declares a
`@before_delete`, in which case the children are read and the rule runs on each
of them inside the transaction. **The declaration decides which of those two it
is, and nothing at the call site can turn the rule off.** That is what keeps
this one verb with one contract rather than a fast path and a careful one to
choose between; *Before a record goes* is where the rule itself is.

It is not sized, which is the delete's answer inherited rather than solved.
Every row counts against DSQL's 3,000 and a generation with children of its own
multiplies, so a set too big for one transaction is refused whole with nothing
removed — `thin`, in *Children of children* below, is the call for a set that
size and pays for it in transactions. And it is a write like the others:
outside a block it commits on its own, so a statement of yours meant to
accompany it is a second transaction unless you opened one.

A child somebody else adds under the same parent while this is in flight
survives it **on the cluster**. DSQL refuses a commit that raced another writer
over a *row*, and a row that did not exist when the clear ran is not one, so
there is nothing here to conflict on — and reading the children `for update`
first would not change that, since a lock flags what a read returned and has no
way to flag an absence. Local PostgreSQL takes locks and reads committed, so
the same late child is swept up instead and its `@before_delete` never runs:
the test that would prove this passes there for the opposite reason, which is
worth knowing before writing one. `person.delete()`'s cascade has the same hole
and always has. Where the set as a whole has to hold still, the answer is the
one *Two people, one rule* gives for every rule over a set: guard the row that
everybody writing the set goes through.

A child set is asked the same three questions a collection is: `find`,
`find_first` and `count`, all of them scoped to this parent, and all of them
counting what is queued but not yet written so a set reads the same either side
of a save. `find()` naming nothing is all of them.

It is also an ordinary sequence, which is what the examples on this page keep
doing with one:

```python
for note in person.notes: ...   # iterate
person.notes[-1]                # index
person.notes[-2:]               # slice
len(person.notes)               # how many
```

The first three are `find()` and then Python, so a slice of two is every child
in the set built to hand back two. `len` is the odd one out and is the `count`,
which reads no rows at all — a number beside a heading is the commonest way to
ask this and has no business building objects. None of that is worth a thought
on the handful of children a parent usually has, and all of it is on a parent
with two thousand.

Two things differ from a collection, and both follow from that queued half.

**A queued child comes after the stored ones**, rather than taking its place in
the declared order — it has no row to be sorted with. So `find()` is the stored
children in the order the class declared and then whatever is waiting, and
`find_first` is the head of that: the first stored child if there is one, and a
queued child only when nothing is stored matches. Save the parent and the order
is the declared one throughout.

**`find_first` builds the set and takes the head**, where a collection's asks the
database for a single row — a `limit 1` cannot see what is queued. On the handful
of children a parent usually has that is nothing; on a parent with two thousand,
it is two thousand records built to hand back one, and the child's own collection
is where to ask instead.

Which is also what a child set has no version of `select_many` and its two for: a
question that leaves this parent behind belongs on that collection, which is what
`collection=` on the declaration is for.

### Children of children

A child is a record, so it can have children of its own. A note is about a
person; the file attached to it is about the note:

```python
@child(of=(Person, Event), name="notes", table="note")
class Note:
    body: str


@child(of=Note, name="attachments", table="attachment")
class Attachment:
    filename: str
```

```python
note = person.notes.add("Consent form received.")
note.attachments.add("consent-2026-03.pdf")
person.save()
```

Three rows, one transaction. Nothing about the middle generation is special —
`note` is queued and has never been written, and the attachment is queued
against it anyway, because *the parent's id is never the problem*: it exists
from the moment the object does, whichever generation it is.

`person.delete()` takes the notes and their attachments, and `note.delete()`
takes that note's attachments and leaves the ones next to it. There is no depth
at which this stops — a child names its immediate parent and nothing counts how
far down it is.

> **What DSQL is doing.** No foreign keys, so nothing in an attachment's row
> says which person it belongs to — only which note. Deleting the person means
> reaching the attachments through the notes, which dray does with one statement
> per generation, deepest first. Every row counts against the 3,000: fifty notes
> with twenty attachments each is a thousand rows for one person, and a delete
> is one transaction that does not split. Depth costs nothing; fanout compounds.

**What that looks like when it is too big.** The transaction is refused and
nothing is deleted — `transaction row limit exceeded`, as psycopg raised it,
with the record and every generation under it exactly where they were. It is a
clean no rather than a half-finished tree, which is worth knowing because it is
the failure you are choosing to accept by not sizing the delete first. dray
does not size it: a bulk write measures what is queued in front of it, and a
delete removes rows nobody has read, so counting them would be a round trip per
generation on every delete to predict a refusal that is already safe.

A record with thousands of descendants is not something to delete this way, and
the way through is to thin it first. `thin` is a set removal that does not
finish: one call is one pass, and a pass takes up to `at_a_time` rows from
**one** generation, in a transaction of its own, and says how many it took.

```python
while person.notes.thin(at_a_time=500):
    pass

person.delete()
```

Nought means this set and everything under it are gone. The loop is yours, and
so is the trade it makes: several transactions rather than the one `delete` and
`clear` promise, which is the only way past a ceiling that counts a transaction.

**The bound is a generation and not the set**, and that is the part a
hand-rolled version of this gets wrong. The 3,000 is a limit on a transaction
rather than on a statement, so bounding the top generation bounds nothing —
a pass taking 200 notes with twenty attachments under each is 4,200 rows and a
refusal, however the limit on the notes is phrased. One generation a pass is a
real row count with no fanout for you to know, which is what makes `at_a_time`
mean something. It is also why a number past what one transaction holds is
refused where you wrote it: no pass that size would ever be taken.

Deepest first, and **every pass starts again at the deepest generation that
still has rows**. So a tree of notes and attachments goes attachments,
attachments, attachments, then notes — and an attachment somebody adds under a
surviving note while the loop is running is found by a later pass rather than
walked past by one that has moved on. Nothing is carried from one pass to the
next, which is what makes that true.

**Stopping half way leaves a shortened tree rather than a broken one.** The
person is still there with fewer notes, every generation below the one being
thinned is already gone, and nothing is orphaned — nor is there anything in the
set to say a loop was ever running. That is what the several transactions buy
back.

Two things it does differently from `clear`, both of them following from what a
pass is. **What is queued is left alone**, where `clear` drops it: this takes
rows and a queued child is not one, so the loop reaching nought means no rows
left rather than an empty set. And a `@before_delete` on the child class runs
for the children each pass takes, which means **it can run for some of a
generation and never for the rest** — `clear` runs it for every child or for
none. A rule that writes also rides on the pass's budget, so five hundred notes
whose rule adds a line is a thousand rows in that transaction: `at_a_time`
bounds what dray removes rather than what the pass costs. *Before a record
goes* is where that sits beside the other doors.

**Do not thin inside a block you opened.** Every write on a child set joins the
transaction you are already in, and this one is no exception — so the passes
stop being separate transactions and the loop rebuilds the single one it exists
to escape. dray adds up what the passes in a block have taken and refuses the
one that would put the total past what a transaction holds, which is the same
count it refuses an oversized bulk write on: you get a sentence at that pass
rather than `transaction row limit exceeded` several passes later, and the block
rolls back with every row where it was. A short loop in there is left alone,
because it is a set that fits in one transaction and `clear` is the call that
takes it.

Which of the two you want is a question about size and nothing else. `clear` is
one transaction, so it is the call for a generation that fits in one and it is
all or nothing: refused whole, or gone whole, with every rule it ran going back
with it. `thin` is the call for the set that does not fit, and the promise it
cannot make is exactly that one.

### The order they come back in

`order_by` decides that, and it defaults to `id` — which is total and stable and
means nothing, because ids are random. So a child declared as above comes back in
no particular order, and a page that cares has to say what it is ordered by:

```python
@child(of=Person, name="notes", table="note", order_by="written_at")
class Note:
    body: str
    # `clock` is further down
    written_at: datetime | None = field(default=None, on_add=clock)
```

dray puts `id` on the end of whatever you name, so a read is always total — and
that promises less than it sounds like. The same rows come back in the same order
every time, which is what stops a list reshuffling when somebody refreshes it.
Rows tied on everything you named fall through to that random id, so among
themselves the order is one nobody chose: forty notes imported from one
spreadsheet all carrying the same date are listed in whatever order their ids
happen to fall, and the same forty rows written again — restored, migrated,
seeded into another environment — are listed in a different one. If the order in
front of a reader matters, this is where it gets decided.

Which is what more than one field is for, and `desc` for the ones that read
backwards:

```python
from dray import desc

@child(of=Person, name="notes", table="note",
       order_by=("written_on", desc("whom")))
class Note:
    body: str
    whom: str = "System"
    written_on: date | None = field(default=None)
```

Ties on `written_on` are settled by `whom`, backwards; anything still tied falls
to the id, as it always does. Every name is checked when the class is declared
rather than at the first read, and a blob field is refused outright — it has no
column to sort on.

**Where the empty ones go is already decided, and probably the way you wanted
it.** A field that is not always set sorts by a rule neither database makes you
say: PostgreSQL and DSQL both put a row holding nothing last on the way up and
first on the way down. So `order_by="written_on"` is *earliest first, the
undated ones at the bottom*, and `order_by=desc("written_on")` is *latest first,
the undated ones at the top*. Both are the reading a list in front of somebody
usually wants, and neither costs a word — which is worth knowing before you go
looking for SQL of your own over a gap that is not there.

That leaves two orders the default cannot give — up with the empty ones first,
and down with them last — and `nulls=` is how those two are asked for:

```python
from dray import asc

@child(of=Person, name="notes", table="note",
       order_by=asc("written_on", nulls="first"))
class Note:
    body: str
    written_on: date | None = field(default=None)
```

`asc` is a bare name with somewhere to hang `nulls=` and means nothing else, so
`asc("written_on")` and `"written_on"` are the same read; `desc` takes `nulls=`
the same way. A term that says nothing about nulls has nothing about them
written into the statement, so every read that says nothing is the read it
always was. `find` takes these terms too, because it is the same function
reading them.

### Reading across parents

Reaching a child through its parent is the guard, not a wall. Some questions are
about the children themselves rather than about any one parent's — everything
written in the last hour, everything one coordinator wrote all week, every note
holding some particular value. A child's table is an ordinary table with two
ordinary fields on it naming the parent — `parent_type` and `parent_id` unless
the class moved them — so it takes a collection like anything else:

```python
@child(of=(Person, Event), name="notes", table="note", collection="notes")
class Note:
    body: str
    # `clock` is further down
    written_at: datetime | None = field(default=None, on_add=clock)


@collection(of=Note)
class Notes:
    def since(self, when: datetime) -> list[Note]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where written_at >= %s order by written_at desc",
            [when],
        )
```

```python
from datetime import datetime, timedelta

an_hour_ago = datetime.now().astimezone() - timedelta(hours=1)

store.notes.since(an_hour_ago)
store.notes.find(parent_type=Person)
store.notes.count()
```

The two names do different jobs. `name="notes"` is what a parent calls them, and
gives you `person.notes`. `collection="notes"` is what the store calls the whole
table, and gives you `store.notes`. Only `name=` is required. Define a
collection when the same kind of child hangs off several kinds of record — notes,
logs — and you want to ask something of all of them at once, rather than one
parent's at a time.

`parent_type=` takes the record class rather than its table name, so the read
follows a rename; `parent=person` narrows it to one parent's. Both are taken by
`find`, `find_first`, `count` and `in_batches` alike, so the same question
narrowed the same way reads the same whichever of the four you are asking it
through. Both are covered in *The names dray owns*, along with what to do on the
day your domain wants the word `parent` for itself. And what comes back are
`Note` objects, so they save and delete exactly as they would having been
reached through a parent — there is only ever one row either way.

### Counting across parents

A list whose rows carry a number about children that are not on the screen — the
summary column, the badge, the collapsed section, *how much of this is left
without opening it*:

```
Volunteers            notes   mine
──────────────────────────────────
Hemingway, Ernest         4      2
Woolf, Virginia           2      0
Orwell, George            —      —
… 2,000 rows
```

Asked a row at a time that is four thousand statements for four thousand
numbers, because `person.notes.count()` is a round trip and there are two of them
on every row. `counts_for` is the same question asked once, on the child's own
collection, about as many parents as you hand it:

```python
people = store.people.find(
    equals={"status": "volunteer"}, order_by="family_name"
)

notes = store.notes.counts_for(people)
mine = store.notes.counts_for(people, equals={"whom": "rod"})

for person in people:
    print(
        f"{person.family_name}, {person.given_names}"
        f"{notes[person.id] or '—':>8}{mine[person.id] or '—':>7}"
    )
```

Records rather than ids, which is what keeps `parent_type` off the page. That
column holds the *parent's table name*, so a caller writing
`parent_type = 'person'` into a statement of their own has copied out dray's
bookkeeping and will not hear about it on the day the record is renamed. Here the
table comes off the class, exactly as it does for `parent=`.

**Every parent you ask about is in the answer, zero included** — and Orwell's row
is the whole reason this is a method rather than a paragraph of advice. Written
by hand it is `select_rows` with a `group by`, which is the documented way to ask
for an aggregate and gives the right numbers, for the people who have notes. A
`group by` has nothing to group for somebody with none, so the dict built from
those rows has no key for him at all — and the line printing his row is a
`KeyError` in the half of the page nobody tested, because everybody in the test
data had a note.

What comes back is keyed by parent id in the order you passed the parents, so a
page already looping over them indexes straight into it. `equals` narrows what is
counted, spelled as it is on `person.notes.count()`, since *how many unanswered
each* is a more useful column than *how many each*. And queued children count,
the same as they do through a parent, so a list rendered inside a
`store.transaction()` with notes waiting to be written shows numbers that agree
with the objects on the same screen.

None of it is worth a thought at twenty rows, where the count in the loop is
clearer and the page is fast either way. This is for the admin list, the export
and the dashboard.

It needs `collection=` on the child, since `store.notes` is what it hangs off. A
child that named none is reachable only through a parent, and counting there is
`person.notes.count()`, one parent at a time — which is what most children want,
and no loss on the handful of them a page displays.

### Children in bulk

Children queue on their parent, and a set is no different: `save_all` writes
every record along with whatever each has queued against it. Which matters when
one decision touches forty records and each of them still needs its own account
of what happened.

A day of events called off for the weather:

```python
events_for_day = store.events.on(date(2026, 9, 14))

for event in events_for_day:
    event.status = "cancelled"
    event.notes.add("Storms forecast across the Blue Mountains.")

store.events.save_all(events_for_day)
```

Every event carries its own note, so somebody reading one of them next year
cannot tell it was cancelled alongside forty others. Setting a field and adding a
child are the same kind of act here — both are queued against the record they
belong to, and both are written in its transaction.

> **What DSQL is doing.** A note doubles what each event costs: two rows rather
> than one. The 3,000-row ceiling counts rows and not records, so how many events
> fit in a transaction depends on what is queued against them. dray measures that
> for the set in hand rather than assuming, which is why the ceiling is not
> something to work out at each call site.

The one shape that measuring cannot save is a single record carrying more than
a transaction holds, since there is nothing to split it away from itself. That
is refused where you wrote it rather than at the database:

```python
event.notes.add_all(a_few_thousand_notes)
store.events.add(event)
# ValueError: one Event carries 3400 queued children, which is 3401 rows with
# its own, and one transaction holds 2000. A record cannot be split from its
# own children, so save it with fewer and add the rest to it afterwards.
```

Which is what the second half of that message is for: `save` the record with
what fits, then add the rest through the child's own collection, a set at a
time. *Reading across parents* below is where that collection comes from.

Replacing what one record already has, rather than adding to it, is `clear` and
then `add` in that order:

```python
person.notes.clear()
for line in corrected:
    person.notes.add(line)
person.save()
```

The first line happens where it is written and the rest waits for the save, so
those are two transactions unless you open one around them — a decision about
how much it matters that a reader never sees the person with no notes at all,
and one left where the domain can make it.

`clear` is unsized like every other removal here, so a generation past the
3,000-row ceiling is refused whole and the replacement never starts.
`person.notes.thin()` and its loop are what take a set that size off first, at
the cost of the transaction the two lines above are arguing about —
*Children of children* has it.

## Recording changes

A field can name a function to call when its value moves. Setting one is plain
assignment — `person.status = "volunteer"` — so there is no setter to override
and no method to wrap, and `on_change` is where that logic goes instead: declared
on the field, next to the thing it reacts to.

What it does is yours entirely. A common thing to want is a history:

```python
from dray import Change, field, records_change


def flag_for_review(change: Change) -> None:
    if change.old == "volunteer" and change.new != "volunteer":
        change.record.needs_review = True


@record(table="person", collection="people")
class Person:
    family_name: str = field(on_change=records_change(into="logs"))
    given_names: str = field(default="", on_change=records_change(into="logs"))
    status: str = field(
        default="enquiry",
        on_change=[records_change(into="logs"), flag_for_review],
    )
    suburb: str | None = field(
        default=None, stored_in="blob", on_change=records_change(into="logs")
    )
    needs_review: bool = False


@child(of=Person, name="logs", table="person_log")
class PersonLogs:
    message: str
```

`records_change` is supplied with dray, and has to be told which child to write
into — nothing here guesses. `flag_for_review` is not supplied and is not about
recording at all: a handler is a function called when a value moves, and what it
does with that is your business. Somebody leaving volunteer status wants looking
at, so it sets a field, which the same save writes.

A field can name several functions, run in the order listed, and stopping at the
first one that raises. Each field decides for itself, so one that names nothing
does nothing. `needs_review` names nothing on purpose: `flag_for_review` is what
sets it, and a log line about that would be noise rather than history.

The order matters and is the only order that works: a handler names its child by
attribute and only reaches it when a value moves, so it can be written before
that child exists. The child then names the record. Declaring the child first
would mean naming a class that is not there yet.

It queues a `PersonLogs` saying what moved, which the parent's save then writes.
A log is a child like any other, so you can also just add one:

```python
person.suburb = "Wentworth Falls"
person.logs.add("Called and gave a new address.")
person.save()

person.logs[-2:]
# [PersonLogs(message="suburb changed from 'Katoomba' to 'Wentworth Falls'."),
#  PersonLogs(message="Called and gave a new address.")]
```

Nobody wrote the first line and somebody wrote the second. `logs.add` queues one
directly, `on_change` queues one on your behalf, and both are written in the same
transaction as the suburb itself.

A value arriving for the first time and a value being replaced read differently
to whoever looks this up next year, so `describe` writes three sentences rather
than one — `suburb set to 'Katoomba'.` when there was nothing there,
`suburb of 'Katoomba' cleared.` when there is nothing there now, and the form
above when a value moved.

Which is one answer and not the answer. It writes a sentence, and a sentence is
the wrong shape when what you need is the old value as a *value* — to render a
date the way your pages render dates, to show a change as two columns, to let
somebody sort on what the field was. That is a handler of your own rather than a
setting on this one, because the fields it would fill are on a child class only
you have seen:

```python
def moved(change: Change) -> None:
    change.record.logs.add(
        f"{change.field_name} changed.",
        field_name=change.field_name,
        was=change.old,
        now=change.new,
    )
```

Ten lines, the same `Change` handed over, and the child says what it wants to
hold. `records_change` is there so that nobody writes those ten lines to get an
ordinary history, not to be the only way to have one.

That handler words its own sentence because there was nothing else there to
write one, and it does not have to. `describe` is a name you import, like
`Change` beside it, and it hands back exactly what `records_change` would have
written — three cases and all. A handler that wants the sentence *and* the
values as values says `describe(change)` where that f-string is.

`records_change` is generic, so it is built by calling it. A handler of your own
already knows what it does, so it goes in as it is — and it can decide for itself
whether a change is worth anything at all:

```python
from dray import Change

def cancellation(change: Change) -> None:
    if change.new != "cancelled":
        return
    if "notes" in change.record.children:
        change.record.notes.add(f"Cancelled — was {change.old}.")


@record(table="event", collection="events")
class Event:
    name: str
    starts_on: date
    status: str = field(default="planned", on_change=cancellation)
    suburb: str | None = field(default=None, stored_in="blob")
```

Nothing in dray decides that a change deserves a record, or which child it
belongs in. It calls what the field named, handing over a `Change` — the record,
the field's name, the value before and the value after — and the rest is yours.

`record.children` is every kind of child declared for that record, by the name
its parent calls it. Asked here rather than assumed, because one handler can sit
on records of several kinds and not all of them need be noteable. Note the
question it answers: whether this *kind* of record has notes at all, which is not
the same as whether it has any — `if record.notes` would be false for a person
nobody has written about yet, and would read the database to decide.

> **Careful.** Keep a handler to the record in front of it. Setting its fields
> and queuing its children both ride the save that follows; reading another
> record does not. A handler runs on assignment, so there may be no store to
> reach through yet, no transaction open, and `person.status = "volunteer"` in a
> loop of four hundred is four hundred round trips on a line that looks free.
> Close over anything constant when the handler is built, as `records_change`
> does with the child it writes into. Anything needing other records is a
> service, and belongs above dray.

## Early and late assignment

Some values are not known where a record or a child is made. Who is doing this,
which request it belongs to, which import it arrived on — the code building the
object often has no idea, and the code performing the write always does. So the
write says, and the field takes it.

The logs from the last section are the case in point. They say what moved and
nothing about who caused it, and there is no `logs.add(...)` anywhere in your
code to pass an author to, because dray queued them on your behalf. Threading
somebody through every function that might move a field is the plumbing you were
avoiding in the first place.

Give them a field for it:

```python
@child(of=Person, name="logs", table="person_log")
class PersonLogs:
    message: str
    whom: str = "System"
```

and the write fills it in:

```python
person.suburb = "Wentworth Falls"
person.save(given={"whom": "rod"})

person.logs[-1]
# PersonLogs(message="suburb changed from 'Katoomba' to 'Wentworth Falls'.", whom="rod")
```

Nothing between the assignment and the save mentioned an author. The save did,
once, and the line dray wrote has one. `given` is a dict of your own field
names, kept apart from the options a write takes so that neither can be read for
the other — the same division `equals` makes on a read.

Where nobody says — a nightly expiry, a migration, anything with no person behind
it — the declared default stands rather than the write failing:

```python
person.given_names = "Ernest Miller"
person.save()

person.logs[-1]
# PersonLogs(message="given_names changed from 'Ernest' to 'Ernest Miller'.", whom="System")
```

A write assigns by name across everything it is writing, the record and every
child queued against it alike. So a batch job that knows the answer before it
starts says it once, early:

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    imported_from: str | None = None
```

```python
store = Store.connect(
    host="...",
    defaults={"whom": "System import", "imported_from": "2019-spreadsheet"},
)

people = [Person.parse(row) for row in spreadsheet]
for person in people:
    person.notes.add("Imported from the 2019 membership spreadsheet.")

store.people.add_all(people)

people[0].imported_from, people[0].notes[-1].imported_from
# ("2019-spreadsheet", "2019-spreadsheet")
```

Nothing in that loop mentions who is doing the work or which import it is. One
key, said once, reaches the person and the note alike because both declared a
field by that name — and which import a row arrived on is the first thing anybody
asks of imported data.

Saying it on the store is safe here precisely because the store belongs to the
job — a long-lived one serving many actors should carry only what is true for all
of them.

You can assign at any point between building the store and writing the record,
but when two of them disagree the narrowest wins: a note that names its own
author beats the save, which beats the store, which beats the declared default.
Early and late is when you may speak, not who is heard.

Naming a field is any of the three ways of choosing a value — handing it to the
constructor, assigning it afterwards, or `parse` finding it in the data:

```python
note = Note(body="Imported.")
note.whom = "rod"
```

Both of those are choices, and a write fills in only what nobody made — the
values it was told, and the ones a field's own handler works out, which is
*What a write fills in* below. Which is a thing a record has to remember about
itself, because the value alone cannot say: `whom="System"` passed in and `whom`
left alone hold the same string.
Records read back out of the table have named nothing — those values came from
the row rather than from whoever is saving it now — so a save fills them in
exactly as it would on a record nobody had touched.

Nothing in dray knows what `whom` is. It is a field on a record, assigned like
any other, and a child that declares none never meets the idea.

# The rest of it

The rest of what you will need. Some of it early, like the table statements these
classes imply; some of it much later, like what settles two people updating the
same record at once.

## Validation

A field can say what it will accept. `choices` covers the common case, and
anything else is a function that raises to reject:

```python
STATUSES = ("enquiry", "candidate", "volunteer", "lapsed")


def not_blank(value: str) -> None:
    if not value.strip():
        raise ValueError("cannot be blank")


@record(table="person", collection="people")
class Person:
    family_name: str = field(validator=not_blank)
    given_names: str = field(default="")
    status: str = field(default="enquiry", choices=STATUSES)
    suburb: str | None = field(default=None, stored_in="blob")
```

**A `choices` collection is fixed where it is declared.** dray keeps the values
rather than the name it was handed, so appending to `STATUSES` afterwards
changes nothing about what the field accepts. Where the vocabulary really does
move — statuses read out of configuration and reloaded, rather than edited into
a release — say so with a function, and it is asked every time a value is
checked:

```python
def current_statuses() -> tuple[str, ...]:
    return tuple(config["statuses"])


@record(table="person", collection="people")
class Person:
    status: str = field(default="enquiry", choices=current_statuses)
```

Every time is often: every assignment, every `parse`, every value a write is
told. It is handed nothing, so whatever it reads is yours to keep in reach, and
it has to be already in memory — one that queries the database is a round trip
per assignment. What it hands back is this process's answer, so a vocabulary
edited elsewhere arrives when this process refreshes it and not before. An
`Enum` class is a collection here rather than a function, and is never called.

A vocabulary your *users* edit is not this. That one is data rather than
configuration, and it is a record with rows rather than a list somebody reloads
— *When the questions keep changing* below is where that goes.

A field also takes only what its annotation says it takes. `given_names: str`
will not accept a number, and `party_size: int` will not accept `"4"` — which is
what a form posts and a spreadsheet holds, so `parse` is where you hear about it
rather than three functions later when the arithmetic fails.

There is no return value to remember — a validator that says nothing has
accepted the value. Rejection happens on assignment, so a bad value never reaches
the record, let alone the database:

```python
person.status = "voluntear"
# ValueError: status: 'voluntear' is not one of enquiry, candidate, volunteer, lapsed
```

Which also means a rejected value never reaches `on_change`. Nothing is recorded
about a change that did not happen.

Loading a row from the table does not validate it. A record written last year,
under a rule that has since been tightened, still loads — otherwise every change
to a rule makes some of your history unreadable. `parse` is the opposite, and
validates everything, because that data is arriving from outside and now is when
you want to hear about it.

**A filter is held to the annotation, and to nothing else the field says.**
`find(equals={"party_size": "4"})` is refused where `party_size` is an `int`,
because a column holds what the annotation allows and nothing else — a value of
another type is not a narrower question, it is a question the table cannot
answer. Asked anyway it used to reach the driver, which said
`operator does not exist: text = smallint` and named neither dray nor the field.
`choices` and the validators are the part that does not run there, for the
reason above: a filter is how you go and find the rows a tightened rule has left
behind, so a status that is no longer one of `choices` still matches every row
holding it. A field with a converter is the same story here as anywhere else,
and the next section is that.

### Converting what arrives

Refusing `"4"` for a `party_size` is right and it is not the whole answer. A form
posts strings, a spreadsheet holds strings, and somebody has to turn them into
numbers. dray will not do it uninvited — it has no way to know whether
`14/03/2026` is March or December, and a record layer that starts guessing has
started becoming an ORM. But a field can say exactly what the conversion is:

```python
from datetime import date, datetime


def convert_str_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    # Nothing else needs guarding: whatever strptime raises, dray re-raises as
    # a ValidationError naming the field.
    return datetime.strptime(value, "%d/%m/%Y").date()


@record(table="booking", collection="bookings")
class Booking:
    party_size: int = field(default=1, converter=int)
    starts_on: date | None = field(default=None, converter=convert_str_date)
    email: str | None = field(
        default=None, converter=lambda v: v.strip().lower()
    )
```

```python
Booking.parse(
    {"party_size": "4", "starts_on": "14/03/2026", "email": "  Rod@Example.COM "}
)
# Booking(party_size=4, starts_on=date(2026, 3, 14), email="rod@example.com")
```

A converter either returns a value or raises, and that is the whole of its
contract with dray. If it returns, the annotation check and the validators see
what it produced rather than what arrived. If it raises, dray re-raises as a
`ValidationError` naming the field, so `"four"` reads as bad data rather than as
a stack trace out of `int()`. It is one function rather than a list, because
each one hands back a value and two would be two answers with no rule for
choosing between them — and it is not the validator's job for the same reason
in reverse: a validator raises to reject and returns nothing, so a field that
only checks can never accidentally change a value.

**Where it runs.** Wherever a value reaches the field, which is more than the
form. The doors you write at:

| | |
|---|---|
| `Booking.parse(form)` | data from outside |
| `Booking(party_size="4")` | your own code, building a record |
| `booking.party_size = "4"` | assignment, any time after that |
| `find`, `find_first`, `count`, `in_batches` | the value you are filtering on |

and the ones dray goes through on your behalf: what a write is told —
`save(given={"whom": …})` and the store's `defaults` — and whatever an
`on_add`, `on_save` or `derived` handler hands back. A handler decides *which*
value and the field still decides what shape it takes.

The last row of the table is the one that decides whether the rest was worth
doing. The field most likely to want normalising is the one somebody normalised
*in order to look it up*, so a filter that skipped the converter could not find
what the write had put there:

```python
# Stored as rod@example.com, and found by any spelling of it.
booking = store.bookings.add(Booking(email="  Rod@Example.COM "))
store.bookings.find(equals={"email": "ROD@EXAMPLE.COM"})
```

That row is also the one door where the validators stay out of it: a filter is
converted, checked against the annotation, and let through whatever else the
field has to say — which is *Validation* above, and the reason a rule tightened
today leaves yesterday's rows findable.

**The one door it does not run at is a row arriving from the table.** What is
stored came through a converter on its way in, so running one again would at
best waste the trip — and at worst stop the row loading, because the rule it
was written under may have been tightened since.

**Which is what the first line of `convert_str_date` is for.** Running at that
many doors means a converter is handed its own output constantly: a record built
from a `date`, a filter written with a real `int`. `int(4)` and
`.strip().lower()` on a tidy string do not mind being asked twice; `strptime`
does. **A converter has to accept what it returns**, and one that would not
survive its own output has to say what it is looking at.

`as_uuid` is supplied because the field most likely to need one of these is the
field holding another record's id, and the obvious spelling is the wrong one:
`UUID(a_uuid)` raises rather than passing it back, so a field given
`converter=UUID` takes the string off the form and refuses `person.id` — the
ordinary way one of those fields is filled — with `'UUID' object has no attribute
'replace'`. `as_uuid` takes either, and says so by name when it is handed
something that is neither.

```python
from dray import as_uuid

organiser_id: UUID | None = field(default=None, converter=as_uuid)
```

All of which is one claim: normalising belongs on the field rather than in
whatever handled the form, because the answer does not change with where the
data came from — or with whether the data is being stored or searched for.

### A rule about the whole record

A validator is handed one value and nothing else. That is enough for most of
what a field has to say about itself, and it leaves a rule spanning two fields
with nowhere to live: a booking that has to end after it starts, an answer that
has to be one of its own question's choices. Neither field is wrong on its own,
so neither field can be the one to say so.

That rule goes on the class, as a method, marked:

```python
from dray import check


@record(table="booking", collection="bookings")
class Booking:
    starts_on: date | None = field(default=None, converter=convert_str_date)
    ends_on: date | None = field(default=None, converter=convert_str_date)

    @check
    def ends_after_it_starts(self):
        if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
            raise ValueError("a booking cannot end before it starts")
```

The same booking as above, with an end to it and the rest of its fields left out
for room.

**It runs at the two doors a record arrives through**: at `parse`, and again on
the way to storage, once per write, after the write has filled in whatever it
fills. Never on assignment, and that is the point: moving a booking a week on
means writing two dates, and a rule that fired on each of them would pass or fail
on whichever one you happened to write first.

Two doors rather than one because a form is the door most likely to be hit first.
A record `parse` accepted and `add` then refused is a refusal arriving a call
late — after the handler has gone on to do other work, and about data the form
had all along. So what `parse` hands back is a record the write will not refuse
for anything `parse` could already see:

```python
Booking.parse({"starts_on": "14/03/2026", "ends_on": "01/03/2026"})
# ValueError: a booking cannot end before it starts
```

**A rule sees what is set at the moment it runs, and that is not the same at
both.** At `parse` the write has filled in nothing, so a field the store's
`defaults` carry or an `on_add` supplies is simply not there yet; on the way to
storage it is — *What a write fills in*, below. A rule reading such a field says
nothing about it while it is absent and is judged at the write, which is the
first moment there is anything to judge. That is what the two `and`s in
`ends_after_it_starts` are doing, and a rule reading a filled-in field wants the
same guard for the same reason.

Two kinds of field are worth knowing by name here. One an `on_add` fills is
absent at `parse` and there by the write. One `clock` fills is never readable at
all: the value is an expression for the database to work out, and there is
nothing to put on the record until the row comes back.

**The children are the same story**, and they are what lets a rule be about a
record *and the rows written with it* rather than about the record alone.
`self.items` inside a rule is the children already stored and the ones queued
against it together, so a total that has to come out exact lives on the class
instead of in whatever function remembered to call it:

```python
@record(table="parcel", collection="parcels")
class Parcel:
    grams: int = field(default=0)

    @check
    def the_items_account_for_it(self):
        all_items = list(self.items)
        if not all_items:
            return
        short = self.grams - sum(item.grams for item in all_items)
        if short:
            raise ContentsDoNotAddUp(f"out by {short}g")


@child(of=Parcel, name="items", table="item", collection="items")
class Item:
    grams: int = field(default=0)
```

A parcel loaded with two items and given a third is judged on all three, which
is what makes this worth putting on the class rather than at the one call site
that creates one. The guard is the one above and the reason is the one above: at
`parse` there is neither a stored child nor a queued one, so the rule sees an
empty list and has nothing to judge yet. Leaving it off is worse here than for a
field, because every `parse` fails rather than one write.

And the exception is yours — dray hands it on untouched, so what comes out of
`store.parcels.add(...)` is `ContentsDoNotAddUp` and not something a caller has
to translate.

**What it costs is a read.** On a record that came back from a table, the
children are not in hand and `list(self.items)` is a round trip, on every save.
That is nothing for a parcel packed once and sent, and wrong for a record saved
often — and there is nothing at the call site to tell the two apart, so it is
the rule's author who has to know which they have.

A rule that has to read *another* record reaches the store through `self.store`
— *After a record lands* is where that comes from — and there is one to reach
only where the record has already been in a store. Not at `parse`, and not at
the `add` that first writes it, since a record built in memory is attached by
the write rather than before it. Which is the first sign that a rule needing the
database wants the other marker: `@before_save`, under *Before a record is
written*, is the same reach with the write's transaction already open, and it is
what a rule that has to read something before it refuses is for.

Raise to reject and say nothing to accept, exactly as a validator does. What you
raise is what your caller catches, because dray hands it on untouched: a
`ValidationError` if you want dray's name on it, and a plain `ValueError` if you
would rather your own message travelled on its own. Every field is checked first
at either door, so a rule comparing two dates is never the thing that reports a
string where a date belonged — which also means a record that breaks a field rule
*and* a rule of its own hears about the field, because the pass stops there.

The write's pass sitting after the filling has one cost. An `on_add` has already
fired by the time a rule refuses the write — which is nothing at all for a
handler that returns a value, since that is what one is for, and is something for
a handler that also does something in the world. Keep those in `@after_commit`,
where they only happen if the rows did.

**dray finds it by the decorator and never by the method's name.** So the name is
yours to spend on saying what the rule is — `ends_after_it_starts` is what turns
up in the traceback — and `check` stays an ordinary word. A `Booking` whose
`check()` means checking a party in keeps it, and dray never goes looking there.

Several are allowed on one record and all of them run, in the order they are
written, a base class's first. Which makes a class that several records share a
place to put a rule they all keep:

```python
class HasASpan:
    @check
    def ends_after_it_starts(self):
        ...
```

A record may mix in several of those, and may override a rule it inherited
without repeating the decorator — the override is what runs, the way any other
method would be. What it may not do is mix in a rule beside an unrelated class
that happens to spell a method the same way, because dray would then call
whichever one Python's method order reaches and nobody marked that one. It is
refused where the class is written, naming both classes and the method, rather
than left to be found in the traceback of whatever ran.

A child is a record and takes rules the same way, run at the same moment as its
parent's: a set is worked out whole — every record, every queued child, every
field either of them named a handler for — and then judged, before the first
transaction opens. A note the record refuses stops the write it was riding with
rather than being found part way through it, and a rule broken at position 4,000
of a set too big for one transaction leaves the first 2,000 unwritten rather than
durable. One built by `parse` rather than queued by keyword —
`person.notes.add(Note.parse(row))` — has been through its own rules at that
door already, exactly as a record has.

The one child that pass cannot judge is one a `@before_save` queued, since it
did not exist when the pass ran. Its rules run inside the write's transaction
instead, and *Before a record is written* is where that is set out.

Loading a row runs none of them, for the reason loading validates nothing: a
booking written before anybody thought to compare the two dates has to keep
loading.

## Types

An annotation is what a field will hold, and where it is stored decides how much
the database can do about that.

A field with a column of its own gets the database type that matches, and the
database keeps it as that type. A field in the blob goes somewhere else: into the
one jsonb document every record carries, shared by every blob field on it. JSON
has six types — string, number, boolean, object, array and null — and nothing
else, so anything the database has and JSON has not is written down as a string
and read back by what the field declared.

Which is the whole of the difference:

| annotation | stored in a column as | indexed | sorted | stored in the blob as |
|---|---|---|---|---|
| `str` | `text` | yes | yes | `string` |
| `int` | `bigint` | yes | yes | `number` |
| `float` | `double precision` | yes | yes | `number` |
| `bool` | `boolean` | yes | yes | `boolean` |
| `dict`, `list` | `jsonb` | **no** | yes | `object`, `array` |
| `datetime` | `timestamptz` | yes | yes | `string`, ISO |
| `date` | `date` | yes | yes | `string`, ISO |
| `time` | `time` | yes | yes | `string`, ISO |
| `timedelta` | `interval` | **no** | yes | `string`, seconds |
| `Decimal` | `numeric(18,6)` | yes | yes | `string` |
| `UUID` | `uuid` | yes | yes | `string` |
| `bytes` | `bytea` | **no** | yes | `string`, hex |

The storage and the indexing are DSQL's rather than dray's, from [supported
data
types](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html),
which is the source for the whole table and worth reading beside it. Three
types have a column of their own and still cannot be indexed, so an index over
a `timedelta`, `bytes`, `dict` or `list` is refused at declaration for the same
reason it is refused inside the blob: there is nothing there to index, and
finding that out from the cluster after local PostgreSQL accepted it is the
worse way to learn it. What the cluster actually says is `datatype bytea is not
supported in a key`, so the same four annotations are refused as a record's
`id` — a primary key is a key, and that is one nobody asked for.

DSQL will sort anything, including the three it will not index, so `order_by`
takes any field here freely.

One entry in the middle column carries a size, and the size is DSQL's too. A
`numeric` given none is unbounded on local PostgreSQL and `numeric(18,6)` on a
cluster — eighteen digits, six of them after the point — filled in by DSQL and
applied when a row is written rather than when the table is made. dray writes
it out, so local PostgreSQL rounds exactly where a cluster rounds and a test
that passes locally has said something about production.

Money is comfortable at six places. A rate, an FX conversion or a scientific
quantity is not, and says so:

```python
@record(table="conversion", collection="conversions")
class Conversion:
    rate: Decimal | None = field(default=None, precision=12, scale=8)
```

which is `rate numeric(12,8)` in the `create table`. They are the words DSQL's
own documentation and `information_schema` use, and the words in the statement
you read before running it. Say both or neither — `numeric(12)` alone means a
scale of zero, and rounds away everything after the point. DSQL holds a
precision of 38 and a scale of 37 at the most, and a class asking for more is
refused where it is declared rather than by the cluster when somebody deploys
it. So is a size on a field that is not a `Decimal`, or one in the blob: a
document holds a decimal as its own text and hands it back whole, so there is
nothing there to round.

Two other things off that page. A `jsonb` value is compressed before its 1 MiB
limit is applied, so the blob holds a great deal more than the number suggests.
And DSQL implements every JSON operator and function PostgreSQL does, with
identical behaviour — which is what makes the `->` and `->>` in your own
statements mean what they mean anywhere else.

Nothing is expressible on one side of the split that is not expressible on the
other, and that includes having no value at all: a field holding `None` is left
out of the document rather than stored as a null, so `{self.blob} ? 'suburb'`
asks what `suburb is not null` asks of a column. Neither says anything about
*when* — a document is rewritten whole whenever anything in it moves, so its
keys are what the record holds now and never a record of what was asked of it
or when the field was declared. Something that needs to know when a question
started being asked is a fact about the question, and wants a record of its
own.

`datetime`, `date`, `time` and `timedelta` are the ones out of `datetime` — and
`time` is worth saying out loud, because the standard library also has a module
of that name. `from datetime import time` gives the type; a bare `import time`
gives the module, and a field annotated with that becomes a `text` column
without a word said.

Note how much of that right-hand column says `string`. In the stored document a
date, a decimal and a piece of text are the same thing, so what brings one back
is the annotation and not the value — a `str` field holding `"2026-03-14"` stays
a string.

```python
@record(table="sitting", collection="sittings")
class Sitting:
    opens_at: time | None = field(default=None)
    closes_at: time | None = field(default=None, stored_in="blob")
```

```python
sitting.opens_at, sitting.closes_at
# (datetime.time(12, 0), datetime.time(15, 0))
```

Which is the point: a field means the same thing on either side, so moving one
is still a line in the class. For a column psycopg does the work; for the blob
dray does, because jsonb has no such types and somebody has to.

That holds for a record. It holds for SQL you wrote only through
`sql_for(name)`, which is where dray does that work for a statement rather than
for a record — `->>` on its own hands back the string the document is holding,
and what turns it back into a `date`, an `interval` or `bytes` is not something
the annotation says. *Where your names live in SQL you wrote*, below.

`Decimal` goes through text and never `float`, or `4.99` comes back as
`4.990000000000000213`. And a stored value that will not parse is handed back
as it is, because `load` never raises — a row written before the field was a
date has to keep loading.

### Things to know

**An annotation dray does not know becomes a `text` column, quietly.** There is
no error and `drift` will not see it, because drift compares names and not
types. The list above is the whole of what is known; anything else is stored as
its string and comes back as one.

**A naive `datetime` goes in and an aware one comes back.** The column is
`timestamptz` and the session has a timezone, so the database attaches one. The
record in your hand keeps the naive value it was given, so it disagrees with the
stored one until you read it again. Use aware datetimes and the question does
not arise.

**A `Decimal` finer than its column is rounded on the way in, and the record
does not find out.** The column is `numeric(18,6)` where the field said nothing
else, the database rounds to fit, and nothing re-reads after a write — so the
value in your hand keeps every digit it was handed and disagrees with the stored
one until somebody asks for it again. It is the same disagreement as the one
above and a quieter one: a naive datetime announces itself the moment anything
compares it, where a number four places shorter compares perfectly well and only
sums wrong. Two doors answer two different questions here: `precision=` and
`scale=` say what the column is built to hold, and a `converter=` says what the
record is allowed to hold, running on assignment as well as at construction so
the value in hand is one the column can store. They are not the same number
wherever a domain works to fewer places than its column was sized for.

```python
CENTS, SIX = Decimal("0.01"), Decimal("0.000001")


@record(table="conversion", collection="conversions")
class Conversion:
    rate: Decimal = field(converter=lambda v: v.quantize(SIX))
    amount: Decimal = field(
        precision=12, scale=2, converter=lambda v: v.quantize(CENTS)
    )
```

`rate` takes the column dray builds when nothing is said and agrees with it.
`amount` says its own size and works to cents, inside a class that also carries
a rate — which is the case the two doors exist to keep apart. Precision and
scale are said together or not at all, and a scale on its own is refused where
you wrote it rather than at the `create table`.

**Wherever what the store holds may differ from what you hand it, a converter is
what makes them agree.** It is the same answer above, where one on `starts_at`
would attach the timezone the database is going to attach anyway.

## Asking for records

`find` takes a description of the row you are after rather than a query
language. The description is a dict called `equals`, each entry says what one
field holds, and all of them have to match. It reads either side of the split
and takes as many fields as you like:

```python
store.bookings.find(equals={"status": "booked"})
store.bookings.find(equals={"status": "booked", "section": "courtyard"})
store.bookings.find(equals={"table_id": None})
```

**A filter describing nothing matches every row.** `find()` with no filter —
and `find(equals={})`, which is the same thing said by a dict that came back
empty — is a `select` with no `where` on it and a record built for every row in
the table. Two hundred thousand bookings is two hundred thousand objects, which
is the honest answer to the question and a landmine behind a friendly name;
*More records than you can hold* below is the shape that survives it. The same
call on a child set is all of *this parent's*, which is four, and the two read
identically.

Which is what makes an optional filter three ordinary lines. A search box
offering name and phone and email, any of which may have been left blank, is a
dict the caller assembles before the call:

```python
equals = {
    name: posted[name]
    for name in ("family_name", "phone", "email")
    if posted.get(name)
}

store.people.find(equals=equals, order_by="family_name", limit=50)
```

A field nobody filled in contributes no entry, and a form nobody has typed into
asks for the first fifty of everybody. Worth saying because the SQL half is the
awkward one: a statement covering those three carries all three parameters
however few were typed, and `where (%s is null or family_name = %s)` comes back
`psycopg.errors.IndeterminateDatatype: could not determine data type of
parameter $1` — nothing in the statement says what a parameter compared against
`null` is, so each one wants a cast. Up here there is nothing to cast, because a
field nobody named puts no parameter in the statement at all.

Which way it comes back and how much of it are the two things `find` says
beyond that, and *The order, and how many* below is both. They sit beside the
filter rather than in it, so nothing dray takes can be read for a field name of
yours — *The names dray owns* is the whole of that. Everything that compares
rather than describes — a range, a join — is SQL you write, handed to
`select_many`, which puts the rows back into records. An answer that is not
records at all goes to `select_rows`, further down. On a column that reads as it
would anywhere:

```python
@collection(of=Booking)
class Bookings:
    def between(self, opens: datetime, shuts: datetime) -> list[Booking]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            " where starts_at >= %s and starts_at < %s"
            " order by starts_at",
            [opens, shuts],
        )
```

The same question of a blob field has to reach into the document, because that
is where the value is:

```python
    def bigger_than(self, party: int) -> list[Booking]:
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.blob}->'covers' > %s"
            " order by starts_at",
            [jsonb(party)],
        )
```

Two things there are deliberate.

**`->` rather than `->>`.** `->>` hands the value back as text, and text sorts
alphabetically — so a party of 10 would not count as bigger than a party of 9,
and the answer comes back wrong with nothing said. `->` keeps a number a number.

**`jsonb(party)` rather than `party`.** The value has to arrive as jsonb to be
compared against one. `jsonb` is dray's own, and it is also what knows how to
write down a `date` or a `Decimal` — psycopg's `Jsonb` refuses those.

Which is one more reason to give a field a column once you have started asking
about ranges of it.

**Two ids do compare, and the comparison answers nothing.** Say two notes carry
the same `written_on` and the question is which of them was written second. The
tiebreak that suggests itself is the id — `where later.id > earlier.id` — and
it parses, runs and comes back with rows in it, because `uuid` has an ordering
on both databases and nothing in dray reads a `where` clause you wrote. It is
also right about half the time, which is the worst rate there is: an answer
that is wrong every time is found on the first run, where this one passes three
and fails the fourth.

What breaks the tie is a field that orders, which is a column the write fills
in:

```python
    written_at: datetime | None = field(default=None, on_add=clock)
```

`clock` is `clock_timestamp()`, and it advances inside a transaction where
`now()` does not — so even two rows written by one save get different readings,
and the order they were made in is a thing the table knows. *What a write fills
in* below is that field in full. An id somebody chose is a different matter and
none of this is about it: a membership number orders the way whoever issued it
meant it to.

A question can reach across tables, and still come back as records. Who has
nobody written about lately:

```python
@collection(of=Person)
class People:
    def unheard_from(self, since: datetime) -> list[Person]:
        notes = self.store.notes
        return self.select_many(
            f"select {self.columns} from {self.table} p"
            " where p.status = 'volunteer'"
            "   and not exists ("
            f"     select 1 from {notes.table}"
            f"      where {notes.parent_type} = '{self.table}'"
            f"        and {notes.parent_id} = p.{self.id}"
            "        and written_at >= %s"
            "   )"
            " order by p.family_name",
            [since],
        )
```

```python
for person in store.people.unheard_from(a_year_ago):
    print(f"{person.family_name} · nothing since last spring")
```

A child table is an ordinary table, so a question about people can be asked in
terms of their notes. Not a name in it is typed out — another collection knows
its own table and what it calls the two columns naming a parent, and the column
holding the parent's **table** name holds `self.table` here. So the only
parameter is the one thing that varies.

Note the shape: the child is reached in a subquery and the outer select stays on
one table, which is what keeps `{self.columns}` usable. A join proper cannot use
it — every dray table has a key, a guard and a blob, spelled `id`, `etag` and
`data` unless a class moved them, so `select id, ...` across two of them is
ambiguous and you would have to write every column out by hand, where it would
then rot the next time the class changed. That much is at least loud now: a list
that has fallen behind the class is a partial select like any other, and is
refused.

It runs the other way round just as well, and that direction is the one people
look for and do not find. *Every note on a booking that was seated* is a
question about notes, so it is asked from the child's own collection, and it is
the parent's table that goes in the subquery:

```python
@collection(of=Note)
class Notes:
    def on_seated_bookings(self) -> list[Note]:
        bookings = self.store.bookings
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.parent_type} = '{bookings.table}'"
            f"   and {self.parent_id} in ("
            f"     select {bookings.id} from {bookings.table}"
            "        where status = 'seated'"
            "   )"
            " order by written_at desc"
        )
```

Same shape and the same reason for it: one table in the outer select, the other
reached inside, so `{self.columns}` still names a whole `Note` and what comes
back saves and deletes like any other. `parent_type` is narrowed as well as
`parent_id`, which is not decoration — the table holds the notes of every kind
of record that declared them, and a booking id is only unique among bookings.

### An answer that is not records

Some questions are not about records at all. How many people in each status, the
takings by section, a count per parent — the answer is a handful of numbers, and
there is no record for it to become. `select_many` cannot help: it hydrates one
class per statement and refuses a statement that does not select the whole of it.

`select_rows` is the same call without the hydrating:

```python
@collection(of=Person)
class People:
    def by_status(self) -> list[dict]:
        return self.select_rows(
            f"select status, count(*) as people from {self.table}"
            " group by status order by status"
        )
```

```python
store.people.by_status()
# [{"status": "enquiry", "people": 1},
#  {"status": "lapsed", "people": 1},
#  {"status": "volunteer", "people": 2}]
```

What it keeps is the reason for it. `{self.table}`, `{self.columns}` and
`{self.blob}` are still in reach, so the table name and the blob's name are not
copied out by hand into a place that will not notice when the class changes.
And what comes back is keyed rather than positional, so a statement that grows a
column does not silently move `row[0]`.

What it does not do is give you the class's types back. Nothing hydrates, so the
values are the driver's: a blob key read with `->>` is text even where the field
declares a `date`, and a `sum` comes back wider than the column it read — a
`Decimal` over a `bigint` as much as over a `numeric`, because a total of many
rows can outgrow the type each row is in, and widening is the only answer the
database can give without a failure it cannot report. Cast it back in the
statement where you know the total fits. That first one is about `->>` rather
than about this call: `self.sql_for(name)` is the `->>` with the cast the class
implies already on it, so a `date` in the blob comes back a `date`. *Where
your names live in SQL you wrote*, below, is what it is for. Name the computed
ones with `as`, too — two unaliased aggregates come back under one key and one
of them quietly wins.

### The order, and how many

`order_by` says what a read is sorted on, in the same words a child's
declaration uses:

```python
from dray import desc

store.people.find(equals={"status": "volunteer"}, order_by="family_name")
store.people.find(
    equals={"status": "volunteer"},
    order_by=("family_name", desc("given_names")),
)
store.people.find(
    equals={"status": "volunteer"}, order_by="family_name", limit=20
)
```

The same forms a child's `order_by` takes, and the same rules behind them:
every name has to be a field the class declares, a blob field is refused
because it has no column to sort on, and `id` goes on the end so the read is
total. What that promises and what it does not is *The order they come back
in* above, and none of it is different here.

**A read that names none takes the class's own**, which a record declares the
way a child does:

```python
@record(table="person", collection="people", order_by="family_name")
class Person:
    family_name: str
```

So `find()` is that order, `find(order_by=…)` is the order this one call wants
instead, and a class that declared nothing falls back to `id` — total and
stable and meaningless, exactly as a child has always fallen back to it.
*Nobody said* is a question with one answer here rather than two, and what it
buys is that no read arrives in whatever order the database felt like.

The bill is worth knowing rather than discovering. **Any order but the key is
a sort**, on every read that takes it and not only on the call that asked for
it — the same cost a `@child` declaring one has always carried, and there to
be read in `explain`. An index on the column does not spare you one: 2,000 rows
ordered by an indexed column and by an unindexed one both sorted, and cost the
same as each other.

The fallback is genuinely free, and that is worth the sentence it takes.
On DSQL the table *is* the primary-key B-tree, so `order by id` is the order a
scan hands back anyway — the planner drops it, the plan is the one a read
with no `order by` already had, and a bare read was arriving in key order
before anybody asked.

`asc` and `nulls=` come from there too, and so does the half of it that costs
nothing: a bare name already puts the rows holding nothing at the bottom and
`desc` already puts them at the top, on both databases, without anybody saying
so. The other two orders are the ones worth a word:

```python
from dray import asc, desc

store.notes.find(order_by=asc("written_on", nulls="first"))
store.notes.find(order_by=desc("written_on", nulls="last"))
```

A `limit` without an `order_by` is the first twenty in the class's order, and
on a class that declared none it is twenty rows nobody chose — the same twenty
every time, which is a narrower promise than it sounds. A limit is only worth
as much as the order underneath it.

**This is where a sort column from outside belongs.** A list somebody is
looking at usually lets them choose the order, so the column name arrives from
a query string — and that is the one identifier a page genuinely has to take
from a caller. Here it is checked against the declaration before it reaches a
statement. Reaching for an f-string instead is the case AWS's DSQL guidance
names specifically, and the reason it is worth knowing there is somewhere else
to put it.

### One of several values, and none of them

A lifecycle has a list of live statuses and a page has a list of ids, and either
is still a description of one field — `= any(...)` rather than `=`. It is
spelled with a value:

```python
from dray import any_of

store.people.find(equals={"status": any_of("candidate", "volunteer")})
store.people.find(equals={"id": any_of(ids)})
```

Loose arguments or one iterable, whichever reads better where you are standing.
It works on both sides of the split, composes with ordinary equality —
`equals={"status": any_of("candidate", "volunteer"), "suburb": "Leura"}` — and
`count` takes it too. An empty one matches nothing, which is what "in an empty
set" means and saves the caller a special case on the day their list comes back
empty.

A marker rather than a bare list, because a bare list already means something
else. `find(equals={"tags": ["a", "b"]})` is *tags equals that list*, which is
an ordinary question to ask of a `list` field — so if a bare list also meant
"one of these" it would mean opposite things on identical-looking lines, decided
by an annotation nobody can see from the call site.

`none_of` is the same thing the other way round, and it takes its values the
same way:

```python
from dray import none_of

store.bookings.find(equals={"status": none_of("cancelled", "no_show")})
```

**A record whose field was never set matches it.** "Everything except cancelled
and no-show" includes the booking whose status nobody has given yet, because
`equals` describes a row and `None` in a filter already means *unset* — a status
that was never given is a status that is none of these. Written by hand it goes
the other way: `status <> all(...)` drops those rows, since `null <>
'cancelled'` is unknown rather than true, and the row that vanishes is exactly
the one where the question was never answered. `none_of` writes `(status is null
or status <> all(...))` instead, on a column and inside the blob alike.

Which leaves one thing it deliberately cannot ask — *has a status, and it is not
one of these* — and that one is `select_many` with SQL you wrote. `none_of(None)`
is refused rather than guessed at, for the same reason.

An empty one matches everything, the mirror of `any_of()` matching nothing. Both
are the right reading, and together they mean a list that came back empty asks
for no rows one way round and every row the other — worth a look wherever the
list arrives from outside.

| the question | how |
|---|---|
| every row there is | `find()` |
| this field is that value | `find(equals={"status": "volunteer"})` |
| and this other one too | `find(equals={"status": "volunteer", "suburb": "Leura"})` |
| nothing in this field | `find(equals={"suburb": None})` |
| one of several values | `find(equals={"status": any_of("candidate", "volunteer")})` |
| none of several values | `find(equals={"status": none_of("lapsed", "enquiry")})` |
| how many of those | `count(equals={"status": "volunteer"})` |
| that one, by id | `by_id("d4e6b31c-1a2f-4a8e-9c0d-3b7e5f21eac1")` |
| in some order | `find(equals={…}, order_by="family_name")` |
| with the empty ones at the top | `find(order_by=asc("written_on", nulls="first"))` |
| the first few of them | `find(equals={…}, order_by=…, limit=20)` |
| just the one of them | `find_first(equals={…}, order_by=…)` |
| a range, a join | `select_many`, with SQL |
| the first row of one | `select_first`, with SQL |
| a sum, a group, a count per something | `select_rows`, with SQL |

The line is description. Every entry says what one field holds — that value,
nothing, one of several, none of several — and what stays outside is comparison:
a range, a pattern, a join. dray writes the statements it can be sure of and
hands you the rest, rather than growing a query language that can almost say
what you mean.

**Which side your busiest read falls on is a property of your domain, not a
measure of how well you are using this.** *Who is a volunteer in Leura* is a
description, and a page built on questions like it lives inside `find`. *What is
running late* is `due_on < %s` however it is phrased, and so is every
other form of it — this week, last month, overdue by a fortnight — so a domain
whose central question is a date will write most of its hot path itself. Both
are ordinary. The half you write goes in a `@collection` where the table and the
columns come off the class, and the call sites read the same either way.

### Nothing found, and which way it says so

Three of those hand back a single record, and they do not agree about nothing
being there — which is deliberate.

**`by_id` raises `RecordNotFound`.** An id is something you already believe in —
it came off a row, or a URL, or a form that was filled in from one. Nothing
answering to it is a broken assumption rather than an answer, and the exception
says which class and which id at the one point where both are still in hand.
Returning `None` there would put an `AttributeError` a few lines further on, in
code that no longer knows what it asked for.

**`find_first` and `select_first` hand back `None`.** They are searches, and a
search matching nothing is an ordinary answer to an ordinary question. Say
`order_by` if you care which one you get: `find_first` falls back to the class's
own order and `select_first` to whatever your statement said, so a first out of
a class that declared no order is the lowest id and not a row anybody chose.

It is also the read a record can ask to have kept for a moment, since a key is
the one thing a write can be matched back to exactly — *Not asking twice*, much
later on, is what that turns on and what it costs.

So: **asking by identity raises, searching returns `None`.** A caller with an id
and no confidence that anything answers to it can search for it instead —
`find_first(equals={"id": …})` is the same row asked for the other way, and the
id is checked the same way going in, because `id` is a field with a converter
like any other. What differs is only what you get for a row that is not there: a
`RecordNotFound` naming the class and the id, or a `None` to branch on.

What that buys is the shape you asked for at the call site. `by_id` hands back a
record and nothing else, so there is no checking to do before using it:

```python
person = store.people.by_id(person_id)
print(person.family_name)
```

and a search hands back something to branch on, which reads as the branch:

```python
if next_up := store.events.find_first(
    equals={"status": "planned"}, order_by="starts_on"
):
    print(f"next · {next_up.name} on {next_up.starts_on}")
```

Nothing to catch, and the quiet diary is the `else` rather than an exception on a
path that was never wrong. It is the same reason a read that matches nothing
hands back `[]` rather than raising: empty is only exceptional where you had
already claimed otherwise, and having an id is that claim. Every read here
returning a record-or-`None` would put that check on every caller, including the
ones that had nothing to check.

`first` rather than `one`, in both names, because neither checks that there was
only one. A statement matching four hundred rows answers `select_first` quite
happily and hands over whichever row came back first.

### More records than you can hold

`find` and `select_many` build everything the statement matches, so two hundred
thousand volunteers is two hundred thousand objects. A limit was never the
missing piece — `find` takes one, `find_first` is one, and so does whatever SQL
you write. What is missing is the walk:

```python
for batch in store.people.in_batches(of=500, equals={"status": "volunteer"}):
    for person in batch:
        person.status = "lapsed"
    store.people.save_all(batch)
```

It takes every filter `find` takes, `any_of` included, though neither of its
options: the order here is the walk's own and the size is `of`. It yields lists
rather than records — because the batch is the unit you hand back to `save_all`.
A read-modify-write over a large set has to chunk both ends, and dray now does
both: 3,000 rows to a transaction going out, and `of` coming in.

It rides `id > last` ordered by id, which is total and stable, and on DSQL the
primary key *is* the table — so the walk needs no index of its own and no second
lookup to fetch what it found. Total and stable is the whole of what a cursor
wants, and it is all that comparison spends: `id > last` divides the set into
the part already visited and the part not, which any total order does. It says
nothing about which record came first — *Asking for records* above is why the
two are worth keeping apart. Two things follow from that. The order is the
id's rather than yours, because a walk is for visiting everything and anything
that cares about order wants `find` with an `order_by`, or SQL of your own where
the set is too large to hold at once. And editing as you go is safe in
one direction only: the walk never goes back, so a record you have edited out of
the filter is never seen twice, while one edited *into* it behind where the walk
has reached is not seen at all.

Where a walk is slow it is slow in the way a pool fixes. It is a sequence of
round trips, and *Pools and threads* below is the section that says the answer
to those on DSQL is more connections rather than more work through one. A batch
is an ordinary list once it is in your hand — nothing about it belongs to the
store it came off, and `save_all` on another store's collection takes it — so
the writing fans out a store per batch and the wall clock becomes the slowest
batch rather than the sum of them. Only the reading stays where it is, because
each statement of the walk needs the id the last one ended on. Hand out a few
at a time rather than the lot, though: submitting every batch the walk can
yield pulls the whole set into memory, which is the thing this section is about
not doing.

> **What DSQL is doing.** The obvious way to read a set too large to hold is a
> server-side cursor, and DSQL lists no `DECLARE` or `FETCH` among the SQL it
> supports. Which is consistent rather than arbitrary: a cursor holds its
> transaction open, and a DSQL transaction is killed at five minutes and 3,000
> mutated rows. The read-modify-write above spends most of its time in your code
> rather than in the database, so a held cursor would die partway through with
> no way to resume. This is a series of ordinary statements instead, one per
> batch, each in its own transaction, and it does not care how long you spend
> between them.
>
> Nothing runs ahead of you, either. It is a generator, so the statement for the
> next batch is built when you ask for the next batch — `break` out of the loop
> and it is never issued, which makes a search that finds its answer in the
> first five hundred of two hundred thousand cost one round trip. Abandoning a
> walk costs nothing and leaves nothing open, where abandoning a cursor leaves a
> transaction that has been ageing against that ceiling the whole time you were
> deciding.

### One statement instead, and what it stops running

The other way to change a great many rows is to build no objects at all and
send one `update` over the set. Past three thousand rows there is no such
statement:

```
update over 1,000 rows:  accepted
update over 4,000 rows:  ProgramLimitExceeded: transaction row limit exceeded
```

A statement sits inside a transaction like everything else, and a DSQL
transaction mutates 3,000 rows and no more. So *merging two of these means
repointing forty thousand rows, which is one statement* is not one statement;
it is a failed one. Local PostgreSQL has no such ceiling and takes it quite
happily, which is why the place that gets found out is a deployment. Past the
line there is nothing left to trade, either: you are walking whichever way you
go, and the only question is whether you walk with what the class declares or
without it.

Under the line one statement really is one round trip, and dray has no call
that makes one. Every write it offers takes records — `add`, `add_all`,
`save_all`, and a `delete` that removes one — so a set-based write has nowhere
to live but the connection, which a collection method is already holding:

```python
@collection(of=Event)
class Events:
    def close_off(self, season_ended: date) -> int:
        """Every planned event now behind us, marked done. One statement and
        one round trip, and none of the class's rules."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"update {self.table} set status = 'done',"
                f" {self.etag} = gen_random_uuid()::text"
                " where status = 'planned' and starts_on < %s",
                [season_ended],
            )
            return cur.rowcount
```

That is allowed, and on a table with nothing declared on it — a link row, a
flag nobody validates — it is often right. What it costs is not the obvious
thing, and all of it is worth reading before deciding it is worth paying:

- **The ids are yours to keep straight.** The names above at least follow a
  rename, because they came off the collection rather than out of memory —
  *Where your names live in SQL you wrote* is the rest of those, and
  `dray.names_of(cls)` is the same seven where there is no collection to stand
  in — but nothing checks that the statement means what the class means, and
  `drift` does not read it.
- **Nothing the class declares runs.** No converter, no validator, no
  `on_save`, no `on_change`. So this is exactly the write that will not keep a
  `derived` column true, which bites hardest where `derived` is standing in for
  an index DSQL would not give you.
- **No change is recorded.** A field carrying `records_change(into="logs")`
  misses an edit that did reach the table, and the log becomes a record of the
  edits that went through dray rather than of the edits that happened.
- **The etag does not move unless you move it**, and that is the item that
  lands on somebody who did not choose it.

Which is why `etag` is in the statement above. Leave it out and the row changes
while its token does not:

```
row changed by the update:                   yes
etag moved:                                  no
guarded save holding the pre-update etag:    accepted
```

So the next person to save with the etag they were shown *before* the statement
ran is waved through by a guard that should have refused them — on every row
the statement touched, until each of them is written again. It costs whoever
wrote the statement nothing whatever. Every write dray makes mints a fresh
token, so setting one here is not a courtesy: it is the one item on this list
that somebody else is relying on.

All of which is one sentence. **Reach behind dray and you have taken on what
dray was doing** — for those rows, in that statement, and for as long as the
rows live.

## What a write fills in

Some fields nobody assigns — when a record was made, who last touched it. Those
are filled by the write, in the same way a change is recorded by the field that
named a handler.

```python
from dray import Write, clock, field

def whoever(write: Write) -> object | None:
    return write.given.get("whom")
```

`clock` is supplied — it hands back `Sql("clock_timestamp()")` and knows nothing
but the database. `whoever` is yours. dray has no idea what an actor is, no
opinion about what a write should be told, and has never heard the word `whom`;
it carries whatever it was given and a field decides what it wants out of it.

Note what `whoever` does not do. It hands back whatever the write was told —
a username, a `User`, an integer key into a table of people — and leaves the
field to make something storable of it with a `converter`. A handler chooses
*which* value; the field says what shape it takes. `Sql` is the exception and
has to be: `clock` returns text for the statement rather than a value, so no
converter is run on it — and for the same reason the field needs a column of its
own. The blob is written as one parameter, so a field inside it has nowhere in
the statement for an expression to sit, and a handler returning `Sql` for one is
refused rather than reaching the database.

`Sql` is a name you import, like `Write` above it, and `clock` is one expression
rather than the only one: a handler of your own hands one back the same way and
under the same two rules. What the database worked out is read back with the
write, so the record holds the value and not the text that produced it.

`on_add` fires the first time a record is written and `on_save` every time it is
saved. Say both when you want both:

```python
@record(table="person", collection="people")
class Person:
    family_name: str = field(validator=not_blank)
    suburb: str | None = field(default=None, stored_in="blob")

    created_at: datetime | None = field(default=None, on_add=clock)
    created_by: str | None = field(default=None, on_add=whoever, converter=str)
    updated_at: datetime | None = field(
        default=None, on_add=clock, on_save=clock
    )
    updated_by: str | None = field(
        default=None, on_add=whoever, on_save=whoever, converter=str
    )
```

```python
person = store.people.add(Person(family_name="Hemingway"), given={"whom": "rod"})

# Your own class, which dray has never met. `str(jo)` is "jo".
jo = User(username="jo")

person.suburb = "Katoomba"
person.save(given={"whom": jo})

person.created_by, person.updated_by
# ("rod", "jo")
```

A string went in the first time and a `User` the second, and both came back as
text. `whoever` handed over whatever it was given without looking at it, and
`converter=str` made a column value of each — which means `User.__str__` decides
what actually lands in the column.

`write.given` is the bag from *Early and late assignment* — the store's defaults,
then the save, then anything said on the record itself. Four lines, and if your
word for it is `by` or `operator` or an integer key into a table of users, they
are four different lines. dray provides the place to put them and nothing else.

A handler runs once per write and not once per attempt, so a write DSQL refuses
and replays does not call it again. That is what makes deriving a value from what
the record currently holds a reasonable thing to do.

**A child a `@before_save` queued is the one exception**, and it cannot be
otherwise. A rule runs once per attempt by design, and the child it builds is a
new object each time, so nothing of a refused attempt survives for a handler to
have been run once against — a field naming an `on_add` on such a child is
filled per attempt. A handler deriving its value from what the child holds gives
the same answer every time and is unharmed by that. One that counts its own
calls, or does anything outside the record, counts attempts: put that in an
`@after_commit`, which runs when the rows are durable and runs once. *Before a
record is written* is where a rule that queues is set out.

It also runs before the write's pass of any rule the record wrote about itself,
so a `@check` reading `created_by` reads what `whoever` put there rather than
the `None` it would have seen at `parse`. What a rule does about that is *A rule
about the whole record*, above.

Nothing is automatic, which means saying only what you want. A record that wants
no timestamps declares none. One that wants *when did this last actually change*,
rather than when it was last written, says that instead:

```python
    last_changed_at: datetime | None = field(default=None, on_save=clock)
    touched: int = field(default=0, on_save=lambda w: w.record.touched + 1)
```

`clock` hands back `Sql` rather than a `datetime` because the value has to be the
database's and not Python's. It advances within a transaction where `now()` does
not, and rows written by one save would otherwise share a timestamp exactly —
leaving anything ordered by it to break the tie on a random id.

**A field you set is not filled in.** A handler says what the write knows and
whoever built the record does not — and sometimes they do know. An import of
2019 records carries 2019 dates, and stamping those with the afternoon the
script ran is not a timestamp but a fiction. So a field somebody named stands,
and the handler fills in the rest:

```python
people = [Person.parse(row) for row in spreadsheet]   # some rows carry a date
store.people.add_all(people)

people[0].created_at   # datetime(2019, 3, 1, 9, 30, ...), off the spreadsheet
people[1].created_at   # the moment the import ran, for the row that said nothing
```

Naming a field is the same three ways it is in *Early and late assignment* —
handed to the constructor, assigned afterwards, or found by `parse` in the
data — and this is that rule one step further out. The narrowest wins, and a
value you wrote down yourself is the narrowest there is.

The record is what remembers, so a choice lasts as long as the object does: the
same record saved twice keeps what you set both times. Reading it back is what
forgets. A row's values were not chosen by whoever is saving it now, so a record
fresh out of the table has named nothing, and an `on_save` fills it exactly as it
would on a record nobody had touched.

### A field that is never yours

Some fields are not the write's answer to a caller who said nothing — they are
not the caller's at all. A search name folded out of a display name; a column
standing in for something an index will not hold. Setting one of those directly
makes it a lie about the fields it is computed from, and the next write works it
out again and throws the value away without a word.

That is a different sentence about a field, and it is one word:

```python
def folded(write: Write) -> str:
    person = write.record
    return f"{person.family_name} {person.given_names}".strip().casefold()


@record(table="person", collection="people")
class Person:
    family_name: str = field(validator=not_blank)
    given_names: str = field(default="")

    search_name: str = field(default="", derived=folded)
```

A `derived` handler is handed the same `Write` as the others and reads the
record rather than what the write was told — `whoever` above is the other half
of that pair. It runs on the first write and on every save, which is `on_add`
and `on_save` naming one function and is what it compiles to, so a field saying
`derived` says neither of those.

What the word adds is that the field is refused at every door a value could
arrive through:

```python
person.search_name = "hemingway ernest"
# AttributeError: Person.search_name is derived: it is worked out from other
# fields of the record on every write, so it is not a value to set. ...

Person.parse({"family_name": "Hemingway", "search_name": "hemingway ernest"})
# ValidationError: Person.search_name is derived: ...

person.save(given={"search_name": "hemingway ernest"})
# ValidationError: Person.search_name is derived: ...
```

The store's `defaults` are the one bag that does not raise, and the difference
is who typed the name. A `given=` on the call names this class's field on this
write, deliberately. A store default names whatever happens to declare a field
of it — which is what it is for — so a job carrying `whom` for everything it
writes cannot be broken by one class deriving a field of that name. It lands
nowhere, and the write works the field out as it does for one nobody named.

**Nothing in that bag is checked at the store, and it cannot be.** A store is
opened before anybody knows which classes will be written through it, so which
fields will read `whom` — and what each of them wants a `whom` to be — is not
knowable until a write reaches one. A value a field will not take is refused
there, in the middle of an operation, rather than at the door it came in by.
Where that bites is an identity taken off a request: checking it is yours to do
at the edge, because the edge is the first place that knows what a good one
looks like.

Which is why the rule above has nothing to say about it. There is no way to have
named a derived field, so there is no contest to settle and no precedence to
remember — and the column cannot drift from what it is computed from, which is
the whole of what it is for. A row arriving from the table is the one door that
is not a caller, and it comes back carrying whatever the last write worked out.

One write leaves it wrong, and it is not one of dray's: a statement you send
down `self.conn` changes the row with no handler running, so the column holds
what the last save through dray worked out until the next one puts it right.
*One statement instead, and what it stops running* above is that trade with the
rest of its bill.

**A `derived` handler may not hand back `Sql`.** `clock` is right on an
`on_add`, where nothing reads the field off the object afterwards. Here it
cannot be: a value the database works out never lands back on the record, so the
field would read empty in Python while its row held the value — and a field
whose whole point is to be true about the record is the one thing that cannot be
a field the record is wrong about. An expression for the database stays with
`on_add` and `on_save`, and a `derived` handler returning one is refused the way
a blob field's is.

## Two people, one record

DSQL's own concurrency control spans a transaction — microseconds. A form sits
open for minutes. Every record carries an `etag` for the gap: dray mints a fresh
one on every write, and nothing else ever sets it.

Hand it out with the values:

```python
person = store.people.by_id(person_id)

form = {
    "family_name": person.family_name,
    "suburb": person.suburb,
    "etag": person.etag,          # "6c6942b5-9d36-4428-a84d-01f7d57af688"
}
```

Or hand out every field at once and let the template pick, which is what
`as_dict` is for:

```python
person.as_dict()
# {"family_name": "Hemingway", "suburb": "Katoomba",
#  "id": UUID("d4e6…ac1"), "etag": "6c6942b5-9d36-4428-a84d-01f7d57af688"}

Person.parse(person.as_dict())      # the same record again, etag and all
```

Every field the record declares, columns and blob alike, with the ones dray
fills among them — which is what makes that second line work, and why a form
built this way comes back with a token to save against. A child's
`parent_type` and `parent_id` are in it too, so what you get does not change
shape depending on what kind of record you asked. It is a snapshot: changing
what you get back does not change the record.

**What is not in it is the children.** `person.notes` is a read of its own, and
folding it in here would make the cost of one call depend on how many notes
somebody turned out to have — a round trip this page can see becoming one it
cannot. A caller who wants them asks for them and maps this over what comes
back, which is also the only version that lets them ask for some.

**A field the class derives is the one asterisk on that second line.** It is
handed out here with everything else, because it is part of what the record
says and a method of this name quietly missing a field the record has is a
surprise nobody can see from the call. But `parse` refuses a derived field
wherever it arrives from — the reason is under *A field that is never yours*,
and it is the same reason at every door — so a dict from a record that derives
anything is a view to render and not a form to post back. You find that out
where you write it, from an error naming the field.

Hand it back on the way in:

```python
person = store.people.by_id(person_id)
person.family_name = posted["family_name"]
person.suburb = posted["suburb"]

person.save(etag=posted["etag"], given={"whom": me})
```

If somebody saved that person while the form was open, their write minted a new
token and this one raises `RecordHasChanged` instead of quietly reverting their
work across every field the form carried. Otherwise it goes through and mints
another.

Two races, two guards, and you need both. The comparison before the write catches
a form that went stale; `and etag = %s` in the statement catches somebody
committing between this request's `by_id` and its `save`, which no amount of
checking beforehand can cover.

**What a refusal rolls back is the row, and not the record in your hand.** It
still holds the values that were refused — the assignment happened long before
the save, and dray never saw what was there before it. It also still holds
whatever those assignments queued: a field with `on_change` writing into a child
queued its line at assignment, and a refused save settles nothing, so the line is
waiting for the next save that succeeds.

```python
person.suburb = posted["suburb"]      # queues "suburb changed from ... to ..."

try:
    person.save(etag=posted["etag"])
except RecordHasChanged:
    person = store.people.find_first(equals={"id": person.id})
    if person is None:
        ...        # somebody removed it while the form was open
```

Re-read rather than repair. Putting the field back by hand and saving writes the
log line for the move that was refused, and then a second one for putting it
back — two entries for something that never reached the table.

**`find_first` rather than `by_id`, because *changed* includes *gone*.** A
guarded save is refused the same way whether somebody wrote the row or removed
it: a row that is no longer there is the furthest a row can have changed, so a
deletion is not an exception to the guard but the guard doing its job. `by_id`
in that handler raises `RecordNotFound` out of the `except` for a different
exception, which is a second thing to go wrong at the worst possible moment. An
unguarded save asks the other question — *is this row there?* — and goes on
answering `RecordNotFound` itself.

Leave the etag off and the last write wins with nothing said, which is the right
default for a job nobody is watching. `etag=None` *is* leaving it off rather
than an error, which is worth knowing in both directions: a deliberate overwrite
can say so in a variable, and `save(etag=form.get("etag"))` guards nothing at
all on the request whose form came back without one.

The values being assigned are inside `given`, so `etag=` beside it can only mean
the guard — there is nothing left for it to be lost among.

### A set read and written in one process

`save_all` is unguarded by default, because a deliberate overwrite is a
legitimate thing a batch job does. Ask for the guard and it needs nothing from
you — every record already carries the etag it was loaded with:

```python
try:
    store.events.save_all(storms, guarded=True)
except RecordHasChanged as moved:
    moved.ids        # the ones somebody else got to first
    moved.records    # each of them as the table now has it
    moved.written    # the ones that had already landed
```

This is the other race, and the one a bulk edit actually sits in. `save(etag=...)`
carries the token a *reader* was shown, which only you know; this carries what
each record was loaded with, so it catches anybody committing between this
process's read and its write. Skipping it means a forty-record edit overwrites
somebody quietly.

**One record read and written in the same breath is the same race**, and it has
no `guarded=` of its own. The read, the change and the save are three lines, and
anybody can commit in the gaps between them:

```python
person = store.people.by_id(person_id)
person.status = "volunteer"
person.save(etag=person.etag)
```

That last line looks like it compares a value to itself and it does not.
`person.etag` is the one that came back with the read, and it goes into the
`where` of the update — so if the row has moved since, nothing matches and the
save is refused rather than winning. Leave it off and you have written the lost
update this section is about, one record at a time and without the forty to make
it obvious.

It is `save(etag=...)` doing the batch's job rather than a second spelling: the
etag a reader was shown and the etag this process loaded arrive through the same
argument, and which race you are guarding against is decided by where you got
the value. A form's etag guards the round trip; the record's own guards the
three lines above.

It raises rather than handing back a report, because a report nobody reads is a
silent overwrite by another name. Every record is tried before anything is said,
so `ids` is the whole list rather than the first name in it.

`records` is those rows read back and hydrated, in the order `ids` names them —
one statement for the whole set, on a path that was already raising, so a save
that goes through sends nothing extra for it. They are records rather than rows,
so the recovery asks them the questions it would ask any other:

```python
except RecordHasChanged as moved:
    theirs = {storm.id: storm for storm in moved.records}

    for event_id in moved.ids:
        if storm := theirs.get(event_id):
            ...      # storm.severity is what the table says now
        else:
            ...      # somebody removed it while this was writing
```

**An id in `ids` with no record among `records` is one that has gone**, which is
how *somebody wrote this* is told from *somebody removed this* without a second
round trip and without dray needing a second exception to name which. The
message says it too: *removed* where they all went, *changed or removed* where a
set holds both.

One place carries neither, and it is worth knowing which. `save(etag=...)` makes
its comparison against the record in your hand before it sends a statement, and
a refusal there has asked the database nothing — so `ids` and `records` are
empty, and the record and its id are already yours. The recovery for that one is
the re-read above.

> **What DSQL is doing.** What rolls back is a transaction and not the call. A
> set past 3,000 rows is several of them, so the chunk holding the conflict is
> undone and the chunks before it are not — which is what `written` is for, and
> is the same thing the page says about any bulk write that fails partway.

## Two collections in one transaction

A collection writes in its own transaction, which is the right unit almost
always: a record and everything queued against it, together or not at all. What
that does not cover is a promise that spans two kinds of record — *a guest must
never find their booking cancelled without the next party having been offered
the table*. That is a service rather than a collection, and a service can open a
transaction of its own:

```python
with store.transaction():
    booking.status = "cancelled"
    booking.notes.add("Cancelled by the guest.")
    booking.save()

    if next_up := store.waitlist.find_first(
        equals={"status": "waiting"}, order_by="joined_at"
    ):
        next_up.status = "offered"
        next_up.save()
```

Writes inside the block join it rather than opening their own, so the two land
together or neither does. Note what is riding along: the note explaining the
cancellation is queued on the booking, so it is in the transaction too without
being mentioned in it.

It is the store's block rather than a collection's because a store is where the
one connection lives, and a transaction belongs to a connection — which is also
why every collection on that store is in it, and why a second store from
`pool.store()` is not.

**This is narrower than it looks.** It is for two saves that have to agree. It
is not a unit of work, and three things are true of it that are not true of an
ordinary save:

- **Five minutes, and 3,000 rows.** A DSQL transaction is killed at
  `transaction age limit of 300s exceeded`, so nothing that waits on a person, an
  HTTP call or a file may sit inside one. "Wrap these two saves" and "wrap this
  import" look identical at the call site and only the first works.
- **Nothing splits in here.** `save_all` fits a set to the row ceiling by
  splitting it across transactions, and there is nothing to split into inside a
  block you opened — every chunk would join yours. A set that would have to
  split is refused before anything is written, rather than failing at the
  database with half of it sent.
- **A refused commit is yours.** dray replays a write DSQL refuses, which is why
  an ordinary save never shows you one. That replay belongs to the transaction
  dray opened, so inside your own block it does not happen: the refusal arrives
  at the `with` as `CommitRefused`, on the first attempt rather than the fifth,
  and running the work again is very likely to land. What to do about that is
  the rest of this section.

### Running it again

**PostgreSQL queues where DSQL refuses**, and that is why this needs a mechanism
rather than a paragraph. Two writers meeting on one row is a wait on PostgreSQL:
the second takes a row lock, waits its turn and then succeeds. DSQL takes no
lock — both go ahead and whoever commits second is thrown out. The same
function, the same contention, thirty attempts each:

| | refused |
|---|---|
| DSQL | 25 of 30 |
| local PostgreSQL, the same code | 0 of 30 |

Extreme by construction — one row and a writer contending with it throughout —
so the difference is the finding rather than the eighty per cent. What it means
is that a refused commit is the ordinary consequence of two people acting at
once, and that **your suite cannot show you one**: the engine it runs against
queues instead. An application with no replay passes everything and then fails
the first time two guests cancel together.

A rollback undoes the rows. It cannot undo the objects in your hand, and the two
are not the same thing.

dray puts back the two things that would otherwise leave a record unusable: the
etag it minted, and the children it queued and then took off the queue. An etag
no row ever carried refuses its own next save, and a discarded child is gone for
good — it was never written, so there is nowhere to read it back from. Those
two are why a block that fails can be run again at all.

**Everything else stands, and the record is now ahead of its row.** Every field
you set is still set, and so is anything an `on_save` handler filled in:

```python
booking.status = "cancelled"      # still "cancelled" after the rollback
```

For your own values that is deliberate — a replay wants what you intended rather
than what the table kept. But it is why putting those two back is not the same
as making the object true, and why the shape below reads the records again
rather than reusing the ones in hand.

So the shape that works is: **read the records again, and run your own function
from the top.** Not the block — the function, and `@replaying` is what runs it.

```python
@replaying
def cancel(store, booking_id: str) -> None:
    booking = store.bookings.by_id(booking_id)        # read inside
    booking.status = "cancelled"
    booking.notes.add("Cancelled by the guest.")      # queued inside
    with store.transaction():
        booking.save()
        offered = store.waitlist.find_first(
            equals={"status": "waiting"}, order_by="joined_at"
        )
        if offered:
            offered.status = "offered"
            offered.save()

cancel(store, booking_id)
```

`@replaying` is that loop, written once. It takes a count and nothing else —
dray's own five by default, and `@replaying(20)` for a job nobody is waiting on,
where a wait costs nothing and giving up costs a run. When the attempts run out
you get `ConcurrencyExhausted` naming your function, with the last refusal on it
as `__cause__`. How long to wait between them is not yours to set: it is the
same wait an ordinary save takes, up to 50 ms and then doubling, because a call
site has no information a wait can be chosen from.

It catches both of the ways a refusal reaches you. `CommitRefused` is the commit
of a block your function opened; `ConcurrencyExhausted` is an ordinary save
inside it that dray had already replayed five times on its own account, which
under real contention on one row happens often enough to be worth catching. The
attempts you set are the depth dray's own five deliberately do not have.

**It cannot be a `with` block**, which is the shape everybody reaches for.
`with store.replaying(3):` will not exist, because Python will not do it: a
context manager may swallow what its body raised and has no way to run that body
again. Replaying wraps a function or it is nothing.

**What it wraps has to be safe to run twice.** Everything the function does is
inside it, nothing is queued on a record before it, and nothing it does is a
side effect a rollback cannot reach — a job enqueued, a mail sent, a line
appended to a list in memory. Those belong in `store.after_commit`, the next
section, which runs when the rows are durable and so once however many attempts
it took.

**Inside a block somebody else opened, it replays nothing** and your function
runs exactly once. It has to: the refusal has already aborted that transaction
with every statement in it spent, so a second attempt would send statements to a
transaction PostgreSQL has stopped accepting — and the wait before it would
sleep inside a block ageing against the five minutes. The refusal goes up to
whoever opened that block, which is the only level that can run the whole of it
again.

**`retrying` is dray's own and is not the one you want**, though `dir(dray)`
offers both. It replays the writes dray owns, it catches a refusal you never see
because `store.transaction()` has already renamed it, and it goes on dray's
three write paths rather than on yours. Yours is `replaying`: dray replays what
it owns, and this replays what you own.

**Why the function and not the block.** Whatever you queue on a record before
the block is not inside the block, and re-running the block alone will not put
it there:

```python
booking.notes.add("Cancelled by the guest.")   # here
with store.transaction():
    booking.save()                             # ...or here
```

Both write the note. But if the block fails and you re-run only the block, the
first one has a note that is still queued and gets written — and the second one
does not, because the `add` was never re-run. Move the `add` inside and the
second works and the first writes it twice. There is no rule that is right for
both, and dray cannot see which you wrote. A function that reads the record and
builds the work is right for both, because everything happens exactly once
against an object that was fetched fresh.

Which is also why dray does not replay the block for you. It would replay one of
those two shapes correctly and lose the note in the other, silently.

### Hoist your reads

The other half of the same problem, and the smaller one: a read inside the block
is a round trip the block waits for, and the block is the window a conflict has
to find you in.

```python
# The natural way to write it — the read is inside
with store.transaction():
    slots = store.table_slots.for_booking(booking)
    for slot in slots:
        slot.delete()
    store.table_slots.add_all(new_slots)

# The same work, with one round trip less of the block open
slots = store.table_slots.for_booking(booking)
with store.transaction():
    for slot in slots:
        slot.delete()
    store.table_slots.add_all(new_slots)
```

One extra round trip inside made that block about a quarter wider — 191 ms
against 154 ms, one hot row and a contending writer throughout, the median of
thirty attempts each way. **How much wider costs you is not a number this page
will give you**, and the honest reason is that measuring it twice gave two
answers: one harness had the read inside refused half again as often, another
found no difference at all and once had it the other way round. The window is
reliably wider. What the window costs depends on how many writers are meeting
on those rows, which is yours rather than dray's.

This is not the five-minute ceiling, which is the other reason to keep a block
short and the one that gets remembered. It is the conflict window, it is
measured in milliseconds rather than minutes, and like everything else in this
section it is invisible against local PostgreSQL.

**It is a trade rather than a free win.** What you hoisted was read before the
block began, so it is a little older than the block's own view of the table.
Where the work depends on what that read said, guard the save with the etag it
came back with — *Two people, one record* above is that guard, and it is the
same answer as for a form somebody had open.

**That guard answers one of the two ways an older read is wrong, and there is a
second it cannot reach.** An etag is about a row you have: somebody moved it
since you read it, and the save is refused. It has nothing to say about a row
that was not there to read. If what you hoisted is *which rows this change
breaks*, a row written in the gap is not stale in your hand — it is absent from
it, written against a state the block is about to close, and on nobody's list to
go and fix. Nothing is refused, because nothing you are holding has moved.

So the question to ask of a read before hoisting it is not how old it is but
what it is for. A read that gathers context the write needs — the slots to
delete above, a rate, somebody's name — is stale at worst, and the etag is the
whole answer. A read that establishes *what this write is responsible for* is a
different thing wearing the same shape, and hoisting it moves the boundary of
the work rather than the cost of it. That one belongs inside the block, and the
round trip is what it costs to be right about which rows it covers.

**A test can hold it, with what `store.watching()` already hands over.** Every
statement inside a block nests under the block's own span, so `depth > 0` is
*inside the block* — and every read dray writes begins with `select`:

```python
with store.watching(kind="statement") as seen:
    swap_the_tables(store, booking)

inside = [span for span in seen if span.depth > 0]
assert not [span for span in inside if span.sql.startswith("select")]
```

Which is *Counting the round trips*, later on, asked one level down: not how many
statements the page sent but which of them the transaction was open for.

### Once it has committed

The reason this needs a mechanism at all is not the confirmation email. It is
the queued job:

```python
with store.transaction():
    booking.save()
    enqueue("send_confirmation", booking.id)     # a worker starts on it now
    store.tables.save_all(held)                  # ...and this has not committed
```

A worker is another process on another connection, so it cannot see rows that
have not committed. It picks the job up, looks the booking id up and gets
`RecordNotFound` — or finds the row as it was before the block and acts on
that. Milliseconds are enough, and a narrow block does not help, because the
worker is not waiting for you. Rails and Django both shipped without a hook for
this and both added one afterwards on the strength of exactly this bug.

**Where you own the block, the simple answer is still the right one: put it
after the block.** A refused commit raises, so the next line is only reached
when the rows are durable.

```python
with store.transaction():
    booking.save()
    store.tables.save_all(held)
enqueue("send_confirmation", booking.id)
```

**`after_commit` is for the code that cannot do that**, because it does not know
whether a block is open above it:

```python
def cancel(booking) -> None:
    booking.status = "cancelled"
    booking.save()
    enqueue("send_cancellation", booking.id)
```

Called on its own that is right — the save owned its transaction and committed
before the job was queued. Called inside somebody's `with store.transaction():`
it is a race: the save enlisted and committed nothing, the worker starts, and
the booking it is asked about is either absent or stale. Nothing in `cancel` can
tell the two apart. The store can, because it is the thing holding the depth:

```python
def cancel(booking) -> None:
    booking.status = "cancelled"
    booking.save()
    store.after_commit(lambda: enqueue("send_cancellation", booking.id))
```

Inside a block that waits for the block; outside one the rows are already
durable and it runs immediately.

**Put it last in the function.** Outside a block it runs where it is written;
inside one it runs at the end of the block. So anything after it in the function
runs before it in the second case and after it in the first, and a handler
written last cannot be surprised by that.

It runs once the transaction has closed, so a handler may write — that write is
a transaction of its own and is not covered by the one that just committed.

**A handler that raises does not stop the ones behind it.** They are independent
of each other — three cancellations are three guests — so all of them run and
then `AfterCommitFailed` is raised, carrying every failure in `.failures`.

That one is worth catching by name. Everything else out of a
`with store.transaction()` means the work did not land, so a caller who catches
broadly and treats failure as "it did not happen" is right about all of them and
wrong about this one: the rows are committed, and running the work again writes
it twice.

The placement that is always wrong is inside the write itself, in an `on_save`
handler. dray replays a write DSQL refuses, so the job would be queued once per
attempt rather than once per save. A step that belongs to a kind of record every
time it is written has a place of its own, which is the next section.

There is no `on_rollback` to go with it. Undoing a side effect is compensation:
it needs to know which half of it happened, which is your service's business
rather than dray's.

**It runs once per write and not once per change**, which is the sentence to
have in mind before writing one. A handler on a `@record` is called every time
that record is saved, so the obvious spelling of *tell whoever was waiting when
this is finished* —

```python
@after_commit
def tell_whoever_was_waiting(self):
    if self.state == "done":
        notify_them(self.id)          # again on every later save
```

— tells them again when somebody corrects the description a week later, and
again when it moves to another area. The condition is true on all of those,
because it is a question about the row rather than about what just happened to
it.

What that wants is the change itself, and `on_change` is where dray knows about
one. It fires on the assignment, so it can leave a note the handler reads and
clears:

```python
def note_the_finish(change: Change) -> None:
    change.record._just_finished = (
        change.new == "done" and change.old != "done"
    )


@record(table="work", collection="work")
class Work:
    state: str = field(default="open", on_change=note_the_finish)

    @after_commit
    def tell_whoever_was_waiting(self):
        if not getattr(self, "_just_finished", False):
            return
        self._just_finished = False
        notify_them(self.id)
```

An undeclared underscored name is the caller's own transient — *There is no
reserved word*, further down, is the whole of that rule — so `_just_finished`
costs no column and is never written anywhere. Clearing it in the handler is
what stops the next save of the same object in the same process announcing the
finish a second time.

**Set *and* unset, by the same handler**, which is the half that is easy to
leave out. A note only ever set survives a transition somebody abandoned: the
state goes to `done`, the block rolls back or nothing is saved at all, the state
goes back to `open` — and the handler that would have cleared the note never
ran, so the next successful save announces a finish for a row that says `open`.
The assignment that takes the record back out of the state is the one thing that
knows, and it is the same handler.

**The hole in that, and it is not obvious.** `on_change` does not fire for a
value handed to the constructor, so a record born in the state you are watching
for never sets the note:

```python
store.work.add(Work(state="done"))     # tells nobody
```

Which is right for a record loaded from a row and wrong for one created finished,
and a domain where the second happens wants the note set on the `add` path too.
Whether that is a transition at all is a question about the domain rather than
about dray, which is why dray does not decide it.

### Two people, one rule

The etag guards one record. Some rules are not about one record at all.

An event with a cap on how many can come, and who is coming:

```python
@record(table="event", collection="events")
class Event:
    name: str
    starts_on: date
    places: int = 20


@record(table="signup", collection="signups", indexes=[index("event_id")])
class Signup:
    event_id: UUID
    person_id: UUID
```

Two people signing up at the same instant:

```python
def sign_up(store, event, person):
    taken = store.signups.count(equals={"event_id": event.id})
    if taken >= event.places:
        raise NoPlacesLeft()
    store.signups.add(Signup(event_id=event.id, person_id=person.id))
```

Both read nineteen, both conclude there is a place, both write. The event has
twenty-one people on it and nothing raised anything.

Every guard on this page so far misses that, and it is worth being exact about
why, because each of them looks as though it should catch it. **A transaction
does not**: putting those three lines in a block makes the two writes land
together and says nothing about another transaction reading the same nineteen,
which is atomicity answering a question nobody asked. **The etag does not**: it
guards a record written twice and nobody here writes the same record, since
each signup is a new row of its own. **DSQL's own check does not**: it fires
when two transactions write the same row, and these two write different rows.
**A unique index does not**: there is no column to put it on, because *at most
twenty of these* is not a fact about any one row.

Which is the shape of it. The rule is over a set, and everything above guards a
row.

**Read the thing the rule is about, `for update`.**

```python
@collection(of=Event)
class Events(Collection):
    def held(self, event_id: UUID) -> Event | None:
        """The event, and this transaction now conflicts with anybody who
        writes it before we commit."""
        found = self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.id} = %s for update",
            [event_id],
        )
        return found[0] if found else None


def sign_up(store, event, person):
    with store.transaction():
        held = store.events.held(event.id)
        taken = store.signups.count(equals={"event_id": event.id})
        if taken >= held.places:
            raise NoPlacesLeft()
        store.signups.add(Signup(event_id=event.id, person_id=person.id))
```

Both callers still write different rows, and they now *read* the same one. One
of them is refused at commit, dray runs the whole block again, and the replay
reads twenty and raises `NoPlacesLeft` — which is the right answer arrived at
the right way. Inside a `transaction()` you opened the refusal is yours to
replay, which is *Running it again* above.

> **What DSQL is doing.** This is not a lock and there are none to take. `for
> update` adds the rows it returned to the transaction's conflict set, so a
> writer of one of those rows makes one of the two fail at commit — the same
> refusal two writers of one row already get, asked for deliberately about a
> row you only read. Nothing waits and nothing blocks; one side is simply told
> no when it gets there. AWS document it as the way to manage write skew, which
> is the name for two transactions reading a common set and writing rows that
> do not overlap.

**What it reaches is what it returned**, and the predicate is yours to choose.
An id, a set of them, a column that is not the key, a range, no `where` at all
— all accepted, and in every case it is the rows that actually came back that
go into the conflict set. So the rule does not have to be about a row you can
name. Measured against a cluster: two writers reading the same set on an
ordinary non-key predicate, each then changing a different row in it, and one
is refused at commit where without the clause both land and the rule they
shared is quietly broken. That is write skew, and it is the shape of a rule
over several rows that exist.

**A predicate is a cost as well as a guard.** Everything it returned is now
serialised against anybody who writes any of it, and a `where` that matched
more than you meant looks exactly like one that did not. Read the rows the rule
is about and no more.

**What it cannot reach is a row that is not there.** So it still cannot ask *is
there any booking overlapping this hour*: the select is legal, it returns
nothing, and nothing is what goes into the conflict set — both callers find
the hour free and both book it, clause or no clause. That one is the slot
pattern's, under *Indexes, and the one unique thing* below: cut the resource
into rows and let a unique index adjudicate. This section is for the scarce
thing with no slots to cut — a budget, a counter, a place on a list — where
what you can name is the thing the rule is about rather than the claim
against it.

**What it costs is that signups to one event are now serialised**, and that is
inherent rather than a price dray is charging: you asked for a rule that holds
across all of them. It is contention on one row of exactly the kind *Running it
again* is about, so a popular event is a replay or two, and an event nobody is
racing for pays a read.

What the rule is still sitting in is a function, and a function is something a
call site can be written without. *Before a record is written*, two sections
down, is the same read and the same refusal put on the record instead.

## After a record lands

`store.after_commit` is for a service function deciding in the moment. When it is
the same step every time a kind of record is written — this booking is confirmed,
so the kitchen is told — the record can say so once, on the class:

```python
from dray import after_commit


@record(table="booking", collection="bookings")
class Booking:
    status: str = field(default="held")

    @after_commit
    def tell_the_kitchen(self):
        enqueue("booking-confirmed", self.id)
```

It is the same mechanism and the same moment, reached from the class instead of
from a function, and everything the section above says about the moment holds
here. Inside a block somebody opened it waits for that block and runs when the
outermost one commits. With no block open the save has already committed by the
time it returns, so it runs straight after. A block that rolls back never runs
it at all, and a block run again starts with nothing queued from the attempt
that failed — so the second run sends the job once rather than twice.

**It runs once per save, not once per attempt.** dray replays a write DSQL
refuses, and this is registered outside the part that is replayed, exactly as an
`on_add` or an `on_save` is filled outside it. That is the opposite of where a
`@before_delete` sits, and deliberately: that one is work a rollback destroyed
and has to redo, and this one is the announcement of a write that is already
durable. A job enqueued from inside the replay goes out five times for one save.

**Two saves are two runs.** It is about the write rather than about the record,
and two writes happened — including two saves of the same record inside one
block, which run one after another when the block commits. If what you want is
one job for a block however many saves went into it, that is a decision only the
code owning the block can make: put the line after the `with`, or register a
`store.after_commit` of your own once.

**It does not run on a delete.** A delete commits too, so the name invites the
assumption, but a handler is called with nothing and could not tell which had
happened — and it would be holding a record whose row has gone and which it
cannot read again. `@before_delete` is that side, and something that has to wait
until the removal is durable goes in a `store.after_commit` registered from
there, which runs once however many times the delete was replayed.

The record is whole and still attached while it runs, so it can be read like any
other — and `self.store` is the store the write went through, which is how a
handler reaches anything that is not on the record in front of it:

```python
@record(table="task", collection="tasks")
class Task:
    state: str = field(default="open")
    assigned_to: str = field(default="")
    unblocks: UUID = field(default=None)

    @after_commit
    def tell_whoever_was_waiting(self):
        if self.state == "done" and self.unblocks:
            waiting = self.store.tasks.by_id(self.unblocks)
            enqueue("task-ready", waiting.assigned_to)
```

A handler is called with `self` and nothing else, and the class is written long
before any store exists — so without this the only store a rule on the class
could reach is one it closed over at import, which a service handing out a store
per request has not got. This is the store the write went through, and its rows
are committed by the time the handler runs, so a read through it sees them. What
it is not is a store to keep: on a pool the connection goes back when
`with pool.store()` ends, which is already true of `person.save()` and is no
different here.

Anything it writes is a transaction of its own, because the one that carried the
record has closed — so that write can fail on its own account, and when it does
the failure arrives as `AfterCommitFailed` rather than as the save failing:

```python
try:
    store.bookings.add(booking)
except AfterCommitFailed:
    ...      # the booking is written. Whatever the handler wanted did not happen
```

Which is the same name, and the same advice, as a handler you registered
yourself: the rows are committed, so running the work again writes it twice. A
handler that raises does not stop the ones behind it, and `.failures` carries all
of them — a set written with `add_all` is one pass of handlers once the last of
its transactions has committed, not one per transaction it took. Which cuts the
other way when a chunk fails: the pass is never reached, so a set whose third
transaction was refused announces nothing at all, including for the two that
landed.

The one thing it must not do is save the record it is on. That is the same write
again, which registers the handler again, and it recurses until Python stops it.

A child takes one the same way, and a queued child is told when the write that
carried it lands — its parent's save, since it has none of its own.

## Before a record is written

A rule about a write already has somewhere to live. `@check` reaches every door
a record goes in by — `add`, `add_all`, `save`, `save_all`, and a queued child
written by its parent's save — which is more than an overridden `save` method
manages, and it is the right place for almost every rule you will write. What it
does not have is the *moment*. Here is one `save()` of a record whose rule reads
another collection, marked each way, as dray's own spans report it:

```
marked @check                     marked @before_save

prepare                           prepare
  statement   <- the rule's read  transaction   <- the write's transaction
    execute                         statement   <- the rule's read
transaction   <- the write's          execute
  statement   <- the update           statement <- the update
    execute                             execute
```

**A `@check` runs before the write's transaction is open.** That is deliberate
and it is what *A rule about the whole record* promises: the whole set is judged
before a single row is sent, so a bad value at position 4,000 leaves the first
2,000 unwritten. Two things follow, and neither is visible from the outside.

**A `@check` that writes leaves its writes behind.** They went in a transaction
of their own, which committed, and the refusal that follows has nothing to take
them back with:

```python
@check
def keep_what_it_said(self):
    self.store.traces.add(Trace(message=f"{self.id} was written"))
    raise ValueError("not this one")   # the trace stays; the record does not
```

Except inside a `store.transaction()` you opened, where it does not — the check
runs inside your block along with everything else, so the rollback takes its
writes with it. Which is the awkward half: the same handler leaves litter or
does not depending on whether its caller opened a block, and it cannot tell.
One more reason a `@check` is for judging rather than for doing, and a rule
that has to write belongs below.

**And a `@check` that reads is reading outside the write.** By the time the row
is written the transaction that read is over, so a rule built on what it found
is a rule about a moment that has passed — and `select … for update`, which is
how *Two people, one rule* makes a read count, flags rows in a transaction that
has already committed and guards nothing. On an `add` it cannot read at all:
the record has not been attached to a store yet, which is the paragraph about
`self.store` in that section.

So the rule of thumb is about what the rule needs rather than about where it
runs. **A rule about the values on the record in front of you is a `@check`** —
simpler, judged once for the whole set, and heard at `parse` as well as at the
write. **A rule that has to ask the database is a `@before_save`**, because only
that one runs where the answer is still true when the row lands.

*Two people, one rule* is the shape that cannot be a check. An event with a cap
on how many can come, and the rule that there is a place left:

```python
from dray import before_save


@record(table="signup", collection="signups")
class Signup:
    event_id: UUID
    person_id: UUID

    @before_save
    def there_is_a_place(self, write):
        held = self.store.events.held(self.event_id)
        taken = self.store.signups.count(equals={"event_id": self.event_id})
        if taken >= held.places:
            raise NoPlacesLeft()
```

`held` is the `for update` read from that section, unchanged. What has moved is
where the rule lives: it was a service function every call site had to remember
to go through, and it is now on the record, so every door that writes a signup
reaches it. The transaction it needs is the write's own, which dray opens and
replays — so the caller writes `store.signups.add(signup)` with no block of
their own, and the one of two racing writers DSQL refuses reads the count again
on the replay and raises. *Running it again*, arrived at without anybody opening
a `with`.

**The second thing it is handed is the write**, which the rule above takes and
has no use for. That is the other half of what the marker buys, and it is what
lets the commonest rule an application has come off its call sites:

```python
@record(table="ticket", collection="tickets")
class Ticket:
    owner: str
    subject: str

    @before_save
    def only_the_owner_may_write(self, write):
        if write.given.get("whom") != write.was.get("owner", self.owner):
            raise NotYours()
```

`write.given` is the bag from *Early and late assignment* — the store's
`defaults` under the `given=` this call was passed — so a
`pool.store(defaults={"whom": current_user})` opened per request and a
`save(given={"whom": someone_else})` on the one call that is different both
reach the rule, and it holds at every door rather than at the doors somebody
remembered.
It is the same `Write` an `on_add` or an `on_save` handler is given, and
`write.record` is this record.

**`write.was` is the other half of it, and the rule does not hold without
it.** A `@before_save` runs on the record as it *will be*: every assignment the
caller made is already on the object, so `self.owner` inside the rule is the
owner this write is about to store rather than the one the row holds. Written
against `self.owner` the rule compares the caller against the caller, and one
assignment defeats it from any caller at any door:

```python
held = store.tickets.by_id(ticket_id)
held.owner = "jo"                       # jo takes it over
held.subject = "b"
held.save(given={"whom": "jo"})         # against self.owner, no refusal
```

`was` is what the record held before this write, for the fields that have moved
since the row was last written and for no others — `{"owner": "rod"}` above, and
no key at all for `subject` on a save that leaves it alone. So
`write.was.get("owner", self.owner)` is the prior owner either way: the mapping
answers where the field moved, and the default answers where it did not, since
a field nobody touched still holds what the row does. That is the whole of the
call, and it is why there is no second accessor for *did this move* — a rule
that has to know can ask `"owner" in write.was`.

It is a diff and not a before-image, which is what makes it free. dray reads
the old value at every assignment anyway, to decide whether `on_change` fires,
so keeping the first of them per field is a `setdefault` and nothing else: a
record nobody assigns to remembers nothing, and a `find` of ten thousand rows
pays for this exactly nothing. A snapshot taken as each row loaded would be a
tax on every read for a question almost nobody asks.

**The row is what *before* means, and the values last until the row has the
write.** They go at the moment an `@after_commit` runs, so a block that rolled
back leaves them standing and the work run again is judged against the same
prior state — the same reasoning as the transient under *A rule that is about
one kind of save*. Inside a block that moment is not the write: it is wherever
the caller's `with` ends, and a field assigned in between is holding a value no
row ever took. What goes back under that field's name is therefore what the
write stored rather than what the object now says, so the next save is still
judged against the row. Two saves of the same record inside one block are both
judged against what the row held before it, because nothing in a block is
durable until it ends.

Writing into `write.was` raises, which is the one place it differs from `given`.
A commit DSQL refuses is replayed against the same `Write`, so a rule that could
edit the mapping would have its second attempt judged against what its first
attempt wrote into it — it would let through what it had just refused, and
nothing would say so.

`write.adding` is true on the write that creates the record and false on a save
of one that exists. It is the only honest way to ask: the etag is minted when
the record is constructed, so a record carries one before its first row and
*has it got an etag* answers a different question than it looks like. `was` is
empty on that write, because a record being created has no prior anything.

**`given` is what the write was told, and dray has finished reading it.** It is
a plain dict and the same one every record in the write is handed, so a rule
*can* write into it — and nothing dray does reads it afterwards. Every chunk is
worked out before the first of them is sent, so by the time any rule runs, every
field a handler fills has been filled and every value the write was told is
already on the records. Writing into it changes nothing.

**Most rules will want none of it, and the parameter is required anyway.** A
rule that writes a line takes `write` and ignores it, as the signup rule above
does. That is the price of one shape instead of two — a marked method that is
sometimes `(self)` and sometimes `(self, write)` is a distinction a reader would
have to learn to tell apart — and it is paid where the class is written rather
than at somebody's first save:

```python
@before_save
def only_the_owner_may_write(self):
    ...

# TypeError: @before_save calls Ticket.only_the_owner_may_write(self, write),
# and only_the_owner_may_write(self) cannot be called that way. ...
```

**The other three marked methods take `self` alone**, and that is not an
exception carved out for this one. **A marked method is handed what it is
about.** Three of them are about the record and nothing else: `@check` is a rule
about the values and runs at `parse`, where no write exists; `@before_delete` is
about a removal, and `delete()` takes no arguments, so nothing was said that a
rule could read; `@after_commit` is about rows that have landed. `@before_save`
is the only one that runs inside a write a caller parameterised, so it is the
only one about two things — this record, and this write.

**It runs on the write that creates the record, not only on saves of one that
exists.** A first signup has nothing to be stale against, and *there is a place
for this one* is exactly the question an insert asks. So is *write a line
whenever this record is written*, which wants the first write most of all. A
rule that wants one of the two and not the other reads `write.adding` and says
so.

**Raise to refuse, and nothing in that transaction happened** — not the row, not
the records beside it in the same set, not what an earlier `@before_save` on the
same record had written. What you raise is what your caller catches, as
everywhere else: dray hands it on untouched.

A set too big for one transaction is several of them, and only the one holding
the refusal rolls back. The chunks before it are committed and are named by
`written` on the refusal, which is *What landed before it stopped* — so a rule
that could have been a `@check` should be one, since that pass judges the whole
set before the first chunk is sent.

**It does not see the records beside it in the same write.** Every rule in a
chunk runs before any of that chunk's statements, which is what makes a refusal
leave nothing written — and it means a rule that counts rows counts what is
already committed:

```python
store.signups.add_all([...])   # each rule sees the count before any of them
```

Two signups added together into one remaining place both find room. Cap a
resource with this where the claims arrive one at a time, which is the shape the
web request has, and reach for the slot pattern under *Indexes, and the one
unique thing* where they do not.

**It runs once per attempt and not once per save**, exactly as a
`@before_delete` does and exactly opposite to an `@after_commit`. DSQL refuses a
commit that raced another writer and dray replays the whole transaction, this
included — which is right for what it writes, because the first attempt's rows
went with the rollback, and is the whole of what makes the capped-event rule
above work. It is wrong for anything a rollback cannot reach: a counter in
memory, an email, a call to another service. Those go in `@after_commit`, which
runs when the rows are durable and runs once. Inside a block you opened there is
no replay of anything, and this runs in your transaction and once for each time
you run the work.

**It is a rule and not a filler.** What the write is about to send was worked out
before this ran, so assigning a field in here does not reliably reach the row —
and it would be assigned again on every replay. A value the write fills in is an
`on_add` or an `on_save`, under *What a write fills in*. The one thing it must
not do is save the record it is on, which is this same write again and recurses
until Python stops it.

The record is whole while it runs and `self.store` is the store the write is
going through, which is how the example above reaches a collection that is not
hanging off this record. On an `add` that means the record is attached to the
store before its row exists — earlier than anything else about the write, and
the only way an insert could have a rule that writes rather than one that can
only refuse. A record whose write is then refused is left believing it came from
a store it has no row in, which is the position a rolled-back `add` already
leaves one in.

A queued child runs its own when the write carrying it lands — its parent's
save, since it has none of its own. That is the other way round from the delete
side's answer about a cascade, and for a reason that does not carry over: a
cascade loads no rows and has nothing to run a hook on, where a queued child is
in memory and whole.

**And a rule may queue one**, on the record it is on, which this same write then
carries:

```python
@record(table="ticket", collection="tickets")
class Ticket:
    owner: str
    subject: str
    status: str = field(default="open")

    @before_save
    def keep_what_it_said(self, write):
        self.history.add(Entry(what=self.status))


@child(of=Ticket, name="history", table="ticket_entry")
class Entry:
    what: str = field(default="")
```

`self.history.add(...)` is the spelling anybody who has used a child anywhere
else in dray reaches for, and it reads as the same thing as the
`self.store.traces.add(...)` a rule reaches another collection with. It is also
what keeps `records_change` working under a rule: that handler queues a line, so
a field carrying one and moved by a `@before_save` writes its line with the row
rather than moving in silence.

Which closes the gap under *Recording changes*: `records_change` writes a line
for every move of a field and nothing for the value a record started at — the
hole named under *Once it has committed*, since `on_change` does not fire for
what the constructor was handed. A rule reading `write.adding` covers that one
door:

```python
    @before_save
    def opening_entry(self, write):
        if write.adding:
            self.history.add(Entry(what=self.status))
```

Every door that creates one reaches it — `add` and `add_all`, and for a child
the parent's save that carries it — which is the difference between a rule and a
line beside every constructor call.

**A child that arrives that way is judged where it arrives.** Everything the
caller queued has its field rules and its own `@check` run before the first
transaction opens, which is what makes a bad value at position 4,000 leave the
first 2,000 unwritten. A child that did not exist at that moment cannot have
been in that pass, so its rules run inside the transaction instead, and what
they refuse takes that transaction — the chunks already committed stay
committed, exactly as they do for a rule that refuses. It runs its own
`@before_save` as well, and may queue in turn: a rule that queues a note whose
own rule queues an attachment writes all three.

**What it cannot do is take the transaction past the row ceiling.** How many
rows a write is was worked out before any rule ran, so dray refuses rather than
sending the cluster a transaction its own arithmetic said would fit:

```
ValueError: a @before_save queued children that take this transaction to 2400
rows, and one transaction holds 2000. How many rows a write is was worked out
before any rule ran, so a rule that queues has to leave room. Queue fewer in the
rule, or hand the write fewer records at a time.
```

**Counted across the chunks of a write, when the write is inside a block you
opened.** Outside one a chunk is a transaction, and what a rule queued in the
last chunk is nothing to do with this one. Inside one every chunk joins the
transaction you opened, so a rule that adds a row per record overshoots by a
chunk's worth at a time and no single chunk ever looks close to the limit — six
hundred and fifty chunks of forty rows is a transaction of twenty-six thousand,
and every one of them sat comfortably inside a two-thousand ceiling. The refusal
counts what the earlier chunks are still costing, and says the other way out,
since a transaction you opened cannot be split.

That refusal is dray's and not the cluster's, and it is the conservative half of
the two: local PostgreSQL does not care how many rows a transaction holds, so a
write only the cluster would refuse is one nobody could reproduce in
development.

**A rollback does not put it back.** A write that rolled back leaves the
caller's queued children queued, because a queued child has no row to be read
again from and losing it loses it for good. What a rule queued is not one of
those — the rule runs again when the work does, and putting its child back as
well would write two of it for one save.

**What it costs a bulk write** is the question that had to be answered before it
could exist, because `save_all` is the call whose whole purpose is not paying
per-record costs. A record that marked nothing is asked once whether it did — a
dictionary lookup, tens of nanoseconds — and is never touched. A record that
marked one pays a Python call per record per attempt, inside the transaction:
four hundred rows against local PostgreSQL took eleven milliseconds, and an
empty handler added under a microsecond each to that. A handler that *reads*,
which is what this is for, is a round trip per record with the transaction open
— four hundred of those is four hundred round trips inside one transaction, and
is the wrong shape for `save_all` however cheap the mechanism is.

**Where it does not fire.** A statement written through `store.conn`, which the
class never sees. A record's own `save` method calling something other than
`_dray_save`. And a delete, which is `@before_delete` below — nothing here could
tell the two apart, and a rule about what a write leaves behind has nothing to
say about a row going away.

### Four things `was` does not see

Three of them are quiet and the first is not, because it is the one place where
`was` agreeing with the record is the failure rather than the answer.

**A blob container edited in place.** `ticket.tags.append("mine")` never reaches
`__setattr__` — nothing was assigned — so `was` says nothing about `tags`, and
the value it would have kept is that same list, which the append has already
changed. A rule reading `write.was.get("tags", self.tags)` gets the edited list
under both halves of the expression and passes whatever it was guarding. The
only way to catch it is to re-serialise every field on every read and compare,
which is the snapshot this exists instead of. **Assign the container rather than
editing it** — `ticket.tags = [*ticket.tags, "mine"]` — which `on_change` and
the change log under *Recording changes* already want for the same reason: an
edit in place moves a value without ever being a move.

**What the write fills in.** An `on_add`, an `on_save` and the `given=` a field
takes all land on the record around assignment, so `updated_by` and
`updated_at` are never in `was` however far they move. That is right for a rule
about what the row said and wrong for anybody expecting a complete before-image,
which this is not.

**The write that creates the record.** Nothing is before it, so `was` is empty
and `write.adding` is the question to ask instead. A record built and then
edited before its `add` is no exception: what it held in memory for a moment was
never stored anywhere, and judging a new record against it would be worse than
judging it against nothing.

**A value handed to the constructor.** `Ticket(owner="rod")` is exempt exactly
as it is exempt from `on_change` — the hole named under *Once it has committed*
— so a field's first value is not a move from anything and never appears.

### A rule that is about one kind of save

The rule this hook grows into, and the one it is worth being careful about: a
price may go up for nothing, and may only come down if the write that drops it
says why.

That is not a question the record can answer. It holds one price and cannot say
which way it moved, so the rule is about the *write* — and the write is handed
both ends of it:

```python
@record(table="product", collection="products")
class Product:
    price: Decimal = field(precision=10, scale=2)
    reason: str = field(default="")

    @before_save
    def a_drop_says_why(self, write):
        if self.price >= write.was.get("price", self.price):
            return
        if "reason" not in write.was:
            raise NoReasonGiven()
```

Two lines, and each is one end of it. The first asks whether this write is a
drop: `self.price` is where the price is going, because the record is what the
write is about to store, and `write.was` is where it came from — with the
default doing the work, since a save that leaves the price alone has no `price`
key and the two agree.

The second is `in` rather than `get`, and that is the load-bearing half.
`reason` is a stored field like any other, so the first drop that gave one
leaves it on the row and `self.reason` reads true forever after: one excused
drop, and every drop behind it is excused too, 200 to 1 with nothing said.
`"reason" not in write.was` asks whether the reason *moved in this write*, which
is what the rule meant. The same reason assigned again is not a move — nothing
moved — so it does not excuse a second drop either, which is the same answer for
the same reason.

The awkward cases come out right without being thought about. A price raised to
120 and then dropped to 90 before anything was saved is a drop from whatever the
row said, because `was` keeps the first prior value per field rather than the
last. A product created at 100 is not a drop, because `was` is empty on the write
that makes the row. One dropped last week and saved again today needs a new
reason, because `was` was emptied when that write committed. And a commit DSQL
refuses is replayed against the same two values rather than against what the
first attempt left on the record.

**None of it survives being written as an `on_change`**, which is the door to
reach for when what a rule needs is one field's move. A handler on `price` fires
at the assignment, so `product.price = 90` followed by `product.reason =
"clearance"` refuses and the same two lines the other way round do not: the
answer depends on which one the caller happened to write first, which is the
reason *A rule about the whole record* gives for `@check` not running on
assignment either.

**Where what a rule needs is not a field's prior value, there is a longer shape
and it is worth knowing.** An `on_change` handler writing a flag onto the
record, the rule reading it, and an `@after_commit` clearing it — three parts,
and what buys them is the moment: assignment is the only place to catch a
container edited in place, or to keep something about the change other than the
value it landed on.

Each part is where it is for a reason. **The handler sets and unsets**, which is
the one easily left out: a flag only ever set survives a transition somebody
abandoned, and the next save of that object announces it. **The rule reads it
and does not clear it**, because a `@before_save` runs once per attempt and a
cleared flag would leave the replay nothing to read. **The `@after_commit`
clears it**, that being the one of the three that runs when the rows are durable
and runs once — which is where `was` is emptied too, and for the same reason.

The hole named under *Once it has committed* carries over to both. Neither
`on_change` nor `was` sees a value handed to the constructor, so a record whose
very first write already carries what the rule is looking for has nothing to
have moved from — and whether that should count is a question about the domain
rather than about dray.

## Before a record goes

`delete` is a write like the others — the row, and one statement for every
generation hanging off it — and like the others it opens a transaction only when
there is not one already. Inside a block you opened it joins yours, exactly as a
save does. So "remove this note and write down what it said" has an obvious
place to be atomic and does not need a hook at all:

```python
with store.transaction():
    person.logs.add(f"note removed: {note.body}")
    person.save()
    note.delete()
```

What a block cannot do is hold for a removal nobody wrapped. A note goes from
the screen that lists them, from the import that supersedes what it said, and
from the request to erase a person's history — three call sites, and the domain
says every removal is accounted for on the person whichever one it came from. A
rule kept at the call sites is kept at all of them but the one somebody adds
next year. That is what goes on the record, marked, and it is the section above
this one with the write swapped for the removal:

```python
from dray import before_delete


@child(of=Person, name="notes", table="note")
class Note:
    body: str = field()

    @before_delete
    def keep_what_it_said(self):
        person = self.store.people.by_id(self.parent_id)
        person.logs.add(f"note removed: {self.body}")
        person.save()
```

It runs inside the delete's transaction — yours, where you opened one — and
before any of the statements, so the line and the removal land together or
neither does. The record is whole and still attached while it runs, which is the
other half of what makes it useful: it can read its own children before they go,
save other records into the same transaction, and reach whatever its fields
point at. `self.store` is the store the delete is going through, and it is what
reaches the person above — the same store and the same transaction, so the line
the handler writes is inside the one the removal is in. The one thing it must
not do is delete itself, which is this same delete again and recurses until
Python stops it.

Which door the note goes out by decides whether the rule runs at all:

```python
note.delete()             # the rule runs
person.notes.clear()      # it runs, once for each note the clear removes
person.notes.thin()       # it runs for the notes this pass takes, and no others
person.delete()           # the notes go with them, and the rule does not
```

The first three load the note, which is what there is to run a rule on — and
the second and third load them *because* the class declared one, so a class that
declared none goes without a row being read. The fourth is deliberate and has a
paragraph of its own below: a cascade loads no rows, so a hook on a child
covers the note deleted on its own account or with the set it was in, and not
the note taken along with its parent.

**The third is the one that is not all or nothing**, and it is the only door in
dray that is not. A pass is its own transaction, so a loop that stops — because
a rule refused, because the process went — leaves the rule having run for the
notes that went and never for the ones still there. Every other door here runs
it for all of them or for none.

**Raise to refuse, and the row is still there.** Nothing else in the transaction
happened either — including anything an earlier `@before_delete` on the same
record had written.

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    status: str = field(default="enquiry")

    @before_delete
    def a_volunteer_is_lapsed_rather_than_removed(self):
        if self.status == "volunteer":
            raise ValueError("lapse a volunteer before deleting them")
```

What you raise is what your caller catches, as a rule about the whole record
does: dray hands it on untouched. Which gives a record whose domain says it is
never deleted one place to say so, rather than a check at every call site.

**A record that has already gone raises, and this has run by then.** Nothing
has looked at whether the row is there when the rule runs — the delete finds
that out from the statement that removes it, which is afterwards, and then
raises `RecordNotFound`:

```python
note.delete()      # the rule runs, and its line lands with the removal
note.delete()      # the rule runs again — and RecordNotFound takes it back
```

The second line writes nothing. The raise rolls the transaction out and the
handler's work goes with it, so what is left is one history line for one
removal. That order is the only honest one: a read in front of the rule would
be a round trip on every delete that still raced the delete it was guarding,
and it is the rowcount that knows. What it costs is a handler run for a
removal that does not happen — which is the paragraph below's warning arrived
at from a second direction, and the same answer applies to it.

**It runs once per attempt and not once per delete.** This is the one thing about
it worth holding on to. DSQL refuses a commit that raced another writer and dray
replays the whole transaction, this included — which is right for what it writes,
because the first attempt's rows went with the rollback and the second attempt
has to put them back. It is wrong for anything a rollback cannot reach:

```python
    @before_delete
    def keep_what_it_said(self):
        removed.append(self.body)              # twice, on a replay
        notify_the_coordinator(self.id)        # twice, and one of them lied
```

A list in memory, an email, a call to another service. Those go in
`store.after_commit`, registered from inside this handler: it runs when the
delete is durable and runs once however many attempts there were. Not
`@after_commit`, which is about a write and does not run for a delete.

Inside a block you opened there is no replay of anything — the refusal comes back
to you as `CommitRefused` and running the work again is yours, exactly as
*Running it again* describes. This runs in your transaction rather than one of
its own, and once for each time you run the work.

**A cascade does not run them.** Deleting a person takes their notes and the
attachments under those notes with one statement per generation, and not a row
of them is loaded, so only the record you called `delete` on runs its own hooks.
Reaching the descendants' would mean reading the whole tree first, which is
exactly the cost this shape of delete exists to avoid — a person with fifty notes
would become fifty-one reads and fifty-one deletes. A rule that has to hold for
every note belongs on the parent, where one hook can see all of them.

`person.notes.clear()` is the same division one generation lower. It reads the
notes and runs the rule on each of them, because it is the generation being
asked for; the attachments under those notes go with a statement that loads
nothing, so theirs does not run. What that costs when the rule is declared is
the read — a set of two thousand is two thousand records built inside the
transaction, where the same set on a class declaring nothing is one statement.

`person.notes.thin()` makes that division a pass at a time. The pass that
reaches the notes reads the ones it is about to take and runs the rule on each;
the passes that took attachments ran nothing, exactly as a cascade does not.
Which is also why the rule's own writes count against the pass: five hundred
notes and a line apiece is a thousand rows in that transaction, and `at_a_time`
was only ever a bound on what dray removes.

**Nothing gets past it.** A record can also refuse from a `delete` method of its
own — *There is no reserved word*, later on — and that one stands in front of
`person.delete()`, outside any transaction, so `store.people.delete(person)`
walks past it. This is the rule about the removal rather than about the call:
every door reaches it, and it is inside the transaction, so it can write as well
as refuse.

## When it goes wrong

dray raises eight things, and they are the ones you can write an `except` for
without knowing which database is underneath. **Every one of them is a
`DrayError`**, so a request handler that turns any refusal into a 400, or a job
that logs and moves on, catches the one name rather than listing them — and
keeps catching whatever a later version adds:

```python
try:
    booking.save(etag=form["etag"])
except dray.DrayError as refused:
    return 400, str(refused)
```

Each keeps a builtin base as well, so code that handles a bad value or a dead
connection generally still catches them without knowing dray is underneath:
`ValidationError` and `DuplicateRecord` are `ValueError`s, `RecordNotFound` is
a `LookupError`, and `ConnectionLost` is a `psycopg.OperationalError`.

**Which is what puts an order on a ladder of them.** `except` clauses are tried
as they are written and the first that matches wins, so a general one above a
particular one silences it, and nothing anywhere says so:

```python
except (ValidationError, ValueError):  return 400
except DuplicateRecord:                return 409   # never reached
```

`DuplicateRecord` is a `ValueError` too, so a unique constraint refusing an
insert is answered 400 and the line that would have said 409 is dead. The rule
is the ordinary one — the particular before the general — and it is worth
saying here only because these bases are dray's doing rather than yours: the
ladder that goes wrong is one written entirely out of dray's own names, by
somebody who read the paragraph above and took it as an unmixed kindness.
`dray.DrayError` catches all eight and belongs at the bottom for the same
reason.


| | |
|---|---|
| `ValidationError` | a field would not take the value, or `parse` met a key nobody declared |
| `RecordNotFound` | `by_id` found nothing, on a collection or a child, or a save or a delete found no row |
| `RecordHasChanged` | the etag did not match — somebody wrote the row first, or removed it. `ids` and `records` say which |
| `DuplicateRecord` | a unique constraint refused an insert — `columns` and `constraint` say which |
| `CommitRefused` | DSQL refused the commit of a block you opened, and dray does not replay those |
| `AfterCommitFailed` | the rows committed and an `after_commit` handler then raised |
| `ConcurrencyExhausted` | DSQL refused the same write five times running, or a `@replaying` function as many times as it was given |
| `ConnectionLost` | the connection was closed underneath the store — see *Connections* |

Everything else is the database's, and arrives as `psycopg` raised it: a check
constraint you added in a migration, a column that does not exist, a syntax
error in a statement you wrote. dray catches four things and renames them, and
passes the rest along rather than wrapping errors it has no better name for.

### What landed before it stopped

**Every one of them carries `written`**, and it holds the keys of the records
that were already committed when the write gave up:

```python
try:
    store.people.add_all(everybody)
except dray.DrayError as refused:
    landed = refused.written          # keys, in the order they were written
    left = [p for p in everybody if key_of(p) not in set(landed)]
```

A set above the row ceiling is several transactions, so the one holding the
failure rolls back and the ones before it do not — *Children in bulk* is where
that arithmetic is. Thirty-five records in chunks of ten, failing on the third,
leaves twenty rows in the table and thirty-five records in your hand, and
without this there is nothing in reach that says which twenty. It is on the
base class rather than on the ones a bulk write can raise, because otherwise a
caller has to know which of them carries it before writing the `except`, and an
attribute that is sometimes absent is worse than one that is usually empty.

**It is empty wherever nothing landed**, which is most places: a single `save`,
a validation error raised before the first transaction opened, a set that
failed in its first chunk. Empty is the answer there rather than the absence of
one.

**And empty inside a `store.transaction()` you opened**, whatever the write got
through before it stopped. A block is one transaction however many chunks dray
writes into it, so a set that failed partway landed none of itself — the keys
of the chunks that went first would name rows that rolled back with the rest,
which is a worse answer than none. What the block carried is the block's to
know.

`AfterCommitFailed` is the reading to be careful with, and the one place
`written` names every record rather than the ones that got through. The rows
are committed — that is what the name is for — so the handlers are what failed
and the write itself landed. The exception is that inside a
`store.transaction()` the handlers wait for the block, so that one arrives from
the `with` and carries nothing: the block is what knows what it held, and the
call inside it does not.

**It is the records you handed over, throughout.** A queued child is a row and
landed with its parent, and is not in `written` — you are holding parents, and
a key for something you never had in your hand is nothing you can go back to.

Which leaves one edge worth knowing. `DuplicateRecord` is raised on the way in
and nowhere else, so the same clash reads differently depending on how you got
there:

```python
store.people.add(person)     # DuplicateRecord
person.save()                # psycopg.errors.UniqueViolation
```

`ValidationError` is a `ValueError`, `RecordNotFound` is a `LookupError`, and
`DuplicateRecord` is a `ValueError` too, so the ordinary Python ones catch them
if you would rather not import anything. `ConnectionLost` is the one that is not
a plain Python exception: it is a `psycopg.OperationalError`, because that is
what it is and what anybody already handling a dead connection catches. It is a
better sentence rather than a new thing to catch.

**`ConcurrencyExhausted` takes about three quarters of a second to arrive**,
which is the number to have when you are choosing a timeout. dray waits between
replays and the wait doubles: at most 50 ms, then 100, 200 and 400. Each of
those is a ceiling rather than a pause — the wait is a random amount up to it,
so that writers that lost together do not come back together — so the usual
wait is well under the total, and 750 ms on top of five round trips is the
worst dray's own replay can add to anything, whether it ends up landing or
raising.

`@replaying` waits the same way and on top of that, which is what makes the
count you give it a number to choose against a timeout somebody is waiting on.
Its waits climb no higher than 400 ms however many attempts you ask for, so
twenty of them is at most eight seconds of waiting and not the hours a doubling
without a ceiling would be.

## Tables

dray does not run migrations. It works out the table a record implies and hands
you the statements to put in one:

```python
from dray import schema

for statement in schema.statements(Person):
    print(statement)
```

One statement per entry, because DSQL takes a single DDL statement per
transaction. Each is written `if not exists`, because DDL and the row recording
that it ran cannot commit together, so a migration has to survive being run
twice.

Which is most of what a migration runner is for, so what you run these with is
open. AWS publish a Flyway plugin for DSQL that handles the awkward parts — a
transaction per statement, `create index async`, a retry when a schema change
loses a commit — and it is a JVM tool. `yoyo-migrations` is the same idea in
Python. Or neither: applying files in order and recording what ran is a page of
your own code, and the `if not exists` above is the property that makes it
enough.

Data is the part to think about rather than schema. A backfill past 3,000 rows
is not one statement on DSQL and no runner makes it one, so anything large is a
script that walks — `in_batches` below — rather than a line of SQL in a
migration.

### Indexes, and the one unique thing

What a table is indexed for gets decided on day one whether you choose it or not,
and it is a fact about the table rather than about any one field, so it is said
on the decorator:

```python
from dray import index

@record(table="event", collection="events",
        indexes=[index("status", "starts_on"),
                 index("name", "starts_on", unique=True)])
class Event:
    name: str
    starts_on: date
    status: str = "planned"
```

Each `index(...)` is the columns it covers, in the order it covers them, and
`unique=True` where those columns are unique together. One list rather than a
word on each field, because an index over two columns has no field to live on
and because the number of them is a budget spent from one place — which is a
thing you can count here and cannot count when it is scattered.

The order is yours and dray keeps it. Only a leading run of an index's columns
can be searched, so `(status, starts_on)` answers a question about a status and a
question about both, and nothing about a date on its own. Which way round they
go is chosen against the questions the table actually gets — dray cannot see
those and does not guess at them. Here it is that way because *what is planned
from today* is the read this table gets, and the status is the part every one of
those questions names.

Which is also what makes one of two indexes refusable. `index("status")` beside
`index("status", "starts_on")` answers nothing the wider one was not already
answering, since a btree is searched by any leading run of its columns — so it
is refused where the class is written, naming both, rather than built and paid
for on every insert. Only a true leading run counts: `(status, starts_on)` and
`(starts_on, status)` are different indexes and both do work, and so are
`(status, starts_on)` and `(status, name)`. A unique index is never the
redundant one, because it enforces something no wider index does:
`index("email", unique=True)` stands beside `index("email", "joined_on")`. Only
a plain index is ever refused, and a wider unique one refuses a narrower plain
one exactly as a wider plain one would — a unique btree is searched by a leading
run like any other.

`unique=True` is a constraint as well as an index, and where the statement for it
goes depends on which table you have. dray decides that rather than asking: on a
table being created it is a constraint inside the `create table`, enforcing from
the moment the table exists, which is what `schema.statements` writes. Against a
table that is already there it is `create unique index async`, which is what
`schema.create_indexes` writes — and that one is a background job enforcing
nothing until the build finishes, so a duplicate written in the meantime is
taken. Adding uniqueness to a table that already holds rows is a migration
question for you, and what to write while the build runs is further down this
section.

**A unique index constrains the rows that have a value.** Two rows holding
`null` in the indexed column do not collide with one another, which is
PostgreSQL's own rule and DSQL's as well — so `unique=True` on a field that can
be empty reads as *unique where it is given* rather than as a promise about the
table. That is worth knowing in both directions. It is the trap if the column is
nullable by accident, because the constraint is quietly absent for every row
that left it out and the table looks guarded. And it is the tool if the column
is nullable on purpose:

```python
@record(table="signup", collection="signups",
        indexes=[index("sent_as", unique=True)])
class Signup:
    person_id: UUID = field()
    sent_as: str | None = field(default=None)
```

A caller that hands a key of its own gets its second attempt refused, so a form
submitted twice on a bad line puts one person on the list; a caller that hands
nothing is unconstrained and costs no row of its own. That is an idempotency
key in one declaration, without a partial index — `create index … where` comes
back `FeatureNotSupported` here — and without a second table to keep in step.

**A key may say where the empty values go, and may not say a direction.** That
is DSQL's rule, and placement is worth saying here only because of what it pairs
with:

```python
from dray import asc

@record(table="task", collection="tasks",
        indexes=[index("area_id", asc("due_on", nulls="first"))])
class Task:
    area_id: UUID = field()
    due_on: date | None = field(default=None)
```

A btree is scanned backwards as readily as forwards, and a backward scan
reverses everything — so the plain `index("area_id", "due_on")` already serves
`order_by="due_on"` forwards and `order_by=desc("due_on")` backwards, which are
exactly the two orders a bare name and `desc` give. The one above serves the
other two, `asc("due_on", nulls="first")` forwards and `desc("due_on",
nulls="last")` backwards. Which is why the two halves are one decision:
placement on the `order_by` alone returns the right rows in the right order and
makes the database sort for them, and placement on the index alone builds
something nothing asks for.

`index(desc("due_on"))` is refused where the class is written, naming what the
cluster would have said — `specifying sort order not supported for index keys`.
**Local PostgreSQL takes it**, so this is one of the few places the two
genuinely disagree and a green suite would have proved nothing. `unique=True`
will not take a placement either, and that one is dray's own limit rather than
the database's: the unique kind goes into a `create table` as a constraint,
`unique (due_on nulls first)` is not a grammar SQL has, and the two tables dray
writes for would come out indexed differently with nothing to say so.

**A composite mixing directions cannot be indexed at all**, which is worth
knowing because it is a shape a busy read reaches for. `order by area_id,
due_on desc` wants one column up and one down: `desc` on a key is refused, a
backward scan reverses both columns rather than one, and no null placement
reaches it. The rows still come back in that order — the database sorts for
them — and the cost is that sort rather than an index handing them over already
ordered. Where it is the read the table exists for, what fixes it is a column
holding the order you actually want, so that both terms run the same way.

What an index buys is worth saying, because it is not the answer. It changes
what a question cost and never what came back, which is what makes a missing one
invisible: the rows are right, the test asserting them passes, and a suite green
in ten seconds has said nothing at all about whether this table is indexed for
the questions it gets. Measured on a cluster, one question over 12,000 rows is a
full scan touching all 12,000 of them at 10.5 ms, and an index scan touching 300
at 4.6 ms once there is an index that serves it — and half the time is the least
of it, since DSQL charges a read by what it touched rather than by what it
returned. At the few hundred rows a fixture holds, both are correct and neither
is noticeable, which is why the first honest question about an index is how much
data there is going to be.

Which is a thing to read rather than guess at. A collection method is SQL you
wrote, so `explain` goes in front of it and `select_rows` hands the plan back
like any other rows. A `find` is a statement dray builds, and *Watching what
dray does* is where it hands that one over — an observer is given the statement
as sent, so the plan you read is the plan for the question that actually ran
rather than for a reconstruction of it. `scripts/flow.py` in this repository
does exactly that end to end, and `explain analyze verbose` is the form to use,
since it is the only one that says what the statement cost in DPU.

Say them where you mean them. An index is a second structure maintained on every
write to that row, so one over columns nobody filters, sorts or joins on is a
cost paid on every save to answer a question nobody asks — and DSQL takes [**24
indexes to a table** and **8 columns to an
index**](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html),
which is generous until a wide record has been given one per field out of habit.
Four single-column indexes where one over the four would serve is four slots
spent for a worse plan, and the list on the decorator is where that is visible.
The statements are yours to read before they run, and this is what to read them
for.

That ceiling is also a budget, and a smaller one than it reads. The quotas page
gives the number; what a cluster adds, when you fill a table up and count what
it is holding at the refusal, is that the primary key is one of the 24. So
twenty-three are yours to spend, and the index a uniqueness rule brings spends
one of those. Which matters most for a record that keeps gaining fields: a new
thing worth counting arrives as a column and an index, and twenty-three of those
is a number a long-lived table reaches — so a record where the *questions* keep
arriving is one where the answer eventually stops being a field at all. *When the
questions keep changing* below is where that goes instead.

Only a column can be indexed. A key inside a shared jsonb document has
nothing to index, so an index naming a field stored in the blob is refused at
declaration rather than at the database — which is also the sharpest reason to
give a field a column, and what *Promoting a field out of the blob* below is for.

Nor will every column take one. A `timedelta`, `bytes`, `dict` or `list` field
becomes an `interval`, `bytea` or `jsonb` column and DSQL indexes none of the
three, so an index over one of those is refused at declaration too — `unique=True`
included, since a unique index is backed by an index on the same columns. What
can be indexed is a field beside them holding what the reads actually match on: a
digest as text, a duration in seconds, the one key lifted out of the document.

The refusal the cluster gives is about keys rather than about indexes, so it
reaches one more thing: a record declaring its own `id` as one of those four
types. A primary key is a key, and nothing asked for it, so

```python
@record(table="thing", collection="things")
class Thing:
    id: bytes = field()
# ValueError: Thing.id is stored as bytea, and DSQL will not have bytea in a key
```

is refused where the class is written rather than at the `create table` local
PostgreSQL would have taken. An id left undeclared is the `uuid` every record
here gets; `str` and `int` are keys too, and a digest that has to be the
identity goes in as its hex.

A child's table is indexed on `(parent_type, parent_id)` whether it declares
anything or not — by dray, or by the class itself where it declares an index
leading with those two columns, which is further down. Every read through a
parent carries them — `by_id`, `find`, `count`, the ordered read, and the delete
that takes a parent's children with it — so the index serving all of them is
decided once rather than argued about per child table.

What a child declares is added beside that one, and says exactly what it says:

```python
@child(of=(Person, Event), name="notes", table="note", collection="notes",
       indexes=[index("whom", "written_at")])
class Note:
    body: str
    whom: str = "System"
    written_at: datetime | None = field(default=None, on_add=clock)
```

Nothing is put in front of those columns, and that is the whole point of them. A
child with a `collection=` is asked about without a parent — that is what the
collection is for — and an index leading with the parent cannot answer a question
that does not mention it, because only a leading run of an index's columns can be
searched on. *What has this coordinator written this week* names neither a person
nor an event, and it is a full scan until something like the above exists.

The two columns naming the parent are fields like any other, so an index may lead
with them where a read through a parent wants more than the pair —
`index("parent_type", "parent_id", "starts_at")` is a sentence you can say. That
one is built and dray's own is not: it leads with the same two columns, so it
answers every read the implicit index was there for and the pair beside it would
be a second slot spent on a question already answered. What dray will not do is
put those columns in front of a declaration that does not name them.

> **What DSQL is doing.** `create index async` builds in the background rather
> than blocking writes on a table that already has rows, and it is the only form
> DSQL takes — which makes it the one statement dray writes that local
> PostgreSQL refuses. `store.create` asks the connection which database it is before
> choosing, so the same code makes the same schema either side.

A statement you write yourself is also where you would reach for a shape DSQL
does not have, so the refusals are worth knowing by sight. A partial index —
`where status = 'volunteer'`, so that only the rows anybody asks about take an
entry — comes
back with `WHERE not supported for CREATE INDEX`. A sort order on a key
comes back `specifying sort order not supported for index keys`, so
`(starts_on desc)` is not a shape either. And the `async` above is not a
preference: a plain `create index` is answered with `unsupported mode. please use
CREATE INDEX ASYNC.`, a unique one no differently, and `alter table … add
constraint … unique` with `unsupported ALTER TABLE ADD CONSTRAINT statement`.
Uniqueness on a table that already has rows is still yours to add — as an index
rather than as a constraint, which is the whole of the difference.

Two more are worth knowing because of what people reach for them for. An
operator class on a key — `(family_name text_pattern_ops)` — comes back `opclass
not supported for index keys`, and another index method — `using gin` — comes
back `USING not supported for CREATE INDEX`. Between them they are why `index()`
takes column names, a placement and a uniqueness flag and nothing else: there is
no third thing a key can carry here, so there is nothing for a class to say.

Which matters most for a name at a counter. On PostgreSQL `like 'rob%'` will
not use a plain index unless the database collates as `C`, and the usual answer
is the operator class above; on a cluster the question does not arise, because
a cluster *does* collate as `C` and the index a class already declares answers
it. A substring match — `like '%rob%'` — has no index to serve it here at all.
That is worth knowing before a search box is designed around one, and it is the
kind of thing local PostgreSQL will not tell you: the same two reads on a
machine collating as `en_US.UTF-8` are a sequential scan and a sequential scan,
which is the opposite of what the deployment does for the first of them.

None of that is a record layer declining to write a statement, and there is no
page to be sent to for it either. AWS's [supported SQL
features](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-sql-features.html)
enumerates what *is* supported and says outright that the list is not exhaustive,
so a refusal is a thing a cluster tells you rather than a thing you can look up.
Every one above was asked of one.

One shape is less refused than unsayable, and it is the one a scarce resource
reaches for. *Nobody is rostered to two things at once* is an exclusion
constraint over a range in PostgreSQL, and both halves of it come back
empty-handed. `alter table … add constraint … exclude using gist (…)` is the
`unsupported ALTER TABLE ADD CONSTRAINT statement` from above. And the index
underneath it has no grammar to be written in at all: `create index async …
using gist (…)` answers `USING not supported for CREATE INDEX`, which is a
`create index` with no `using` clause in it rather than an opinion about one
access method. Those two were asked of a cluster as well. So there is nothing
to declare, and what gets written instead is a `clashing()` read and then an
`add`, which is two callers reading the same gap and both writing into it.

> **A way to say it here.** Cut the resource into slots and the overlap stops
> being a range. One row per (volunteer, hour), unique over the pair, so two
> rosters that overlap name an hour in common and collide on a constraint the
> database adjudicates — rather than on a read that another caller can walk
> through between.

```python
@record(table="shift", collection="shifts",
        indexes=[index("person_id", "hour", unique=True)])
class Shift:
    person_id: UUID = field()
    event_id: UUID = field()
    hour: datetime = field()


def roster(person, event, starts_at, ends_at):
    store.shifts.add_all([
        Shift(person_id=person.id, event_id=event.id, hour=at)
        for at in hours(starts_at, ends_at)
    ])
```

Two coordinators rostering the same volunteer for 09:00–13:00 and 12:00–16:00:
one returns and the other raises `DuplicateRecord`, with nothing of the losing
rostering behind it, since `add_all` is one transaction while the set fits and
its rows land together or not at all. The slots are half open at the end, so a
shift beginning at 13:00 is taken against one that ended there, and so are the
same hours against somebody else.

**What it costs is the row count.** Four hours is four rows rather than one,
against 3,000 to a transaction, so a season's roster written in one go is
arithmetic somebody has to do. The slot is the grain of the answer as well:
hours cannot refuse an overlap of a minute, and quarter-hours are four times the
rows to be able to.

Dropping a rostering is deleting its rows, which is why `Shift` is a record and
not a child — it is about a volunteer and an event both, and a child hangs off
one parent. `find(equals={"person_id": ..., "event_id": ...})` reads them
through the index that is already there, since `person_id` leads it, and a
`with store.transaction()` around the deletes is what takes them together.

**That was asked of a cluster.** Two coordinators released together on two
connections, neither reading before it wrote: one rostering landed, the other
came back `DuplicateRecord`, and none of the loser's rows were behind it. What
differs from local PostgreSQL is the road rather than the answer. Locally the
second writer blocks on the row and is refused the moment the first commits.
DSQL holds no locks, so nothing waits: a writer that reaches commit is refused
there, dray runs the work again, and whichever attempt finds the row already
committed is the one that raises. Where the rows go inside a block of your own
that second attempt is yours to make — the refusal arrives at the `with` as
`CommitRefused`, and *Running it again* above is the shape.

> **What DSQL is doing.** A unique index built `async` does not enforce while it
> is still building. The statement hands back a job id and the build runs behind
> it, so a duplicate written in that window is taken, and refused only once the
> job has finished — `sys.jobs` holds its state and `call sys.wait_for_job(id)`
> waits for it, both on [the async index
> page](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html).
> That page's line about DML being subject to a unique index's constraint until
> you drop it is about a build that *failed* and left the index `INVALID`, which
> is a different state from one still running. The window is the argument for a
> rule a record cannot be without going in the `create table`, where it holds
> from the moment the table exists.

> **A way to say it here.** Two moves, and which one you get is decided by
> whether the table has rows yet. Before the data there is no window at all:
> `unique=True` on the decorator goes into the `create table`. On a table that
> already has rows, the check is yours to hold in your own code — read for the
> value, refuse the insert yourself — until `sys.jobs` says the build is
> complete, **and then delete the check**. That second half is the one that gets
> skipped, and what it costs is a read before every insert, forever, for a rule
> the database has been enforcing since the job finished.

Your own check narrows the window rather than closing it — two callers can both
read and both insert — which is the argument for the first move wherever the
choice is still open.

> **A way to say it here.** A predicate an index will not take goes in a column
> instead. What the partial index above was for — the people who are volunteers,
> out of everybody who has ever enquired — is an ordinary composite once a field
> carries that state: `(volunteer_suburb, family_name)`, where `volunteer_suburb`
> says where this person volunteers and says nothing while they are not one. What
> that gives up is the storage, since every row takes an entry where a partial
> index would have skipped most of them. What it keeps is the read, which is what
> DSQL charges for: a scan touches the entries that match either way.

Which leaves a column to be kept true, and one computed from other fields is
wrong the first time somebody saves a record without thinking of it. `derived`
is what stops that:

```python
def where_they_volunteer(write: Write) -> str:
    person = write.record
    return person.suburb or "" if person.status == "volunteer" else ""


@record(table="person", collection="people",
        indexes=[index("volunteer_suburb", "family_name")])
class Person:
    family_name: str
    suburb: str | None = field(default=None)
    status: str = field(default="enquiry", choices=STATUSES)

    volunteer_suburb: str = field(default="", derived=where_they_volunteer)
```

The column is worked out from the two fields it is about on every write, and it
is nobody's to set — *A field that is never yours*, above. So there is no writer
left who can forget it and none who can put something else there, which is what
makes it worth holding an index over: what the read matches on is maintained by
the same write that moves what it is derived from.

Note the empty string rather than a `None`. A handler handing back `None` is one
with nothing to say, and the field keeps what it had — which is what `whoever`
wants and the opposite of what this wants, because somebody who lapsed a month
ago and still reads as volunteering in Katoomba is the bug the column was built
to avoid, one layer down.

One shape DSQL does take is worth its own paragraph, because AWS recommend it
and it lands differently on dray's two kinds of read. The [SQL dialect
post](https://aws.amazon.com/blogs/database/dsql-sql-dialect-how-amazon-aurora-dsql-differs-from-single-instance-postgresql/)
advises covering indexes here more than elsewhere:

```sql
create index async orders_customer on orders (customer_id)
    include (order_total, status)
```

On a cluster that is exactly what happens. The plan is an `Index Only Scan` while
the select list names nothing the index is not already holding, and falls back to
an `Index Scan` and a `Storage Lookup` the moment it names one thing more. `id`
is one of those things.

Which splits dray's reads in two. A record read hydrates, so its select list is
`{self.columns}` and always the whole class — `select_many` refuses a statement
that is not — and a covering index serving one has to carry every column the
class has, which is a second copy of the table maintained on every write to buy
back a single lookup. `select_rows` is the other half and inverts the answer: it
hands rows back rather than records, the caller writes the projection, and a
narrow select list is the ordinary case rather than the refused one. A count per
status or a sum by section, asked often enough to matter, is the read that post
has in mind, and `{self.table}` and `{self.columns}` are in reach for writing the
index that serves it.

A read by key needs none of it. DSQL builds a table's own primary key index
covering — the key, with every other column included — so `by_id` is index-only
already.

What earns its keep is the other direction:

```python
schema.drift(store.conn, Person)
# ["person.wwcc_number is declared but not in the table",
#  "person has no index 'person_family_name', which the class asks for"]
```

The blob has no constraints, so a field that quietly stopped being saved looks
exactly like one nobody has filled in. Asking the database what it actually has
is the only place that can be noticed.

Indexes as well as columns, and that is half the reason for declaring them: a
table with every column and none of its indexes reads exactly like a table that
is right, until somebody times a query on it. Only the indexes dray names itself
— anything else on the table is deliberate work and none of its business.

Which is why the name matters. An index dray asks for is called the table and
then its columns, joined with underscores — `event_name_starts_on` — and that
name is what drift goes looking for. A unique index carries it whichever way it
was made, so the constraint in the `create table` is named rather than left to
the database, and the same class reports the same answer against a table built
either way.

Which is also why a long one is cut. An identifier holds 63 bytes on either
database and a longer name is stored at 63 with nothing said about it, so a name
dray generated past that would be a name it then reported missing for ever on a
table that has exactly what the class asked for. dray cuts it the same way
instead — on a character boundary, so a table or column name outside ASCII does
not come out split down the middle of a character. Nothing about that is
refused: an over-long name is a working index and a legal declaration. What is
refused, where the class is written, is two indexes on one table that come out of
that cut as the same name — `create index async if not exists` finds the first
and succeeds having built nothing, so the class asks for two indexes, the table
carries one, and on DSQL there is even a job id for the build of nothing.

One blind spot is left. A column of the wrong *type* is present and correctly
named, so an annotation dray does not know becomes `text` and drift says nothing.

### Promoting a field out of the blob

Deleting `stored_in="blob"` changes what dray does from that moment on, and says
nothing about the rows already written. Those still carry the value under the
blob key, where `find` — now reading the column, correctly — cannot see it, and
where `load` still lets it override the column it is supposed to have moved to.
The record says `'Leura'` and nothing can find `'Leura'`.

So it is four steps, and dray writes three of them:

```python
for statement in schema.promote(Person, "suburb"):
    print(statement)

# alter table person add column if not exists suburb text
# update person set suburb = (data->>'suburb')::text where data ? 'suburb'
# update person set data = data - 'suburb' where data ? 'suburb'
```

Add the column, copy across what the blob is holding, drop the key so it stops
shadowing. The last one is the step nobody thinks of and the one that bites.

Change the class first — `promote` refuses a field still declared
`stored_in="blob"`, because running these before the class changes leaves a
column nothing writes to. Each survives being run twice, and the two updates skip
rows that no longer carry the key, so stopping halfway and resuming is safe.

SQL you wrote has the mirror of the same problem, and it is quieter. A report
reaching in with `data->>'suburb'` sees only the rows written before the change
until it sees none, and then comes back as one group of nulls — nothing raises,
and `drift` compares tables to classes and never looks at a statement. Build the
projection with `sql_for` and the statement answers the same before and after
this runs, unedited: *Where your names live in SQL you wrote*, below.

> **What DSQL is doing.** 3,000 rows to a transaction, so each of those updates
> is one transaction and a table past that has to be walked in batches. These are
> statements to read and put in a migration that knows how big the table is —
> dray is not a migration runner and does not pretend to be one.

## The names dray owns

Every table dray makes carries a key, a stale-write guard and the jsonb column
the blob lives in, and a child's carries two more naming its parent. They are
spelled `id`, `etag`, `data`, `parent_type` and `parent_id` — plain words
rather than `dray_id` and `dray_etag`, because a table is read by people who
never use dray. An analyst at a psql prompt, a reporting job, the service next
door: none of them should have to read your machinery to find your data.

What plain words cost is that they are words your domain might want. So each is
a *role* with an option naming the column that fills it, and the default is the
plain word:

| role | option | default | if your class declares that name |
|---|---|---|---|
| the key | `key=` | `id` | yours — your field becomes the key |
| the guard | `etag=` | `etag` | refused; move dray's with `etag=` |
| the blob | `blob=` | `data` | refused; move dray's with `blob=` |
| a child's parent | `parent_type=`, `parent_id=` | those | refused; move dray's |

The key is the one dray hands over outright, because it only ever needed the
name. A record needs exactly one key and there is no reason it cannot be
yours — declare `id: str` and an employee number is the primary key, with
`check_key` holding you to a type DSQL will take. The other three carry values
dray mints and reads on every write, so it cannot give them up; it can only
stand somewhere else.

`key` rather than `id` because the option names the role and the value names
the column. `key="ref"` reads as *my key column is called ref*; `id="ref"`
would read as *id is called ref*, which is the thing it is trying to say is not
so. `by_id` keeps its spelling regardless — it is dray's own word for asking
by the key, and asking is the same question whatever the column is called.

A key you declared reaches the children of that record, because `parent_id`
holds the key it points at and is typed and converted as that key is. Declare
`id: str` and a note on one of those has a `parent_id text` carrying an
employee number, where a note on a record that left its id alone has a `uuid`.
Which is the one thing `of=` cannot paper over — a child table has one of those
columns, and one column holds one type and runs one converter — so several
records named in one `@child` have to be keyed alike, and naming a date-keyed
record beside a uuid-keyed one is refused where it is written:

```python
@child(of=(Day, Person), name="notes", table="note")
class Note:
    body: str
# TypeError: Note hangs off records whose keys are not all of one type:
# Day (date), Person (UUID)
```

Declare a child per key type instead. That costs less than it reads: the
tables were separate anyway, `name=` gives each parent the same accessor, and
`of=(Person, Event)` — two records that both left their ids to dray — goes on
sharing one table as it always did.

Most records never meet any of this, and say nothing about names because there
is nothing to say:

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    suburb: str = field(default=None)
```

```sql
create table if not exists person (
    id uuid primary key,
    family_name text,
    suburb text,
    etag text,
    data jsonb not null default '{}'::jsonb
)
```

A system being moved onto dray is where they all turn up at once, and usually
the reason to keep the old vocabulary is that other things already read the
table. Say the business has always called an employee number an `id`, mirrors
an upstream API that sends an `etag`, and has a legacy `data` column nobody is
willing to rename:

```python
@record(table="person", collection="people",
        key="ref", etag="dray_etag", blob="payload")
class Person:
    id: str                              # the employee number
    etag: str = field(default=None)      # the upstream API's ETag
    data: dict = field(default=None)     # whatever the old system put there
    family_name: str = field(default=None)
```

```sql
create table if not exists person (
    ref uuid primary key,                      -- dray's key
    id text,                                   -- the employee number
    etag text,                                 -- the upstream API's ETag
    data jsonb,                                -- the old system's column
    family_name text,
    dray_etag text,                            -- dray's guard
    payload jsonb not null default '{}'::jsonb -- dray's blob
)
```

Three columns read the way the business says them and dray's two are the ones
wearing a prefix, which is the right way round — the machinery is what should
look like machinery. `person.id` is the employee number and `person.ref` is
dray's key. `drift` watches all seven, because every one of them is on the
class.

Which leaves one thing to say for code that does not know what it is holding.
An admin screen, a serialiser or an audit log works across record types and
still needs the key, and on this class `person.id` is somebody's employee
number:

```python
dray.key_of(person)      # the key, whatever this class calls it
```

Domain code should keep saying `person.id`, because it knows. `key_of` is for
the code that cannot, and it is a function rather than a member so that it
costs no name on the record.

The same question gets asked of a class rather than of a record, and gets the
same answer. Code working across record types often needs the names dray's own
columns wear rather than a value out of one — a report assembled over fields
named at runtime, a test emptying every table it made — and a collection
publishes all seven of them. The class is the door that always answers:
`@child` takes `collection=` and most children never say it, so for most of the
classes in a `store.create(...)` there is no `store.<something>` to ask.

```python
dray.names_of(Person).table      # 'person'
dray.names_of(Person).id         # 'ref', the column dray's key is in
dray.names_of(Person).etag       # 'dray_etag'
```

`table`, `columns`, `blob`, `id`, `etag`, `parent_type` and `parent_id`, off
the class, and answered by the same code `store.people` answers with — so the
two doors cannot come to differ. It takes a record as happily as its class,
because the answer is a fact about the class either way.

A function rather than a member for the reason `key_of` is one, and here that
is not only tidiness. Two of the seven are words already spent: `person.id` is
the key's *value* on every record ever built, so no binding could make
`Person.id` mean the column's name without contradicting it. The other five are
words a domain may want, and a restaurant that declared a `table` field would
have been putting a domain default into a statement. This way none of the seven
costs a record a name.

### Where your names live in a call

A filter is your field names and an option is dray's, so they sit in different
places and cannot be read for each other:

```python
store.people.find(
    equals={"status": "volunteer", "suburb": "Katoomba"},
    order_by="family_name",
    limit=20,
)
```

Which is why a record may have a field called `parent`, or `order_by`, or
`equals`, and filtering on it reads like filtering on anything else:

```python
@record(table="section", collection="sections")
class Section:
    name: str
    parent: str = field(default=None)     # a courtyard inside a terrace

store.sections.find(equals={"parent": "terrace"})
```

A field named for one of dray's options is a different question from a field
named for one of the database's. `limit` is a word PostgreSQL keeps, so
`create table section (limit bigint)` is refused however dray feels about it,
and a field of that name has to live in the blob. dray does not quote the
identifiers it writes.

Writes divide the same way. What a write hands to `on_add` and `on_save` is
yours and goes in `given`; the guard is dray's and is spelled out beside it:

```python
person.save(given={"whom": "rod"}, etag=posted["etag"])
store.events.save_all(storms, given={"whom": "rod"}, guarded=True)
```

Because the values being assigned live inside `given`, `etag=` can only mean
the guard — even on a record carrying an `etag` of its own. And because every
remaining keyword is dray's, a misspelling is refused rather than quietly
dropping the thing you asked for:

```python
person.save(etaG=posted["etag"])
# TypeError: save() got an unexpected keyword argument 'etaG'
```

`equals` takes a value, `None`, `any_of` and `none_of`, and nothing further. A
range, a pattern or a join is `select_many` with SQL you wrote — the same
boundary as ever, and the reason the argument is called `equals` rather than
`where`.

### Where your names live in SQL you wrote

A collection method builds its statement out of the class rather than out of
memory, so a moved column costs the statement nothing:

```python
@collection(of=Note)
class Notes:
    def for_person(self, person):
        return self.select_many(
            f"select {self.columns} from {self.table}"
            f" where {self.parent_type} = %s and {self.parent_id} = %s",
            ["person", person.id],
        )
```

`table`, `columns`, `blob`, `id`, `etag`, `parent_type` and `parent_id` all
come off the collection and all of them follow a rename. Typing `parent_id`
into the string works today and stops working the day somebody moves it, which
is the whole of why they are there. Code holding the class rather than a
collection asks `dray.names_of(Note)` for the same seven — *The names dray
owns* above, and the door a `@child` declared without `collection=` has instead
of one on the store.

For the ordinary case there is an option and no SQL at all, on every read that
takes a filter:

```python
store.notes.find(parent=person)
store.notes.find(parent_type=Person, equals={"kind": "call"})
store.notes.find_first(parent=person, order_by=desc("written_at"))
store.notes.count(parent_type=Person)

for batch in store.notes.in_batches(of=500, parent_type=Person):
    ...
```

Naming both a parent and the column underneath it in one call is allowed, and
the one you wrote out longhand wins: `parent=person` beside
`equals={"parent_id": …}` reads whoever the filter names. The scope is a default
for those two columns rather than a lock on them, which is the rule a set read
through a parent has always followed.

The write goes the other way, and deliberately: `add(record, parent=person)` on
a record already naming somebody else is refused rather than resolved. A filter
that disagrees with a scope narrows a read and nothing is left behind; a parent
that disagrees with a record stores a child under one of the two, and whichever
was not chosen is where somebody will go looking for it.

A write also asks that the parent be one of the kinds in `of=`, where a read
takes any record and finds nothing. Reading past the declaration costs an empty
list; writing past it leaves a row under a record whose delete does not cascade
to it and whose key was never the type `parent_id` was sized for. Queueing has
never allowed it — `person.notes` exists because `Note` named `Person` — so
this is the two doors agreeing rather than a rule of its own.

Those seven are the names dray owns. Your own fields are the other half of a
statement, and they are the half that will not sit still: a field with a column
of its own is that column's name, and a field in the blob is a key inside a
jsonb document with a cast on it. Which of the two a given name is, is a line
in the class — so a statement naming a field that arrived as data rather than
one somebody typed cannot be written out at all. `sql_for` is that name, as
SQL:

```python
@collection(of=Person)
class People:
    def counts_by(self, field_name: str) -> list[dict]:
        return self.select_rows(
            f"select {self.sql_for(field_name)} as g, count(*) as people"
            f" from {self.table} group by 1 order by 1"
        )
```

```python
store.people.counts_by("status")     # status has a column
# [{"g": "enquiry", "people": 1},
#  {"g": "lapsed", "people": 1},
#  {"g": "volunteer", "people": 2}]

store.people.counts_by("suburb")     # suburb is in the blob
# [{"g": "Katoomba", "people": 2},
#  {"g": "Leura", "people": 1},
#  {"g": "Wentworth Falls", "people": 1}]
```

`sql_for("status")` is `status` — character for character what you would have
typed. `sql_for("suburb")` is `(data->>'suburb')::text`, and the cast is the
part that cannot be guessed from the annotation: a `timedelta` comes back
through `make_interval` and a `bytes` through `decode`, because both are
encoded on the way into the document. Written by hand, the first of those
raises on every row and the second is wrong on every row with nothing said.

It is on a collection and on `dray.names_of(cls)`, like the seven, and it
answers for the key and the guard columns as readily as for anything else —
they are fields on the class like the rest. A name the class never declared is
refused before any SQL is built, which is what keeps the promise in *On the
f-strings*, near the top of the page: nothing that came from a caller reaches
the statement.

What it buys is that the statement is the same statement before and after the
field is promoted out of the blob. The text is not edited — where the
hand-written `data->>'suburb'` sees the rows written before the change until it
sees none, and reports a group of nulls with nothing raised.

What it costs is six decimal places. A `Decimal` read out of the blob comes back
rounded to `numeric(18,6)`, where the document itself holds every digit and a
record hands it back whole. A field in the blob cannot ask for a different size
— `precision=` and `scale=` are refused there, for the reason under *Types*
above — so that is the only size there is to round to, and it is the size the
column would have if nobody said. Casting to a bare `numeric` would keep the
digits, and would make the answer change on the day of an ordinary promotion,
which is the one thing this is here to stop.

**Which leaves one promotion that does move the answer**, and it is worth
knowing before you plan one. Deleting `stored_in="blob"` and saying
`precision=12, scale=8` in the same breath is two changes, and the second is
the one the statement can see: the column is built to the size you named, so the
same unedited statement starts answering with digits it used to round away.
That is not this going wrong — it is you having asked for a different number —
but it is the field most likely to want promoting, since *Types* is where the
page says a rate or a conversion should declare its own size. Promote first and
size afterwards if you want the change to arrive on a day you chose.

### There is no reserved word

dray puts six things on a record you are meant to call — `save`, `delete`,
`parse`, `as_dict`, `children` and `store` — and it holds its own copy of each
under a name no field can take:

```python
person.save()            #  the same method as
person._dray_save()      #  this one
```

So a field may be called any of them, and the one that turns up is `children` —
a household has some, and dray's way of reaching child records is spelled the
same way:

```python
@record(table="household", collection="households")
class Household:
    address: str
    children: int = field(default=0)     # how many live here
```

`household.children` is the number, and everything dray does still works,
because it never went looking there. What you gave up is the reading it would
have offered, and the second spelling still has it:

```python
household.children         # 3 — the number you declared
household._dray_children   # every kind of child declared for a household
```

`store` is the other one a domain is likely to want — a chain has stores — and
it goes the same way: declare the field and `outlet.store` is the shop, with
`outlet._dray_store` still reaching the store the record came from. That is also
the spelling to write in a `@check` or an `@after_commit` shared across record
types, for the reason `_dray_children` is: the class in hand may have spent the
plain word on something of its own.

The reason to know the second spelling is that it lets you put a rule in front
of dray rather than beside it. A class that defines one of the six keeps its
own, and calls dray's when it is ready:

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    status: str = field(default="enquiry")

    def delete(self):
        """A volunteer is lapsed, never removed."""
        if self.status == "volunteer":
            raise ValueError("lapse a volunteer before deleting them")
        self._dray_delete()

    def save(self, **kw):
        self.family_name = self.family_name.strip()
        return self._dray_save(**kw)
```

`person.delete()` now refuses the ones your domain says are never deleted, at
the record rather than at each of the eleven places that call it. This is the
whole of what "dray leaves your method alone" buys, and without the second
spelling it was a way to *lose* dray's behaviour rather than to sit in front of
it.

**Both of them stand in front of the *call* and not in front of the write**, and
that is the thing to know before a rule is put in one.
`store.people.delete(person)` does not go through the `delete` above — see
*Before a record goes* for the version of that rule which every door reaches.
The `save` is the same and has more doors to be missed at:
`store.people.save(person)`, `save_all`, `add`,
`add_all` and anything written through `store.conn` all go straight past it, so
a rule kept there holds for `person.save()` and for nothing else. A rule that
has to hold on every write is a `@check` if it is about the values and a
`@before_save` if it has to write or to read — *A rule about the whole record*
and *Before a record is written*.

The method may sit on a base class rather than in the body, which is where it
goes once several record types keep the same rule — the same place a shared
`@check` goes, and for the same reason. dray leaves the inherited one alone and
lends `_dray_delete` underneath it, so the base class calls dray's without
having to know what the record in hand declared:

```python
class LapsesRatherThanDeletes:
    def delete(self):
        if self.status == "volunteer":
            raise ValueError("lapse a volunteer before deleting them")
        self._dray_delete()


@record(table="person", collection="people")
class Person(LapsesRatherThanDeletes):
    family_name: str
    status: str = field(default="enquiry")
```

Where two classes a record is built on both spell one of the six, the one
Python's method order reaches is the one that runs, and dray has nothing to add
to that: using the word is the whole of what says the class meant it, so there
is no second answer to learn beside the language's.

Three more sit behind `=`, `==` and `hash()` rather than behind a word, and
they have the second spelling for the same reason. `__setattr__` is where every
converter, validator and `on_change` on the class runs, and `__eq__` and
`__hash__` are the identity described under *Which record this is* — the thing
a set and a dict key ask. Python looks each of them up by its own name, so a
class defining one keeps it, and calls dray's when its rule has passed:

```python
@record(table="person", collection="people")
class Person:
    family_name: str
    email: str = field(default=None)

    def __setattr__(self, name, value):
        if name == "email" and value:
            value = value.strip()
        self._dray_setattr(name, value)
```

Defining `__eq__` leaves a class unhashable, which is Python's rule rather than
dray's, so a class comparing its own way and still wanting a set of records
defines both, in the same body:

```python
    def __eq__(self, other):
        return self.family_name == other.family_name

    def __hash__(self):
        return self._dray_hash()
```

Everything else dray needs from a record wears that same prefix —
`_dray_validate`, `_dray_load`, `_dray_blob` — because those are how a record
is built and stored rather than things you call. Which makes `_dray_` the whole
of what a field may not be called: a field declared under it is refused, and
every plain word stays yours. They are named here so that finding one in a
traceback is not a mystery, and for no other reason.

It holds in the other direction too, for the methods dray calls rather than the
ones it lends. The `@check` under *A rule about the whole record*, the
`@after_commit` under *After a record lands*, the `@before_save` under *Before a
record is written* and the `@before_delete` under *Before a record goes* are all
found by the marker the decorator leaves and never by what the method is
spelled, so dray reserves no word there either — and a method dray was not shown
is a method dray does not call, however it is named.
Where two classes a record is built on define the same method and only one of
them marked it, the class is refused rather than the marker being resolved away
by Python's method order — see *A rule about the whole record*.

One underscore in, the same idea buys you a transient. A plain name the class
never declared is refused on assignment, because a value nobody declared is a
value the next save would drop without a word. A name starting with an
underscore is not:

```python
booking._just_seated = True      # fine

booking.just_seated = True
# AttributeError: Booking has no field 'just_seated'. Declare it on the class;
# nothing is stored that was not declared.
```

Which is the ordinary Python reading of a leading underscore — mine, not
yours — and it is what a transient is. Hang something on the record for the
rest of this request, hand it to the template rendering the row, and know that
no write looks at it and no read brings it back: the same booking fetched again
has never heard of it. Something you want to survive the request is a field,
and this is the other thing.

## What your editor can see

`@record` is applied rather than inherited, and the class it hands back is the
one you wrote — the same object, with fields worked out and dray's own members
bound onto it. A type checker cannot watch that happen. It reads class bodies,
and everything above is settled by a decorator at import.

Most of it it can be told, and dray says as much as the language allows. The
declarations are read, so the constructor has parameters and the fields have
their types:

```python
@record(table="person", collection="people")
class Person:
    family_name: str = field()
    suburb: str | None = field(default=None, stored_in="blob")


Person(family_nmae="Hemingway")     # caught, rather than found by a test
person.suburb                       # str | None, on hover
```

**Which is why `default=None` is spelled out above.** A checker reads a bare
`field()` as a field with no default and therefore required, and it is right
to: dray hands back `None` for it, which is a `None` where the line says
`str`.
Saying `default=None` is the honest version of what was always happening, and
the fields where you meant *required* now say so to the checker as well.

### The members dray attaches

`save`, `delete`, `parse`, `as_dict`, `children`, `store`, `id` and `etag` are
bound when the decorator runs, so there is nothing in the class body for an
editor to find. Inherit `Record` and they are declared, along with the `_dray_`
spellings a rule standing in front of one of them has to call — `_dray_save`,
`_dray_delete`, `_dray_parse`, `_dray_as_dict`, and `_dray_validate`,
`_dray_blob` and `_dray_hash` besides:

```python
from dray import Record

@record(table="person", collection="people")
class Person(Record):
    family_name: str = field()

person.save()               # a call, rather than an unknown attribute
```

`Record` carries no behaviour and is not there when the program runs — it is
declared for the checker and empty otherwise, which is the only shape that
works. dray asks the whole hierarchy whether a word has been spoken for, so a
base holding real methods would read as your domain having claimed all six, and
`person.as_dict()` would quietly hand back nothing.

Leaving it off changes nothing about the record. It is worth adding on the
classes you extend, because a rule standing in front of a write is the one
place a checker was always able to see `self`:

```python
@record(table="person", collection="people")
class Person(Record):
    family_name: str = field()

    def save(self, **kw):
        self.family_name = self.family_name.strip()
        return self._dray_save(**kw)      # declared, so it resolves
```

A collection is the same idea with a real base rather than a declared one:
`@collection` already rebuilds your class on `Collection`, so writing it down
tells the editor what was true anyway.

```python
from dray import Collection

@collection(of=Event)
class Events(Collection):
    def upcoming(self) -> list[Event]:
        return self.select_many(f"select {self.columns} from {self.table} ...")
```

### The one thing that has to be said twice

`person.notes` is bound from the `name=` on the child, and a name inside a
decorator argument is not something a checker can turn into an attribute. So
the parent says it too, if you want it seen:

```python
from typing import ClassVar
from dray.child import ChildSet

@record(table="person", collection="people")
class Person(Record):
    family_name: str = field()
    notes: ClassVar[ChildSet["Note"]]


@child(of=Person, name="notes", table="note")
class Note(Record):
    body: str = field(default="")
```

`ClassVar` is what keeps it out of the fields — an ordinary annotation of that
name would be a column — and it takes no value, because dray fills it in.
`person.notes.find()` hands back `list[Note]` once it is there.

It tells dray nothing dray did not already know, and that is the honest
description of it: rent paid to an editor. It is optional for that reason, and
a child works identically without it. The quotes are because the child does not
exist yet where the parent is written, which is also why `of=` is on the child
rather than the parent — a record is declared before the things that hang off
it, so only the child is in a position to name both ends.

### What is still unknown, and will be

`store.people` is `Any`, and a collection method reached through it is too.
Collections are attached to the store by name when it connects, so there is
nothing on `Store` for a checker to read and no declaration that would help.
The same goes for what `parse`, `by_id` and `find` hand back.

There is no plugin coming to fix that. Pyright, which is what VS Code runs, has
no plugin system and its maintainers have refused one deliberately — so the
answer other libraries reached for is not available, and the answer they
reached *instead* is the one above: say it in the class body.

## When the questions keep changing

A field is the right home for something you will record from now on: it gets a
type, a default, a validator, a converter, a column or a place in the blob, a
place in an index if it earns one, and `drift` watching that the table still
matches. That is a great deal for one line, and it is paid for at declaration —
which means it suits a fact that changes about as often as your code does.

Some things change faster than that. A domain where the questions themselves are
edited — which of them are being asked this month, what answers each will take,
when each started — is one where a new question is *data* rather than a release,
and the shape that fits is a row rather than a field:

```python
@record(table="question", collection="questions",
        indexes=[index("key", unique=True), index("asking_since")])
class Question:
    key: str = field(default="")
    prompt: str = field(default="")
    asks_for: str = field(default="yes_no", choices=("yes_no", "choice", "text"))
    asking_since: date | None = field(default=None)


@child(of=(Person, Event), name="answers", table="answer", collection="answers")
class Answer:
    question_key: str = field(default="")
    said: str = field(default="")
```

Two declarations, and the child rides its parent's transaction like any other,
so an answer lands with the record it is about. The tenth question after that
costs a row.

**Know what it costs, because it is not free.** Everything the first list
promised is now yours to write: the vocabulary a question accepts is a check you
run rather than `choices=`, the type is text and a conversion you wrote rather
than an annotation, and `drift` has nothing to say because a question is data
and not schema. That is the trade — you are choosing it deliberately, and above
some rate of change it is plainly the right way round.

Where that line falls is a judgement about your domain and nobody else can make
it. What is worth knowing is that the line exists, that both sides of it are
ordinary, and that dray is the same size on either side.

**The database and dray are a partnership rather than an ownership**, and that
is the shape of the whole library. dray declares records over tables and reads
them back. The schema is yours: the index you add for a read only you can see
coming, the constraint your domain needs, the column somebody tunes next year,
the table dray has never heard of. None of that is dray working around you or
you working around dray — it is the half of the job that was always going to be
yours. Which is why the DDL comes to you as statements to read rather than
something run behind you, why `drift` reports only what dray itself named, and
why an application that needs to understand its own changes over time builds
that out of records like the two above, exactly as it builds everything else its
domain asks for.

## Tests and local PostgreSQL

dray needs DSQL for DSQL's behaviour — the row ceiling, the refused commits, the
transaction that ages out. Everything it writes is ordinary PostgreSQL, so a test
suite or a local database hands a connection over instead:

```python
store = Store(psycopg.connect("dbname=roster"))
```

and makes the tables from the classes rather than from a migration:

```python
store.create(Person, Event, Note, Attachment)
```

**Every class with a table of its own, and a child is one of those.** A
`@child` says which records it hangs off and nothing about who makes its table,
so a `create` naming only the records leaves a schema that looks complete: the
people go in, the events go in, and the first save carrying a queued note comes
back `relation "note" does not exist` from the database. Children of children
are classes too, and `schema.statements` above takes one class and writes one
table for the same reason.

That is for an empty database and nothing else. A schema you intend to keep is
statements you have read, taken from *Tables* above and put in a migration —
`create` is not a migration runner and does not pretend to be one.

**And it is one process's job, not every process's.** `create table if not
exists` looks like it settles a race and does not: PostgreSQL asks whether the
table is there and then writes the catalogue, so two connections doing it
together collide on `pg_type_typname_nsp_index` — measured, four starting at
once against an empty database, three refused. Which is the shape a deployment
has, where several containers come up cold on the same schema at the same
moment. Swallowing the clash is not the fix either: the one that won is partway
through its own set of tables, so the ones that lost carry on into a schema
that is half there. The tables belong to whatever deploys the code, made once
before anything serves a request.

That same tuple is usually what a suite empties between runs, and every write
dray offers takes records — so emptying a set is a statement of your own down
`store.conn`, and `names_of` is where its table names come from:

```python
EVERYTHING = (Person, Event, Note, Attachment)

def empty_everything(store):
    with store.conn.cursor() as cur:
        for cls in EVERYTHING:
            cur.execute(f"delete from {dray.names_of(cls).table}")
```

`store.people.table` would name the records and leave the children out, most of
which have no collection to ask — which is why this reads the classes. `delete
from` rather than `truncate` because DSQL takes the one and not the other, and
each of those is a transaction of its own, so the 3,000-row ceiling is per
table rather than over the lot and a suite is nowhere near either. On a table
somebody is using, a set-based write costs more than it looks like: *One
statement instead, and what it stops running* is the bill.

## Connections

A store is one connection, used by one thread, and short-lived by design. Build
one per request or per job and let it go. *Pools and threads* below is how that
stops being expensive advice.

There is no password: DSQL authenticates with an IAM token, minted by AWS's own
connector, and the region is read out of the hostname, since it is already in
there and a host and a region that disagree is a confusing way to fail.

```python
store = Store.connect(host="ab12cd.dsql.ap-southeast-2.on.aws")
store = Store.connect(host="...", user="orders_app")
```

The token is picked to match the user — `admin` gets an admin token, every other
name gets the one a scoped role needs. Which is worth knowing because AWS's
advice is to use a scoped role for anything an application does and keep `admin`
for setting the cluster up.

> **What DSQL is doing.** Two clocks, and only one of them matters. A token
> expires — fifteen minutes by default — but only for *opening* a connection: an
> established one carries on working long after its token has gone. The
> connection is the real limit, and DSQL closes every one after **an hour**
> whether it is busy or idle, so that applications keep landing on healthy
> infrastructure. It jitters the closures, and it will not close one mid
> transaction. A store that lives minutes never meets any of this; a pool
> retires its connections before the hour so nothing else does either.

A store that does meet it hears about it in those terms rather than the driver's.
dray does not reconnect — a store holds the one connection it was handed — so
what it can do is name what happened, on a read and on a write alike:

```
dray.store.ConnectionLost: this store's connection was closed underneath it,
and dray does not reconnect. DSQL closes every connection after about an hour,
busy or idle, which is usually what this is: a store built by hand and kept — a
warm container, a long-running job — holding a connection that was closed while
nothing was happening. A store is short-lived by design, so build one per
request or per job; anything longer-lived wants a `dray.Pool`, which retires its
connections before DSQL closes them.
```

Only for a connection that was closed by somebody other than you. Calling
`store.close()` and then using the store is a different mistake — a store used
after it was finished with — and psycopg's own `the connection is closed` is
already the truth about that one, so dray leaves it alone rather than sending you
looking for an hour that never passed.

The connection is verified rather than merely encrypted — `sslmode=verify-full`,
against the CA bundle that comes with the connector. Neither libpq nor the
connector asks for that much on its own, so dray says it. Anything else you pass
goes to the driver, and saying anything `ssl`-shaped yourself replaces both.

A store can also be told which records it serves:

```python
store = Store(psycopg.connect("dbname=roster"), records=[Person, Event])
```

It is optional — a record is reachable from the moment its module is imported —
but saying it means two records sharing a collection name cannot be resolved by
import order behind your back. Say it later with `store.serves(Person, Event)`
when the store is built somewhere the records are not.

And the connection is dray's once handed over: it is switched to autocommit. dray
opens its own transactions, and a connection sitting inside one between requests
is a transaction ageing against DSQL's five-minute ceiling.

### Pools and threads

A store belongs to one thread at a time. Everything it does goes down one
connection, and a connection has one session and one transaction, so two threads
in one store are two threads in one transaction whether they meant to be or not.
dray refuses that rather than allowing it:

```
# thread A is inside a save; thread B reaches for the same store
RuntimeError: this Store is in use by thread 6104618496 and was reached from
thread 6121444864. A store is one connection, so it belongs to one thread at a
time ... Give each thread its own: `with pool.store() as store:`
```

Which is what a pool is for. It is not a change to the shape — it is the answer
to what the shape costs, since a store per request is a handshake per request
without one:

```python
from dray import Pool

pool = Pool(host="ab12cd.dsql.ap-southeast-2.on.aws", max_size=8)

with pool.store() as store:
    store.people.add(Person(family_name="Hemingway"))
```

The store is ordinary in every way — the same collections, the same records,
the same everything below this line. What changed is where its connection came
from and that it goes back at the end. `Pool` also takes one you already have,
the same way `Store` takes a connection:

```python
pool = Pool(psycopg_pool.ConnectionPool("dbname=roster", configure=dray.store.ready))
```

Anything the pool carries, every store it makes carries — `defaults`, `records`,
`namespace`, the `observer` of *Watching what dray does* below — and a store may
add to the defaults without disturbing the pool's:

```python
pool = Pool(host="...", defaults={"whom": "System"})

with pool.store(defaults={"whom": current_user}) as store:
    ...
```

Threads are where this pays, and the reason is worth saying plainly: a driver
holds a lock per connection, so several threads sharing one store would queue on
one socket and take as long as doing it in order. A store each is what makes
concurrent questions concurrent.

A page usually needs several unrelated answers, and nothing about one of them
waits on another. Ask them at once and the page costs the slowest question
rather than the sum of all of them:

```python
from concurrent.futures import ThreadPoolExecutor


def ask(question):
    """A store each, so a question is a connection rather than a place in a
    queue. It goes back the moment the answer is in."""
    with pool.store() as store:
        return question(store)


with ThreadPoolExecutor() as threads:
    upcoming = threads.submit(ask, lambda store: store.events.upcoming())
    to_review = threads.submit(
        ask, lambda store: store.people.find(equals={"needs_review": True})
    )
    lately = threads.submit(ask, lambda store: store.notes.since(a_week_ago))
    strength = threads.submit(
        ask, lambda store: store.people.count(equals={"status": "volunteer"})
    )

page = Coordinators(
    upcoming=upcoming.result(),
    to_review=to_review.result(),
    lately=lately.result(),
    strength=strength.result(),
)
```

Four questions of three different collections, none of which knows the others
are being asked. `submit` starts each one; `result` waits for the one it is
called on and no longer, so by the time the last of them lands they have all
landed. Four round trips, one round trip's wait.

Each `ask` opens its own store and gives it back at the end, so the pool needs
to be as large as the widest fan-out on the page — four here — and never larger
than the cluster will thank you for.

> **What DSQL is doing.** This is the shape it is built for. A cluster takes ten
> thousand connections and there are no shared buffers for them to contend on,
> so the way to go faster is more connections rather than more work through one.
> The hour-long connection lifetime says the same thing from the other side:
> connections are meant to be made, used and let go. `max_lifetime` defaults to
> fifty-five minutes so the pool retires them first, and nothing above ever
> meets a connection DSQL closed **on the hour**.
>
> Which is the only closing a lifetime sees coming. A connection dropped by
> anything else — a blip, a failover, an idle one cut in between — is handed
> out and fails on the statement after it, and `max_lifetime` has no opinion
> about that. `psycopg_pool` will test one on the way out if you ask it
> to, and dray does not ask on your behalf because the test is a round trip and
> a checkout is a thing you do constantly:
>
> ```python
> from psycopg_pool import ConnectionPool
>
> pool = Pool(ConnectionPool(dsn, configure=dray.store.ready,
>                            check=ConnectionPool.check_connection))
> ```
>
> A short-lived process never notices either way. A container that stays warm
> for hours and serves a request every few minutes is where an unscheduled
> closure lands, and where paying the round trip is the cheaper of the two.

### Where each of them lives

Everything above is one decision said twice: **a store lives for one piece of
work and a pool lives for the process.** Getting that round the wrong way is the
commonest way to be slow or to be broken, and the two mistakes look nothing
alike.

Both of the programs below import their records from a module of their own,
which is all that module has to be:

```python
# myapp/records.py
from datetime import date
from uuid import UUID

from dray import as_uuid, child, field, index, record


@record(table="person", collection="people")
class Person:
    family_name: str
    status: str = "enquiry"


@child(of=Person, name="notes", table="note", collection="notes")
class Note:
    body: str = ""


@record(table="signup", collection="signups",
        indexes=[index("person_id", unique=True)])
class Signup:
    person_id: UUID | None = field(default=None, converter=as_uuid)
    on_date: date | None = field(default=None)
```

`records=` below is how a store is told which of them it serves. It is
optional — a record is reachable the moment its module is imported — but saying
it means two sharing a collection name cannot be resolved by import order behind
your back, which in a Lambda is your packaging tool's business rather than
yours.

Declarations and nothing else — no connection, no store, nothing that runs.
They live in a file of their own because a script, a handler, a test and a
migration all need the same ones. `store.people` exists because `Person` said
`collection`, and so do `store.notes` and `store.signups`.

A script is the easy case, because it is over before any of this matters:

```python
import os

from dray import Store

from myapp.records import Person


def main() -> None:
    store = Store.connect(host=os.environ["DSQL_HOST"], records=[Person])
    for person in store.people.find(equals={"status": "volunteer"}):
        print(person.family_name)


if __name__ == "__main__":
    main()
```

One connection, one thread, seconds of life. No pool, because a handshake you
pay once is not worth pooling, and nothing to close, because the process ending
is what lets it go. This shape is exactly the one not to copy into anything that
stays up.

A Lambda is where the two lifetimes come apart, because the container outlives
the invocation:

```python
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from dray import AfterCommitFailed, CommitRefused, DrayError, Pool

from myapp.records import Note, Person, Signup

log = logging.getLogger(__name__)

# Module scope. A warm container reuses this, so the handshake is paid on the
# cold start and not on every invocation.
pool = Pool(
    host=os.environ["DSQL_HOST"], max_size=2,
    records=[Person, Note, Signup],
)


def summary(pool) -> dict:
    """Its own unit of work, so it takes the pool and not a store.

    Two questions at once is two stores, which is two connections and two
    transactions. Nothing in here can be atomic with anything outside it, and
    the signature is what says so.
    """
    def ask(question):
        with pool.store() as store:
            return question(store)

    with ThreadPoolExecutor(max_workers=2) as threads:
        volunteers = threads.submit(
            ask, lambda s: s.people.count(equals={"status": "volunteer"})
        )
        enquiries = threads.submit(
            ask, lambda s: s.people.count(equals={"status": "enquiry"})
        )
    return {"volunteers": volunteers.result(), "enquiries": enquiries.result()}


def make_them_a_volunteer(store, person_id) -> None:
    """Takes a store, so it can be part of something atomic. It opens no
    transaction of its own and closes nothing: both belong to whoever called
    it."""
    person = store.people.by_id(person_id)
    person.status = "volunteer"
    person.notes.add(body="Signed up.")
    person.save()


def lambda_handler(event, context):
    try:
        with pool.store() as store:
            with store.transaction():
                make_them_a_volunteer(store, event["id"])
                store.signups.add(
                    Signup(person_id=event["id"], on_date=date.today())
                )
        outcome = {"ok": True}
    except CommitRefused:
        # Transient: dray does not replay a block you opened, and this one
        # would very likely land second time. Raising hands it to whoever
        # invoked the function — see below, because that is not always
        # somebody who will try again.
        raise
    except AfterCommitFailed as failed:
        # The rows are committed. Raising would ask for a replay, and the
        # replay would be refused by the unique index and reported as a
        # duplicate — a lie about a write that worked. So it is an answer, and
        # the part that did not happen is named rather than hidden.
        log.exception("after the commit landed: %s", failed)
        outcome = {"ok": True, "after": str(failed)}
    except DrayError as refused:
        # The block rolled back, and running it again would be refused the same
        # way.
        outcome = {"ok": False, "why": str(refused)}

    # Outside the block on purpose: inside, these reads are on other
    # connections and cannot see what the block has not committed yet.
    return {**outcome, **summary(pool)}
```

**Two functions, two signatures, and the signature is the promise.** A function
that takes a `store` can be part of something atomic — `make_them_a_volunteer`
opens no transaction and closes nothing, because both belong to whoever called
it, and the handler puts it in a block with a second write so the two land
together or neither does. A function that takes the *pool* is a unit of work on
its own, and is saying it cannot be part of anything: `summary` takes a store
per question, and nothing it does can be atomic with anything outside it.

Taking a connection per question is the DSQL way round rather than an
extravagance. A cluster takes ten thousand of them and there are no shared
buffers for them to contend on, so more connections is how you go faster —
where on a single-writer PostgreSQL they are the scarce thing and an
application hoards them. *Pools and threads* above is that argument in full.

**Which is the trap, and it is not obvious.** A transaction lives in a store and
a store lives on one thread, so fanned-out work can never be *inside* your
transaction — there is no transaction across two connections and DSQL offers
none. Wanting a page to be both fast and all-or-nothing is wanting two shapes at
once.

What makes it a trap rather than a refusal is that nothing stops you. Move
`summary(pool)` inside the block and it runs, and it stays parallel — its stores
come from the pool on connections of their own, which the one-thread guard never
sees. What you get is two quiet costs instead of an error: the fan-out reads
committed rows, so it cannot see the work in hand and reports numbers stale by
exactly the thing you just did; and the block stays open while it happens,
ageing against DSQL's five minutes and widening the window for the conflict that
makes a commit refused.

**And two different mechanisms are keeping that handler honest**, which is
worth separating because neither does the other's job. The **transaction** makes
it all-or-nothing: if the signup is refused, the status and the note go with it,
rather than leaving somebody marked a volunteer with a note explaining it and no
signup. The **unique index** on `person_id` makes it at-most-once: Lambda
delivers at least once, so the same event arriving twice would otherwise write a
second note and a second signup, and instead the second `add` raises
`DuplicateRecord` and the whole block rolls back. Nobody checks first — the
database adjudicates, which is *A way to say it here* further up.

**Which is why there are two `except` clauses and why their order matters.**
`DrayError` is the one name a handler needs — it catches every refusal dray
raises, including whatever a later version adds, which is *When it goes wrong*
above. But `CommitRefused` is one of those, and it is the one that must not be
answered. dray replays a write DSQL refuses, so an ordinary save never shows you
one; that replay belongs to the transaction dray opened, and inside a block you
opened it does not happen. The refusal arrives at the `with` on the first
attempt rather than the fifth, and running the work again is yours.

**In a handler, `raise` means *run this again*.** That is the whole of what the
three clauses are choosing between, and the question each answers is *what
happened to the rows, and would running it again help?*

A refused commit wrote nothing and would very likely land second time, so it is
raised. A duplicate signup wrote nothing and would be refused identically for
ever, so it is answered rather than raised — a replay would only be refused
again. And `AfterCommitFailed` means the rows **did** commit and something after
them did not, so raising it is the worst of the three: the replay reaches the
unique index, comes back `DuplicateRecord`, falls into the last clause, and the
caller is finally told the write failed. Two invocations to arrive at a lie
about a write that worked. It is answered instead, `ok` stays true because `ok`
is about the rows, and the part that did not happen is named and logged.

This handler has no `after_commit` on any of its records, so that clause is
unreachable today. It is there because the day somebody adds one is not the day
anybody re-reads this, and without it the same replay-to-a-duplicate happens
with nothing on the page having warned them.

Collapse the three into one `except DrayError` and each goes wrong differently
and quietly. Running the work again is safe here at all because of the unique
index rather than because of anything the handler does.

So pass the store rather than reaching for one — including in a web framework,
where `flask.g` or a context variable is the tempting place to put it. Hidden,
you can no longer tell from a function's signature whether it is inside a
caller's transaction, and both ways of being wrong are quiet.


**Both halves of that are load-bearing.** A `Pool` built inside the handler is a
fresh pool per invocation, so every call pays the connection — which is the
largest single number in `scripts/flow.py`'s output and dwarfs the work. And a
`Store.connect` at module scope is the opposite mistake: it survives the warm
start, and an hour later it is holding a connection DSQL closed while nothing was
happening, which arrives as `ConnectionLost` on whatever unlucky invocation is
next. The pool retires its own before that hour, which is the whole reason it is
the thing that gets to live long.

**Raising is only half an answer, and which half depends on how the function is
invoked.** Asynchronous invocations are retried twice by Lambda before going to
a dead-letter queue, and a queue or stream source retries by its own redrive
policy — so for those, raising is the retry. A **synchronous** invocation is
not retried at all: Lambda hands the error straight back, and API Gateway
relays it to whoever made the request. Behind an API, then, raising a
`CommitRefused` turns a conflict that would have landed on a second attempt
into a 502 and a lost write, and the retry has to be yours — read the work
again and run it from the top, which is `@replaying` and *Running it again*
above.

`max_size` is the widest fan-out one invocation does — two here, because
`summary` asks two questions at once. A Lambda container serves one invocation
at a time, so a pool there is for reuse across invocations rather than for
concurrency between them; an invocation that fans out inside itself still needs
room for it. A web server is the other way about, and *Pools and threads* above
is that case.

### Where the seam goes, if a layer of yours is in the way

Most applications do not call dray from the handler. There is something of
theirs in between — a service, a domain package, whatever the house calls it —
and the question is which side of it dray lives on. Both programs above answer
it in passing and it is worth saying outright, because it is the one decision
here that is hard to walk back.

**Your layer owns the records, the rules and the errors. The store and the
transaction belong to whoever called it.** `make_them_a_volunteer(store, …)`
above is the whole pattern: a function of yours that takes a store, opens no
transaction of its own, and closes nothing.

The reason is what the caller loses otherwise. A caller who cannot say where a
unit of work begins and ends cannot make two of your operations land together,
and cannot put one of yours and one of somebody else's in the same transaction
— and both of those are the ordinary reason a request has a boundary at all. A
layer that opens its own transaction per call has decided, for everybody above
it and for good, that no two things it does can ever be atomic.

**What you may hide is dray itself.** A caller that never receives a record and
never catches a `CommitRefused` does not have to know what is underneath — hand
back your own types and raise your own exceptions, and the seam is real. That
costs nothing, because none of it is what the caller needed control of.

**What does not cross the seam is a decorator.** `store.after_commit` is a
method on an object you were handed, so a wrapper of yours forwards it without
ceremony. `@replaying` cannot be forwarded, because it works by running a
function again and the function that would have to run again is the caller's —
which your layer has never seen. So a refused commit inside a block your layer
opened is a refused commit your layer cannot replay. It can only hand it back
and say whether another attempt is worth making. A caller that owns the
transaction has `@replaying` and a caller that gave it away does not, which is
the sharpest reason the boundary sits where it does.

### Two services, one cluster

DSQL gives a cluster exactly one database, called `postgres`, and there is no
`create database`. So two services that both have a `person` share a cluster or
argue about the name, and the way out is a schema:

```python
store = Store.connect(host="...", namespace="orders")
```

`None` by default, meaning touch nothing. Given one, it is a `search_path` and
nothing else — every statement dray writes names its table bare, so the SQL you
write in a collection lands in the same schema as the SQL dray generates.
Qualifying names internally would have split those two apart.

The schema has to exist first. Making one is an admin operation, so it is a
statement for a migration rather than something a store does behind you:

```python
schema.create_namespace("orders")
# create schema if not exists orders
```

Two stores on two connections can serve the same records into different
namespaces at once, which is why this is on the store rather than on the record —
the same `Person` deploys into staging and production without editing the class.

> **What DSQL is doing.** This is more than tidiness there, because DSQL manages
> permissions with schema-level grants: the admin role owns `public`, and a
> non-admin role creates its objects in a schema made for it. `Store.connect`
> connects as `admin`, so working inside a named schema is what any deployment
> that would rather not run as cluster admin has to do. The grants themselves are
> yours — without them a namespace is a naming convention with no teeth. There is
> a cap of ten schemas, which is why this is for a handful of services and not
> for a schema per customer.

## Not asking twice

Every read is a round trip, and a page asks the same small questions over and
over. The regions a form offers are read on every request and written twice a
year; a person looked up at the top of a request is looked up again by two
things further down it; a list page pays for its `find` whether or not anybody
has written since it was last drawn.

Two things can be given a lifetime, and what separates them is which of them
dray can take back. A record says how long one of its **rows** may be answered
out of memory, and dray drops the ones its own writes touched. A collection
method says how long its **answer** may be kept, and nothing drops that at all
— that is *A question of your own*, further down.

The first:

```python
@record(table="region", collection="regions", cached_for=1800,
        indexes=[index("code", unique=True)])
class Region:
    name: str
    code: str


@record(table="person", collection="people", cached_for=10)
class Person:
    family_name: str
```

Off unless it is said. The number is seconds, and it is on the record rather
than on a collection class because how often a row is re-read is a storage fact
like the table and the key beside it — and because a record with no
`@collection` of its own still has `by_id` to serve. Two records want two
numbers: a lookup table read constantly and written twice a year is happy with
half an hour where a person is not happy with ten seconds.

**The shape it is for is fan out, then read straight.** A page needs half a
dozen answers and nothing about one of them waits on another, so they are asked
at once — a store each, on a thread each, as *Pools and threads* above — and the
page costs the slowest question rather than the sum. Then ordinary sequential
code runs, with no threads and no futures in it, and every record it asks for
has already been read. The cache is what joins those two halves: it is not
there to save a repeated `by_id` inside one function, it is there to let the
warming and the reading be written as though they were unrelated.

Which is why it lives on the pool. A store is built per checkout and let go at
the end of the request, so a cache belonging to one would be filled by the
threads that warmed it and empty for the thread that read. A store built by
hand — `Store.connect`, or a connection you handed over — has no pool to keep
one in, so it keeps its own and they go when it does. That is the right answer
for a script or a job and no answer at all for a fan-out, but it means
`cached_for=` says the same thing wherever the store came from rather than
depending on how the store was built.

### Only by id, and everything else fills it

`by_id` is the one read dray serves from memory on its own account, because it
is the only one dray can invalidate exactly. A write to any record can change
what a `find` matches and dray has no way to know which sets it touched, so a
set is read every time. Keeping one anyway is a thing you can ask for, on a
method of your own where you are the one saying how stale it may be — *A
question of your own*, below — and it is not something dray does behind you.

But every read of whole records **fills** it:

```python
people = store.people.find(equals={"status": "volunteer"})  # fetched
first = store.people.find_first(order_by="family_name")     # fetched
leura = store.people.in_suburb("Leura")                     # fetched, your SQL

store.people.by_id(people[0].id)                            # from the cache
store.people.by_id(first.id)                                # from the cache
store.people.by_id(leura[0].id)                             # from the cache
```

Which is most of the win and costs no invalidation that a write does not
already do. `find`, `find_first`, `in_batches` and the SQL you wrote through
`select_many` all leave their rows where `by_id` will find them.

A read that comes back too large fills nothing rather than a prefix of itself.
Three thousand rows would evict everything a fan-out had just warmed, and the
person who wrote that `find` is not the person who wrote the warming — a
partly-filled cache would make which records were free unpredictable, which is
worse than none of them being. The ceiling is a quarter of `cache_most`, so
250 rows unless the record moves `cache_most` itself. A walk with `in_batches`
is over it by default and leaves nothing behind, which is what you want from a
walk over a table.

A read that had a write land under it fills nothing either. What comes back is
what the table said when the statement started, so a save that commits while
the rows are in flight has already dropped the keys it wrote and would find
them put back as they were before it — the process contradicting its own
commit, which is the one thing the eviction exists to prevent. Coarse, and
worth knowing before you measure it: **any** eviction anywhere in that
collection, of any key, throws away the whole of that read's filling. A list
page drawn while somebody saves a record of the same kind pays for its `by_id`s
afterwards. That is the trade taken deliberately — a discarded fill costs a
round trip somebody was going to pay anyway, where a kept one costs a row that
says something the process itself has overwritten.

### A key of your own

A unique index has already told dray that its columns identify one row, so a
`find_first` naming all of them is a lookup by a key of yours:

```python
store.regions.find_first(equals={"code": "BM"})   # a statement, the first time
store.regions.find_first(equals={"code": "BM"})   # neither, the second
```

What is remembered is the key and an **id** — never the row. A row cached under
its own key and again under its code would be two entries to drop on a write and
two that can disagree; one entry under one key, reached through an id, is
invalidated by the eviction that already exists. The record that comes back is
checked against the filter it came back for, so a key naming a row that has
since been written to, or removed, costs a round trip and corrects itself rather
than answering with the wrong record.

Every column of the index, and a leading run is not enough: an index over
`(area, code)` says nothing about how many rows share an area. Naming more than
the index covers is fine — at most one row can hold the indexed values, and the
rest of the filter decides whether that row is the answer.

Children are read through their parent with `find`, so a set is not cached any
more than any other set is. What a `cached_for=` on a `@child` buys is the child
asked for by its own id, through the `collection=` that gives it a door of its
own — and a child written by its parent's save has its key dropped by that save
like anything else the write landed.

### What a write does about it

**dray sees its own writes, and this is the part a caller cannot do for
itself.** A save drops exactly the key it wrote — not the collection, not the
cache — so the process that changed a row never reads back what it replaced:

```python
person = store.people.by_id(person_id)
person.status = "volunteer"
person.save()

store.people.by_id(person_id).status     # 'volunteer', read again
```

Once, and after the commit. It waits for the outermost block, so a write inside
a `store.transaction()` that rolls back drops nothing — an eviction that fired
early would empty the cache and let the next read fill it from a transaction
that then went away, which is worse than having no cache at all. And it is
outside the replay, so a write DSQL refuses four times still evicts once.

A `delete` drops the record it removed. Everything the cascade took goes
wholesale instead, and so does a `clear` or a `thin` of a child set: those name
a parent rather than the rows they take, so there are no keys to drop one at a
time and the only honest thing to say about that generation afterwards is that
all of it may be wrong.

**Inside a block you opened, the cache is neither read nor filled.** A block is
where a read sees this store's own uncommitted writes, so filling from one would
publish rows a rollback then takes away, and answering from one would hand back
the row from before a write in the same block. Outside a block neither can
happen: every statement is its own transaction, and a write has dropped its keys
by the time the next read asks.

### A question of your own

Everything above is dray keeping a record it can recognise again. The other
half is the question dray has no name for, and it is where an application's
expensive reads actually live — the summary, the roll-up across three tables,
the search behind a box somebody types in. How much one of those costs, and how
stale its answer may be, are both things only the person who wrote it knows. So
they are said on the method:

```python
from dray import cached_for


@collection(of=Region)
class Regions:
    @cached_for(1800)
    def by_code(self, code: str) -> Region | None:
        return self.find_first(equals={"code": code})

    @cached_for(60)
    def named(self, names: tuple) -> list[Region]:
        return self.find(equals={"name": any_of(*names)})

    @cached_for(60)
    def strength(self) -> list[dict]:
        """People per region — a join and a group by."""
        return self.select_rows(...)
```

The answer is kept under the arguments the method was called with, so the same
question asked again inside the lifetime does not reach the database at all.
dray never looks inside the method: whatever it returns is what is kept.

**Nothing evicts this, and it is the whole of what to know about it.** A save
drops the keys it wrote because a key is a thing a write can be matched back
to. A question is not: a write to any record can change what a method of yours
answers, and nothing here can say which. So the number you chose is the bound
and there is no second promise underneath it — which is also why it is yours to
choose rather than dray's to allow, because you are the one who knows what the
answer tolerates. A figure that costs four seconds to build is very often fine
a minute old; a figure somebody is about to act on is not fine ten seconds old,
and the way to say so is not to put a lifetime on it.

**The arguments are the key, so they have to be hashable.** A call carrying a
list or a set is refused where it is written:

```
TypeError: Regions.named is @cached_for, so what it is called with is what its
answer is kept under — and names was given a list, which cannot be a key. Pass
something hashable in its place: a tuple rather than a list, a frozenset rather
than a set. ...
```

rather than quietly going to the database on every call. A method with a
lifetime on it is one somebody found expensive, and a call that silently opted
out of the cache is the kind of thing nobody finds for a year.

The rest of it is what it is for a row, and deliberately so. The answers are
kept on the pool, so every store shares them and `pool.forget_all()` empties
them along with everything else — which is what dray adds over reaching for a
cache library inside the method, where the map would be a module-level thing
outliving every store in the process and answerable to nothing. Three threads
asking one question at once ask it once. A call inside a `store.transaction()`
or a `store.uncached()` block is neither answered from memory nor kept, for the
two reasons a read by key is not. And every caller is handed its own copy of
the answer.

It goes on a collection's methods and not on a record's. A method on a record
is about one row, which is the thing `by_id` already keeps and the one thing a
write *can* be matched back to — so a second cache in front of it under a
different key would be two answers about one row with two lifetimes, and dray
refuses it where it is called.

### Copies, and never the record

What is kept is the row. Every caller is handed a record built fresh from it,
so two threads holding one record never hold one object — and neither do their
lists:

```python
first = store.people.by_id(person_id)
second = store.people.by_id(person_id)

first is second        # False
first == second        # True — the same record, as *Which record this is* says
```

That costs one `load` per read and it is worth it. A cache that handed back the
object itself would hand two threads a record one of them had edited and not
saved, and the thread behind would read edits that are in nobody's database.
What is kept here is the row and not the record — values, with no identity of
their own — so hydrating a fresh one is the only way out of it, and every
caller gets theirs.

A kept answer is copied for the same reason, whatever it happens to be: a
record, a list of them, a page of rows. The one thing the copy does not carry
over is dray's own way back to the store, which every record holds and which a
connection sits at the end of — a record answered out of memory is pointed at
the store that *asked*, not at the store that computed the answer, whose
connection went back to the pool long ago and may be in another thread's hands
by now. So a record out of a `@cached_for` method is a working record and not a
snapshot, and saving it does what saving it always did.

### The read that has to be true

Some reads must not be answered from memory, and the caller knows which:

```python
with store.uncached():
    person = store.people.by_id(person_id)
```

Everything read in the block goes to the database and nothing read in it is
remembered — a `@cached_for` method of your own included, since a block saying
*this must be true* said it about everything inside it. It empties nothing — the
block is about this read rather than about the cache — and a write inside it
still drops the keys it wrote, because that eviction is about every other store
sharing the cache and is not this store's to skip.

There is no *this came from cache* flag on a record, deliberately. The caller
who must not read a stale row wants to say so at the read, which is the block
above; the caller who wants to know what the cache is doing wants numbers, which
is `cache_info`. A flag on the object answers neither, and has no answer at all
to what it means once you have edited the record and saved it.

The rest of the vocabulary is for what moved some other way — a statement of
your own through `store.conn`, a migration, a job in another language:

```python
store.people.forget(person_id)     # one record
store.people.forget_all()          # one collection
store.pool.forget_all()            # every collection, every store on this pool
store.forget_all()                 # the same, said to a store with no pool

store.people.cache_info()
# CacheInfo(hits=41, misses=3, size=3)
```

`forget_all` and `cache_info` cover both of the things a collection remembers —
its rows and its kept answers — because *is anything here being served from
memory* is one question and two sets of counters would be two things to add up
at the call site. `forget` is the rows alone: a question is not about an id.
`cache_info` answers `None` for a collection that remembers nothing at all,
which is a different thing from a cache nobody has used yet. A read that waited
on another thread's round trip counts as a hit, because that is what it cost;
rows a `find` left behind are neither, since nobody asked for those by key.

`cache_info` counts them and a trace draws them. A hit opens a `cache` span, so
a page that has stopped reading and a page that never read are two different
pictures rather than one — *Watching what dray does*, below, and
`store.watching(kind="cache")` is how a test asserts a number about it.

### How stale it can be, said plainly

**The cache is in this process and there is no way to tell anybody else.** A
Lambda that writes cannot reach the other warm containers; a second instance
behind a load balancer cannot be told; a job on another host will go on
answering from what it read. The bound is the lifetime and nothing else: for
`cached_for=10`, ten seconds after somebody else's write, in every process but
the one that made it.

That is the whole of the promise, and it is why the number is per record and
per question rather than one for the pool. A region list ten minutes out of date
is a page nobody notices; a figure somebody is about to act on may not survive
ten seconds — and the way to say so is to leave `cached_for` off, or to read it
inside `store.uncached()`.

A kept answer is stale in one more way than a kept row, and it is worth saying
twice: this process's own write moves the row and leaves the answer where it
was. For `cached_for=` on a record the bound is *a lifetime after somebody
else's write*; for `@cached_for` on a method it is *a lifetime after any write
at all, this process's included*.

Three misses concurrent on one key make one round trip and two waits, rather
than three round trips. That is not a detail: a warming phase that fanned out
across eight threads and asked twice for the same record would otherwise do the
very thing it exists to avoid.

And a cache is not the answer to a read that is wrong. A `count()` that reads
every note is a query to fix rather than a result to keep, and a page doing
forty round trips because it asks a question per row wants
`counts_for` or a `find` with `any_of` — putting a lifetime in front of it makes
it forty round trips every ten seconds and hides which of them was the problem.

> **What DSQL is doing.** The reason this earns its place here rather than in an
> application is distance. A cluster is a service across the network and a
> multi-region cluster commits across regions, so a read is a millisecond at
> best and rather more than that from somewhere else — where a heap-storage
> database next door on the same machine answers a primary-key read out of its
> own buffer pool for microseconds. There is no shared buffer pool here to be
> hit instead, and no connection-level cache either; the closest thing to one is
> the memory of the process that asked.

## Watching what dray does

dray sends statements you never wrote. That is the point of it, and it is also
why a page that takes four seconds is hard to argue with from above: you cannot
see the statement, you cannot count the round trips, and four seconds of
database looks exactly like four seconds of turning rows into records.

So hand it an observer:

```python
def log_it(span):
    if span.phase == "close" and span.kind == "statement":
        log.debug("%s %r %.1fms", span.sql, span.params, span.elapsed_ns / 1e6)


pool = Pool(host="...", observer=log_it)
store = Store.connect(host="...", observer=log_it)
```

It is an observer and not a logging framework. dray owns no levels, no
formatters, no destination and no redaction policy — Python has `logging` and
every application configures it differently, and this is the same split
`after_commit` makes: the moment, and not the mechanism. **It is also the only
honest answer about the parameters.** dray cannot know which of your columns is
a medical note, so it hands over what it already gave the driver and where that
goes is your decision.

**Off by default, and off costs nothing.** No clock is read, no span is built and
no stack is touched on a store nobody is watching. A library that sells itself on
knowing what each call costs cannot charge three percent for an instrument nobody
switched on.

### Spans, and there are ten kinds

Everything emitted is a span: a start, an end, a kind and a parent. A statement
is the leaf case, and the ones around it are usually the interesting part.

| kind | opened by |
|---|---|
| `checkout` | a store taken from the pool |
| `connect` | a connection made, the IAM handshake included |
| `caller` | `with store.span("render the booking page")` |
| `transaction` | `store.transaction()`, and the one a save opens for itself |
| `statement` | one statement sent |
| `execute` | the wait inside a statement |
| `hydrate` | building records from rows, which is dray's time and not the database's |
| `cache` | a read answered out of memory, which is the statement that never happened |
| `returning` | the fetch after an insert or an update |
| `prepare` | validation, handlers and chunking, before a bulk write sends anything |

There is nothing to subscribe to. `kind` is a fact on the span and filtering is
your own `if`, because a subscription taxonomy would mean dray owning the cuts
and somebody always wants one it did not think of.

`hydrate` nests *under* the read it belongs to, which is what makes that work:
keep only `kind == "statement"` and you have the flat view, with the parent's
elapsed still the honest total. It is the one number in the tree that is dray's
own, and it is worth having on its own account — a `find()` with no filter
reading `execute 40ms, hydrate 3,900ms` is a completely different problem from
a slow query, and from the call site the two are indistinguishable.

**A hit is a node, not an absence.** A read the cache answered sends no
statement, so once a record has `cached_for=` on it a page that stopped reading
and a page that never read are the same picture — and *is the cache earning its
place* is exactly the question somebody brings to a trace. So a hit opens a
`cache` span. A `by_id` out of memory is one, with the `hydrate` that built the
record underneath; a `find_first` by a key of your own is one for the key, with
a second under it for the row or the statement that had to fetch it; a
`@cached_for` method of yours is one on its own, labelled with the method's
name, since `cls` on both of them is the record class. A miss opens none,
because the statement under it already says it went to the database, and the
rows a read leaves behind for later are not hits — a `find` that seeded forty
of them is still one statement and no `cache` at all.

**And it says what the hit cost, the waiting included.** Three threads missing
on one key make one round trip and two waits, and the two that waited count as
hits because that is what the read cost them. Their spans cost it too: a hit
that queued behind somebody else's round trip is as long as the round trip it
queued behind, rather than the tenth of a millisecond of building a record from
a row that was already there. Which is the number worth having, because a
warming phase that is really eight threads in a queue looks exactly like one
that worked until you can see what each hit paid.

**A wait is not always a round trip, and on a write it usually is not.** A set
goes out together — `add_all` of twenty sends twenty statements and waits
once — so the first `execute` is the whole trip and the nineteen behind it are
the cost of reading a result already in hand. The statements are still twenty
spans, because a count of them is how an N+1 is found and halving somebody's
number to flatter a change would be the wrong kind of quiet. What the shape of
their `execute` times says is that they travelled together.

**A span arrives twice, once when it opens and once when it closes.** Parents
therefore open before their children, so a tree can be drawn top-down as it
happens rather than assembled at the end. The cost is double the volume and a
handler that ignores the half it does not want; `phase` says which is which, and
everything measured is on the close.

```python
Span(
    id=41,                  # process-wide, and a parent is named by id
    parent_id=38,           # None marks a root
    depth=2,                # free at emission, so a flat log line can indent
    kind="statement",
    phase="close",          # 'open' | 'close'
    at_ns=884_112_990_318,  # perf_counter_ns
    wall=1_781_942_400_000, # time_ns, for correlating outside the process
    thread_ident=6104618496,
    thread_name="ThreadPoolExecutor-0_1",
    label=None,             # your name for a `caller` span, and the method
                            # name on the `cache` one a `@cached_for` opens
    elapsed_ns=5_612_000,
    sql="select id, family_name, etag, data from person"
        " where status = %s order by id",
    params=["volunteer"],
    cls=Person,
    rowcount=40,
    attempt=None,
    error=None,
)
```

**Two clocks, and they do not do each other's job.** `at_ns` is monotonic and is
the only one an elapsed is ever derived from — one clock across the whole
process, which is what lets spans from several threads sit on a single axis.
`wall` is adjustable and can step backwards under NTP, so it is there to line a
span up against a request log or the DSQL console and for nothing else.
Nanoseconds as integers, because float seconds lose precision exactly where a
200µs statement lives.

**`attempt` is the field nothing but dray needs.** DSQL refuses a commit that
conflicted rather than blocking to avoid one, and dray replays it. Without this
a write refused twice reads as three unrelated transactions, and the thing that
actually happened is invisible in the only place anybody would look for it. It
is on the `transaction` close and not on any statement, because the replay owns
the transaction rather than a statement inside it.

`cls` is there so that *everything about bookings* is `span.cls is Booking`
rather than a `startswith` on the SQL, and `rowcount` separates two problems a
timing figure cannot: *this took two seconds* and *this came back with forty
thousand rows* want different fixes.

### A name of your own

```python
with store.span("render the booking page"):
    page = render(store)
```

The only kind dray does not open for itself, and the one that answers the
question actually being asked — not what each statement cost, but what the page
cost and where the rest of it went when the statements account for a tenth.
Everything the store does inside the block nests under it. It is free to leave
in the code: with no observer the block runs and nothing is timed.

### Threads, which nest within and group across

A store is one connection and one thread. So the parent stack is per store and
per thread, and **a `parent_id` never points at a span another thread opened.**

The consequence is deliberate: in a fan-out each worker's spans are a separate
root, laid beside the caller's on the clock rather than under it.

```
              0ms       10        20        30        40
main      ├── render the booking page ───────────────────┤   42.0ms

worker-0        ├── transaction ──────┤                        8.1ms
                    ├── select Booking ──┤                     5.6ms
                    │     ├── execute          3.2ms
                    │     └── hydrate  40      2.4ms
                    └── insert Note      ┤                     1.1ms

worker-1        ├── select Person ┤                            1.9ms
worker-2        ├── select Note ──┤                            2.4ms
```

Four roots, not one tree. dray takes no position on how work fans out, copies no
context into threads it did not create, and needs no `pool.store(under=...)`.
What it gives up is proof that the workers belong to that request: in a process
serving forty requests through one pool, a window catches them all and thread
identity is the only thing separating them. Filtering to the threads your
application spawned is your job, which is the same line as everything else here.

An observer on a `Pool` reaches every store the pool makes, so **it is called
from every thread at once** and making it safe to call concurrently is yours —
exactly as it is for an `after_commit` handler.

### Counting the round trips

The other question is a test's: *this page does one read, not six.* A callback
gives you nowhere to put the tally, so everybody writes the same closure over a
list — and the list needs a lock the moment the page fans out, which is exactly
when the question is worth asking.

```python
with store.watching() as seen:
    render_the_page()

assert len(seen) == 1
```

Statements by default, because that is the question `len(seen) == 1` asks: one
read emits six spans once its `execute`, its `hydrate` and the transaction
around it are counted. `kind="transaction"` is how long a block was open,
`kind=None` is the whole tree, and what you get back is a plain sequence of
closed spans — `len`, indexing and iteration.

It catches this store and every store checked out of this store's pool while the
block is open, which is what makes it answer for a fan-out: the workers take
theirs from the pool inside the block. It does not reach back to a store checked
out somewhere else beforehand — a store decides whether it is watched when it is
made, which is what keeps an unwatched one free — and on a store with no pool it
collects that store alone.

`kind="cache"` asks the other half of the same question, and is the way to
prove *Not asking twice* is doing anything:

```python
with store.watching(kind="cache") as hits:
    render_the_page()

assert len(hits) == 4
```

A read answered out of memory emits no statement, so a count of statements says
the page did less and never says why. These are the reads that did not happen,
counted the same way and reaching the same fan-out — and the number is the one
`cache_info` reports, which is per map answered rather than per call. So a
`find_first` by a key of your own counts two where the key and the row were
both in memory, and one where a write since dropped the row and left the key
naming it: the span is the key, and the statement under it fetched the row.

### Seeing it

`scripts/flow.py` in this repository is the whole idea in one file: an observer
that collects, a tree drawn from what it collected, and then — afterwards, on a
second store nobody is watching — `explain analyze verbose` for each read, hung
under the span that ran it. `just flow` runs it against a cluster.

```
╰─ caller the volunteer page············  249.88ms
   ├─ statement Person··················   36.58ms 4 rows
   │  │  Full Scan (btree-table) on public.person  …
   │  │      Filters: (status = 'volunteer'::text)
   │  │  Statement DPU Estimate:
   │  │    Read: 0.00195 DPU (Transaction minimum: 0.00375)
   │  ├─ execute Person·················   36.24ms
   │  ╰─ hydrate Person·················    0.08ms 4 rows
   ╰─ transaction·······················  158.13ms
```

Three things are visible there that no amount of timing the call from outside
would tell you. The read is a **full scan**, because nothing declared an index
for a filter on `status` and an order on `family_name`. `execute` against
`hydrate` is 450 to 1, so the time is the wire and not dray turning rows into
records — which is worth knowing before anybody optimises the wrong half. And
the read did 0.00195 DPU of work against a **transaction minimum of 0.00375**,
which is the sentence to take away: what a page costs on this database is partly
how much it reads and partly **how many transactions it opens**, and a block
around four small reads pays one floor rather than four.

The numbers move between runs and the shape does not. Run it rather than trust
the ones above.

### What a handler must not do, and what it does not see

**A handler that queries is a bug, and dray says so.** A handler calling back
into a store somebody is watching would emit a span, which would call the
handler, without end — and it would present as a hang rather than as an error.
dray raises a `RuntimeError` instead, the first time. It is not in *When it goes
wrong* above because it is a programming mistake and not a refusal anybody
writes an `except` for. A handler that raises anything else raises where dray
was and takes the statement down with it, so an observer is not the place for
work that can fail — hand the span somewhere else and let that thing worry.

**An observer sees the statements dray wrote.** A hand-written `select_many`
body is watched, because dray sent it; a statement you put down `store.conn` is
yours and is not, which is the same boundary dray keeps everywhere else. So a
count of round trips includes the SQL you wrote through a collection and not the
SQL you ran around one.

> **What DSQL is doing.** A transaction is killed at five minutes, and every
> millisecond it is open is a wider window for the optimistic conflict that
> makes a replay fire — so *how long was this transaction open* matters here in
> a way it does not on PostgreSQL, and an observer that only saw statements
> could tell you the six of them took 40ms and never that the block around them
> was open for four seconds because a read waited on an HTTP call in the middle.
