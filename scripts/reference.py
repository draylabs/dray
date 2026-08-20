#!/usr/bin/env python
"""
Draw `docs/reference.html` — the page the manual's prose never assembles.

Two halves. One is read out of the library at render time: the members on a
pool, a store, a collection, a child set, and what `@record` lends a class. Add
a method and it appears here by itself, under a heading saying nobody has
described it yet.

The other half is a map written down below — what each member is for, which
door runs which stage, where the transaction is, and what one assignment line
does about a name. None of that is derivable, and all of it was measured
rather than remembered: a probe record with an instrumented converter,
validator, `on_change` and `@check`, pushed through every door, and every
assignment in the table made against a real record. The two cells about a
caller's own block were settled against the cluster, since a refused commit is
the one thing local PostgreSQL will not produce.

The three panels of module-level functions are in that written-down half, and
are checked against `__all__` all the same, because a written-down list is the
half that can silently go short.

    just reference

Which rewrites the page in place. Mermaid is fetched from a CDN rather than
inlined, so the structure diagram is raw flowchart text where there is no
network, and on github.com, which shows an `.html` file as its source.
Accepted: this page is meant to be opened in a browser, and the alternative is
a third of a megabyte of vendored JavaScript in the repository. A change to
what a caller can call, to when a hook fires, or to what a transaction covers
is a change to this file as well.
"""

import dataclasses
import pathlib
import types

import dray
from dray import Collection, Pool, Store, Write
from dray.child import ChildSet
from dray.model import _RECORD_HOOKS, _RECORD_LENT, _RECORD_MEMBERS

# --------------------------------------------------------------------------
# The maps. Everything below them is layout.
# --------------------------------------------------------------------------

GLOSS = {
    "add": "write one, and whatever it has queued",
    "add_all": "write many, in transactions that fit",
    "save": "write one that exists",
    "save_all": "write many that exist",
    "delete": "remove it and everything below it",
    "find": "equality, and nothing else",
    "find_first": "the first, or None",
    "by_id": "by key, raising if it is not there",
    "count": "how many match",
    "counts_for": "how many per parent, in one statement",
    "in_batches": "walk more than you can hold",
    "select_many": "SQL you wrote, records back",
    "select_first": "SQL you wrote, one record or None",
    "select_rows": "SQL you wrote, rows back",
    "table": "the table name, for a statement of your own",
    "columns": "every column, for a select that must be whole",
    "id": "the key column",
    "etag": "the guard column",
    "blob": "the jsonb column",
    "parent_type": "a child's parent's table column",
    "parent_id": "a child's parent's key column",
    "sql_for": "one field of yours, as the SQL that reads it",
    "conn": "the connection, which is yours to use",
    "forget": "drop one record, for a row that moved some other way",
    "forget_all": "drop every row cached for this collection",
    "cache_info": "hits, misses and size — None where nothing is cached",
    "uncached": "a block whose reads must not come out of memory",
    "parse": "build from data that came from outside — strict",
    "as_dict": "every field, by name",
    "children": "what kinds hang off this record",
    "store": "where it came from, so a rule can read across",
    "connect": "make a store, with the IAM handshake",
    "create": "the tables these records imply",
    "transaction": "a block of your own",
    "watching": "collect what dray does",
    "span": "a name of your own in the trace",
    "after_commit": "work for once the rows are durable",
    "in_transaction": "whether a block is open",
    "close": "give the connection back",
    "dsql": "whether this is DSQL or PostgreSQL-shaped",
    "serves": "the records this store knows",
}

GROUPS = {
    "Collection": [
        ("writing", ["add", "add_all", "save", "save_all", "delete"]),
        ("prebuilt reads — the 90%",
         ["find", "find_first", "by_id", "count", "counts_for", "in_batches"]),
        ("reads you wrote — the other 10%",
         ["select_many", "select_first", "select_rows"]),
        ("names for your own statements",
         ["table", "columns", "id", "etag", "blob", "parent_type",
          "parent_id", "sql_for", "conn"],
         "The seven names and <code>sql_for</code> come off a record class "
         "too, through <code>dray.names_of(cls)</code> — which is the only "
         "door a <code>@child</code> declared without <code>collection=</code> "
         "has. <code>sql_for</code> is the one that answers for a field named "
         "at runtime: the column's name where the class gives it one, and the "
         "cast out of the blob where it does not."),
        ("what is remembered between reads",
         ["forget", "forget_all", "cache_info"],
         "Two things, and they are invalidated differently. A record that said "
         "<code>cached_for=</code> has its rows kept and served to "
         "<code>by_id</code> alone, filled by every read of whole records, and "
         "dropped by dray after dray&#39;s own writes — so <code>forget</code> "
         "is for the row that moved some other way. A method of yours marked "
         "<code>@cached_for(...)</code> has its answer kept under the arguments "
         "it was called with, and <b>nothing evicts that</b>: a write to any "
         "record can change what it answers and dray cannot know which. "
         "<code>forget_all</code> and <code>cache_info</code> cover both."),
    ],
    "ChildSet": [
        ("queued on the parent, written by its save", ["add"]),
        ("a write that happens where it is called", ["clear", "thin"],
         "Neither waits for the parent's save, because a removal has no change "
         "to ride with. What they run is decided by the child class: one "
         "statement per generation where it declares no "
         "<code>@before_delete</code>, and the children read and the rule run "
         "on each where it does. <code>clear</code> is one transaction and all "
         "or nothing; <code>thin</code> is one transaction per pass and the "
         "only door here that can leave a rule run for some of a generation "
         "and never for the rest."),
        ("read, always scoped to this parent",
         ["by_id", "count", "find", "find_first"]),
    ],
    "Pool": [
        ("one each, cheaply", ["store"]),
        ("what every store on it shares", ["forget_all"]),
        ("what it is", ["opened", "close"]),
    ],
    "Store": [
        ("getting one", ["connect", "close"]),
        ("schema", ["create"]),
        ("transactions", ["transaction", "in_transaction", "after_commit"]),
        ("what is remembered between reads", ["uncached", "forget_all"]),
        ("seeing what it did", ["watching", "span"],
         "Ten kinds of span, and <code>watching(kind=…)</code> takes one of "
         "them: <code>checkout</code>, <code>connect</code>, "
         "<code>caller</code>, <code>transaction</code>, "
         "<code>statement</code>, <code>execute</code>, <code>hydrate</code>, "
         "<code>cache</code>, <code>returning</code>, <code>prepare</code>. "
         "<code>cache</code> is the odd one out: it is the read that did not "
         "happen, opened on a hit and on nothing else, so a page the cache "
         "answered is a node in the tree rather than a gap in it."),
        ("what it is", ["conn", "dsql", "serves"]),
    ],
    "Write": [
        ("what a rule is handed", ["record", "adding", "given", "was"],
         "Frozen, <code>was</code> included, and one per record per save — one "
         "object for every attempt of a commit DSQL refuses. <code>record</code>"
         " is the record as it <i>will be</i>, so a rule about what the row "
         "said a moment ago reads <code>write.was.get(name, record.name)</code>"
         " — the mapping where the field moved, the record where it did not."),
    ],
}

DECORATORS = [
    ("@record", "a plain class becomes a record",
     "table= collection= indexes= key= etag= blob= order_by= cached_for= "
     "cache_most="),
    ("@child", "a record that belongs to another",
     "of= name= table= collection= order_by= indexes= key= etag= "
     "blob= parent_type= parent_id= cached_for= cache_most="),
    ("@collection", "the vocabulary a record has of its own", "of="),
    ("field()", "what a field will take and what fills it",
     "default= default_factory= stored_in= choices= converter= "
     "validator= on_add= on_save= on_change= derived= precision= scale="),
    ("index()", "what the table is indexed for", "*columns unique="),
]

MARKERS = [
    ("@check", "a rule about the whole record, past what a "
                "field's validator can see"),
    ("@before_save", "a rule inside the write's own transaction, so it "
                     "may read and write — and the only marked method "
                     "handed the write as well as the record"),
    ("@before_delete", "a rule no removal door can avoid"),
    ("@after_commit", "work for once the rows are durable"),
]

# The rest of the module surface, which neither list above carries. Grouped by
# where the name goes, because that is what already separates the two panels
# beside it: one is written on a class and one on a method, and these are
# either handed to a declaration or called in code of your own. `jsonb` and
# `as_uuid` are honestly both, and sit where a reader meets them first.
#
# A card would have been the other place to put these, and is wrong twice over:
# `card()` drops any name the class does not actually offer, so they would
# vanish silently, and listing `any_of` under a collection would say a
# collection offers it — which is the reading these were made functions to
# avoid.
CALLS = [
    ("handed to a declaration", [
        ("clock", "the database&#39;s clock, for a field recording when "
                  "something happened. Handed uncalled: dray runs it on the "
                  "server, once per write", ""),
        ("records_change()", "queue a line about the change, in the child "
                             "named", "into="),
        ("asc()", "forwards, and where the empty ones go", "name nulls="),
        ("desc()", "newest first, and where the empty ones go", "name nulls="),
        ("jsonb()", "a value on its way to the <code>jsonb</code> column, or "
                    "to a filter against one", "value"),
        ("as_uuid()", "a <code>UUID</code>, from one or from the string a URL "
                      "hands you", "value"),
    ]),
    ("called in code of your own", [
        ("any_of()", "equal to any of these", "*values"),
        ("none_of()", "equal to none of these, and a field holding nothing "
                      "counts as one", "*values"),
        ("key_of()", "the key of a record, whatever this class calls the "
                     "column", "record"),
        ("names_of()", "the names dray owns on a record class, for a "
                       "statement you wrote", "cls"),
        ("describe()", "what happened, in a sentence", "change"),
        ("@replaying", "run a function of yours again when DSQL refuses what "
                       "it wrote", "attempts=5"),
        ("@cached_for()", "keep what a collection method of yours answers, for "
                          "this many seconds. A lifetime and nothing else — no "
                          "write evicts it, because dray cannot know which "
                          "answers a write changed", "seconds cache_most="),
        ("@retrying", "dray&#39;s own, and not yours. It catches what "
                      "<code>store.transaction()</code> has already turned "
                      "into a <code>CommitRefused</code>, so on your function "
                      "it replays nothing and looks like it works. Reachable, "
                      "and outside <code>__all__</code> on purpose", "work"),
    ]),
]

SHAPE = """flowchart TD
    PL(["a pool<br/><i>connections, retired before DSQL closes them</i>"])
    S(["a store<br/><i>one connection, one thread</i>"])

    PC["store.people<br/><b>a collection</b>"]
    P["Person<br/><b>a record</b>"]
    NS["person.notes<br/><b>a child set</b><br/><i>scoped to this person</i>"]
    N["Note<br/><b>a child — which is a record</b><br/><i>plus parent_type, parent_id</i>"]
    NC["store.notes<br/><b>a collection</b><br/><i>only with collection=</i>"]

    PL -->|"with pool.store() as store<br/><i>one per request</i>"| S
    S -->|"the records it was told about"| PC
    PC -->|"add · find · by_id · save · delete"| P
    P -->|"@child(of=Person, name='notes')"| NS
    NS -->|"add queues · clear removes · find reads"| N
    S -.->|"a second door, unscoped"| NC
    NC -->|"add(note, parent=person) · find(parent=person)"| N
    N -.->|"has everything a record has:<br/>save · delete · parse · as_dict · its own @check"| P

    classDef pool fill:#f7f4ea,stroke:#8a7534,color:#332b0f
    classDef store fill:#eef0f2,stroke:#5b6169,color:#22262b
    classDef coll fill:#fdf3e6,stroke:#a8620a,color:#3d2405
    classDef rec fill:#e8eef5,stroke:#2d6a9f,color:#12263a
    classDef kid fill:#e9f5ef,stroke:#1a7f5a,color:#0d3625

    class PL pool
    class S store
    class PC,NC coll
    class P rec
    class NS,N kid
"""

WALK = [
    ("pool", 'pool = dray.Pool(host="…")', "a pool"),
    ("", "", ""),
    ("store", "with pool.store() as store:", "a store, for this request"),
    ("", "", ""),
    ("#", "# Add a person and a note about them, in one transaction.", ""),
    ("rec", '    person = Person(family_name="…")', "a record — in memory"),
    ("kid", '    person.notes.add("Called back.")', "a child set — queued"),
    ("coll", "    store.people.add(person)", "a collection — both rows"),
    ("", "", ""),
    ("#", "# Ask about notes themselves, across every person — which a "
          "child set cannot, being scoped to one.", ""),
    ("coll", "    store.notes.since(a_week_ago)", "the second door"),
]

WALK_COLOUR = {
    "pool": "#8a7534", "store": "#5b6169", "coll": "#a8620a",
    "rec": "#2d6a9f", "kid": "#1a7f5a",
}

DOORS = [
    ("Person(...)", "your own code", "#2d6a9f"),
    ("Person.parse(row)", "data from outside", "#2d6a9f"),
    ("a read → _dray_load", "a row this table holds", "#5b6169"),
    ("record.field = value", "at any time after", "#2d6a9f"),
    ("add · add_all", "the first write", "#a8620a"),
    ("save · save_all", "every write after", "#a8620a"),
]

STAGES = [
    ("refuse an unknown name", "a name the class declares no field for",
     ["run", "run", "na", "run", "na", "na"],
     'Person.parse({"family_name": "hemingway", "«give_anme»": "raises"})'),
    ("converter", "turns what arrived into what the field holds",
     ["run", "run", "skip", "run", "filled", "filled"],
     'family_name: str = field(«converter»=«str.title»)'),
    ("the declared type", "what the annotation says it holds",
     ["skip", "run", "skip", "run", "run", "run"],
     'joined_on: «date | None» = field()'),
    ("choices=", "one of a list, and nothing else",
     ["skip", "run", "skip", "run", "run", "run"],
     'status: str = field(«choices»=STATUSES)'),
    ("validator=", "a function of your own, raising to refuse",
     ["skip", "run", "skip", "run", "run", "run"],
     'family_name: str = field(«validator»=not_in_capitals)'),
    ("on_add", "fills a field the first write owns",
     ["na", "na", "na", "na", "run", "skip"],
     'created_at: datetime | None = field(«on_add»=clock)'),
    ("on_save", "fills a field every later write owns",
     ["na", "na", "na", "na", "skip", "run"],
     'updated_at: datetime | None = field(«on_add»=clock, «on_save»=clock)'),
    ("derived=", "worked out from other fields, never assigned",
     ["skip", "skip", "skip", "skip", "run", "run"],
     'full_name: str = field(«derived»=lambda r: f"{r.given} {r.family}")'),
    ("@check", "a rule reaching across more than one field",
     ["skip", "run", "skip", "skip", "run", "run"],
     "«@check»\ndef volunteer_status_requires_suburb(self): ..."),
    ("the value is set", "and marked as said, which is what a save writes",
     ["run", "run", "run", "run", "run", "run"],
     'person.suburb «=» "Katoomba"'),
    ("what it was, kept", "the first prior value per field, for a rule to "
                          "read off the write as <code>was</code>",
     ["skip", "skip", "skip", "moved", "skip", "skip"],
     'if write.«was».get("owner", self.owner) != write.given["whom"]: ...'),
    ("on_change", "and only if the value actually moved",
     ["skip", "skip", "skip", "moved", "skip", "skip"],
     'status: str = field(«on_change»=records_change(into="logs"))'),
]

NAMES = [
    ("the key", "«person.id» = something",
     "refused", "AttributeError",
     "a key cannot be changed once the record exists"),
    ("a derived= field", "«person.full_name» = something",
     "refused", "AttributeError",
     "it is worked out from other fields, so there is nothing to set"),
    ("a field you declared", "«person.family_name» = something",
     "managed", "converter → rules → set → what it was → on_change",
     "however it is spelled — a leading underscore does not exempt it"),
    ("the etag", "«person.etag» = something",
     "managed", "set, and then overwritten",
     "no refusal, and no use either: every write mints a fresh one"),
    ("a name you never declared,<br>starting with _", "«person._seated» = True",
     "yours", "straight to the object",
     "no column, never written, never read back — your own transient"),
    ("a name you never declared", "«person.nope» = 1",
     "refused", "AttributeError",
     "nothing is stored that was not declared"),
]

CELL = {
    "run": ("●", "#1a7f5a", 1),
    "skip": ("○", "#c8ced4", 0),
    "na": ("", "transparent", 0),
    "moved": ("◐", "#b8860b", 1),
    "filled": ("◐", "#b8860b", 1),
}

OUTCOME = {
    "refused": "#b4453a", "managed": "#1a7f5a", "yours": "#7d3cb5",
}

TX = [
    ("a read", "find · count · by_id · in_batches",
     ("no transaction of its own",
      "one statement, its own at the database"),
     ("joins yours", "and sees what you have written in it")),
    ("a write", "add · add_all · save · save_all",
     ("one of its own, per chunk",
      "replayed whole if DSQL refuses the commit"),
     ("joins yours", "never replayed — CommitRefused, and the work is "
      "yours to run again")),
    ("a delete", "delete · clear, and every generation under what goes",
     ("one of its own",
      "deepest first, so nothing is orphaned partway"),
     ("joins yours", "and the 3,000-row ceiling is yours to stay under")),
    ("thin", "one generation, up to at_a_time rows",
     ("one of its own, per pass",
      "so the loop is several — which is the only way past a ceiling that "
      "counts a transaction"),
     ("joins yours", "so the whole loop is one transaction again, which is "
      "the thing thin exists to avoid")),
    ("@check", "before any statement, and late on a child a rule queued",
     ("outside the transaction",
      "so anything it writes survives its own refusal — a late child's "
      "runs inside, and does not"),
     ("inside yours", "so anything it writes goes back with the block")),
    ("@before_save", "before the statements",
     ("inside, and inside the replay",
      "run again on every attempt, which is why it may write"),
     ("inside yours", "and run once, because there is no replay")),
    ("@after_commit", "when the rows are durable",
     ("after the commit, once",
      "outside the replay — it must not fire twice"),
     ("held until your block commits", "so it means what it says")),
]

DECLARING = """@«record»(table="person", collection="people",
        indexes=[«index»("status", "family_name")])
class Person:
    family_name: str = «field»()
    status: str = «field»(default="enquiry", choices=STATUSES)
    suburb: str | None = «field»(stored_in="blob")


@«child»(of=Person, name="notes", table="note",
       collection="notes")
class Note:
    body: str = «field»(default="")


@«collection»(of=Note)
class Notes(Collection):
    def since(self, when): ..."""

MARKER_CODE = {
    "@check": "@check\ndef volunteer_status_requires_suburb(self): ...",
    "@before_save": "@before_save\ndef event_must_have_room(self, write): ...",
    "@before_delete": "@before_delete\ndef record_their_departure(self): ...",
    "@after_commit": "@after_commit\ndef send_goodbye_notification(self): ...",
}

# --------------------------------------------------------------------------
# Layout
# --------------------------------------------------------------------------

CSS = """
  :root { --line: #e7ebef; --soft: #f1f4f6; }
  * { box-sizing: border-box; }
  body { font: 15px/1.55 ui-sans-serif, system-ui, -apple-system, sans-serif;
         margin: 0; background: #fbfbfa; color: #1c1c1a; }
  .wrap { max-width: 90rem; margin: 0 auto; padding: 2.5rem 2rem 5rem; }
  h1 { font-size: 1.7rem; font-weight: 640; margin: 0 0 .4rem; }
  h2 { font-size: 1.15rem; font-weight: 640; margin: 0 0 .3rem; }
  h3 { font-size: .98rem; font-weight: 620; margin: 2.2rem 0 .3rem; }
  .sub { opacity: .62; margin: 0 0 1rem; max-width: 62rem; }
  .lead { opacity: .62; margin: 0 0 1.2rem; max-width: 62rem;
          font-size: .88rem; }
  code { font: 12.5px ui-monospace, Menlo, monospace; }
  section.part { padding: 2.6rem 0 0; }
  section.part + section.part { border-top: 1px solid var(--line);
                                margin-top: 3rem; }
  nav { display: flex; gap: 1.2rem; font-size: .82rem; margin: 1.4rem 0 0;
        padding-bottom: 1.6rem; border-bottom: 1px solid var(--line); }
  nav a { color: #2d6a9f; text-decoration: none; }
  nav a:hover { text-decoration: underline; }

  /* the structure diagram and the walk beside it */
  .pair { display: grid; grid-template-columns: minmax(0,1fr) minmax(0,1.05fr);
          gap: 1rem; margin-bottom: 1.6rem; align-items: stretch; }
  @media (max-width: 62rem) { .pair { grid-template-columns: 1fr; } }
  .shape { background: #fff; border: 1px solid var(--line); border-radius: 12px;
           padding: 1.4rem; overflow-x: auto;
           display: flex; justify-content: center; align-items: center; }
  .shape svg { max-width: 32rem !important; width: 100% !important;
               height: auto; }
  .walk { background: #fff; border: 1px solid var(--line); border-radius: 12px;
          padding: 1.5rem 1.4rem; font-size: 12.5px; line-height: 1.9; }
  .wl { display: flex; align-items: baseline; gap: .55rem; white-space: nowrap; }
  .wd { width: 7px; height: 7px; border-radius: 50%; flex: 0 0 7px;
        transform: translateY(-1px); }
  .wg { font-size: 11px; opacity: .72; margin-left: .7rem;
        font-family: ui-sans-serif, system-ui, sans-serif; }
  .wc { white-space: normal; margin-top: .5rem; }
  .wc code { color: #7b848d; font-style: italic; line-height: 1.6; }

  /* the member cards */
  .cols { display: grid; gap: 1rem; align-items: start;
          grid-template-columns: repeat(5, minmax(0, 1fr)); }
  @media (max-width: 82rem) { .cols { grid-template-columns: repeat(3, 1fr); } }
  @media (max-width: 54rem) { .cols { grid-template-columns: repeat(2, 1fr); } }
  .card { background: #fff; border: 1px solid var(--line); border-radius: 12px;
          padding: 1.1rem 1.2rem 1.3rem; border-top: 3px solid var(--accent); }
  .card h2 { font: 600 14px ui-monospace, Menlo, monospace; margin: 0 0 .2rem;
             color: var(--accent); }
  .cs { font-size: .78rem; opacity: .6; margin: 0 0 .9rem; line-height: 1.45; }
  .g { font-size: .68rem; text-transform: uppercase; letter-spacing: .06em;
       opacity: .45; margin: .95rem 0 .35rem; font-weight: 600; }
  .gn { font-size: .74rem; opacity: .55; margin: 0 0 .5rem; line-height: 1.4; }
  .gn code { font-size: 11.5px; }
  .m { display: flex; gap: .6rem; align-items: baseline;
       padding: .17rem 0; border-bottom: 1px solid #f2f4f6; }
  .m code { font: 12.5px ui-monospace, Menlo, monospace; font-weight: 600;
            white-space: nowrap; }
  .m span { font-size: .74rem; opacity: .58; line-height: 1.35; }
  .decs { margin-top: 2rem; }
  .panel { background: #fff; border: 1px solid var(--line);
           border-radius: 12px; padding: 1.2rem 1.4rem; }
  .panel .dec:last-child { border-bottom: none; padding-bottom: 0; }
  .dec { display: grid; grid-template-columns: 8.5rem 1fr; gap: .8rem;
         padding: .5rem 0; border-bottom: 1px solid #eceff2;
         align-items: baseline; }
  .dec code:first-child { font-weight: 600; color: #7d3cb5; }
  .dec .what { font-size: .82rem; }
  .dec .args { font-size: .72rem; opacity: .5; display: block;
               margin-top: .15rem; }
  .dec3 { grid-template-columns: 8.5rem minmax(0,1fr) 22rem; }
  .pair2 { display: grid; grid-template-columns: minmax(0,1fr) 27rem;
           gap: 2rem; align-items: start; }
  @media (max-width: 68rem) { .pair2 { grid-template-columns: 1fr; }
                              .dec3 { grid-template-columns: 8.5rem 1fr; } }
  .ex { margin: 0; padding: .1rem 0 0;
        font: 10.5px/1.65 ui-monospace, Menlo, monospace; color: #7a848e;
        white-space: pre; }
  .ex b { color: #7d3cb5; font-weight: 700; }
  .ex.small { padding: .5rem 0; line-height: 1.5; }

  /* the grids */
  table { border-collapse: collapse; width: 100%; background: #fff;
          border: 1px solid var(--line); border-radius: 12px; }
  th, td { padding: 0; }
  thead th { vertical-align: bottom; padding: 1.1rem .5rem .9rem;
             border-bottom: 1px solid var(--line); }
  .grp th { font-size: 10px; text-transform: uppercase; letter-spacing: .09em;
            font-weight: 700; opacity: .45; padding: .9rem .5rem .1rem;
            border-bottom: none; text-align: center; }
  .grp th[colspan] { border-bottom: 2px solid var(--soft);
                     padding-bottom: .4rem; }
  .dh span:first-child { display: block;
                         font: 600 12px ui-monospace, Menlo, monospace; }
  .dw { display: block; font-size: 10.5px; opacity: .5; margin-top: .2rem; }
  tbody th { text-align: left; padding: .55rem 1.1rem .55rem 1.2rem;
             vertical-align: middle; max-width: 15rem; }
  .sn { display: block; font: 600 12.5px ui-monospace, Menlo, monospace;
        line-height: 1.4; }
  .sw { display: block; font-size: 10.5px; opacity: .52; line-height: 1.4;
        white-space: normal; }
  .cell { position: relative; height: 3.05rem; display: flex;
          align-items: center; justify-content: center; }
  .rail { position: absolute; top: 0; bottom: 50%; width: 2px; }
  .dot { font-size: 15px; position: relative; z-index: 1; background: #fff;
         padding: 0 .35rem; }
  td.code { padding: .35rem 1rem .35rem 1.3rem; border-left: 1px solid var(--soft);
            vertical-align: middle; width: 23rem; }
  td.code pre { margin: 0; font: 10.5px/1.5 ui-monospace, Menlo, monospace;
                color: #7a848e; white-space: pre; }
  td.code b { color: #1c2126; font-weight: 700; }
  .tag { font-size: 10px; text-transform: uppercase; letter-spacing: .07em;
         font-weight: 700; border: 1px solid; border-radius: 4px;
         padding: .1rem .4rem; white-space: nowrap; }
  td.res { padding: .55rem 1.2rem; }
  td.res b { display: block; font: 600 11.5px ui-monospace, Menlo, monospace; }
  td.res span { display: block; font-size: 10.5px; opacity: .55;
                margin-top: .15rem; }
  tbody tr + tr th, tbody tr + tr td { border-top: 1px solid var(--soft); }
  .key { margin-top: 1rem; font-size: .78rem; opacity: .7; display: flex;
         gap: 1.4rem; flex-wrap: wrap; }
  .ws { display: grid; gap: 1rem; margin-top: 2.2rem;
        grid-template-columns: repeat(auto-fit, minmax(16rem, 1fr)); }
  .w { background: #fff; border: 1px solid var(--line); border-radius: 10px;
       padding: .9rem 1rem; }
  .w b { font-size: .82rem; }
  .w p { font-size: .78rem; opacity: .68; margin: .3rem 0 0; line-height: 1.5; }
  #tx tbody th { max-width: none; width: 17rem; }
  #tx .sw { white-space: normal; }
  td.tx { padding: .7rem 1.2rem; vertical-align: top; }
  td.tx b { display: block; font-size: 12.5px; font-weight: 600; }
  td.tx span { display: block; font-size: 10.5px; opacity: .58;
               margin-top: .18rem; line-height: 1.45; }
"""


def offered(cls):
    return sorted(n for n, v in vars(cls).items() if not n.startswith("_"))


def carried(cls):
    """What a frozen dataclass holds, in the order it was declared.

    `vars` finds none of it — a field with no default leaves nothing on the
    class — so `Write` would otherwise be a card the library could not check,
    which is the half this page exists to keep honest. A member added to what a
    hook is handed and not described lands under the red heading like anything
    else."""
    return [spec.name for spec in dataclasses.fields(cls)]


def bare(name):
    """The identifier behind the way a panel writes it — `@record`,
    `field()`, `clock` — so a written-down list can be compared with the
    library."""
    return name.lstrip("@").removesuffix("()")


def module_functions():
    """Every function a reader can reach on the package.

    `__all__` rather than `dir(dray)`, plus the one name held back from it.
    `dir` would find `retrying` by itself and would also hand back the five
    submodules bound as import side effects, which belong on no panel and
    would nag here forever.
    """
    named = {n for n in dray.__all__
             if isinstance(getattr(dray, n), types.FunctionType)}
    return named | {"retrying"}


# Per card, because a bare name is not unique: `store` means one thing on a
# pool and another on a record, and `add` writes on a collection and only
# queues on a child set. A flat table quietly kept whichever was written last.
OWN = {
    "a pool": {"store": "a store for this request, given back at the end",
               "opened": "how many connections are live",
               "forget_all": "drop every cached row, of every kind — for the "
                             "process that has written past dray"},
    "a store": {"forget_all": "drop every cached row this store can see — the "
                              "pool's, or its own where it has no pool"},
    "a child set": {
        "add": "queue one, written by the parent's next save",
        "clear": "empty it now — the stored rows and the queue alike",
        "thin": "one pass, one generation, up to at_a_time rows — loop it",
    },
    "a record": {"store": "where it came from, so a rule can read across"},
    "a write": {
        "record": "the record this write is about, with every assignment on it",
        "adding": "true on the write that creates it, false on a save",
        "given": "what the write was told — the store's defaults under given=",
        "was": "what it held before this write, for the fields that moved",
    },
}


def card(title, sub, groups, real, accent, own=None):
    seen = set()
    body = []
    # A group may carry a third item, which is a sentence about the group
    # rather than about any one member — where the same names are reachable
    # from, what the whole set is for. The heading is a label and cannot hold
    # one; a gloss is per member and would have to be said eight times.
    for heading, names, *rest in groups:
        rows = []
        for n in names:
            if n not in real:
                continue           # gone from the library, so gone from here
            seen.add(n)
            says = (own or {}).get(n) or GLOSS.get(n, "")
            rows.append(
                f'<div class="m"><code>{n}</code><span>{says}</span></div>'
            )
        if rows:
            note = f'<p class="gn">{rest[0]}</p>' if rest else ""
            body.append(
                f'<div class="g">{heading}</div>{note}' + "".join(rows)
            )
    missing = [n for n in real if n not in seen]
    if missing:
        body.append('<div class="g" style="color:#b4453a">not grouped here yet</div>')
        body.extend(
            f'<div class="m"><code>{n}</code><span></span></div>' for n in missing
        )
    return (
        f'<section class="card" style="--accent:{accent}">'
        f'<h2>{title}</h2><p class="cs">{sub}</p>{"".join(body)}</section>'
    )


def lent_card():
    rows = []
    rows.append('<div class="g">lent — yours to claim, and dray\'s is still there '
                'under <code>_dray_</code></div>')
    for n in sorted(_RECORD_LENT):
        says = OWN["a record"].get(n) or GLOSS.get(n, "")
        rows.append(f'<div class="m"><code>{n}</code>'
                    f'<span>{says}</span></div>')
    rows.append('<div class="g">bound outright, under dray\'s own prefix</div>')
    for n in sorted(_RECORD_MEMBERS):
        rows.append(f'<div class="m"><code>{n}</code><span></span></div>')
    rows.append('<div class="g">dunders, lent the same way</div>')
    for n in sorted(_RECORD_HOOKS):
        rows.append(f'<div class="m"><code>__{n}__</code><span></span></div>')
    rows.append('<div class="g">plus every field you declared, and '
                '<code>id</code> and <code>etag</code></div>')
    return ('<section class="card" style="--accent:#2d6a9f">'
            '<h2>a record</h2><p class="cs">What <code>@record</code> puts on '
            'your class. A name you define yourself is left alone — dray binds '
            'the plain word only where the hierarchy has not spoken for it.'
            f'</p>{"".join(rows)}</section>')


def walk_html():
    rows = []
    for kind, code, gloss in WALK:
        if not code:
            rows.append('<div class="wl">&nbsp;</div>')
            continue
        if kind == "#":
            rows.append(f'<div class="wl wc"><code>{code}</code></div>')
            continue
        c = WALK_COLOUR.get(kind, "")
        dot = (f'<span class="wd" style="background:{c}"></span>' if c
               else '<span class="wd" style="background:none"></span>')
        note = (f'<span class="wg" style="color:{c or "#8b939c"}">{gloss}</span>'
                if gloss else "")
        code = code.replace(" ", "&nbsp;")
        rows.append(f'<div class="wl">{dot}<code>{code}</code>{note}</div>')
    return '<div class="walk">' + "".join(rows) + "</div>"


def code(text):
    return (text.replace("&", "&amp;").replace("<", "&lt;")
            .replace("«", "<b>").replace("»", "</b>"))


rows = []
for i, (name, what, cells, snippet) in enumerate(STAGES):
    tds = []
    for j, c in enumerate(cells):
        glyph, colour, on = CELL[c]
        above = i > 0 and CELL[STAGES[i - 1][2][j]][2]
        rail = "#dfe6ec" if (above and on) else "transparent"
        tds.append(
            f'<td><div class="cell">'
            f'<span class="rail" style="background:{rail}"></span>'
            f'<span class="dot" style="color:{colour}">{glyph}</span></div></td>'
        )
    rows.append(
        f'<tr><th><span class="sn">{name}</span>'
        f'<span class="sw">{what}</span></th>{"".join(tds)}'
        f'<td class="code"><pre>{code(snippet)}</pre></td></tr>'
    )

heads = "".join(
    f'<th class="dh"><span style="color:{c}">{n}</span>'
    f'<span class="dw">{w}</span></th>' for n, w, c in DOORS
)
names = "".join(
    f'<tr><th><span class="sn">{n}</span></th>'
    f'<td class="code"><pre>{code(ex)}</pre></td>'
    f'<td><span class="tag" style="color:{OUTCOME[o]};'
    f'border-color:{OUTCOME[o]}33">{o}</span></td>'
    f'<td class="res"><b>{res}</b><span>{why}</span></td></tr>'
    for n, ex, o, res, why in NAMES
)
tx_rows = "".join(
    f'<tr><th><span class="sn">{n}</span><span class="sw">{k}</span></th>'
    f'<td class="tx"><b>{a}</b><span>{aw}</span></td>'
    f'<td class="tx"><b>{b}</b><span>{bw}</span></td></tr>'
    for n, k, (a, aw), (b, bw) in TX
)
decs = "".join(
    f'<div class="dec"><code>{n}</code><div><span class="what">{w}</span>'
    f'<code class="args">{a}</code></div></div>' for n, w, a in DECORATORS
)
marks = "".join(
    f'<div class="dec dec3"><code>{n}</code>'
    f'<div><span class="what">{w}</span></div>'
    f'<pre class="ex small">'
    f'{MARKER_CODE[n].replace("&", "&amp;").replace("<", "&lt;")}</pre></div>'
    for n, w in MARKERS
)
written = ({bare(n) for n, *_ in DECORATORS}
           | {bare(n) for n, *_ in MARKERS}
           | {bare(n) for _, rows in CALLS for n, *_ in rows})
calls = ""
for heading, entries in CALLS:
    calls += f'<div class="g">{heading}</div>'
    calls += "".join(
        f'<div class="dec"><code>{n}</code><div><span class="what">{w}</span>'
        f'<code class="args">{a}</code></div></div>' for n, w, a in entries
    )
# The same promise the cards make, kept by the half that is written down: what
# the library exports and no panel above describes is named here rather than
# left off the page.
adrift = sorted(module_functions() - written)
if adrift:
    calls += ('<div class="g" style="color:#b4453a">exported, and not '
              'described anywhere on this page yet</div>')
    calls += "".join(
        f'<div class="dec"><code>{n}</code><div></div></div>' for n in adrift
    )
WHY = [
    ("the constructor judges nothing",
     "It converts and stops, so a wrong type sits on the record and is refused "
     "at the write. That leniency is bought deliberately: a row written last "
     "year has to rebuild through the same constructor."),
    ("a loaded row is judged by nothing at all",
     "Not even the converter — or a rule tightened since would make some of "
     "your history unreadable."),
    ("<code>@check</code> waits for a whole record",
     "A rule reaching across fields cannot be judged halfway through changing "
     "one, which is why assignment falls past it and <code>parse</code> and "
     "every write do not."),
    ("a leading underscore means nothing by itself",
     "What matters is whether you declared it. A declared "
     "<code>_field</code> is managed like any other; an undeclared one is "
     "your own and dray never looks at it."),
]
whys = "".join(f'<div class="w"><b>{t}</b><p>{b}</p></div>' for t, b in WHY)




PAGE = f"""<!doctype html>
<meta charset="utf-8"><title>dray — the reference</title>
<style>{CSS}</style>
<div class="wrap">

<h1>dray, at a glance</h1>
<p class="sub">The two things the page says across a dozen paragraphs and never
assembles: what the pieces are and how you get between them, and which door
runs what. One <code>Person</code> throughout.</p>
<nav>
  <a href="#pieces">the pieces</a>
  <a href="#offers">what each offers</a>
  <a href="#doors">every door, and what it runs</a>
  <a href="#tx">where the transaction is</a>
  <a href="#names">what one assignment does</a>
</nav>

<section class="part" id="pieces">
<h2>How you get from one to another</h2>
<p class="lead">A child is a record — it keeps everything a record has and gains
two columns naming its parent. What differs is the door: a child set is reached
through one record and every read it does is scoped to that record, where a
collection is reached through the store and is not.</p>
<div class="pair">
<pre class="mermaid shape">{SHAPE}</pre>
{walk_html()}
</div>
</section>

<section class="part" id="offers">
<h2>What each one offers</h2>
<div class="cols" style="margin-top: 1.2rem">
{card("a pool", "How an application gets a store. Retires connections before DSQL closes them at an hour.", GROUPS["Pool"], offered(Pool), "#8a7534", OWN["a pool"])}
{card("a store", "One connection and one thread, and the records it was told about.", GROUPS["Store"], offered(Store), "#5b6169", OWN["a store"])}
{card("a collection", "What <code>store.people</code> is, and what a <code>@collection</code> class inherits.", GROUPS["Collection"], offered(Collection), "#a8620a")}
{lent_card()}
{card("a child set", "What <code>person.notes</code> is. Reads are always scoped to this parent, which is the point of reaching children through one.", GROUPS["ChildSet"], offered(ChildSet), "#1a7f5a", OWN["a child set"])}
{card("a write", "What a <code>@before_save</code> is handed as well as the record, and what an <code>on_add</code>, an <code>on_save</code> or a <code>derived</code> handler is given.", GROUPS["Write"], carried(Write), "#7d3cb5", OWN["a write"])}
</div>
<div class="decs">
<h3>Declaring it</h3>
<div class="panel pair2">
  <div>{decs}</div>
  <pre class="ex">{code(DECLARING)}</pre>
</div>
<h3>Markers you put on a method</h3>
<div class="panel">{marks}</div>
<h3>Functions, and where each one goes</h3>
<div class="panel">{calls}</div>
</div>
</section>

<section class="part" id="doors">
<h2>Every door, and what it runs</h2>
<p class="lead">A column is one door and reads downward as the path it takes; a
row is one stage and reads across as which doors bother with it. Read
<code>@check</code> across: it runs where a whole record arrives from outside
and at every write, and nowhere else — a rule reaching across fields cannot be
judged halfway through changing one. The three middle rows are every way a
field can refuse a value; <code>precision=</code> is not a fourth, since no
door judges it — a too-precise number is carried untouched and rounded by the
column when the row lands, and only a number too large for the column is
refused, by the database rather than by dray. And a write runs neither the
converter nor <code>on_change</code>: it judges what is there and fills what it
owns.</p>
<table>
<thead>
<tr class="grp"><th></th>
  <th colspan="4">in — a record arrives, or a value is changed</th>
  <th colspan="2">out — a record is written</th>
  <th></th></tr>
<tr><th></th>{heads}<th class="dh"><span>in code</span></th></tr>
</thead>
<tbody>{"".join(rows)}</tbody>
</table>
<div class="key">
  <span><b style="color:#1a7f5a">●</b> runs</span>
  <span><b style="color:#c8ced4">○</b> deliberately skipped</span>
  <span><b style="color:#b8860b">◐</b> partly — see the row</span>
  <span>blank — nothing to do at this door</span>
</div>
</section>

<section class="part" id="tx">
<h2>Where the transaction is</h2>
<p class="lead">The boundary nothing above draws, and the one that decides what
a refusal takes back with it. A read opens none of its own; a write opens one
and replays it whole when DSQL refuses the commit. Open a block yourself and
every one of them joins it instead — which turns the replay off, and moves
<code>@check</code> inside, so the one thing it cannot normally undo it now
can.</p>
<table>
<thead><tr><th></th>
  <th class="dh"><span>on its own</span></th>
  <th class="dh"><span>inside a <code>store.transaction()</code> you opened</span></th>
</tr></thead>
<tbody>{tx_rows}</tbody>
</table>
</section>

<section class="part" id="names">
<h2>What one assignment line does</h2>
<p class="lead">The <code>record.field = value</code> column above is one cell
wide and hides six different endings. Which one you get depends on nothing but
what the name is — two are refusals that never reach a converter, and one is
not dray's business at all.</p>
<table><tbody>{names}</tbody></table>
<div class="ws">{whys}</div>
</section>

</div>
<script type="module">
  import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
  mermaid.initialize({{
    startOnLoad: true, theme: "neutral",
    themeVariables: {{ fontSize: "12.5px", fontFamily:
      "ui-sans-serif, system-ui, -apple-system, sans-serif" }},
    flowchart: {{ curve: "basis", nodeSpacing: 26, rankSpacing: 40,
                  padding: 8 }}
  }});
</script>
"""

# Behind `__main__` so the maps above can be imported and checked. At module
# scope, importing this file to read `CALLS` rewrote `docs/reference.html` as a
# side effect, which made the check that guards the page a reason to distrust
# it.
if __name__ == "__main__":
    out = (pathlib.Path(__file__).resolve().parent.parent
           / "docs" / "reference.html")
    out.write_text(PAGE)
    print("wrote", out)
