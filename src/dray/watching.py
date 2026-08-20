"""
What dray is doing, while it is doing it.

An observer is a callable handed spans, and what happens next is entirely the
caller's. dray owns no levels, no formatters, no destination and no redaction
policy — Python has `logging` and every application configures it differently.
It is the same split `after_commit` makes: the moment, and not the mechanism.

Nothing in here runs unless somebody asked for it. A store nobody is watching
holds `UNWATCHED`, one preallocated object whose every method does nothing and
returns itself, so an unwatched read takes no timestamp, builds no span and
touches no stack. That promise is why the call sites read as though they were
always watched.
"""

import itertools
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter_ns, time_ns
from typing import Any

# Every kind dray opens. Written out so a collector can be told it is filtering
# on a word that exists, rather than coming back empty because somebody typed
# `statements`.
KINDS = (
    "checkout",
    "connect",
    "caller",
    "transaction",
    "statement",
    "execute",
    "hydrate",
    "cache",
    "returning",
    "prepare",
)

# Process-wide and never reset. `next` on one of these is atomic in CPython, so
# two threads cannot be handed the same number, and a span only ever has to be
# unique within the process that emitted it.
_ids = itertools.count(1)

# Whether this thread is already inside a handler. Thread-local rather than one
# flag per store, because the re-entry worth catching is a handler that queries
# a *different* store: one store's flag cannot see it, and what it costs is
# unbounded recursion that presents as a hang rather than as an error.
_firing = threading.local()


@dataclass(frozen=True, slots=True)
class Span:
    """
    One thing dray did, or is about to.

    A span arrives twice — once when it opens and once when it closes — so a
    consumer can draw a tree top-down as it happens rather than assembling one
    at the end. `phase` says which of the two this is, and everything measured
    is on the close: an open event knows only where it sits.

    `kind` is a fact rather than something to subscribe to. There are ten of
    them, listed in `KINDS`, and filtering is your own `if` — a subscription
    taxonomy would mean dray owning the cuts, and somebody always wants one it
    did not think of.

    Two clocks, and they are not interchangeable. `at_ns` is `perf_counter_ns`:
    monotonic, one clock across the whole process, which is what lets spans
    from different threads sit on a single axis, and the only one `elapsed_ns`
    is ever derived from. `wall` is `time_ns`, which steps under NTP and is
    therefore no use for a duration — it is there to line a span up against a
    request log or the DSQL console. Nanoseconds as integers, because float
    seconds lose precision exactly where a 200µs statement lives.

    `parent_id` is an id and not a reference, because events get serialised,
    buffered and shipped, and an object reference survives none of that while
    pinning every parent alive. `None` marks a root. A parent is always from
    the same thread as its child; work that fanned out is several roots laid
    beside each other on the clock rather than one tree.

    `sql` and `params` are as sent, `%s` and all: dray cannot know which column
    is a medical note, so it hands over what it already gave the driver and
    where that goes is your decision.

    `attempt` is the one nothing else carries. dray replays a commit DSQL
    refused, and it is on the `transaction` close rather than on any statement
    because the replay owns the transaction. Without it a write refused twice
    reads as three unrelated statements and the thing that actually happened is
    invisible in the only place anybody would look.
    """

    id: int
    parent_id: int | None
    depth: int
    kind: str
    phase: str
    at_ns: int
    wall: int
    thread_ident: int
    thread_name: str
    label: str | None = None
    elapsed_ns: int | None = None
    sql: str | None = None
    params: Any = None
    cls: type | None = None
    rowcount: int | None = None
    attempt: int | None = None
    error: BaseException | None = None


class _Opened:
    """
    A span that has started, and the facts gathered about it since.

    Not a `Span`, because a `Span` is frozen and shipped and this is a
    scratchpad: the body of the `with` writes `rowcount`, `error` and the rest
    onto it as they become known, and only when it closes is any of that turned
    into the second event.
    """

    __slots__ = (
        "id",
        "parent_id",
        "depth",
        "kind",
        "label",
        "at_ns",
        "wall",
        "thread_ident",
        "thread_name",
        "sql",
        "params",
        "cls",
        "rowcount",
        "attempt",
        "error",
    )

    def __init__(
        self,
        parent_id: int | None,
        depth: int,
        kind: str,
        label: str | None,
        cls: type | None,
        ago: int = 0,
    ) -> None:
        self.id = next(_ids)
        self.parent_id = parent_id
        self.depth = depth
        self.kind = kind
        self.label = label
        self.cls = cls
        self.sql = None
        self.params = None
        self.rowcount = None
        self.attempt = None
        self.error = None
        thread = threading.current_thread()
        self.thread_ident = thread.ident or 0
        self.thread_name = thread.name
        # Last, so the two clocks are read as close as possible to the work
        # starting rather than to the bookkeeping above.
        #
        # `ago` backdates both by the same amount, for work that was already
        # under way by the time anything knew there was a span to open. A read
        # answered out of memory is the case: whether it was a hit is not known
        # until the map has answered, and a caller that waited on somebody
        # else's round trip spent that wait inside the hit. Both clocks move
        # together, so `wall` still lines up with a request log.
        self.wall = time_ns() - ago
        self.at_ns = perf_counter_ns() - ago

    def began(self) -> Span:
        return Span(
            id=self.id,
            parent_id=self.parent_id,
            depth=self.depth,
            kind=self.kind,
            phase="open",
            at_ns=self.at_ns,
            wall=self.wall,
            thread_ident=self.thread_ident,
            thread_name=self.thread_name,
            label=self.label,
        )

    def ended(self) -> Span:
        at = perf_counter_ns()
        return Span(
            id=self.id,
            parent_id=self.parent_id,
            depth=self.depth,
            kind=self.kind,
            phase="close",
            at_ns=at,
            wall=time_ns(),
            thread_ident=self.thread_ident,
            thread_name=self.thread_name,
            label=self.label,
            elapsed_ns=at - self.at_ns,
            sql=self.sql,
            params=self.params,
            cls=self.cls,
            rowcount=self.rowcount,
            attempt=self.attempt,
            error=self.error,
        )


class Watch:
    """
    The handlers one store emits to, and the parent stack underneath them.

    One of these per store, so the nesting is per store — which is also per
    connection and per thread, and is what keeps a `parent_id` from ever
    pointing at a span another thread opened. The stack is thread-local rather
    than shared, because a store with no block open may legitimately be read
    from several threads at once and a shared list would hand one thread's
    statement the other's parent.
    """

    def __init__(
        self,
        observer: Callable[[Span], None] | None = None,
        collectors: Sequence[Callable[[Span], None]] = (),
    ) -> None:
        self.observer = observer
        self.collectors = tuple(collectors)
        self._local = threading.local()
        self._settle()

    @classmethod
    def of(
        cls,
        observer: Callable[[Span], None] | None = None,
        collectors: Sequence[Callable[[Span], None]] = (),
    ) -> "Watch | _Unwatched":
        """A watch, or the one that is not there when nobody asked."""
        if observer is None and not collectors:
            return UNWATCHED
        return cls(observer, collectors)

    def _settle(self) -> None:
        # The handlers as one tuple, rebuilt when the set changes rather than
        # per emission: a span is emitted twice and there are several per
        # statement, so this is the loop that would pay for it.
        self._handlers = (
            self.collectors
            if self.observer is None
            else (self.observer, *self.collectors)
        )

    def add(self, collector: Callable[[Span], None]) -> None:
        self.collectors = (*self.collectors, collector)
        self._settle()

    def drop(self, collector: Callable[[Span], None]) -> None:
        self.collectors = tuple(
            one for one in self.collectors if one is not collector
        )
        self._settle()

    def __bool__(self) -> bool:
        return bool(self._handlers)

    @property
    def _stack(self) -> list[int]:
        stack = getattr(self._local, "stack", None)
        if stack is None:
            stack = self._local.stack = []
        return stack

    def opened(
        self,
        kind: str,
        *,
        label: str | None = None,
        cls: type | None = None,
        ago: int = 0,
    ) -> _Opened:
        """Start a span, tell everybody, and make it the parent of whatever
        opens next on this thread.

        `ago` says the work began this many nanoseconds before now, for a span
        nothing could know to open until it was already running."""
        stack = self._stack
        span = _Opened(
            stack[-1] if stack else None, len(stack), kind, label, cls, ago
        )
        # Emitted before it is pushed, so a handler that raises — the re-entry
        # guard below is one — leaves no half-open span behind for a `close`
        # that is never coming.
        self._emit(span.began())
        stack.append(span.id)
        return span

    def closed(self, span: _Opened) -> None:
        """Finish a span and tell everybody what it cost."""
        stack = self._stack
        if stack and stack[-1] == span.id:
            stack.pop()
        elif span.id in stack:
            # Everything closes in order through a `with`, so this is only ever
            # reached if something above went wrong; truncating rather than
            # removing keeps the stack a stack.
            del stack[stack.index(span.id) :]
        self._emit(span.ended())

    @contextmanager
    def span(
        self,
        kind: str,
        *,
        label: str | None = None,
        cls: type | None = None,
        ago: int = 0,
    ) -> Iterator[Any]:
        """
        A span around this block, closed however the block leaves.

        What it yields is the scratchpad: set `rowcount`, `cls` or anything
        else on it and the close event carries it. Whatever the block raises is
        recorded and re-raised, because a statement that failed is the one
        somebody debugging most wants to see.

        `ago` backdates the start, for a block that is only part of what it is
        timing — see `opened`.
        """
        opened = self.opened(kind, label=label, cls=cls, ago=ago)
        try:
            yield opened
        except BaseException as error:
            opened.error = error
            raise
        finally:
            self.closed(opened)

    def cursor(self, cur: Any, cls: type | None = None) -> "_Watched":
        return _Watched(cur, self, cls)

    def _emit(self, span: Span) -> None:
        handlers = self._handlers
        if not handlers:
            return
        if getattr(_firing, "on", False):
            raise RuntimeError(
                f"an observer handler asked dray for something, and dray was "
                f"already inside that handler. Watching the query a handler "
                f"makes would call the handler again, without end. Take the "
                f"reading you need off the span you were handed, or query from "
                f"a store nothing is watching. The handler is {handlers[0]!r}."
            )
        _firing.on = True
        try:
            for handler in handlers:
                handler(span)
        finally:
            _firing.on = False


class _Unwatched:
    """
    The watch that is not there.

    One object, shared by every unwatched store, and every method on it returns
    something the call sites can go on using: it is its own context manager, its
    own scratchpad, and it hands back the driver's cursor untouched. So a read
    on a store nobody is watching costs a method call and nothing else — no
    clock read, no stack, no object built — which is the promise the page makes
    and the reason it can be made.
    """

    __slots__ = ()

    def span(self, kind: str, **facts: Any) -> "_Unwatched":
        return self

    def opened(self, kind: str, **facts: Any) -> "_Unwatched":
        return self

    def closed(self, span: Any) -> None:
        pass

    def cursor(self, cur: Any, cls: type | None = None) -> Any:
        return cur

    def __enter__(self) -> "_Unwatched":
        return self

    def __exit__(self, *dead: Any) -> bool:
        return False

    def __setattr__(self, name: str, value: Any) -> None:
        # `span.rowcount = ...` in a call site that is not being watched. It
        # goes nowhere, and that is cheaper than the call site asking first.
        pass

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "UNWATCHED"


UNWATCHED = _Unwatched()


class _Watched:
    """
    A cursor that says what went down it.

    One statement span at a time, because a cursor has one statement at a time:
    the next `execute` closes the last one and so does leaving the `with`. That
    is what makes the nesting free — anything opened while a statement is on
    top of the stack is its child, so `hydrate` and `returning` land underneath
    without a single call site having to say so.

    Everything else is the driver's cursor. `description`, `rowcount`,
    `fetchall` and the rest go straight through, so this is a cursor everywhere
    dray already treats one as a cursor.
    """

    def __init__(self, cur: Any, watch: Watch, cls: type | None = None) -> None:
        self._cur = cur
        self._watch = watch
        self._cls = cls
        self._span: _Opened | None = None

    def execute(self, statement: Any, params: Any = None, **rest: Any) -> Any:
        self.finish()
        span = self._watch.opened("statement", cls=self._cls)
        span.sql = statement
        span.params = params
        try:
            # The round trip on its own, inside the statement that is still
            # open: the difference between the two is dray's time, which is the
            # whole reason `hydrate` is worth having.
            with self._watch.span("execute", cls=self._cls):
                self._cur.execute(statement, params, **rest)
        except BaseException as error:
            span.error = error
            self._watch.closed(span)
            raise
        # Read now rather than at close, because the next statement on this
        # cursor overwrites it.
        span.rowcount = self._cur.rowcount
        self._span = span
        return self

    def finish(self) -> None:
        """Close the statement this cursor is holding open, if it has one."""
        span, self._span = self._span, None
        if span is not None:
            self._watch.closed(span)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)

    def __iter__(self) -> Any:
        return iter(self._cur)


class Seen:
    """
    What a `with store.watching()` block caught, in the order it closed.

    A sequence of closed spans — `len(seen)`, `seen[0]`, and iteration — and by
    default only the statements, because the question this exists for is *did
    this page do one read or six*. Pass `kind=None` to `watching()` for every
    span, or another kind to count something else.

    Safe to append to from several threads, which is the whole reason it ships
    rather than being five lines everybody writes: a pool hands out a store per
    thread and they all report to the one collector.
    """

    def __init__(self, kind: str | None = "statement") -> None:
        self.kind = kind
        self._lock = threading.Lock()
        self._spans: list[Span] = []

    def __call__(self, span: Span) -> None:
        # Closes only. An open event has no elapsed and nothing to count, and
        # counting both would make every answer twice what anybody expected.
        if span.phase != "close":
            return
        if self.kind is not None and span.kind != self.kind:
            return
        with self._lock:
            self._spans.append(span)

    def __len__(self) -> int:
        return len(self._spans)

    def __getitem__(self, which: Any) -> Any:
        return self._spans[which]

    def __iter__(self) -> Iterator[Span]:
        # Over a copy, since a fan-out is still appending while a caller reads.
        return iter(list(self._spans))

    def __repr__(self) -> str:
        of = "spans" if self.kind is None else self.kind
        return f"<Seen: {len(self._spans)} {of}>"
