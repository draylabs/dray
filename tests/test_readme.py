"""
The README's examples, executed exactly as printed.

They are the first code anybody runs, and they were wrong: the opening example
declared a record and then wrote one without ever creating the table, so a
reader following it got `UndefinedTable` on line four. It had been checked by
pasting the *declarations* into a test that made the tables itself, which is
the one arrangement where the omission cannot show.

So this runs the blocks verbatim instead — no edits, no harness around them —
and the only thing standing in for the world is a cluster that is not there.
"""

import pathlib

import pytest

import dray

README = pathlib.Path(__file__).resolve().parent.parent / "README.md"
BLOCKS = [b.split("```")[0] for b in README.read_text().split("```python")[1:]]


@pytest.mark.parametrize("n", range(len(BLOCKS)))
def test_a_readme_example_runs_exactly_as_it_is_printed(n, store, monkeypatch):
    """Every fenced `python` block on the page, run as written.

    `Store.connect` is the one line that cannot work here — it authenticates
    against a cluster — so it is pointed at the test's own store and nothing
    else is touched. A block that needs a name the page does not define, or a
    table the page never creates, fails here rather than on somebody's first
    afternoon.
    """
    monkeypatch.setattr(
        dray.Store, "connect", classmethod(lambda cls, **kw: store)
    )
    exec(compile(BLOCKS[n], f"<README block {n + 1}>", "exec"), {})
