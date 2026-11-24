# shipper — ordered, credit-checked delivery to fixed statistics groups

A small, dependency-free Python library that moves a strictly ordered stream of
records from a source to a set of pre-declared **groups**, under a shared byte
budget, with at-most-once hand-off and re-connectable sessions.

Everything is local and in-process: the *source*, the *receiver* and the
*delivery program* are all provided here, and the tests drive them together.

## The model

```
   source stream  ──►  session  ──►  group A  (accepts "a", "c")
   (strictly ordered)   │  │   └──►  group B  (accepts "b", "c")
                        │  └──────►  group C  (accepts "a")
                        │
                  shared quota B  (bytes pulled but not yet settled count!)
```

* **Source** — strictly ordered, non-empty records of at most `M` bytes.  The
  source may *announce* the next record (category and length) **without
  consuming it**; the delivery program may ignore the announcement, or skip a
  record nobody receives.
* **Groups** — each declares, up front, which categories it receives.  A group
  is only ever shown the *oldest record it has not yet acknowledged*.
* **Acknowledgement** — by a **non-reusable ticket number**.  A wrong number is
  refused and changes nothing, so a record can never be skipped.  Retrying the
  ticket of an already-settled record has no effect.
* **Exit** — a group may leave; it abandons everything it still owed and every
  future record, releasing those bytes, and can never rejoin.
* **Byte quota `B`** — shared by all groups, `B >= M`.  A record's bytes are
  charged when pulled (including bytes pulled but not yet handed off) and
  released exactly once, when the last group owing them settles or leaves.
  **Nothing is pulled when it would not fit**; acknowledging or leaving makes
  room and pulling resumes.
* **End** — a normal end of stream and a source-contract violation are
  distinct outcomes.  Sources are independent, so a fault on one does not hide
  records already valid on the others: a group keeps draining every healthy
  source and only sees the error once nothing can deliver to it any more.  In
  either case records already staged stay usable.

## Using it

```python
from shipper import MockSource, Session, uniform_specs

src = MockSource.uniform(10_000, "abc", min_len=1, max_len=64, seed=1)
sess = Session(src.link(),
               {"a": ["A", "C"], "b": ["B"], "c": ["A", "B"]},
               max_len=64, quota=256)

a = sess.group("A")
rec = a.fetch()                     # the head; never confirms it
print(rec.category, rec.length, rec.ticket)
a.confirm(rec.ticket)               # only this advances A

print(sess.announce())              # (category, length, source) — non-consuming
print(sess.finish())                # "end" | "source_error" | "pending"
```

A simple consumer loop that services every group (this is how the fixtures are
driven, and how a real deployment would run):

```python
handles = {n: sess.group(n) for n in sess.groups()}
while True:
    progressed = False
    for h in handles.values():
        rec = h.fetch(block=False)      # None = nothing right now
        if rec in (None, False):
            continue
        h.confirm(rec.ticket)
        progressed = True
    if not progressed and sess.finish() in ("end", "source_error"):
        break
```

`fetch(block=True, timeout=...)` waits and wakes on any release (an
acknowledgement, an exit, a reconnect), so no polling is needed.

## Guarantees, and where they are tested

| Requirement | Test file |
|---|---|
| Strict order, each record pulled once, delivered once | `test_basic.py` |
| Groups see only their categories and only their head | `test_basic.py` |
| Acknowledgement by non-reusable number; wrong number refused; retry harmless | `test_basic.py`, `test_reconnect.py` |
| Exit abandons this group only; no re-entry | `test_basic.py` |
| `B >= M`; no pull without room; acks/exits release and resume | `test_credit.py` |
| Bytes pulled but not handed off are billed | `test_credit.py` |
| Slow group never blocks another's read / ack / exit | `test_credit.py` |
| Released payload keeps no bytes; indexes stay bounded | `test_credit.py` |
| Announcement is non-consuming; a lie is a source error | `test_source.py` |
| Normal end vs source error distinguished; earlier records survive | `test_source.py` |
| Reconnect mid-session without replay or loss | `test_reconnect.py` |
| Parallel runs equivalent to a sequential order | `test_reconnect.py`, `test_random.py` |
| Streams far larger than memory | `test_credit.py` (`50k` records, `B=M`ish) |

## Running the tests

```bash
python3 -m unittest discover -s tests -t tests -v
```

No third-party packages are required.  (`PYTHONPATH=.` if you run a single test
module directly from the repository root.)

## A note on the two "copy" readings

The brief says *"each record that is still owed must keep at least one copy;
extra copies are also billed, and no already-released payload may be retained."*
This implementation takes the reading that makes the accounting meaningful:
**one record is one billed unit**, shared by reference among every group that
receives it.  There is never a second physical copy of the bytes.  The ledger
charge is released only when the *last* group owing that record settles or
leaves, so a payload is not freed while any group could still need it, and is
never retained after the last claim is gone.
