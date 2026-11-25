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

## Persistent jobs: recovery points that survive the process

A **job** binds one session to a directory on local disk.  The
configuration and the source identities are fixed when the job is created;
`save()` commits a *recovery point* — one consistent cut, taken under the
session lock, of source positions, per-group acknowledgement progress,
unconfirmed payloads, ticket numbers, the byte ledger and the end/error
state.  After the process exits, opening the job resumes from the last
committed point: confirmed records are not delivered again, records still
owed stay owed to the same group with the same tickets, and pulling
continues from exactly the saved position.

```python
from shipper import FeedWriter, FileSource, JobStore

# init: write a feed once, then create the job (fixes config + sources)
with FeedWriter("feed.bin", max_len=64) as w:
    w.append(1, "a", b"...")          # strictly ordered, 1..M bytes each
job = JobStore("jobs/").create("nightly", FileSource("feed.bin"),
                               {"a": ["A", "C"], "b": ["B"]},
                               max_len=64, quota=256)

# work: job.session is an ordinary Session — everything above applies
rec = job.session.group("A").fetch()
job.session.group("A").confirm(rec.ticket)
job.save()                            # commit a recovery point
job.close()                           # ... process exits ...

# later, in another process: resume from the last committed point
job = JobStore("jobs/").open("nightly", FileSource("feed.bin"))
```

* **Visibility** — a recovery point exists only once it is complete: the
  file is staged, fsynced and renamed into place, and only then is `HEAD`
  advanced.  A crash mid-save leaves the previously committed point
  untouched and the new one invisible.
* **Binding** — a point loads only with the job's own configuration and the
  very sources it was created with (content-bound identity tokens plus a
  configuration fingerprint).  Another source or another configuration is
  refused loudly, never silently adopted.
* **Explicit failure** — loading validates magic, length, a SHA-256 of the
  body, the fingerprint, the identities and the state's internal
  consistency.  Damage, incompatibility or an incomplete file raises and
  yields no session at all, so a half-restored session can never be used;
  earlier committed points stay openable
  (`store.open(name, src, checkpoint=2)`).
* **Scale** — `FileSource` reads one record at a time from disk, so the
  stream can far exceed memory; a recovery point itself holds only the
  unconfirmed payloads, which the quota already bounds.
* One live handle per job at a time (an advisory file lock), so two
  processes cannot resume — and doubly deliver — the same job.

`MockSource` is resumable too (`identity()` / `open(position)`), which is
what the tests use; `FileSource` is what crosses real process boundaries.

Run the demo — every step below is a separate process:

```bash
python3 demo_persist.py init --records 30000
python3 demo_persist.py work --count 9000      # commit, exit
python3 demo_persist.py work --count 9000      # resume, commit, exit
python3 demo_persist.py finish                 # drain + verify exactly-once
python3 demo_persist.py wrong-source           # a foreign feed is refused
```

## Event rollups: recoverable time-window summaries

So far each statistics group could only handle one supply record at a time.
A session created with `event_window=W` instead interprets every record's
payload as one JSON **event**

```json
{"event_time": 1700000123, "metric": "clicks", "value": 42}
```

and keeps, per group, the running sum of `value` over fixed, left-closed /
right-open windows of `W` UTC seconds (the demo and tests use 300).  Window
`w` covers `[w·W, (w+1)·W)`, so `event_time=299` falls in window 0 and
`event_time=300` in window 1; event times may arrive out of source order and
still land in the window their timestamp belongs to.

* **Only confirmed records count, per group.**  An event is folded into a
  group's rollup at the moment *that group* confirms the record; fetching
  never counts, another group confirming a shared record does not count it for
  this one, and re-confirming an old ticket never double-counts.
* **Bad payloads fail loudly and cannot be confirmed.**  Malformed JSON, a
  non-object document, duplicate keys, a missing/extra field, or a field of
  the wrong type (booleans and floats are not accepted as integers) raises
  `EventError` *before* anything is settled: the ticket stays live and no
  total changes.  The group can keep the record for inspection, or `exit()` to
  abandon it.
* **The summary, not the events, is what is stored.**  `EventRollup` holds one
  running `(sum, count)` per `(window, category, metric)` key and forgets each
  event once folded, so memory grows with the number of distinct keys — never
  with the number of source records.  A stream far larger than memory is
  summarised without caching its raw events.
* **It is part of the recovery point.**  The rollup is cut under the session
  lock inside every `save()` and validated on open, including a cross-check
  that each group's folded-event count equals its confirmed-record count.
  After a restart the totals are exactly those of the group's confirmed
  records, and a damaged or forged aggregate is refused like any other
  corrupt checkpoint.

```python
sess = Session(link, {"heat": ["A", "C"], "load": ["B"]},
               max_len=64, quota=256, event_window=300)
a = sess.group("A")
rec = a.fetch()
a.confirm(rec.ticket)          # parses the JSON event, then folds it in
for row in a.event_windows():  # sorted by window, then category, then metric
    print(row.window, row.category, row.metric, row.total, row.events)
a.event_windows(category="heat", metric="clicks")   # optional filters
a.event_total()                 # summed value over all confirmed events
a.event_count()                 # how many events were folded
```

The same option is fixed in a persistent job's configuration and fingerprint:

```python
job = JobStore("jobs/").create("nightly", FileSource("feed.bin"),
                               categories, 64, 256, event_window=300)
```

Run the event demos — the first in one process, the second as four:

```bash
python3 demo_events.py
python3 demo_events_persist.py --dir demo-events run-all   # init, work, work, verify
```

`demo_events_persist.py verify` recomputes the expected windows independently
by scanning the feed file itself, and asserts the recovered rollup equals that
per-group recomputation after the process restarts.


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
| Recovery points resume exactly; confirmed never replays, owed is kept | `test_job.py` |
| Points bind to their config and sources; foreign ones refused | `test_job.py` |
| Crashed saves invisible; damaged points fail; earlier points open | `test_job.py` |
| Feed files: O(1) resume, one record in memory at a time | `test_job.py` |
| Strict event parsing; bad/missing/mistyped payloads cannot confirm | `test_events.py` |
| 300s left-closed/right-open windows; out-of-order times land correctly | `test_events.py` |
| Only per-group confirmed events count; retries don't double-count | `test_events.py` |
| Per-group/category/metric sums differ correctly across groups | `test_events.py` |
| Rollup resumes with the job and matches confirmed records; forgery refused | `test_events.py` |
| Aggregate memory bounded by distinct keys, not by event count | `test_events.py` |

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
