#!/usr/bin/env python3
"""Local, in-process demo of the event rollup.

    python3 demo_events.py [--dir D]

It writes a small feed of JSON events to a temporary on-disk source, opens one
event-mode session (300-second windows) and walks through four things:

1. window boundaries -- t=299 lands in window 0, t=300 in window 1;
2. out-of-order event times -- a later source sequence can carry an earlier
   ``event_time`` and still lands in the window its timestamp belongs to;
3. group differences -- overlapping categories give each statistics group its
   own totals, and only records that group confirmed are counted;
4. invalid payloads -- malformed JSON and missing/typed-wrong fields fail the
   confirmation explicitly, leave the record unconfirmed and never touch the
   totals.

Everything runs in one process here; ``demo_events_persist.py`` shows the same
aggregates surviving real process restarts.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile

from shipper import EventError, FeedWriter, FileSource, Session, window_bounds

WIDTH = 300
CATEGORIES = {"heat": ["A", "C"], "load": ["B"], "shared": ["A", "B"]}

# (seq, category, event_time, metric, value) -- note the deliberately
# shuffled event_times: source order (seq) decides delivery order only.
EVENTS = [
    (1, "heat", 10, "clicks", 10),       # A,C w0
    (2, "load", 299, "cpu", 5),          # B   w0 -- 299 is still in w0
    (3, "heat", 300, "clicks", 7),       # A,C w1 -- 300 opens a new window
    (4, "shared", 610, "hits", 3),       # A,B w2
    (5, "shared", 599, "hits", 4),       # A,B w1 -- earlier time, later seq
    (6, "heat", -1, "clicks", 2),        # A,C w-1 (negative UTC second)
]

BAD_PAYLOADS = [
    ("malformed JSON", b"{not json"),
    ("missing event_time",
     json.dumps({"metric": "m", "value": 1}).encode()),
    ("extra field",
     json.dumps({"event_time": 1, "metric": "m", "value": 1,
                 "extra": 2}).encode()),
    ("value is a string",
     json.dumps({"event_time": 1, "metric": "m", "value": "1"}).encode()),
    ("event_time is a float",
     json.dumps({"event_time": 1.5, "metric": "m", "value": 1}).encode()),
    ("boolean is not an integer",
     json.dumps({"event_time": True, "metric": "m", "value": 1}).encode()),
]


def event_payload(t, metric, value):
    return json.dumps({"event_time": t, "metric": metric,
                       "value": value}).encode("utf-8")


def write_feed(path, records):
    max_len = max(len(data) for _, _, data in records)
    with FeedWriter(path, max_len) as w:
        for seq, category, data in records:
            w.append(seq, category, data)
    return max_len


def print_rows(title, handle):
    print(title)
    rows = handle.event_windows()
    if not rows:
        print("  (no confirmed events)")
        return
    for r in rows:
        start, end = window_bounds(r.window, WIDTH)
        print("  w=%-3d [%6d,%6d) category=%-6s metric=%-7s "
              "sum=%4d events=%d"
              % (r.window, start, end, r.category, r.metric, r.total,
                 r.events))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=None,
                    help="working directory (a temp dir by default)")
    args = ap.parse_args()
    base = args.dir or tempfile.mkdtemp(prefix="shipper-events-demo-")
    os.makedirs(base, exist_ok=True)

    records = [(seq, cat, event_payload(t, metric, value))
               for seq, cat, t, metric, value in EVENTS]
    feed = os.path.join(base, "events.bin")
    max_len = write_feed(feed, records)

    src = FileSource(feed)
    sess = Session(src.link(), CATEGORIES, max_len, max_len * 4,
                   event_window=WIDTH)
    handles = {n: sess.group(n) for n in sess.groups()}

    print("== 1. window boundaries and 2. out-of-order event times ==")
    print("source order is seq order; event_time may go backwards "
          "(records #4/#5)\n")
    # Service in strict source order: every group takes its head as it appears.
    while True:
        progressed = False
        for name, handle in handles.items():
            rec = handle.fetch(block=False)
            if rec in (None, False):
                continue
            handle.confirm(rec.ticket)
            progressed = True
        if not progressed and sess.finish() in ("end", "source_error"):
            break
    for name in sorted(handles):
        print_rows("group %s:" % name, handles[name])
        print("  -> total value confirmed: %d over %d event(s)\n"
              % (handles[name].event_total(),
                 handles[name].event_count()))

    print("== 3. group differences for the same source ==")
    a, b, c = handles["A"], handles["B"], handles["C"]
    expectations = {
        "A": (26, 5),   # heat 10+7+2 + shared 3+4
        "B": (12, 3),   # load 5 + shared 3+4
        "C": (19, 3),   # heat 10+7+2 only
    }
    for name, (total, count) in expectations.items():
        got = (handles[name].event_total(), handles[name].event_count())
        print("  group %s: total=%d events=%d %s"
              % (name, got[0], got[1],
                 "OK" if got == (total, count) else "MISMATCH"))
        assert got == (total, count)
    print("  shared records are billed once but counted separately by each "
          "group that confirms them.\n")

    print("== 4. invalid payloads fail explicitly and never count ==")
    # Each bad payload gets its own small session so the unconfirmable head of
    # one never blocks the demonstration of the next.
    for i, (label, data) in enumerate(BAD_PAYLOADS):
        one = os.path.join(base, "bad-%d.bin" % i)
        write_feed(one, [(1, "heat", data)])
        s = FileSource(one)
        ss = Session(s.link(), {"heat": ["Q"]}, len(data) + 4,
                     (len(data) + 4) * 4, event_window=WIDTH)
        h = ss.group("Q")
        rec = h.fetch()
        assert rec.data == data
        try:
            h.confirm(rec.ticket)
        except EventError as exc:
            print("  %-26s -> EventError: %s" % (label, exc))
        else:
            raise AssertionError("a bad payload was confirmed: %r" % label)
        # The head, its ticket and the empty totals are all unchanged.
        assert h.peek().ticket == rec.ticket
        assert h.event_count() == 0 and h.event_total() == 0
        s.close()
    print("\nall %d invalid payloads were refused; no aggregate was touched."
          % len(BAD_PAYLOADS))
    print("stats snapshot:", json.dumps(sess.stats()["events"],
                                        sort_keys=True))

    src.close()
    print("\nOK: windows align on 300s boundaries, out-of-order times land in "
          "their own window,")
    print("each group totals only its own confirmed records, and bad payloads "
          "fail loudly.")


if __name__ == "__main__":
    main()
