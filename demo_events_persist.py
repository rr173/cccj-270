#!/usr/bin/env python3
"""Persistent event-rollup demo: the aggregates survive the process.

    python3 demo_events_persist.py init   [--dir D]
    python3 demo_events_persist.py work   [--dir D] [--count K]
    python3 demo_events_persist.py work   [--dir D] [--count K]   # new process
    python3 demo_events_persist.py verify [--dir D]
    python3 demo_events_persist.py run-all [--dir D]              # all of the above

Every ``work`` invocation is a separate process: it opens the persistent job,
confirms about K records, commits a recovery point containing the rollup and
exits.  The next process resumes from that point -- confirmed events are not
folded again, and the per-group window/category/metric totals continue exactly.
``verify`` drains whatever remains and recomputes the expected totals
independently, by scanning the feed file itself, then asserts the recovered
rollup equals the rollup of every per-group confirmed record.

The feed also contains two malformed records.  They are delivered to a group
that has no healthy tail to block, shown to fail confirmation explicitly, and
proved never to enter any aggregate.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

from shipper import (EventError, FeedWriter, FileSource, JobStore,
                     window_bounds, window_of)

WIDTH = 300
JOB = "events"
CATEGORIES = {"heat": ["A", "C"], "load": ["B"], "shared": ["A", "B"],
              "junk": ["Q"], "trash": ["R"]}

# (seq, category, event_time, metric, value); event_times deliberately shuffled.
EVENTS = [
    (1, "heat", 10, "clicks", 10),       # A,C w0
    (2, "load", 299, "cpu", 5),          # B   w0 -- 299 stays in w0
    (3, "heat", 300, "clicks", 7),       # A,C w1 -- 300 opens w1
    (4, "shared", 610, "hits", 3),       # A,B w2
    (5, "shared", 599, "hits", 4),       # A,B w1 -- earlier time, later seq
    (6, "heat", -1, "clicks", 2),        # A,C w-1
    (7, "junk", 0, "ignored", 1),        # Q only: malformed, below
    (8, "heat", 900, "clicks", 6),       # A,C w3
    (9, "load", 899, "cpu", 8),          # B   w2 -- 899 still in w2
    (10, "shared", 300, "hits", 9),      # A,B w1
    (11, "trash", 0, "ignored", 1),      # R only: malformed, below
    (12, "heat", 1199, "clicks", 1),     # A,C w3 -- 1199 still in w3
]
BAD = {
    7: b"{not json",
    11: json.dumps({"event_time": 1, "metric": "m"}).encode(),  # no value
}


def _paths(d):
    return {"feed": os.path.join(d, "events.bin"),
            "jobs": os.path.join(d, "jobs"),
            "meta": os.path.join(d, "demo.json"),
            "rejected": os.path.join(d, "rejected.json")}


def _payload(seq, t, metric, value):
    return json.dumps({"event_time": t, "metric": metric,
                       "value": value}).encode("utf-8")


def cmd_init(args):
    p = _paths(args.dir)
    os.makedirs(args.dir, exist_ok=True)
    records = []
    for seq, cat, t, metric, value in EVENTS:
        data = BAD.get(seq, _payload(seq, t, metric, value))
        records.append((seq, cat, data))
    max_len = max(len(data) for _, _, data in records)
    with FeedWriter(p["feed"], max_len) as w:
        for seq, cat, data in records:
            w.append(seq, cat, data)
    quota = max_len * 4
    src = FileSource(p["feed"])
    job = JobStore(p["jobs"]).create(JOB, src, CATEGORIES, max_len, quota,
                                     event_window=WIDTH)
    with open(p["meta"], "w") as f:
        json.dump({"max_len": max_len, "quota": quota, "width": WIDTH}, f)
    print("feed            :", p["feed"], "(%d records)" % len(records))
    print("groups/categories:", CATEGORIES)
    print("window width    : %d seconds (left-closed/right-open)" % WIDTH)
    print("recovery point  : %d committed" % job.checkpoint_id)
    job.close()
    src.close()


def _service_some(sess, budget):
    """Confirm up to ``budget`` healthy records across all live groups.

    Returns ``(confirmed, bad)``; malformed heads raise and are skipped over by
    abandoning the group that owes them, which owes nothing else.  Groups that
    already left in an earlier process are simply absent.
    """
    handles = {}
    for n in sess.groups():
        try:
            handles[n] = sess.group(n)
        except Exception:
            pass
    confirmed, bad = [], []
    while len(confirmed) < budget:
        progressed = False
        for name in list(handles):
            handle = handles[name]
            rec = handle.fetch(block=False)
            if rec in (None, False):
                continue
            try:
                handle.confirm(rec.ticket)
            except EventError as exc:
                bad.append((rec.seq, str(exc)))
                handle.exit()
                del handles[name]
                progressed = True
                continue
            confirmed.append(rec.seq)
            progressed = True
            if len(confirmed) >= budget:
                break
        if not progressed:
            if sess.finish() in ("end", "source_error"):
                break
    return len(confirmed), bad


def _record_rejected(path, bad):
    existing = []
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)
    existing.extend(seq for seq, _ in bad)
    with open(path, "w") as f:
        json.dump(sorted(set(existing)), f)


def cmd_work(args):
    p = _paths(args.dir)
    src = FileSource(p["feed"])
    job = JobStore(p["jobs"]).open(JOB, src)
    sess = job.session
    count = args.count or 3
    confirmed, bad = _service_some(sess, count)
    chk = job.save()
    if bad:
        _record_rejected(p["rejected"], bad)
    snapshot = {}
    for n in sess.groups():
        try:
            snapshot[n] = {"total": sess.event_total(n),
                           "events": sess.event_count(n)}
        except Exception:
            snapshot[n] = {"total": 0, "events": 0, "gone": True}
    print("process %d did  : %d confirmations, %d rejected, recovery point %d"
          % (os.getpid(), confirmed, len(bad), chk))
    for seq, msg in bad:
        print("  rejected record #%d: %s" % (seq, msg))
    print("rollup now     :", json.dumps(snapshot, sort_keys=True))
    job.close()
    src.close()


def _expected_from_feed(feed_path):
    """Independent recomputation by reading the feed frames directly."""
    from shipper.feed import FRAME_HEAD, HEADER_SIZE, HEADER
    fd = os.open(feed_path, os.O_RDONLY)
    try:
        header = os.pread(fd, HEADER_SIZE, 0)
        (magic, version, cat_cap, max_len, _res, count, _stream,
         frame_size) = HEADER.unpack(header[:HEADER.size])
        expected = {}
        for i in range(count):
            off = HEADER_SIZE + i * frame_size
            head = os.pread(fd, FRAME_HEAD.size, off)
            seq, length, cat_len = FRAME_HEAD.unpack(head)
            cat = os.pread(fd, cat_cap, off + FRAME_HEAD.size)[:cat_len] \
                .decode("utf-8")
            data = os.pread(fd, length,
                            off + FRAME_HEAD.size + cat_cap)[:length]
            if seq in BAD:
                continue                          # never confirmable
            doc = json.loads(data.decode("utf-8"))
            window = window_of(doc["event_time"], WIDTH)
            for group in CATEGORIES[cat]:
                key = (window, cat, doc["metric"])
                bucket = expected.setdefault(group, {})
                bucket[key] = (bucket.get(key, (0, 0))[0] + doc["value"],
                               bucket.get(key, (0, 0))[1] + 1)
        return expected
    finally:
        os.close(fd)


def cmd_verify(args):
    p = _paths(args.dir)
    src = FileSource(p["feed"])
    job = JobStore(p["jobs"]).open(JOB, src)
    sess = job.session
    confirmed, bad = _service_some(sess, 10 ** 9)
    assert sess.finish() == "end", sess.finish()
    if bad:
        _record_rejected(p["rejected"], bad)
    chk = job.save()
    job.close()
    src.close()

    expected = _expected_from_feed(p["feed"])
    ok = True
    print("final recovery point: %d (%d confirmations this process, "
          "%d rejected)" % (chk, confirmed, len(bad)))
    for group in sorted(expected):
        rows = {(r.window, r.category, r.metric): (r.total, r.events)
                for r in sess.event_windows(group)}
        match = rows == expected[group]
        ok &= match
        print("  group %s: %d window/category/metric rows, %s"
              % (group, len(rows), "OK" if match else "MISMATCH"))
        for key in sorted(set(rows) | set(expected[group])):
            got, want = rows.get(key), expected[group].get(key)
            if got != want:
                print("    MISMATCH at %r: got %r want %r" % (key, got, want))
        assert sess.event_count(group) == sum(
            n for _, n in expected[group].values())
    # Rejections are accumulated across every process that worked the job.
    with open(p["rejected"]) as f:
        rejected_seqs = json.load(f)
    print("rejected records   :", rejected_seqs,
          "(across all processes)")
    assert rejected_seqs == sorted(BAD), bad
    assert ok, "recovered rollup differs from the per-group confirmed records"
    print("\nrollup detail for group A (independent of the shipper's code "
          "path):")
    for r in sess.event_windows("A"):
        start, end = window_bounds(r.window, WIDTH)
        print("  w=%-3d [%6d,%6d) %-6s/%-7s sum=%4d events=%d"
              % (r.window, start, end, r.category, r.metric, r.total,
                 r.events))
    print("\nOK: after several process restarts the recovered totals equal "
          "an independent")
    print("recomputation from the feed; malformed records failed and never "
          "counted.")


def cmd_run_all(args):
    """Drive init/work/work/verify as four real OS processes."""
    env = dict(os.environ, PYTHONPATH=os.path.dirname(
        os.path.abspath(__file__)))
    steps = [
        [sys.executable, __file__, "init", "--dir", args.dir],
        [sys.executable, __file__, "work", "--dir", args.dir, "--count", "3"],
        [sys.executable, __file__, "work", "--dir", args.dir, "--count", "4"],
        [sys.executable, __file__, "verify", "--dir", args.dir],
    ]
    for i, cmd in enumerate(steps, 1):
        print("\n----- process %d: %s -----" % (i, " ".join(cmd[1:])))
        result = subprocess.run(cmd, env=env, check=False)
        if result.returncode != 0:
            sys.exit(result.returncode)


def main():
    # A parent parser lets --dir be given either before or after the command.
    # Subparsers use default=argparse.SUPPRESS so a --dir parsed before the
    # command is not clobbered by the subparser's own default.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dir", default="shipper-events-demo")
    sub_common = argparse.ArgumentParser(add_help=False)
    sub_common.add_argument("--dir", default=argparse.SUPPRESS)
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[common])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", parents=[sub_common]).set_defaults(fn=cmd_init)
    pw = sub.add_parser("work", parents=[sub_common])
    pw.add_argument("--count", type=int, default=None)
    pw.set_defaults(fn=cmd_work)
    sub.add_parser("verify", parents=[sub_common]).set_defaults(fn=cmd_verify)
    sub.add_parser("run-all", parents=[sub_common]) \
        .set_defaults(fn=cmd_run_all)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
