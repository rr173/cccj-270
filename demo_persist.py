#!/usr/bin/env python3
"""Persistent-job demo: the statistics groups survive the process.

    python3 demo_persist.py init   [--records N] [--max-len M] [--quota B] \
        [--seed S] [--dir D]
    python3 demo_persist.py work   [--dir D] [--count K]
    python3 demo_persist.py finish [--dir D]
    python3 demo_persist.py status [--dir D]
    python3 demo_persist.py wrong-source [--dir D]

Every subcommand is a separate process on purpose.  ``work`` opens the job,
delivers about K records, commits a recovery point and exits; the next
process resumes exactly there: confirmed records are not delivered again,
records still owed are still owed to the same group, and the shared byte
quota is exactly where it was.  ``finish`` drains the stream and verifies,
across all processes, that every group received exactly its records, in
order, with no replays and no losses.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys

from shipper import (FeedWriter, FileSource, JobMismatch, JobStore,
                     SourceError, uniform_specs)

CATEGORIES = {"a": ["A", "C"], "b": ["B"], "c": ["A", "B"]}
JOB = "nightly"


def _data(seq, length):
    head = ("%d:" % seq).encode("ascii")
    return (head * (length // len(head) + 1))[:length]


def _paths(d):
    return {
        "feed": os.path.join(d, "feed.bin"),
        "jobs": os.path.join(d, "jobs"),
        "meta": os.path.join(d, "demo.json"),
        "seen": os.path.join(d, "seen"),
    }


def _load_meta(d):
    with open(_paths(d)["meta"]) as f:
        return json.load(f)


def _open_job(d):
    p = _paths(d)
    src = FileSource(p["feed"])
    return src, JobStore(p["jobs"]).open(JOB, src)


def _record_seen(d, chk, got):
    seen_dir = _paths(d)["seen"]
    os.makedirs(seen_dir, exist_ok=True)
    with open(os.path.join(seen_dir, "%06d.json" % chk), "w") as f:
        json.dump(got, f)


def cmd_init(args):
    p = _paths(args.dir)
    os.makedirs(args.dir, exist_ok=True)
    quota = args.quota or args.max_len * 4
    specs = uniform_specs(args.records, "abc", min_len=1,
                          max_len=args.max_len, seed=args.seed)
    with FeedWriter(p["feed"], args.max_len) as w:
        for s in specs:
            w.append(s.seq, s.category, _data(s.seq, s.length))
    src = FileSource(p["feed"])
    job = JobStore(p["jobs"]).create(JOB, src, CATEGORIES, args.max_len,
                                     quota)
    with open(p["meta"], "w") as f:
        json.dump({"records": args.records, "max_len": args.max_len,
                   "quota": quota, "seed": args.seed}, f)
    print("feed            : %s (%d records, %.1f MB)"
          % (p["feed"], len(specs), os.path.getsize(p["feed"]) / 1e6))
    print("stream identity :", src.identity())
    print("groups          :", CATEGORIES)
    print("quota B         : %d (M=%d)" % (quota, args.max_len))
    print("recovery point  : %d committed" % job.checkpoint_id)
    job.close()
    src.close()


def cmd_work(args):
    meta = _load_meta(args.dir)
    src, job = _open_job(args.dir)
    sess = job.session
    handles = {n: sess.group(n) for n in sess.groups()}
    got = {n: [] for n in handles}
    target = args.count or max(1, meta["records"] // 3)
    try:
        while sum(len(v) for v in got.values()) < target:
            progressed = False
            for name, h in handles.items():
                rec = h.fetch(block=False)
                if rec in (None, False):
                    continue
                got[name].append(rec.seq)
                h.confirm(rec.ticket)
                progressed = True
            if not progressed:
                if sess.finish() in ("end", "source_error"):
                    break
    except SourceError as exc:
        print("source error    :", exc)
    chk = job.save()
    _record_seen(args.dir, chk, got)
    stats = sess.stats()
    print("process did     : %d confirmations" % sum(map(len, got.values())))
    print("recovery point  : %d committed" % chk)
    print("bytes billed    : %d (peak %d of B=%d)"
          % (stats["billed"], stats["peak_billed"], stats["quota"]))
    print("source state    :", sess.state)
    job.close()
    src.close()


def cmd_finish(args):
    meta = _load_meta(args.dir)
    src, job = _open_job(args.dir)
    sess = job.session
    got = {n: [] for n in sess.groups()}
    handles = {n: sess.group(n) for n in sess.groups()}
    while True:
        progressed = False
        for name, h in handles.items():
            rec = h.fetch(block=False)
            if rec in (None, False):
                continue
            got[name].append(rec.seq)
            h.confirm(rec.ticket)
            progressed = True
        if not progressed and sess.finish() in ("end", "source_error"):
            break
    chk = job.save()
    _record_seen(args.dir, chk, got)
    stats = sess.stats()
    job.close()
    src.close()

    # Verify across every process that ever worked on this job.
    specs = uniform_specs(meta["records"], "abc", min_len=1,
                          max_len=meta["max_len"], seed=meta["seed"])
    seen_files = sorted(glob.glob(os.path.join(_paths(args.dir)["seen"],
                                               "*.json")))
    delivered = {n: [] for n in CATEGORIES for n in
                 {g for gs in CATEGORIES.values() for g in gs}}
    for path in seen_files:
        with open(path) as f:
            phase = json.load(f)
        for name, seqs in phase.items():
            delivered[name].extend(seqs)
    ok = True
    for name, seqs in sorted(delivered.items()):
        expected = [s.seq for s in specs if name in CATEGORIES[s.category]]
        status = "ok" if seqs == expected else "MISMATCH"
        ok &= seqs == expected
        print("group %s         : %d records, %s" % (name, len(seqs), status))
    print("recovery points :", len(seen_files), "committed across processes")
    print("peak bytes      : %d (B=%d)" % (stats["peak_billed"],
                                           stats["quota"]))
    print("bytes owed      :", stats["billed"])
    print("source state    :", stats["state"])
    assert ok, "delivered records do not match the stream exactly"
    assert stats["state"] == "end"
    assert stats["billed"] == 0
    assert stats["peak_billed"] <= stats["quota"]
    print("OK: no record was replayed, lost or reordered across the "
          "process restarts.")


def cmd_status(args):
    p = _paths(args.dir)
    store = JobStore(p["jobs"])
    for name in store.jobs():
        print("job", name)
        print("  recovery points:", store.checkpoints(name))
    with open(os.path.join(p["jobs"], JOB, "job.json")) as f:
        meta = json.load(f)
    print("  config        :", json.dumps(meta["config"], sort_keys=True))
    print("  fingerprint   :", meta["config_fingerprint"][:32], "...")
    print("  sources       :", [s["identity"][:24] + "..."
                                for s in meta["sources"]])


def cmd_wrong_source(args):
    meta = _load_meta(args.dir)
    p = _paths(args.dir)
    other = os.path.join(args.dir, "other.bin")
    with FeedWriter(other, meta["max_len"]) as w:
        for s in uniform_specs(10, "abc", min_len=1,
                               max_len=meta["max_len"], seed=999):
            w.append(s.seq, s.category, _data(s.seq, s.length))
    store = JobStore(p["jobs"])
    try:
        store.open(JOB, FileSource(other))
    except JobMismatch as exc:
        print("refused, as it must be:", exc)
        return
    print("ERROR: a foreign feed was accepted!", file=sys.stderr)
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default="shipper-demo")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pi = sub.add_parser("init", help="write the feed and create the job")
    pi.add_argument("--records", type=int, default=30_000)
    pi.add_argument("--max-len", type=int, default=64)
    pi.add_argument("--quota", type=int, default=None)
    pi.add_argument("--seed", type=int, default=1)
    pi.set_defaults(fn=cmd_init)
    pw = sub.add_parser("work", help="deliver some records, commit, exit")
    pw.add_argument("--count", type=int, default=None)
    pw.set_defaults(fn=cmd_work)
    pf = sub.add_parser("finish", help="drain and verify the whole stream")
    pf.set_defaults(fn=cmd_finish)
    ps = sub.add_parser("status", help="show the job's recovery points")
    ps.set_defaults(fn=cmd_status)
    px = sub.add_parser("wrong-source",
                        help="show a foreign feed cannot take over")
    px.set_defaults(fn=cmd_wrong_source)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
