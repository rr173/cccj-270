#!/usr/bin/env python3
"""A runnable demonstration of the shipper.

    python3 demo.py [--records N] [--quota B] [--seed S] [--threads]

It builds a synthetic stream, declares three groups with overlapping
categories, services them (optionally from several threads), and prints the
accounting at the end.  The important numbers: ``peak_billed`` never exceeds
the quota, and ``billed`` returns to zero once everything is acknowledged.
"""

from __future__ import annotations

import argparse
import threading
import time

from shipper import MockSource, Session


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=20_000)
    ap.add_argument("--max-len", type=int, default=32)
    ap.add_argument("--quota", type=int, default=None)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--threads", action="store_true")
    args = ap.parse_args()

    quota = args.quota or args.max_len * 4
    categories = {"a": ["A", "C"], "b": ["B"], "c": ["A", "B"]}
    src = MockSource.uniform(args.records, "abc", min_len=1,
                             max_len=args.max_len, seed=args.seed)
    sess = Session(src.link(), categories, args.max_len, quota)
    handles = {n: sess.group(n) for ns in categories.values() for n in ns}

    counts = {n: 0 for n in handles}
    lock = threading.Lock()
    stop = threading.Event()

    def consume(name):
        h = handles[name]
        while not stop.is_set():
            rec = h.fetch(block=True, timeout=0.2)
            if rec is False:
                with lock:
                    counts[name] += 0
                return
            if rec is None:
                continue
            h.confirm(rec.ticket)
            with lock:
                counts[name] += 1

    start = time.monotonic()
    if args.threads:
        threads = [threading.Thread(target=consume, args=(n,))
                   for n in handles]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    else:
        remaining = dict(counts)
        while True:
            progressed = False
            for name, h in handles.items():
                rec = h.fetch(block=False)
                if rec in (None, False):
                    continue
                h.confirm(rec.ticket)
                counts[name] += 1
                progressed = True
            if not progressed and sess.finish() == "end":
                break
            if not progressed:
                time.sleep(0)
    elapsed = time.monotonic() - start

    stats = sess.stats()
    print("records in source :", args.records)
    print("delivered         :", sum(counts.values()))
    print("per group         :", counts)
    print("quota B           :", quota)
    print("peak bytes billed :", stats["peak_billed"])
    print("bytes billed now  :", stats["billed"])
    print("source state      :", sess.state)
    print("elapsed           : %.2fs" % elapsed)
    assert sess.state == "end", sess.state
    assert stats["peak_billed"] <= quota
    assert stats["billed"] == 0


if __name__ == "__main__":
    main()
