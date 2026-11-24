"""Shared helpers for the test suite (unittest, so no third-party install)."""

import contextlib
import threading
import unittest

from shipper import MockSource, Session, uniform_specs


def make(specs, categories, quota, max_len=None):
    """A MockSource + Session pair, with the link kept reachable."""
    if max_len is None:
        max_len = max(s.length for s in specs)
    src = MockSource(specs, max_len=max_len)
    sess = Session(src.link(), categories, max_len, quota)
    return src, sess


def drain_group(g, also=None):
    """Fetch-and-confirm until the group has nothing more, returning seqs.

    ``also`` is an iterable of other groups that must also be serviced, because
    their unacknowledged records occupy the shared quota (the fixture uses one
    source and one quota, exactly as the brief specifies).
    """
    got = []
    if also is None:
        others = []
    elif hasattr(also, "fetch"):
        others = [also]
    else:
        others = list(also)
    while True:
        for other in others:
            while True:
                rec = other.fetch(block=False)
                if rec in (None, False):
                    break
                other.confirm(rec.ticket)
        for other in others:
            while True:
                rec = other.fetch(block=False)
                if rec in (None, False):
                    break
                other.confirm(rec.ticket)
        rec = g.fetch(block=True, timeout=5)
        if rec is False:
            for other in others:
                while True:
                    orec = other.fetch(block=False)
                    if orec in (None, False):
                        break
                    other.confirm(orec.ticket)
            return got
        if rec is None:
            raise AssertionError("group %r stalled" % (g.name,))
        got.append(rec.seq)
        g.confirm(rec.ticket)


def service(sess, names=None, timeout=10):
    """Consume a whole session the way a real deployment would: every group is
    serviced, so one group's unacknowledged records do not starve the others.

    Returns ``{group: [seq, ...]}``.
    """
    import time

    names = list(sess.groups() if names is None else names)
    handles = {n: sess.group(n) for n in names}
    got = {n: [] for n in names}
    done = set()
    deadline = time.monotonic() + timeout
    idle = 0
    while len(done) < len(names):
        progressed = False
        for n in names:
            if n in done:
                continue
            handle = handles[n]
            rec = handle.fetch(block=False)
            if rec is False:
                # Only trust "my stream is over" once the session as a whole
                # has ended; otherwise it is just this group's quota pause.
                if sess.finish() in ("end", "source_error"):
                    done.add(n)
                progressed = True
                continue
            if rec is None:
                continue
            got[n].append(rec.seq)
            handle.confirm(rec.ticket)
            progressed = True
        if not progressed:
            idle += 1
            if sess.finish() in ("end", "source_error") and sess.stats()["billed"] == 0:
                break
            if time.monotonic() > deadline:
                raise AssertionError("session stalled: %r" % (got,))
            time.sleep(0.001)
    return got


class Recorder:
    """Records calls that must be in a sequential order."""

    def __init__(self):
        self.events = []
        self._lock = threading.Lock()

    def add(self, *items):
        with self._lock:
            self.events.extend(items)

    def __iter__(self):
        return iter(self.events)


def run_threads(count, target):
    """Run ``target(i)`` in ``count`` threads; collect exceptions."""
    errors = []

    def wrap(i):
        try:
            target(i)
        except BaseException as exc:        # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=wrap, args=(i,)) for i in range(count)]
    for t in threads:
        t.start()
    return threads, errors


def join(threads, timeout=20):
    for t in threads:
        t.join(timeout)
    assert not any(t.is_alive() for t in threads), "a worker did not finish"
