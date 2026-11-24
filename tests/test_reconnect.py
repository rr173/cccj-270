"""Reconnect while the session is alive, and parallel equivalence.

Reconnect replaces transport only: confirmed stays confirmed, staged stays
staged, and the stream is not replayed or skipped.  Nothing is required to
survive a process or disk loss, so the position lives at the live source.
"""

import random
import threading
import time
import unittest

from shipper import FlakyLink, LinkDown, MockSource, Session, SourceError
from shipper.source import Spec

from helpers import drain_group, make, run_threads, join


class ReconnectTests(unittest.TestCase):
    def test_reconnect_keeps_position_and_staging(self):
        specs = [Spec(i + 1, "ab"[i % 2], 3) for i in range(20)]
        src = MockSource(specs, max_len=3)
        link1 = FlakyLink(src.link())
        sess = Session(link1, {"a": ["A"], "b": ["B"]}, 3, 12)
        a = sess.group("A")
        b = sess.group("B")
        first = a.fetch()
        self.assertEqual(first.seq, 1)
        a.confirm(first.ticket)
        held = b.fetch()
        self.assertEqual(held.seq, 2)

        # The transport dies; the session notices and records it.
        link1.down = True
        a.fetch(block=False)
        self.assertTrue(sess.down())

        # A fresh link to the *same* stream; nothing is replayed.
        sess.reconnect(src.link())
        self.assertFalse(sess.down())
        self.assertEqual(b.fetch().ticket, held.ticket)   # still staged
        b.confirm(held.ticket)
        got = drain_group(a, also=b)
        self.assertEqual(got, [s.seq for s in specs
                               if s.category == "a" and s.seq > first.seq])
        self.assertEqual(sess.finish(), "end")
        self.assertEqual(src.consumed, 20)

    def test_reconnect_to_a_different_stream_is_refused(self):
        src1 = MockSource.uniform(5, "a", max_len=4, seed=1)
        src2 = MockSource.uniform(5, "a", max_len=4, seed=2)
        sess = Session(src1.link(), {"a": ["A"]}, 4, 8)
        with self.assertRaises(ValueError):
            sess.reconnect(src2.link())

    def test_no_replay_after_reconnect(self):
        specs = [Spec(i + 1, "a", 2) for i in range(30)]
        src = MockSource(specs, max_len=2)
        sess = Session(FlakyLink(src.link()), {"a": ["A"]}, 2, 10)
        a = sess.group("A")
        seen = []
        for i in range(30):
            rec = a.fetch(block=True, timeout=5)
            if rec is False:
                break
            seen.append(rec.seq)
            a.confirm(rec.ticket)
            if i in (5, 11, 19):
                sess.reconnect(src.link())
        self.assertEqual(seen, list(range(1, 31)))
        self.assertEqual(sess.finish(), "end")

    def test_staged_records_survive_a_transport_failure(self):
        specs = [Spec(i + 1, "a", 4) for i in range(10)]
        src = MockSource(specs, max_len=4)
        sess = Session(FlakyLink(src.link()), {"a": ["A"]}, 4, 12)
        a = sess.group("A")
        a.fetch()
        self.assertEqual(sess.finish(), "pending")
        staged = [r.seq for r in sess.pending()["A"]]
        self.assertTrue(staged)
        sess.reconnect(src.link())
        self.assertEqual([r.seq for r in sess.pending()["A"]], staged)
        while True:
            rec = a.peek()
            if rec is None:
                break
            a.confirm(rec.ticket)
        self.assertEqual(sess.stats()["billed"], 0)

    def test_link_down_is_not_a_source_error(self):
        specs = [Spec(i + 1, "a", 2) for i in range(4)]
        src = MockSource(specs, max_len=2)
        link = FlakyLink(src.link())
        sess = Session(link, {"a": ["A"]}, 2, 8)
        sess.group("A").fetch()
        link.down = True
        sess.group("A").fetch(block=False)
        self.assertIsNone(sess.error)
        self.assertNotEqual(sess.state, "source_error")
        sess.reconnect(src.link())
        self.assertEqual(sess.finish(), "end")


class ParallelEquivalenceTests(unittest.TestCase):
    """A concurrent run must equal some sequential order consistent with the
    real-time order of the calls: each group's records in source order, each
    record delivered exactly once, every acknowledgement of its own head."""

    def test_groups_in_parallel_match_sequential(self):
        specs = [Spec(i + 1, "abcd"[i % 4], 1 + i % 8) for i in range(400)]
        src = MockSource(specs, max_len=8)
        sess = Session(src.link(), {"a": ["A"], "b": ["B"],
                                    "c": ["C"], "d": ["D"]}, 8, 40)
        got = {n: [] for n in "ABCD"}
        lock = threading.Lock()
        stop = threading.Event()

        def worker(name):
            h = sess.group(name)
            while not stop.is_set():
                rec = h.fetch(block=True, timeout=0.5)
                if rec is False:
                    return
                if rec is None:
                    continue
                with lock:
                    got[name].append(rec.seq)
                h.confirm(rec.ticket)

        threads, errors = run_threads(4, lambda i: worker("ABCD"[i]))
        join(threads, timeout=30)
        self.assertFalse(errors)
        for name in "ABCD":
            expected = [s.seq for s in specs if s.category == name.lower()]
            self.assertEqual(got[name], expected)
        self.assertEqual(sess.finish(), "end")

    def test_confirming_from_many_threads_is_safe(self):
        # Four threads all try to confirm the group's current head at once:
        # exactly one may advance it, the rest are harmless retries or are
        # refused, and no record is ever skipped.
        specs = [Spec(i + 1, "a", 1) for i in range(20)]
        src = MockSource(specs, max_len=1)
        sess = Session(src.link(), {"a": ["A"]}, 1, 4)
        a = sess.group("A")
        for _ in range(5):
            head = a.fetch()
            before = a.delivered
            barrier = threading.Barrier(4)

            def confirmer():
                barrier.wait(5)
                try:
                    a.confirm(head.ticket)
                except Exception:          # noqa: BLE001 - refusal is fine
                    pass

            threads, errors = run_threads(4, lambda i: confirmer())
            join(threads)
            self.assertFalse(errors)
            self.assertEqual(a.delivered, before + 1)
            # The next head is exactly the successor of the one just settled.
            nxt = a.fetch()
            self.assertEqual(nxt.seq, head.seq + 1)
            a.confirm(nxt.ticket)

    def test_mixed_read_confirm_exit_in_parallel(self):
        specs = [Spec(i + 1, "abc"[i % 3], 2) for i in range(300)]
        src = MockSource(specs, max_len=2)
        sess = Session(src.link(), {"a": ["A"], "b": ["B"],
                                    "c": ["C"]}, 2, 12)
        got = {"A": [], "B": []}
        lock = threading.Lock()

        def reader(name):
            h = sess.group(name)
            for _ in range(1000):
                rec = h.fetch(block=True, timeout=0.2)
                if rec is False or rec is None:
                    continue
                with lock:
                    got[name].append(rec.seq)
                h.confirm(rec.ticket)

        t1, e1 = run_threads(1, lambda i: reader("A"))
        t2, e2 = run_threads(1, lambda i: reader("B"))
        # Meanwhile C leaves almost immediately.
        time.sleep(0.01)
        sess.group("C").exit()
        join(t1 + t2, timeout=30)
        self.assertFalse(e1 or e2)
        self.assertEqual(got["A"], [s.seq for s in specs if s.category == "a"])
        self.assertEqual(got["B"], [s.seq for s in specs if s.category == "b"])

    def test_random_acks_never_skip(self):
        # Adversarial: try random ticket numbers; only the head may advance.
        specs = [Spec(i + 1, "a", 1) for i in range(200)]
        src = MockSource(specs, max_len=1)
        sess = Session(src.link(), {"a": ["A"]}, 1, 8)
        a = sess.group("A")
        rng = random.Random(11)
        seqs = []
        while True:
            rec = a.fetch(block=True, timeout=2)
            if rec is False:
                break
            if rec is None:
                continue
            for _ in range(3):            # a few wrong/garbage attempts
                bad = rng.choice([rec.ticket + 1, rec.ticket + 5, 10_000,
                                  rng.randint(0, 500)])
                if bad == rec.ticket:
                    continue
                try:
                    a.confirm(bad)
                except Exception:
                    pass
            self.assertEqual(a.peek().ticket, rec.ticket)
            a.confirm(rec.ticket)
            seqs.append(rec.seq)
        self.assertEqual(seqs, list(range(1, 201)))


if __name__ == "__main__":
    unittest.main()
