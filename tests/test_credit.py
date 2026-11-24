"""The shared quota B: nothing is pulled without room, and room returns.

Properties the brief calls out explicitly, each tested here:

* ``B >= M`` is enforced, and the next record must fit before anything is
  pulled;
* bytes pulled but not yet acknowledged count against ``B``;
* a released payload keeps no byte behind;
* a group stalled on credit still cannot stop another group from
  *acknowledging*, *reading what already fits*, or *leaving*;
* once bytes are released (acknowledge or exit) the pull path resumes.
"""

import threading
import time
import unittest

from shipper import MockSource, Session
from shipper.source import Spec

from helpers import drain_group, make, service


class NoRoomNoPullTests(unittest.TestCase):
    def test_pull_is_refused_rather_than_overdrawn(self):
        # M = 10, B = 10: exactly one record fits at a time.
        specs = [Spec(i + 1, "a", 10) for i in range(30)]
        src, sess = make(specs, {"a": ["A"]}, quota=10)
        a = sess.group("A")
        first = a.fetch()
        self.assertEqual(first.seq, 1)
        # Fetching again shows the same head; nothing more was consumed.
        self.assertEqual(a.fetch().ticket, first.ticket)
        self.assertEqual(src.consumed, 1)
        self.assertLessEqual(sess.stats()["peak_billed"], 10)
        a.confirm(first.ticket)
        self.assertEqual(a.fetch().seq, 2)
        self.assertEqual(src.consumed, 2)

    def test_production_stops_at_the_quota(self):
        # B = 16, M = 8: at most two records may exist at once.
        specs = [Spec(i + 1, "a", 8) for i in range(100)]
        src, sess = make(specs, {"a": ["A"]}, quota=16)
        a = sess.group("A")
        a.fetch()
        a.fetch()
        self.assertLessEqual(src.consumed, 2)
        self.assertLessEqual(sess.stats()["peak_billed"], 16)

    def test_unacked_bytes_count_against_the_quota(self):
        specs = [Spec(i + 1, "abc"[i % 3], 6) for i in range(30)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"], "c": ["C"]}, quota=24)
        for _ in range(3):
            sess.group("A").fetch()
        stats = sess.stats()
        pending = sum(len(v) for v in sess.pending().values())
        self.assertEqual(stats["billed"],
                         sum(r.length for v in sess.pending().values()
                             for r in v))
        self.assertLessEqual(stats["billed"], 24)
        self.assertEqual(stats["staged"], pending)

    def test_ack_releases_bytes_and_pulling_resumes(self):
        specs = [Spec(i + 1, "a", 7) for i in range(50)]
        src, sess = make(specs, {"a": ["A"]}, quota=7)
        a = sess.group("A")
        seen = []
        for _ in range(10):
            rec = a.fetch()
            self.assertIsNotNone(rec)
            seen.append(rec.seq)
            self.assertLessEqual(sess.stats()["billed"], 7)
            a.confirm(rec.ticket)
            self.assertEqual(sess.stats()["billed"], 0)
        self.assertEqual(seen, list(range(1, 11)))

    def test_exit_releases_bytes_and_pulling_resumes(self):
        specs = [Spec(i + 1, "ab"[i % 2], 9) for i in range(20)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=18)
        a, b = sess.group("A"), sess.group("B")
        first = a.fetch()
        self.assertEqual(first.seq, 1)
        self.assertEqual(b.fetch().seq, 2)
        self.assertEqual(sess.stats()["billed"], 18)
        self.assertEqual(src.consumed, 2)       # no room for a third
        self.assertEqual(b.exit(), 1)           # releases 9 bytes
        self.assertEqual(sess.stats()["billed"], 9)
        a.confirm(first.ticket)                 # releases the rest
        self.assertEqual(a.fetch().seq, 3)
        self.assertEqual(src.consumed, 3)

    def test_quota_smaller_than_m_is_rejected(self):
        src = MockSource.uniform(3, "a", max_len=10, seed=1)
        with self.assertRaises(ValueError):
            Session(src.link(), {"a": ["A"]}, 10, 9)


class SlowGroupTests(unittest.TestCase):
    def test_stalled_group_does_not_block_another_groups_acks(self):
        # A takes records and never acknowledges them.  B must still be able to
        # acknowledge the records it already holds -- this is the "slow group
        # must not make the fast group re-send" requirement, and the ack path
        # must not be serialised behind A's progress.
        specs = [Spec(i + 1, "ab"[i % 2], 2) for i in range(10)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=12)
        a, b = sess.group("A"), sess.group("B")
        a.fetch()                                 # A holds its head, never acks
        brec = b.fetch()
        self.assertEqual(brec.seq, 2)
        self.assertEqual(b.confirm(brec.ticket), True)
        self.assertEqual(a.delivered, 0)          # A still held up
        self.assertEqual(b.fetch().seq, 4)        # and B moves on regardless

    def test_stalled_group_does_not_block_another_groups_exit(self):
        specs = [Spec(i + 1, "ab"[i % 2], 5) for i in range(20)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=20)
        a, b = sess.group("A"), sess.group("B")
        a.fetch()
        b.fetch()
        self.assertEqual(b.exit(), 1)
        self.assertIsNotNone(a.fetch(block=False))

    def test_stalled_group_does_not_block_reading_what_fits(self):
        # A takes its head and stops.  B reads and confirms every record its
        # own bytes allow, until *the quota* (not A) stops the stream, and can
        # carry on to the very end once A leaves.
        specs = [Spec(i + 1, "a" if i % 4 == 0 else "b", 4) for i in range(40)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=16)
        a, b = sess.group("A"), sess.group("B")
        a.fetch()
        got = []
        while True:
            rec = b.fetch(block=False)
            if rec in (None, False):
                break
            got.append(rec.seq)
            b.confirm(rec.ticket)
        expected_b = [s.seq for s in specs if s.category == "b"]
        self.assertEqual(got, expected_b[:len(got)])
        self.assertGreater(len(got), 0)
        a.exit()
        self.assertEqual(drain_group(b), expected_b[len(got):])

    def test_a_waiting_fetch_wakes_when_bytes_are_released(self):
        # B has nothing yet and B's next record cannot fit; the thread blocks.
        # A.ack frees room; B must wake and get its record -- without polling.
        specs = [Spec(1, "a", 8), Spec(2, "b", 8), Spec(3, "b", 8)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=8)
        a, b = sess.group("A"), sess.group("B")
        first = a.fetch()
        box = {}

        def waiter():
            box["rec"] = sess.group("B").fetch(block=True, timeout=5)

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        self.assertNotIn("rec", box)             # genuinely blocked
        a.confirm(first.ticket)
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertEqual(box["rec"].seq, 2)

    def test_a_blocked_group_can_be_exited_from_another_thread(self):
        specs = [Spec(1, "a", 8), Spec(2, "b", 8)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=8)
        a = sess.group("A")
        a.fetch()
        box = {}

        def waiter():
            try:
                box["rec"] = sess.group("B").fetch(block=True, timeout=5)
            except BaseException as exc:         # noqa: BLE001
                box["exc"] = exc

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        a.exit()
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertEqual(box["rec"].seq, 2)


class NoLeakTests(unittest.TestCase):
    def test_released_bytes_are_not_retained(self):
        specs = [Spec(i + 1, "a", 6) for i in range(200)]
        src, sess = make(specs, {"a": ["A"]}, quota=12)
        a = sess.group("A")
        for _ in range(200):
            rec = a.fetch()
            if rec is False:
                break
            payload = rec._payload
            a.confirm(rec.ticket)
            self.assertIsNone(payload.data)
            self.assertFalse(payload.billed)
        self.assertEqual(sess.stats()["billed"], 0)
        self.assertEqual(sess.stats()["staged"], 0)

    def test_ticket_index_is_empty_after_every_ack(self):
        specs = [Spec(i + 1, "ab"[i % 2], 4) for i in range(500)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=40)
        got = service(sess)
        self.assertEqual(len(got["A"]), 250)
        stats = sess.stats()
        for name in ("A", "B"):
            self.assertEqual(stats["groups"][name]["tickets_live"], 0)
        self.assertEqual(stats["billed"], 0)

    def test_peak_billed_never_exceeds_quota(self):
        specs = [Spec(i + 1, "abc"[i % 3], 1 + i % 13) for i in range(300)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"], "c": ["C"]}, quota=40)
        service(sess)
        self.assertLessEqual(sess.stats()["peak_billed"], 40)
        self.assertEqual(sess.stats()["billed"], 0)


class LargeStreamTests(unittest.TestCase):
    def test_stream_larger_than_any_buffer(self):
        # 50k records of up to 32 bytes with a 64-byte quota: only a couple of
        # records can ever be in flight, yet the whole stream flows.
        src = MockSource.uniform(50_000, "abcd", min_len=1, max_len=32, seed=7)
        sess = Session(src.link(), {"a": ["A"], "b": ["B"],
                                    "c": ["C"], "d": ["D"]}, 32, 64)
        got = {n: 0 for n in "ABCD"}
        handles = {n: sess.group(n) for n in "ABCD"}
        for _ in range(400_000):
            progressed = False
            for n, h in handles.items():
                rec = h.fetch(block=False)
                if rec is False or rec is None:
                    continue
                got[n] += 1
                h.confirm(rec.ticket)
                progressed = True
            if not progressed:
                if sess.finish() == "end":
                    break
        self.assertEqual(sess.finish(), "end")
        self.assertEqual(sum(got.values()), 50_000)
        self.assertEqual(sess.stats()["billed"], 0)
        self.assertLessEqual(sess.stats()["peak_billed"], 64)


if __name__ == "__main__":
    unittest.main()
