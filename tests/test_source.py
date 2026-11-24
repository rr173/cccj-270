"""Source faults vs normal end, and the non-consuming announcement.

A source error must be distinguishable from a normal end *and* must not
retract valid data: everything pulled before the fault stays fetchable and
acknowledgeable, and only the stream's tail is lost.
"""

import unittest

from shipper import MockSource, ProtocolError, Session, SourceError
from shipper.source import Spec, SyntheticLink
from shipper.wire import Announcement, Pulled

from helpers import make, service


class AnnouncementTests(unittest.TestCase):
    def test_announce_never_consumes_or_bills(self):
        specs = [Spec(i + 1, "ab"[i % 2], 5) for i in range(8)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=20)
        before = sess.stats()
        for _ in range(5):
            self.assertEqual(sess.announce(), ("a", 5, 0))
        after = sess.stats()
        self.assertEqual(src.consumed, 0)
        self.assertEqual(before["billed"], after["billed"])
        self.assertEqual(after["billed"], 0)

    def test_announcement_can_be_ignored_or_skipped(self):
        # Announcing and then not pulling must not corrupt anything.
        specs = [Spec(i + 1, "ab"[i % 2], 3) for i in range(6)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=16)
        for _ in range(4):
            sess.announce()
        got = service(sess)
        self.assertEqual(got["A"], [1, 3, 5])
        self.assertEqual(got["B"], [2, 4, 6])
        self.assertEqual(sess.finish(), "end")

    def test_end_of_stream_announcement_is_none(self):
        specs = [Spec(1, "a", 2)]
        src, sess = make(specs, {"a": ["A"]}, quota=4)
        a = sess.group("A")
        self.assertEqual(sess.announce(), ("a", 2, 0))
        rec = a.fetch()
        a.confirm(rec.ticket)
        self.assertIsNone(sess.announce())

    def test_lying_announcement_is_a_source_error(self):
        # The source announces a record size the format does not allow.
        specs = [Spec(1, "a", 4), Spec(2, "a", 40)]
        src = MockSource(specs, max_len=40)
        src.corrupt(2, length=0)          # announced length is outside 1..M
        sess = Session(src.link(), {"a": ["A"]}, 40, 80)
        a = sess.group("A")
        first = a.fetch()
        self.assertEqual(first.seq, 1)
        a.confirm(first.ticket)
        with self.assertRaises(SourceError):
            for _ in range(5):
                nxt = a.fetch(block=False)
                if nxt not in (None, False):
                    a.confirm(nxt.ticket)
        self.assertEqual(sess.state, "source_error")
        self.assertEqual(sess.finish(), "source_error")


class SourceErrorTests(unittest.TestCase):
    def test_error_is_distinguishable_from_end(self):
        specs = [Spec(i + 1, "a", 4) for i in range(10)]
        src = MockSource(specs, max_len=4)
        src.corrupt(6, length=99)
        sess = Session(src.link(), {"a": ["A"]}, 4, 8)
        a = sess.group("A")
        seen = []
        with self.assertRaises(SourceError):
            for _ in range(100):
                rec = a.fetch(block=False)
                if rec is False:
                    break
                if rec is None:
                    continue
                seen.append(rec.seq)
                a.confirm(rec.ticket)
                if sess.state == "source_error":
                    raise SourceError(sess.error)
        self.assertEqual(sess.finish(), "source_error")
        self.assertNotEqual(sess.finish(), "end")
        self.assertIn(1, seen)

    def test_records_before_the_error_are_still_usable(self):
        # The broken record is #4.  #1..#3 were pulled and are staged; the
        # error must not drop them, and they must still be confirmable.
        specs = [Spec(i + 1, "a", 5) for i in range(8)]
        src = MockSource(specs, max_len=5)
        sess = Session(src.link(), {"a": ["A"]}, 5, 40)   # room for 8 records
        a = sess.group("A")
        a.fetch()
        src.corrupt(4, length=0)          # invalid length, discovered on announce
        self.assertEqual(sess.finish(), "source_error")
        staged = a.pending
        self.assertGreaterEqual(staged, 1)
        self.assertLessEqual(staged, 3)   # #1..#3 only; #4 is the fault
        got = []
        while True:
            rec = a.peek()
            if rec is None:
                break
            got.append(rec.seq)
            self.assertTrue(a.confirm(rec.ticket))
        self.assertEqual(got, list(range(1, len(got) + 1)))
        self.assertEqual(sess.stats()["billed"], 0)
        self.assertEqual(sess.finish(), "source_error")

    def test_error_at_the_end_is_still_an_error(self):
        specs = [Spec(1, "a", 2), Spec(2, "a", 2)]
        src = MockSource(specs, max_len=2)
        src.corrupt(2, length=0)          # invalid length at the very end
        sess = Session(src.link(), {"a": ["A"]}, 2, 4)
        with self.assertRaises(SourceError):
            for _ in range(10):
                rec = sess.group("A").fetch(block=False)
                if rec in (None, False):
                    sess.finish()
                else:
                    sess.group("A").confirm(rec.ticket)
        self.assertEqual(sess.finish(), "source_error")

    def test_normal_end_is_not_an_error(self):
        specs = [Spec(i + 1, "a", 2) for i in range(5)]
        src, sess = make(specs, {"a": ["A"]}, quota=4)
        service(sess)
        self.assertEqual(sess.finish(), "end")
        self.assertIsNone(sess.error)

    def test_ended_session_keeps_serving_buffered_records(self):
        # Reading to the end does not invalidate tails already staged: pull and
        # confirm to the very end, then check the last buffered records are
        # still there and still confirmable.
        specs = [Spec(i + 1, "a", 2) for i in range(6)]
        src = MockSource(specs, max_len=2)
        sess = Session(src.link(), {"a": ["A"]}, 2, 6)
        a = sess.group("A")
        seen = []
        while True:
            rec = a.fetch(block=False)
            if rec is None:
                self.assertEqual(sess.finish(), "pending")
                continue
            if rec is False:
                break
            seen.append(rec.seq)
            a.confirm(rec.ticket)
        self.assertEqual(seen, [1, 2, 3, 4, 5, 6])
        self.assertEqual(sess.finish(), "end")
        self.assertEqual(a.pending, 0)
        self.assertEqual(sess.stats()["billed"], 0)


class MultiSourceErrorTests(unittest.TestCase):
    def test_a_fault_on_one_source_does_not_hide_another(self):
        # Source 2 breaks at its second record; source 1 is entirely fine and
        # its records must all be obtainable before the error is reported.
        s1 = MockSource([Spec(1, "a", 2), Spec(3, "a", 2)], max_len=2)
        s2 = MockSource([Spec(2, "b", 2), Spec(4, "b", 2)], max_len=2)
        s2.corrupt(4, length=0)
        sess = Session([s1.link(), s2.link()], {"a": ["A"], "b": ["B"]}, 2, 8)
        a = sess.group("A")
        got = []
        with self.assertRaises(SourceError):
            for _ in range(20):
                rec = a.fetch(block=False)
                if rec in (None, False):
                    continue
                got.append(rec.seq)
                a.confirm(rec.ticket)
        self.assertEqual(got, [1, 3])
        self.assertEqual(sess.state, "source_error")
        self.assertEqual(sess.finish(), "source_error")


class SyntheticStreamTests(unittest.TestCase):
    def test_generated_stream_works_without_prebuilding(self):
        from shipper import stream
        link = stream(10_000, "ab", min_len=1, max_len=8, seed=3)
        sess = Session(link, {"a": ["A"], "b": ["B"]}, 8, 24)
        got = {"A": 0, "B": 0}
        handles = {"A": sess.group("A"), "B": sess.group("B")}
        for _ in range(100_000):
            progressed = False
            for n, h in handles.items():
                rec = h.fetch(block=False)
                if rec is False or rec is None:
                    continue
                self.assertIn(rec.category, ("a", "b"))
                got[n] += 1
                h.confirm(rec.ticket)
                progressed = True
            if not progressed and sess.finish() == "end":
                break
        self.assertEqual(sum(got.values()), 10_000)
        self.assertEqual(sess.finish(), "end")


class AnnouncementMismatchAtTheWireTests(unittest.TestCase):
    def test_wire_rejects_a_lying_source(self):
        # The store is the authority; make it promise one thing and deliver
        # another, entirely beneath the session.
        specs = [Spec(1, "a", 3)]
        src = MockSource(specs, max_len=3)
        link = src.link()
        ann = link.announce()
        # Mutate the truth after the promise was made.
        src.store.specs[0] = Spec(1, "a", 2)
        with self.assertRaises(SourceError):
            link.take()
        self.assertIsNotNone(src.store.error)


if __name__ == "__main__":
    unittest.main()
