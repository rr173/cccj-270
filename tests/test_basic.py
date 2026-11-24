"""The basic contract: order, single pull, per-group categories."""

import unittest

from shipper import GroupGone, MockSource, ProtocolError, Session
from shipper.source import Spec

from helpers import drain_group, make, service


class OrderingTests(unittest.TestCase):
    def test_each_record_is_delivered_once_and_in_order(self):
        specs = [Spec(i + 1, "ab"[i % 2], 1 + i % 5) for i in range(60)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=64)
        got = service(sess)
        self.assertEqual(got["A"], [s.seq for s in specs if s.category == "a"])
        self.assertEqual(got["B"], [s.seq for s in specs if s.category == "b"])
        self.assertEqual(sorted(got["A"] + got["B"]),
                         sorted(s.seq for s in specs))
        self.assertEqual(sess.finish(), "end")
        self.assertEqual(src.consumed, len(specs))

    def test_group_only_sees_records_of_its_categories(self):
        specs = [Spec(i + 1, "abc"[i % 3], 2) for i in range(30)]
        src, sess = make(specs, {"a": ["A"], "c": ["C"]}, quota=32)
        got = []
        a = sess.group("A")
        for _ in range(30):
            rec = a.fetch()
            if rec is False:
                break
            self.assertEqual(rec.category, "a")
            got.append(rec.seq)
            a.confirm(rec.ticket)
        self.assertEqual(got, [s.seq for s in specs if s.category == "a"])
        # b was never accepted by anybody: it must have been skipped, not billed.
        self.assertGreater(sess.stats()["sources"][0]["skipped"], 0)

    def test_one_source_many_groups_share_records(self):
        specs = [Spec(i + 1, "x", 3) for i in range(10)]
        src, sess = make(specs, {"x": ["A", "B"]}, quota=32)
        self.assertEqual(drain_group(sess.group("A")), list(range(1, 11)))
        self.assertEqual(drain_group(sess.group("B")), list(range(1, 11)))
        self.assertEqual(sess.finish(), "end")
        # Shared payloads are billed once, not once per receiving group.
        self.assertLessEqual(sess.stats()["peak_billed"], 32)


class ConfirmProtocolTests(unittest.TestCase):
    def setUp(self):
        specs = [Spec(i + 1, "a", 2) for i in range(6)]
        self.src, self.sess = make(specs, {"a": ["A"]}, quota=8)
        self.a = self.sess.group("A")

    def test_fetch_does_not_advance(self):
        rec = self.a.fetch()
        again = self.a.fetch()
        self.assertEqual(rec.ticket, again.ticket)
        self.assertEqual(self.a.delivered, 0)

    def test_confirm_is_the_only_way_forward(self):
        rec = self.a.fetch()
        self.assertTrue(self.a.confirm(rec.ticket))
        nxt = self.a.fetch()
        self.assertNotEqual(nxt.ticket, rec.ticket)
        self.assertGreater(nxt.seq, rec.seq)

    def test_retrying_a_settled_ticket_is_harmless(self):
        rec = self.a.fetch()
        self.a.confirm(rec.ticket)
        seqs_before = self.a.delivered
        self.assertTrue(self.a.confirm(rec.ticket))
        self.assertTrue(self.a.confirm(rec.ticket))
        self.assertEqual(self.a.delivered, seqs_before)
        self.assertEqual(self.a.fetch().seq, rec.seq + 1)

    def test_a_wrong_number_changes_nothing(self):
        rec = self.a.fetch()
        with self.assertRaises(ProtocolError):
            self.a.confirm(rec.ticket + 1)      # would jump over the head
        self.assertEqual(self.a.peek().ticket, rec.ticket)
        with self.assertRaises(ProtocolError):
            self.a.confirm(999)                 # never issued
        self.assertEqual(self.a.peek().ticket, rec.ticket)
        self.assertEqual(self.a.delivered, 0)
        self.assertTrue(self.a.confirm(rec.ticket))

    def test_confirm_after_exit_is_refused(self):
        rec = self.a.fetch()
        self.a.exit()
        with self.assertRaises(GroupGone):
            self.a.confirm(rec.ticket)
        with self.assertRaises(GroupGone):
            self.a.fetch()

    def test_group_cannot_rejoin_after_exit(self):
        self.a.exit()
        with self.assertRaises(GroupGone):
            self.sess.group("A")


class ExitTests(unittest.TestCase):
    def test_exit_drops_only_that_groups_records(self):
        specs = [Spec(i + 1, "ab"[i % 2], 2) for i in range(12)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=32)
        a, b = sess.group("A"), sess.group("B")
        first = a.fetch()
        self.assertEqual(first.seq, 1)
        self.assertTrue(a.confirm(first.ticket))
        self.assertEqual(a.fetch().seq, 3)          # #3 now unacknowledged
        self.assertEqual(b.fetch().seq, 2)
        self.assertEqual(a.exit(), 1)               # drops #3
        # A's unacknowledged records are gone for good; B carries on alone.
        self.assertEqual(drain_group(b), [2, 4, 6, 8, 10, 12])
        self.assertEqual(sess.finish(), "end")
        self.assertEqual(sess.stats()["billed"], 0)

    def test_exited_groups_category_is_skipped(self):
        specs = [Spec(i + 1, "ab"[i % 2], 2) for i in range(10)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=16)
        a, b = sess.group("A"), sess.group("B")
        a.exit()
        self.assertEqual(drain_group(b), [2, 4, 6, 8, 10])
        # 'a' records went nowhere, so they were never materialised.
        self.assertEqual(sorted(src.produced), [2, 4, 6, 8, 10])
        self.assertEqual(sess.finish(), "end")


if __name__ == "__main__":
    unittest.main()
