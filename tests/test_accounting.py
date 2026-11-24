"""Fine-grained accounting and ticket-identity checks."""

import unittest

from shipper import MockSource, ProtocolError, Session
from shipper.source import Spec

from helpers import make, service


class TicketIdentityTests(unittest.TestCase):
    def test_a_ticket_from_another_group_is_refused(self):
        specs = [Spec(1, "a", 2), Spec(2, "b", 2), Spec(3, "a", 2)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=16)
        a, b = sess.group("A"), sess.group("B")
        arec = a.fetch()
        brec = b.fetch()
        self.assertNotEqual(arec.ticket, brec.ticket)
        with self.assertRaises(ProtocolError):
            b.confirm(arec.ticket)          # not B's head
        self.assertEqual(b.peek().ticket, brec.ticket)
        self.assertTrue(b.confirm(brec.ticket))

    def test_tickets_are_never_reused_across_groups(self):
        specs = [Spec(i + 1, "ab"[i % 2], 1) for i in range(20)]
        src, sess = make(specs, {"a": ["A"], "b": ["B"]}, quota=8)
        seen = set()
        a, b = sess.group("A"), sess.group("B")
        for _ in range(200):
            progressed = False
            for g in (a, b):
                rec = g.fetch(block=False)
                if rec in (None, False):
                    continue
                self.assertNotIn(rec.ticket, seen)
                seen.add(rec.ticket)
                g.confirm(rec.ticket)
                progressed = True
            if not progressed and sess.finish() == "end":
                break
        self.assertEqual(len(seen), 20)
        self.assertEqual(sess.finish(), "end")


class BillingTests(unittest.TestCase):
    def test_billed_equals_sum_of_staged_lengths(self):
        specs = [Spec(i + 1, "abcd"[i % 4], 1 + i % 11) for i in range(80)]
        src = MockSource(specs, max_len=11)
        sess = Session(src.link(), {"a": ["A"], "b": ["B"],
                                    "c": ["C"], "d": ["D"]}, 11, 40)
        handles = {n: sess.group(n) for n in "ABCD"}
        # Prefetch as far as the quota permits without acknowledging anything.
        for h in handles.values():
            h.fetch()
        for _ in range(20):
            progressed = False
            for h in handles.values():
                if h.pending < 3:
                    before = sess.stats()["staged"]
                    h.fetch()
                    progressed = progressed or sess.stats()["staged"] > before
            if not progressed:
                break
        pending = sess.pending()
        billed = sum(r.length for v in pending.values() for r in v)
        # Each billed record is present exactly once, but a shared record is
        # counted once per owing group in |pending|; billing counts it once.
        by_seq = {}
        for name, recs in pending.items():
            for r in recs:
                by_seq.setdefault(r.seq, r.length)
        self.assertEqual(sess.stats()["billed"], sum(by_seq.values()))
        self.assertLessEqual(sess.stats()["billed"], 40)

    def test_a_record_pulled_but_not_handed_off_is_still_billed(self):
        # The payload enters the ledger at pull time, and the charge precedes
        # the staging append; "pulled but not yet handed off" is therefore
        # always inside the ledger.  Observable consequence: the next record is
        # refused before it is produced, even though staging is momentarily
        # one record shallow.
        specs = [Spec(i + 1, "a", 5) for i in range(10)]
        src = MockSource(specs, max_len=5)
        sess = Session(src.link(), {"a": ["A"]}, 5, 11)
        a = sess.group("A")
        a.fetch()                              # 5 billed
        sess.finish()                          # greedily prefetch to the quota
        self.assertLessEqual(sess.stats()["billed"], 11)
        self.assertEqual(src.consumed, 2)      # a third would not fit
        self.assertEqual(len(src.produced), 2)  # never even materialised
        self.assertLessEqual(sess.stats()["billed"], 11)

    def test_shared_payload_billed_once(self):
        specs = [Spec(i + 1, "x", 4) for i in range(20)]
        src = MockSource(specs, max_len=4)
        sess = Session(src.link(), {"x": ["A", "B", "C"]}, 4, 12)
        for name in "ABC":
            sess.group(name).fetch()
        self.assertEqual(sess.stats()["billed"], 4)     # one record, one charge
        self.assertEqual(sess.stats()["staged"], 3)     # three claims
        # A leaves: the bytes stay because B and C still owe the record.
        sess.group("A").exit()
        self.assertEqual(sess.stats()["billed"], 4)
        sess.group("B").exit()
        self.assertEqual(sess.stats()["billed"], 4)
        sess.group("C").exit()
        self.assertEqual(sess.stats()["billed"], 0)


if __name__ == "__main__":
    unittest.main()
