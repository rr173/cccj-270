"""Persistent jobs: recovery points that survive the process.

A job fixes its configuration and its sources at creation; ``save`` commits
one consistent recovery point; after the process (here: the job handle) is
gone, opening the job resumes exactly there.  Confirmed records are not
delivered again, owed records stay owed to the same group with the same
tickets, and a damaged, foreign or incomplete point fails loudly without
touching the previously committed ones.
"""

import json
import os
import shutil
import struct
import tempfile
import threading
import time
import unittest

from shipper import (CheckpointCorrupt, FeedWriter, FileSource, GroupGone,
                     JobError, JobExists, JobMismatch, JobNotFound, JobStore,
                     MockSource, ProtocolError, SourceError, uniform_specs)
from shipper.feed import FRAME_HEAD, HEADER_SIZE
from shipper.source import Spec

from helpers import drain_group, join, run_threads, service

CATS = {"a": ["A", "C"], "b": ["B"], "c": ["A", "B"]}


def alternating(n, cats="ab", length=3):
    return [Spec(i + 1, cats[i % len(cats)], length) for i in range(n)]


def write_feed(path, specs, max_len, stream_id=None):
    """A feed whose data bytes identify the record: ``b"0007:......"``."""
    with FeedWriter(path, max_len, stream_id=stream_id) as w:
        for s in specs:
            head = ("%d:" % s.seq).encode("ascii")
            w.append(s.seq, s.category, (head * (s.length // len(head) + 1))
                     [:s.length])
    return path


def feed_data(seq, length):
    head = ("%d:" % seq).encode("ascii")
    return (head * (length // len(head) + 1))[:length]


class JobTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="shipper-job-")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.store = JobStore(os.path.join(self.dir, "jobs"))

    def feed(self, specs, max_len, name="feed.bin"):
        return write_feed(os.path.join(self.dir, name), specs, max_len)


class ResumeContinuesTests(JobTestCase):
    def test_confirmed_stays_confirmed_and_owed_stays_owed(self):
        specs = alternating(20)
        src = MockSource(specs, max_len=3)
        job = self.store.create("nightly", src, {"a": ["A"], "b": ["B"]}, 3, 12)
        a, b = job.session.group("A"), job.session.group("B")
        first = a.fetch()
        a.confirm(first.ticket)                    # #1 is confirmed
        held_a = a.fetch()                         # #3 owed to A
        held_b = b.fetch()                         # #2 owed to B
        chk = job.save()
        self.assertEqual(chk, 2)                   # 1 is the initial point
        job.close()

        job2 = self.store.open("nightly", src)
        a2, b2 = job2.session.group("A"), job2.session.group("B")
        # The owed records are still there, for the same group, same ticket.
        self.assertEqual(a2.fetch().ticket, held_a.ticket)
        self.assertEqual(a2.fetch().seq, 3)
        self.assertEqual(b2.fetch().ticket, held_b.ticket)
        self.assertEqual(b2.fetch().seq, 2)
        # The confirmed record is not delivered again.
        self.assertNotEqual(a2.fetch().seq, 1)
        self.assertNotEqual(b2.fetch().seq, 1)
        a2.confirm(held_a.ticket)
        b2.confirm(held_b.ticket)
        got = service(job2.session)
        self.assertEqual([1, 3] + got["A"],
                         [s.seq for s in specs if s.category == "a"])
        self.assertEqual([2] + got["B"],
                         [s.seq for s in specs if s.category == "b"])
        self.assertEqual(job2.session.finish(), "end")
        self.assertEqual(job2.session.stats()["billed"], 0)
        job2.close()

    def test_unconfirmed_payload_bytes_survive(self):
        specs = alternating(6, length=9)
        path = self.feed(specs, 9)
        job = self.store.create("j", FileSource(path),
                                {"a": ["A"], "b": ["B"]}, 9, 18)
        a = job.session.group("A")
        held = a.fetch()                           # #1, owed at the save
        job.save()
        job.close()

        job2 = self.store.open("j", FileSource(path))
        rec = job2.session.group("A").fetch()
        self.assertEqual(rec.seq, held.seq)
        self.assertEqual(rec.ticket, held.ticket)
        self.assertEqual(rec.data, feed_data(1, 9))
        job2.close()

    def test_ticket_numbers_are_never_reused(self):
        specs = [Spec(i + 1, "a", 2) for i in range(10)]
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"]}, 2, 8)
        a = job.session.group("A")
        first = a.fetch()
        a.confirm(first.ticket)                    # ticket 1 settled
        second = a.fetch()                         # ticket 2 staged
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        a2 = job2.session.group("A")
        self.assertEqual(a2.fetch().ticket, second.ticket)
        # Retrying the pre-crash settled ticket is still harmless.
        self.assertTrue(a2.confirm(first.ticket))
        # A number that was never issued is still refused.
        with self.assertRaises(ProtocolError):
            a2.confirm(second.ticket + 100)
        a2.confirm(second.ticket)
        nxt = a2.fetch()
        self.assertGreater(nxt.ticket, second.ticket)   # no reuse
        self.assertEqual(nxt.seq, 3)
        job2.close()

    def test_billed_bytes_and_quota_survive(self):
        # B = 2 * M: exactly two records fit; the save happens with the
        # quota fully consumed by unconfirmed records.
        specs = [Spec(i + 1, "a", 2) for i in range(10)]
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"]}, 2, 4)
        a = job.session.group("A")
        a.fetch()
        self.assertEqual(job.session.finish(), "pending")   # quota full
        self.assertEqual(job.session.stats()["billed"], 4)
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        a2 = job2.session.group("A")
        self.assertEqual(job2.session.stats()["billed"], 4)
        self.assertLessEqual(job2.session.stats()["peak_billed"], 4)
        rec = a2.fetch()
        self.assertEqual(rec.seq, 1)
        a2.confirm(rec.ticket)
        self.assertEqual(job2.session.stats()["billed"], 2)
        got = [rec.seq] + drain_group(a2)
        self.assertEqual(got, list(range(1, 11)))
        self.assertEqual(job2.session.stats()["billed"], 0)
        job2.close()

    def test_normal_end_state_is_remembered(self):
        specs = alternating(8, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 8)
        service(job.session)
        self.assertEqual(job.session.finish(), "end")
        delivered_a = job.session.group("A").delivered
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        self.assertEqual(job2.session.finish(), "end")
        self.assertEqual(job2.session.state, "end")
        self.assertFalse(job2.session.group("A").fetch(block=False))
        self.assertEqual(job2.session.group("A").delivered, delivered_a)
        self.assertEqual(job2.session.stats()["billed"], 0)
        job2.close()

    def test_source_error_state_is_remembered(self):
        specs = [Spec(i + 1, "a", 5) for i in range(8)]
        src = MockSource(specs, max_len=5)
        job = self.store.create("j", src, {"a": ["A"]}, 5, 40)
        a = job.session.group("A")
        a.fetch()
        src.corrupt(4, length=0)
        self.assertEqual(job.session.finish(), "source_error")
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        sess = job2.session
        self.assertEqual(sess.state, "source_error")
        self.assertIsNotNone(sess.error)
        a2 = sess.group("A")
        got = []
        while True:
            rec = a2.peek()                      # staged tail survives
            if rec is None:
                break
            got.append(rec.seq)
            a2.confirm(rec.ticket)
        self.assertEqual(got, [1, 2, 3])
        self.assertEqual(sess.stats()["billed"], 0)
        self.assertEqual(sess.finish(), "source_error")
        job2.close()

    def test_exit_is_remembered(self):
        specs = alternating(12, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 8)
        a = job.session.group("A")
        a.fetch()
        a.exit()
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        with self.assertRaises(GroupGone):
            job2.session.group("A")
        got = drain_group(job2.session.group("B"))
        self.assertEqual(got, [s.seq for s in specs if s.category == "b"])
        # After the exit, 'a' records were skipped, never materialised.
        self.assertEqual(sorted(src.produced),
                         [1] + [s.seq for s in specs if s.category == "b"])
        self.assertEqual(job2.session.finish(), "end")
        job2.close()

    def test_two_sources_restore_their_own_positions(self):
        a_specs = [Spec(2 * i + 1, "a", 3) for i in range(10)]
        b_specs = [Spec(2 * i + 2, "b", 3) for i in range(10)]
        p1 = self.feed(a_specs, 3, "a.bin")
        p2 = self.feed(b_specs, 3, "b.bin")
        job = self.store.create("j", [FileSource(p1), FileSource(p2)],
                                {"a": ["A"], "b": ["B"]}, 3, 12)
        a, b = job.session.group("A"), job.session.group("B")
        for _ in range(3):
            ra, rb = a.fetch(), b.fetch()
            a.confirm(ra.ticket)
            b.confirm(rb.ticket)
        job.save()
        job.close()

        job2 = self.store.open("j", [FileSource(p1), FileSource(p2)])
        got = service(job2.session)
        self.assertEqual([1, 3, 5] + got["A"], [s.seq for s in a_specs])
        self.assertEqual([2, 4, 6] + got["B"], [s.seq for s in b_specs])
        self.assertEqual(job2.session.finish(), "end")
        job2.close()

    def test_delivery_counters_and_stats_continue(self):
        specs = alternating(30, cats="abc", length=4)
        src = MockSource(specs, max_len=4)
        job = self.store.create("j", src, CATS, 4, 16)
        for name in ("A", "B", "C"):
            h = job.session.group(name)
            rec = h.fetch()
            h.confirm(rec.ticket)
        before = {n: job.session.group(n).delivered for n in "ABC"}
        hw = job.session.stats()["groups"]["A"]["tickets_issued"]
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        for n in "ABC":
            self.assertEqual(job2.session.group(n).delivered, before[n])
        self.assertEqual(job2.session.stats()["groups"]["A"]
                         ["tickets_issued"], hw)
        got = service(job2.session)
        for n in "ABC":
            expected = [s.seq for s in specs if n in CATS[s.category]]
            self.assertEqual(before[n] + len(got[n]), len(expected))
        job2.close()


class LifecycleTests(JobTestCase):
    def test_create_writes_an_initial_point_and_open_resumes_it(self):
        specs = alternating(4, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 4)
        self.assertEqual(job.checkpoints(), (1,))
        self.assertEqual(job.checkpoint_id, 1)
        job.close()

        job2 = self.store.open("j", src)
        self.assertEqual(job2.session.group("A").fetch().seq, 1)
        self.assertEqual(job2.session.stats()["billed"], 2)
        job2.close()

    def test_create_refuses_a_taken_name(self):
        src = MockSource(alternating(2, length=2), max_len=2)
        job = self.store.create("j", src, {"a": ["A"]}, 2, 4)
        with self.assertRaises(JobExists):
            self.store.create("j", src, {"a": ["A"]}, 2, 4)
        job.close()
        # Still refused once the first handle is gone.
        with self.assertRaises(JobExists):
            self.store.create("j", src, {"a": ["A"]}, 2, 4)

    def test_open_requires_an_existing_job(self):
        with self.assertRaises(JobNotFound):
            self.store.open("nope", MockSource(alternating(2, length=2),
                                               max_len=2))

    def test_a_job_cannot_be_opened_twice(self):
        src = MockSource(alternating(4, length=2), max_len=2)
        job = self.store.create("j", src, {"a": ["A"]}, 2, 4)
        with self.assertRaises(JobError):
            self.store.open("j", src)
        job.close()
        job2 = self.store.open("j", src)         # after close it works
        job2.close()

    def test_save_after_close_is_refused(self):
        src = MockSource(alternating(2, length=2), max_len=2)
        job = self.store.create("j", src, {"a": ["A"]}, 2, 4)
        job.close()
        with self.assertRaises(JobError):
            job.save()

    def test_open_specific_checkpoint(self):
        specs = alternating(10, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 8)
        a = job.session.group("A")
        r = a.fetch()
        a.confirm(r.ticket)                        # point 2: #1 confirmed
        job.save()
        r = a.fetch()
        a.confirm(r.ticket)                        # point 3: #3 confirmed
        job.save()
        job.close()

        older = self.store.open("j", src, checkpoint=2)
        self.assertEqual(older.session.group("A").fetch().seq, 3)
        older.close()
        latest = self.store.open("j", src)
        self.assertEqual(latest.session.group("A").fetch().seq, 5)
        latest.close()
        with self.assertRaises(JobError):
            self.store.open("j", src, checkpoint=99)


class BindingTests(JobTestCase):
    def test_another_source_cannot_take_over(self):
        specs = alternating(6, length=2)
        src1 = MockSource(specs, max_len=2)
        src2 = MockSource(specs, max_len=2)      # same content, other stream
        job = self.store.create("j", src1, {"a": ["A"], "b": ["B"]}, 2, 4)
        job.save()
        job.close()
        with self.assertRaises(JobMismatch):
            self.store.open("j", src2)
        # The failed open left nothing behind: the right source still opens.
        job2 = self.store.open("j", src1)
        job2.close()

    def test_another_feed_cannot_take_over(self):
        specs = alternating(6, length=4)
        p1 = self.feed(specs, 4, "one.bin")
        p2 = self.feed(specs, 4, "two.bin")
        job = self.store.create("j", FileSource(p1), {"a": ["A"]}, 4, 8)
        job.close()
        with self.assertRaises(JobMismatch):
            self.store.open("j", FileSource(p2))
        job2 = self.store.open("j", FileSource(p1))
        job2.close()

    def test_tampered_metadata_is_refused(self):
        src = MockSource(alternating(4, length=2), max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 4)
        job.close()
        meta_path = os.path.join(self.dir, "jobs", "j", "job.json")
        with open(meta_path) as f:
            meta = json.load(f)
        meta["config"]["quota"] = 999            # pretend B was larger
        with open(meta_path, "w") as f:
            json.dump(meta, f)
        with self.assertRaises(CheckpointCorrupt):
            self.store.open("j", src)

    def test_a_foreign_checkpoint_is_refused(self):
        src1 = MockSource(alternating(6, length=2), max_len=2)
        src2 = MockSource(alternating(6, length=2), max_len=2)
        j1 = self.store.create("one", src1, {"a": ["A"], "b": ["B"]}, 2, 4)
        j2 = self.store.create("two", src2, {"a": ["A"], "b": ["B"]}, 2, 4)
        j1.close()
        j2.close()
        # Drop job "two"'s recovery point into job "one"'s directory.
        shutil.copyfile(os.path.join(self.dir, "jobs", "two", "checkpoints",
                                     "000001.chk"),
                        os.path.join(self.dir, "jobs", "one", "checkpoints",
                                     "000001.chk"))
        with self.assertRaises(JobMismatch):
            self.store.open("one", src1)
        job = self.store.open("two", src2)       # its own point is fine
        job.close()

    def test_a_position_off_the_stream_is_refused(self):
        specs = alternating(6, length=4)
        path = self.feed(specs, 4)
        src = FileSource(path)
        with self.assertRaises(JobMismatch):
            src.open({"consumed": 2, "last_seq": 999})
        with self.assertRaises(JobMismatch):
            src.open({"consumed": 99, "last_seq": 99})
        with self.assertRaises(JobMismatch):
            src.open({"consumed": -1, "last_seq": 0})
        link = src.open({"consumed": 2, "last_seq": 2})
        self.assertEqual(link.take().seq, 3)
        src.close()


class IntegrityTests(JobTestCase):
    def _job_with_two_points(self):
        specs = alternating(12, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 8)
        a = job.session.group("A")
        r = a.fetch()
        a.confirm(r.ticket)
        job.save()                                 # point 2
        r = a.fetch()
        a.confirm(r.ticket)
        job.save()                                 # point 3
        job.close()
        return src

    def test_damaged_head_point_fails_but_earlier_points_open(self):
        src = self._job_with_two_points()
        chk3 = os.path.join(self.dir, "jobs", "j", "checkpoints",
                            "000003.chk")
        with open(chk3, "r+b") as f:             # destroy the latest point
            f.truncate(os.path.getsize(chk3) // 2)
        with self.assertRaises(CheckpointCorrupt):
            self.store.open("j", src)
        older = self.store.open("j", src, checkpoint=2)
        self.assertEqual(older.session.group("A").fetch().seq, 3)
        # Saving from the older point commits a new head; the job is whole.
        chk = older.save()
        older.close()
        self.assertEqual(chk, 4)
        job = self.store.open("j", src)
        self.assertEqual(job.session.group("A").fetch().seq, 3)
        job.close()

    def test_a_crashed_save_is_invisible(self):
        specs = alternating(8, length=2)
        src = MockSource(specs, max_len=2)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"]}, 2, 8)
        r = job.session.group("A").fetch()
        job.session.group("A").confirm(r.ticket)
        job.save()
        job.close()
        chkdir = os.path.join(self.dir, "jobs", "j", "checkpoints")
        # A torn commit: a garbage next point, HEAD never advanced.
        with open(os.path.join(chkdir, "000003.chk"), "wb") as f:
            f.write(b"SHPCHK1 not-a-checkpoint")
        with open(os.path.join(chkdir, "000004.chk.tmp"), "wb") as f:
            f.write(b"half-written")
        job2 = self.store.open("j", src)
        self.assertEqual(job2.checkpoints(), (1, 2))
        self.assertEqual(job2.session.group("A").fetch().seq, 3)
        with self.assertRaises(JobError):
            self.store.open("j", src, checkpoint=3)
        # The next save overwrites the orphan and commits cleanly.
        self.assertEqual(job2.save(), 3)
        job2.close()
        job3 = self.store.open("j", src)
        self.assertEqual(job3.checkpoints(), (1, 2, 3))
        job3.close()

    def test_a_missing_checkpoint_file_is_an_explicit_failure(self):
        src = self._job_with_two_points()
        os.unlink(os.path.join(self.dir, "jobs", "j", "checkpoints",
                               "000003.chk"))
        with self.assertRaises(CheckpointCorrupt):
            self.store.open("j", src)
        job = self.store.open("j", src, checkpoint=2)
        job.close()

    def test_a_corrupted_body_is_detected(self):
        src = self._job_with_two_points()
        chk3 = os.path.join(self.dir, "jobs", "j", "checkpoints",
                            "000003.chk")
        with open(chk3, "r+b") as f:
            blob = bytearray(f.read())
            blob[-10] ^= 0xFF                    # flip a bit in the body
            f.seek(0)
            f.write(blob)
        with self.assertRaises(CheckpointCorrupt):
            self.store.open("j", src)


class SemanticsAfterRestoreTests(JobTestCase):
    def test_reconnect_still_works_after_restore(self):
        specs = alternating(30, length=3)
        path = self.feed(specs, 3)
        job = self.store.create("j", FileSource(path),
                                {"a": ["A"], "b": ["B"]}, 3, 12)
        a = job.session.group("A")
        for _ in range(3):
            r = a.fetch()
            a.confirm(r.ticket)
        job.save()
        job.close()

        job2 = self.store.open("j", FileSource(path))
        sess = job2.session
        # Replace the transport mid-session; nothing is replayed or lost.
        pos = sess._sources[0].link.position()
        sess.reconnect(FileSource(path).open(pos))
        got = service(sess)
        self.assertEqual([1, 3, 5] + got["A"],
                         [s.seq for s in specs if s.category == "a"])
        self.assertEqual(got["B"],
                         [s.seq for s in specs if s.category == "b"])
        self.assertEqual(sess.finish(), "end")
        job2.close()

    def test_save_during_concurrent_traffic_is_consistent(self):
        specs = [Spec(i + 1, "abc"[i % 3], 1 + i % 8) for i in range(2000)]
        src = MockSource(specs, max_len=8)
        job = self.store.create("j", src, {"a": ["A"], "b": ["B"],
                                           "c": ["C"]}, 8, 64)
        got = {"A": [], "B": [], "C": []}
        lock = threading.Lock()
        stop = threading.Event()

        def worker(name):
            h = job.session.group(name)
            while not stop.is_set():
                rec = h.fetch(block=True, timeout=0.2)
                if rec is False:
                    return
                if rec is None:
                    continue
                with lock:
                    got[name].append(rec.seq)
                h.confirm(rec.ticket)

        threads, errors = run_threads(3, lambda i: worker("ABC"[i]))
        while job.session.finish() != "end":
            job.save()                             # snapshot under traffic
            time.sleep(0.001)
        stop.set()
        join(threads, timeout=30)
        self.assertFalse(errors)
        job.save()
        job.close()

        job2 = self.store.open("j", src)
        self.assertEqual(job2.session.finish(), "end")
        for name in "ABC":
            expected = [s.seq for s in specs if s.category == name.lower()]
            self.assertEqual(got[name], expected)
            self.assertEqual(job2.session.group(name).delivered,
                             len(expected))
        self.assertEqual(job2.session.stats()["billed"], 0)
        job2.close()

    def test_large_feed_across_many_recovers(self):
        # 40k records of up to 64 bytes with a 128-byte quota, saved and
        # reopened every 4k records: the stream is far larger than anything
        # ever held in memory, and every recovery is exact.
        n = 40_000
        specs = uniform_specs(n, "abc", min_len=1, max_len=64, seed=5)
        path = self.feed(specs, 64)
        self.assertGreater(os.path.getsize(path), 128 * 1000)
        expected = {g: [s.seq for s in specs if g in CATS[s.category]]
                    for g in "ABC"}
        got = {g: [] for g in "ABC"}
        peak = 0
        job = self.store.create("j", FileSource(path), CATS, 64, 128)
        while True:
            handles = {g: job.session.group(g) for g in "ABC"}
            target = 4_000
            while target > 0:
                progressed = False
                for g, h in handles.items():
                    rec = h.fetch(block=False)
                    if rec in (None, False):
                        continue
                    got[g].append(rec.seq)
                    h.confirm(rec.ticket)
                    target -= 1
                    progressed = True
                    if target == 0:
                        break
                if not progressed:
                    break
            peak = max(peak, job.session.stats()["peak_billed"])
            done = job.session.finish() == "end"
            job.save()
            job.close()
            if done:
                break
            job = self.store.open("j", FileSource(path))
        self.assertEqual(got, expected)
        self.assertLessEqual(peak, 128)
        self.assertEqual(job.session.stats()["billed"], 0)


class FeedTests(JobTestCase):
    def test_roundtrip_and_positions(self):
        specs = alternating(5, length=7)
        path = self.feed(specs, 7)
        src = FileSource(path)
        self.assertEqual(src.count, 5)
        self.assertEqual(src.max_len, 7)
        link = src.link()
        self.assertEqual(link.announce().category, "a")
        self.assertEqual(link.announce().category, "a")   # non-consuming
        for i, s in enumerate(specs):
            pulled = link.take()
            self.assertEqual(pulled.seq, s.seq)
            self.assertEqual(pulled.data, feed_data(s.seq, s.length))
            self.assertEqual(link.position(),
                             {"consumed": i + 1, "last_seq": s.seq})
        self.assertIsNone(link.announce())
        with self.assertRaises(StopIteration):
            link.take()
        src.close()

    def test_resume_reads_exactly_the_tail(self):
        specs = alternating(10, length=5)
        path = self.feed(specs, 5)
        src = FileSource(path)
        link = src.open({"consumed": 4, "last_seq": 4})
        self.assertEqual([link.take().seq for _ in range(6)], [5, 6, 7, 8,
                                                               9, 10])
        with self.assertRaises(StopIteration):
            link.take()
        src.close()

    def test_identity_is_stable_per_file_and_differs_between_files(self):
        specs = alternating(3, length=4)
        p1 = self.feed(specs, 4, "x.bin")
        p2 = self.feed(specs, 4, "y.bin")
        self.assertEqual(FileSource(p1).identity(), FileSource(p1).identity())
        self.assertNotEqual(FileSource(p1).identity(),
                            FileSource(p2).identity())

    def test_a_truncated_feed_is_a_source_error(self):
        specs = alternating(4, length=8)
        path = self.feed(specs, 8)
        with open(path, "r+b") as f:
            f.truncate(HEADER_SIZE + 2 * (FRAME_HEAD.size + 32 + 8))
        with self.assertRaises(SourceError):
            FileSource(path)

    def test_a_lying_frame_is_a_source_error(self):
        specs = alternating(3, length=8)
        path = self.feed(specs, 8)
        # Corrupt the length field of the second record on disk.
        off = HEADER_SIZE + (FRAME_HEAD.size + 32 + 8) + 8
        with open(path, "r+b") as f:
            f.seek(off)
            f.write(struct.pack(">I", 0))
        src = FileSource(path)
        link = src.link()
        link.take()
        with self.assertRaises(SourceError):
            link.announce()
        with self.assertRaises(SourceError):     # sticky
            link.take()
        src.close()

    def test_a_promise_mismatch_is_a_source_error(self):
        specs = alternating(3, length=8)
        path = self.feed(specs, 8)
        src = FileSource(path)
        link = src.link()
        link.announce()
        # Rewrite the first frame's category underneath the live promise.
        with open(path, "r+b") as f:
            f.seek(HEADER_SIZE + FRAME_HEAD.size)
            f.write(b"z")
        with self.assertRaises(SourceError):
            link.take()
        src.close()


if __name__ == "__main__":
    unittest.main()
