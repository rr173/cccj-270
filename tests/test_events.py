"""Event rollups: strict parsing, 300s windows, per-group totals, recovery.

The event mode is an option on top of the ordinary delivery contract: records
are JSON events, and only a record a group has actually *confirmed* is folded
into that group's time-window rollup.  These tests cover

* window alignment (left-closed/right-open, including negative times);
* strict payload validation -- bad JSON and any missing/extra/mistyped field
  fail loudly and leave the record unconfirmed and the totals untouched;
* out-of-order event times, per-group/category/metric sums and filters;
* duplicate confirmations never double-count, unconfirmed records never count;
* rollups that survive a recovery point, including damaged-point detection;
* memory bounded by distinct keys rather than by the number of events.
"""

import json
import os
import shutil
import tempfile
import unittest

from shipper import (CheckpointCorrupt, EventError, EventRollup, FeedWriter,
                     FileSource, JobMismatch, JobStore, ProtocolError,
                     parse_event, window_bounds, window_of)
from shipper.job import decode_checkpoint, encode_checkpoint
from shipper.source import Spec

from helpers import service

WIDTH = 300
CATS = {"heat": ["A", "C"], "load": ["B"], "shared": ["A", "B"]}


def event_bytes(event_time, metric, value):
    return json.dumps({"event_time": event_time, "metric": metric,
                       "value": value}).encode("utf-8")


def feed(path, records, max_len=None):
    """``records`` is a list of ``(seq, category, payload_bytes)``."""
    if max_len is None:
        max_len = max(len(p) for _, _, p in records)
    with FeedWriter(path, max_len) as w:
        for seq, category, data in records:
            w.append(seq, category, data)
    return path, max_len


def event_records(specs):
    """``(seq, category, t, metric, value)`` tuples -> feed record tuples."""
    out = []
    for seq, category, t, metric, value in specs:
        out.append((seq, category, event_bytes(t, metric, value)))
    return out


class WindowMathTests(unittest.TestCase):
    def test_left_closed_right_open_alignment(self):
        self.assertEqual(window_of(0, WIDTH), 0)
        self.assertEqual(window_of(299, WIDTH), 0)
        self.assertEqual(window_of(300, WIDTH), 1)
        self.assertEqual(window_of(599, WIDTH), 1)
        self.assertEqual(window_of(600, WIDTH), 2)
        self.assertEqual(window_bounds(0, WIDTH), (0, 300))
        self.assertEqual(window_bounds(1, WIDTH), (300, 600))

    def test_negative_times_floor_into_the_window_they_belong_to(self):
        self.assertEqual(window_of(-1, WIDTH), -1)
        self.assertEqual(window_of(-300, WIDTH), -1)
        self.assertEqual(window_of(-301, WIDTH), -2)
        self.assertEqual(window_bounds(-1, WIDTH), (-300, 0))

    def test_a_member_is_ge_start_and_lt_end(self):
        for t in (-601, -300, -1, 0, 299, 300, 599, 600, 123456789):
            w = window_of(t, WIDTH)
            start, end = window_bounds(w, WIDTH)
            self.assertGreaterEqual(t, start)
            self.assertLess(t, end)


class ParseEventTests(unittest.TestCase):
    def test_a_valid_event_decodes(self):
        self.assertEqual(parse_event(event_bytes(123, "m", -7)),
                         (123, "m", -7))

    def test_malformed_json_fails(self):
        for bad in (b"{not json", b"", b"\xff\xfe", b"[1,2]", b"123",
                    b'"a string"', b"null"):
            with self.subTest(bad=bad):
                with self.assertRaises(EventError):
                    parse_event(bad)

    def test_missing_fields_fail(self):
        for doc in ({"event_time": 1, "metric": "m"},
                    {"metric": "m", "value": 1},
                    {"event_time": 1, "value": 1},
                    {}):
            with self.subTest(doc=doc):
                with self.assertRaises(EventError):
                    parse_event(json.dumps(doc).encode())

    def test_extra_fields_fail(self):
        doc = {"event_time": 1, "metric": "m", "value": 1, "extra": 2}
        with self.assertRaises(EventError):
            parse_event(json.dumps(doc).encode())

    def test_wrong_types_fail(self):
        good = {"event_time": 1, "metric": "m", "value": 1}
        variants = [
            {"event_time": 1.5}, {"event_time": "1"}, {"event_time": True},
            {"event_time": None}, {"metric": 1}, {"metric": None},
            {"metric": ["m"]}, {"value": 1.0}, {"value": "1"},
            {"value": True}, {"value": None},
        ]
        for change in variants:
            doc = dict(good, **change)
            with self.subTest(doc=doc):
                with self.assertRaises(EventError):
                    parse_event(json.dumps(doc).encode())

    def test_duplicate_keys_fail(self):
        text = b'{"event_time": 1, "metric": "m", "value": 1, "value": 2}'
        with self.assertRaises(EventError):
            parse_event(text)

    def test_big_integers_are_accepted(self):
        big = 10 ** 30
        t, metric, value = parse_event(event_bytes(big, "m", -big))
        self.assertEqual((t, metric, value), (big, "m", -big))


class RollupUnitTests(unittest.TestCase):
    def test_sums_and_counts_per_key(self):
        roll = EventRollup(WIDTH)
        roll.add("A", "heat", 0, "clicks", 10)
        roll.add("A", "heat", 299, "clicks", 5)     # same window/key
        roll.add("A", "heat", 300, "clicks", 7)     # next window
        roll.add("A", "heat", 610, "views", 2)      # another metric
        roll.add("B", "load", 0, "clicks", 100)     # another group
        rows = {(r.window, r.category, r.metric): (r.total, r.events)
                for r in roll.windows("A")}
        self.assertEqual(rows[(0, "heat", "clicks")], (15, 2))
        self.assertEqual(rows[(1, "heat", "clicks")], (7, 1))
        self.assertEqual(rows[(2, "heat", "views")], (2, 1))
        self.assertEqual(roll.total("A"), 24)
        self.assertEqual(roll.total("A", metric="clicks"), 22)
        self.assertEqual(roll.total("A", category="heat"), 24)
        self.assertEqual(roll.event_count("A"), 4)
        self.assertEqual(roll.total("B"), 100)

    def test_rows_are_sorted_and_filters_narrow(self):
        roll = EventRollup(WIDTH)
        roll.add("A", "c", 600, "z", 1)
        roll.add("A", "c", 0, "a", 1)
        roll.add("A", "c", 0, "z", 1)
        keys = [(r.window, r.metric) for r in roll.windows("A")]
        self.assertEqual(keys, [(0, "a"), (0, "z"), (2, "z")])
        self.assertEqual([r.metric for r in roll.windows("A", metric="z")],
                         ["z", "z"])
        self.assertEqual(roll.windows("A", category="missing"), ())
        self.assertEqual(roll.windows("nobody"), ())

    def test_snapshot_roundtrip(self):
        roll = EventRollup(WIDTH)
        roll.add("A", "heat", -5, "m", 9)
        roll.add("B", "load", 700, "n", -3)
        again = EventRollup.restore(roll.snapshot())
        self.assertEqual(again.width, WIDTH)
        self.assertEqual(
            {(r.window, r.category, r.metric, r.total, r.events)
             for r in again.windows("A")},
            {(window_of(-5, WIDTH), "heat", "m", 9, 1)})
        self.assertEqual(again.total("B"), -3)

    def test_restore_rejects_damaged_documents(self):
        good = EventRollup(WIDTH)
        good.add("A", "heat", 0, "m", 1)
        base = good.snapshot()
        bad_docs = []
        d = json.loads(json.dumps(base))
        d["width"] = 0
        bad_docs.append(d)
        d = json.loads(json.dumps(base))
        d["groups"][0]["rows"][0]["events"] = 0
        bad_docs.append(d)
        d = json.loads(json.dumps(base))
        d["groups"][0]["rows"][0]["total"] = "1"
        bad_docs.append(d)
        d = json.loads(json.dumps(base))
        d["groups"].append(dict(d["groups"][0]))     # group twice
        bad_docs.append(d)
        d = json.loads(json.dumps(base))
        d["groups"][0]["rows"].append(
            dict(d["groups"][0]["rows"][0]))        # row key twice
        bad_docs.append(d)
        for doc in bad_docs:
            with self.subTest(doc=doc):
                from shipper import CheckpointCorrupt
                with self.assertRaises(CheckpointCorrupt):
                    EventRollup.restore(doc)


class EventSessionTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="shipper-events-")
        self.addCleanup(shutil.rmtree, self.dir, True)

    def make_job(self, specs, cats=CATS, name="j", quota=None,
                 window=WIDTH):
        path = os.path.join(self.dir, name + ".bin")
        path, max_len = feed(path, event_records(specs))
        quota = quota or max_len * 4
        store = JobStore(os.path.join(self.dir, "jobs"))
        job = store.create(name, FileSource(path), cats, max_len, quota,
                           event_window=window)
        return job, path, max_len, quota

    def reopen(self, path, max_len, quota, cats=CATS, window=WIDTH,
               checkpoint=None):
        store = JobStore(os.path.join(self.dir, "jobs"))
        return store.open("j", FileSource(path), checkpoint=checkpoint)


class EndToEndTests(EventSessionTestCase):
    SPECS = [
        # seq, category, event_time, metric, value
        (1, "heat", 10, "clicks", 10),       # A,C w0
        (2, "load", 299, "cpu", 5),          # B   w0 (299 still in w0)
        (3, "heat", 300, "clicks", 7),       # A,C w1 (boundary)
        (4, "shared", 610, "hits", 3),       # A,B w2; time later, seq earlier
        (5, "shared", 599, "hits", 4),       # A,B w1; out-of-order event time
        (6, "heat", -1, "clicks", 2),        # A,C w-1 (negative time)
    ]

    def test_window_boundaries_and_out_of_order_times(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        got = service(job.session)
        self.assertEqual(got["A"], [1, 3, 4, 5, 6])
        a = job.session.group("A")
        rows = {(r.window, r.category, r.metric): (r.total, r.events)
                for r in a.event_windows()}
        self.assertEqual(rows[(-1, "heat", "clicks")], (2, 1))
        self.assertEqual(rows[(0, "heat", "clicks")], (10, 1))
        self.assertEqual(rows[(1, "heat", "clicks")], (7, 1))
        # out-of-order: t=599 (w1) and t=610 (w2) land in different windows
        self.assertEqual(rows[(1, "shared", "hits")], (4, 1))
        self.assertEqual(rows[(2, "shared", "hits")], (3, 1))
        self.assertEqual(a.event_total(), 26)
        self.assertEqual(a.event_count(), 5)
        job.close()

    def test_groups_keep_separate_totals_for_shared_records(self):
        job, path, *_ = self.make_job(self.SPECS)
        service(job.session)
        sess = job.session
        a, b, c = (sess.group(n) for n in ("A", "B", "C"))
        # A receives heat + shared; B receives load + shared; C only heat.
        self.assertEqual(a.event_total(), 26)
        self.assertEqual(b.event_total(), 12)          # 5 + 3 + 4
        self.assertEqual(c.event_total(), 19)          # 10 + 7 + 2
        self.assertEqual(
            a.event_total(category="shared"), 7)
        self.assertEqual(
            b.event_total(category="shared"), 7)
        self.assertEqual(
            c.event_total(category="heat"), 19)
        # The shared payload was billed once but counted per confirming group.
        self.assertEqual(sess.event_count("A"), 5)
        self.assertEqual(sess.event_count("B"), 3)
        self.assertEqual(sess.event_count("C"), 3)
        job.close()

    def test_unconfirmed_records_are_not_counted(self):
        job, path, *_ = self.make_job(self.SPECS)
        sess = job.session
        a = sess.group("A")
        first = a.fetch()                          # staged, not confirmed
        self.assertEqual(first.seq, 1)
        self.assertEqual(a.event_windows(), ())
        self.assertEqual(a.event_total(), 0)
        # Another group confirming a shared record does not count it for A.
        b = sess.group("B")
        rec_b = b.fetch()
        b.confirm(rec_b.ticket)
        self.assertEqual(a.event_windows(), ())
        self.assertEqual(b.event_count(), 1)
        job.close()

    def test_retrying_a_confirmation_does_not_double_count(self):
        job, path, *_ = self.make_job(self.SPECS)
        a = job.session.group("A")
        rec = a.fetch()
        self.assertTrue(a.confirm(rec.ticket))
        total_after = a.event_total()
        count_after = a.event_count()
        self.assertTrue(a.confirm(rec.ticket))      # harmless retry
        self.assertTrue(a.confirm(rec.ticket))
        self.assertEqual(a.event_total(), total_after)
        self.assertEqual(a.event_count(), count_after)
        job.close()

    def test_wrong_ticket_is_refused_and_nothing_is_counted(self):
        job, path, *_ = self.make_job(self.SPECS)
        a = job.session.group("A")
        rec = a.fetch()
        with self.assertRaises(ProtocolError):
            a.confirm(rec.ticket + 1)
        self.assertEqual(a.event_count(), 0)
        self.assertEqual(a.peek().ticket, rec.ticket)
        job.close()


class BadPayloadTests(EventSessionTestCase):
    BAD = [
        # Well-formed JSON with semantic problems first, so each failure mode
        # is actually reached: a bad record blocks the head behind it, and the
        # purely syntactic cases (malformed/truncated JSON) go last.  The
        # parser-level syntax rejections are covered in ParseEventTests.
        (1, "heat", json.dumps({"metric": "m", "value": 1}).encode()),
        (2, "heat", json.dumps(
            {"event_time": 1, "metric": "m", "value": 1, "x": 0}).encode()),
        (3, "heat", json.dumps(
            {"event_time": 1.5, "metric": "m", "value": 1}).encode()),
        (4, "heat", json.dumps(
            {"event_time": 1, "metric": 7, "value": 1}).encode()),
        (5, "heat", json.dumps(
            {"event_time": 1, "metric": "m", "value": "1"}).encode()),
        (6, "heat", json.dumps(
            {"event_time": True, "metric": "m", "value": 1}).encode()),
        (7, "heat", b"{not json"),                          # malformed JSON
        (8, "heat", b'{"event_time": 1, "metric": "m'),     # truncated JSON
    ]

    def test_each_bad_payload_fails_and_blocks_its_confirmation(self):
        # One job per bad payload: the bad record is the head, a good record
        # sits behind it, and the failed confirmation must leave both the
        # ticket and the totals untouched while the head stays in place.
        for seq, category, data in self.BAD:
            records = [(seq, category, data),
                       (seq + 100, category, event_bytes(0, "ok", 4))]
            max_len = max(len(p) for _, _, p in records) + 4
            path = os.path.join(self.dir, "bad-%d.bin" % seq)
            feed(path, records, max_len=max_len)
            store = JobStore(os.path.join(self.dir, "jobs-%d" % seq))
            job = store.create("j", FileSource(path),
                               {"heat": ["A"]}, max_len, max_len * 4,
                               event_window=WIDTH)
            a = job.session.group("A")
            rec = a.fetch()
            self.assertEqual(rec.seq, seq)
            with self.assertRaises(EventError):
                a.confirm(rec.ticket)
            # Nothing moved: same ticket still the head, empty totals.
            self.assertEqual(a.peek().ticket, rec.ticket)
            self.assertEqual(a.event_count(), 0)
            self.assertEqual(a.event_total(), 0)
            # The good record behind it is not reachable while the head is bad.
            self.assertEqual(a.fetch().seq, seq)
            job.close()

    def test_a_bad_head_blocks_until_exited_then_tail_flows(self):
        good = event_records([(9, "heat", 0, "ok", 4)])
        all_records = self.BAD[:1] + good
        max_len = max(len(p) for _, _, p in all_records) + 4
        path = os.path.join(self.dir, "bad-head.bin")
        feed(path, all_records, max_len=max_len)
        store = JobStore(os.path.join(self.dir, "jobs-head"))
        job = store.create("j", FileSource(path), {"heat": ["A"]},
                           max_len, max_len * 4, event_window=WIDTH)
        a = job.session.group("A")
        rec = a.fetch()
        self.assertEqual(rec.seq, 1)
        with self.assertRaises(EventError):
            a.confirm(rec.ticket)
        # Exiting abandons the unconfirmable head; the group is gone, and no
        # event was ever folded for it.
        self.assertEqual(a.exit(), 1)
        self.assertEqual(job.session.stats()["billed"], 0)
        job.close()

    def test_bad_record_in_one_group_does_not_touch_another_group(self):
        bad = [(1, "heat", b"{broken"), (2, "load", event_bytes(0, "cpu", 8))]
        path = os.path.join(self.dir, "mix.bin")
        path, max_len = feed(path, bad)
        store = JobStore(os.path.join(self.dir, "jobs"))
        job = store.create("j", FileSource(path),
                           {"heat": ["A"], "load": ["B"]}, max_len,
                           max_len * 4, event_window=WIDTH)
        a, b = job.session.group("A"), job.session.group("B")
        bad_rec = a.fetch()
        with self.assertRaises(EventError):
            a.confirm(bad_rec.ticket)
        good = b.fetch()
        b.confirm(good.ticket)
        self.assertEqual(b.event_total(), 8)
        self.assertEqual(a.event_total(), 0)
        job.close()


class RecoveryTests(EventSessionTestCase):
    SPECS = EndToEndTests.SPECS

    def test_rollup_resumes_exactly_after_restart(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        a = job.session.group("A")
        for _ in range(2):
            rec = a.fetch()
            a.confirm(rec.ticket)
        before = [(r.window, r.category, r.metric, r.total, r.events)
                  for r in a.event_windows()]
        chk = job.save()
        job.close()

        job2 = self.reopen(path, max_len, quota)
        self.assertEqual(job2.session.event_window, WIDTH)
        a2 = job2.session.group("A")
        self.assertEqual(a2.event_count(), 2)
        self.assertEqual(
            [(r.window, r.category, r.metric, r.total, r.events)
             for r in a2.event_windows()], before)
        # Finish the stream in the new process; totals match all confirms.
        service(job2.session)
        self.assertEqual(a2.event_total(), 26)
        self.assertEqual(a2.event_count(), 5)
        self.assertEqual(job2.session.group("B").event_total(), 12)
        self.assertEqual(job2.session.group("C").event_total(), 19)
        job2.save()
        job2.close()

        job3 = self.reopen(path, max_len, quota)
        self.assertEqual(job3.session.group("A").event_total(), 26)
        self.assertEqual(job3.session.finish(), "end")
        job3.close()
        self.assertGreaterEqual(chk, 2)

    def test_an_older_checkpoint_reopens_its_own_rollup(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        a = job.session.group("A")
        a.confirm(a.fetch().ticket)
        first = job.save()
        service(job.session)
        job.save()
        job.close()
        old = self.reopen(path, max_len, quota, checkpoint=first)
        self.assertEqual(old.session.group("A").event_count(), 1)
        old.close()

    def test_unconfirmed_at_save_is_not_counted_after_reopen(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        a = job.session.group("A")
        held = a.fetch()                          # never confirmed
        job.save()
        job.close()
        job2 = self.reopen(path, max_len, quota)
        a2 = job2.session.group("A")
        self.assertEqual(a2.event_count(), 0)
        self.assertEqual(a2.fetch().ticket, held.ticket)
        a2.confirm(a2.fetch().ticket)
        self.assertEqual(a2.event_count(), 1)
        job2.close()

    def test_a_damaged_rollup_is_refused_not_silently_adopted(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        service(job.session)
        job.save()
        job.close()
        chk = os.path.join(self.dir, "jobs", "j", "checkpoints",
                           "%06d.chk" % job.head)
        # Flip a byte inside the JSON body; the checksum must catch it.
        with open(chk, "r+b") as f:
            blob = bytearray(f.read())
            blob[-10] ^= 0xFF
            f.seek(0)
            f.write(blob)
        with self.assertRaises(CheckpointCorrupt):
            self.reopen(path, max_len, quota)

    def test_rollup_disagreement_with_delivery_counter_is_refused(self):
        from shipper.job import _decode_state, _encode_state
        job, path, max_len, quota = self.make_job(self.SPECS)
        a = job.session.group("A")
        a.confirm(a.fetch().ticket)
        job.save()
        head = job.head
        job.close()
        chk = os.path.join(self.dir, "jobs", "j", "checkpoints",
                           "%06d.chk" % head)
        with open(chk, "rb") as f:
            doc = decode_checkpoint(f.read(), "point")
        state = _decode_state(doc["state"], "point")
        # Forge an extra folded event that no confirmation ever produced.
        state["events"]["groups"][0]["rows"][0]["events"] += 1
        state["events"]["groups"][0]["rows"][0]["total"] += 1
        doc["state"] = _encode_state(state)
        with open(chk, "wb") as f:
            f.write(encode_checkpoint(doc))
        with self.assertRaises(CheckpointCorrupt):
            self.reopen(path, max_len, quota)

    def test_event_mode_is_part_of_the_job_configuration(self):
        job, path, max_len, quota = self.make_job(self.SPECS)
        job.close()
        with open(os.path.join(self.dir, "jobs", "j", "job.json")) as f:
            meta = json.load(f)
        self.assertEqual(meta["config"]["event_window"], WIDTH)
        # The fingerprint covers the event mode: tampering is detected.
        meta["config"]["event_window"] = 60
        with open(os.path.join(self.dir, "jobs", "j", "job.json"), "w") as f:
            json.dump(meta, f)
        with self.assertRaises((JobMismatch, CheckpointCorrupt)):
            self.reopen(path, max_len, quota)


class ScaleTests(EventSessionTestCase):
    def test_memory_is_bounded_by_distinct_keys_not_event_count(self):
        # Many events, but only a handful of (window, category, metric) keys.
        n = 20_000
        specs = []
        for i in range(1, n + 1):
            t = (i * 7) % 900                   # only windows 0..2 appear
            metric = "m%d" % (i % 4)           # four metrics
            specs.append((i, "heat", t, metric, i % 97 - 48))
        path = os.path.join(self.dir, "scale.bin")
        path, max_len = feed(path, event_records(specs))
        small_quota = max_len * 2
        store = JobStore(os.path.join(self.dir, "jobs"))
        job = store.create("j", FileSource(path), {"heat": ["A"]},
                           max_len, small_quota, event_window=WIDTH)
        quota = small_quota
        service(job.session)
        rollup = job.session.events
        # 3 windows * 4 metrics = 12 keys, regardless of the 20k events.
        self.assertEqual(rollup.key_count(), 12)
        self.assertEqual(job.session.group("A").event_count(), n)
        snap = job.session._snapshot()
        rows = sum(len(g["rows"]) for g in snap["events"]["groups"])
        self.assertEqual(rows, 12)
        # The staged payloads in the snapshot stay bounded by the small quota.
        self.assertLessEqual(len(snap["payloads"]), 2)
        chk = job.save()
        job.close()

        job2 = self.reopen(path, max_len, quota, cats={"heat": ["A"]})
        self.assertEqual(job2.session.group("A").event_count(), n)
        self.assertEqual(job2.session.events.key_count(), 12)
        # The on-disk point stores aggregates, not the 20k raw events.
        size = os.path.getsize(os.path.join(
            self.dir, "jobs", "j", "checkpoints", "%06d.chk" % chk))
        self.assertLess(size, 4_096)
        job2.close()

    def test_rollup_works_without_persistence_too(self):
        specs = event_records([(1, "heat", 0, "m", 2),
                               (2, "heat", 350, "m", 3)])
        path = os.path.join(self.dir, "plain.bin")
        path, max_len = feed(path, specs)
        from shipper import Session
        src = FileSource(path)
        try:
            sess = Session(src.link(), {"heat": ["A"]}, max_len,
                           max_len * 4, event_window=WIDTH)
            service(sess)
            self.assertEqual(sess.group("A").event_total(), 5)
            self.assertEqual(sess.stats()["events"]["window"], WIDTH)
        finally:
            src.close()

    def test_non_event_session_has_no_rollup(self):
        specs = [Spec(1, "heat", 4)]
        path = os.path.join(self.dir, "plain2.bin")
        with FeedWriter(path, 4) as w:
            w.append(1, "heat", b"abcd")
        from shipper import Session
        src = FileSource(path)
        try:
            sess = Session(src.link(), {"heat": ["A"]}, 4, 16)
            service(sess)
            self.assertIsNone(sess.events)
            self.assertIsNone(sess.stats()["events"])
            with self.assertRaises(ValueError):
                sess.event_windows("A")
        finally:
            src.close()


if __name__ == "__main__":
    unittest.main()
