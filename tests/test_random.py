"""Randomised model check: the fixture must behave exactly like a reference.

The reference model is deliberately naive: it computes, for every record, which
groups receive it, and for every group the ordered list of records it owes.
Whatever the driver does (random prefetching, random acknowledges, random
tickets, random exits, random reconnects), the outcome must be explainable as
"some prefix of the model's sequences" and the accounting must return to zero.
"""

import random
import unittest

from shipper import ProtocolError, SourceError, GroupGone, MockSource, Session
from shipper.source import Spec

from helpers import make


def reference(specs, cats):
    """``{group: [seq...]}`` of what each group must be offered, in order."""
    out = {name: [] for name in {n for ns in cats.values() for n in ns}}
    for s in specs:
        for name in cats.get(s.category, ()):
            out[name].append(s.seq)
    return out


class RandomDriver:
    def __init__(self, sess, cats, rng):
        self.sess = sess
        self.rng = rng
        self.groups = {n: sess.group(n) for ns in cats.values() for n in ns}
        self.delivered = {n: [] for n in self.groups}
        self.gone = set()
        self.errors = []

    def run(self, steps=4000):
        rng = self.rng
        for _ in range(steps):
            live = [n for n in self.groups if n not in self.gone]
            if not live:
                break
            name = rng.choice(live)
            g = self.groups[name]
            move = rng.random()
            try:
                if move < 0.55:
                    rec = g.fetch(block=False)
                    if rec in (None, False):
                        continue
                    if rng.random() < 0.75:
                        g.confirm(rec.ticket)
                        self.delivered[name].append(rec.seq)
                    elif rng.random() < 0.5:
                        # A wrong number must be refused and change nothing.
                        try:
                            g.confirm(rec.ticket + rng.randint(1, 3))
                        except ProtocolError:
                            pass
                        self.delivered[name].append(rec.seq)
                        g.confirm(rec.ticket)
                elif move < 0.70:
                    rec = g.fetch(block=True, timeout=0.002)
                    if rec in (None, False):
                        continue
                    g.confirm(rec.ticket)
                    self.delivered[name].append(rec.seq)
                elif move < 0.80:
                    g.peek()
                elif move < 0.85:
                    self.sess.announce()
                elif move < 0.90:
                    self.sess.finish()
                elif move < 0.95:
                    if rng.random() < 0.25:
                        g.exit()
                        self.gone.add(name)
                else:
                    self.sess.reconnect(self.sess._sources[0].link)
            except GroupGone:
                self.gone.add(name)
            except SourceError as exc:
                self.errors.append(exc)
                return
            except ProtocolError:
                raise
        self.finish_tail()

    def finish_tail(self):
        for name in list(self.groups):
            if name in self.gone:
                continue
            g = self.groups[name]
            while True:
                rec = g.fetch(block=False)
                if rec in (None, False):
                    break
                g.confirm(rec.ticket)
                self.delivered[name].append(rec.seq)
        while True:
            progressed = False
            for name, g in self.groups.items():
                if name in self.gone:
                    continue
                rec = g.fetch(block=False)
                if rec in (None, False):
                    continue
                g.confirm(rec.ticket)
                self.delivered[name].append(rec.seq)
                progressed = True
            if not progressed:
                break
        for _ in range(100):
            if self.sess.finish() == "end":
                break
            progressed = False
            for name, g in self.groups.items():
                if name in self.gone:
                    continue
                rec = g.fetch(block=False)
                if rec in (None, False):
                    continue
                g.confirm(rec.ticket)
                self.delivered[name].append(rec.seq)
                progressed = True
            if not progressed and self.sess.finish() != "end":
                break


class RandomisedEquivalenceTests(unittest.TestCase):
    def _one(self, seed):
        rng = random.Random(seed)
        n = rng.randint(1, 120)
        cats = {"a": ["A"], "b": ["B"], "c": ["C", "D"]}
        alphabet = "abc"
        specs = [Spec(i + 1, rng.choice(alphabet), rng.randint(1, 12))
                 for i in range(n)]
        max_len = max(s.length for s in specs)
        quota = rng.randint(max_len, max_len * 4 + 8)
        src = MockSource(specs, max_len=max_len)
        sess = Session(src.link(), cats, max_len, quota)
        drv = RandomDriver(sess, cats, rng)
        drv.run()

        ref = reference(specs, cats)
        for name in drv.groups:
            if name in drv.gone:
                # Every acknowledged record must be a prefix of the model's
                # sequence: no skipping, no reordering, no duplicates.
                self.assertEqual(drv.delivered[name],
                                 ref[name][:len(drv.delivered[name])],
                                 "seed %d group %s" % (seed, name))
            else:
                self.assertEqual(drv.delivered[name], ref[name],
                                 "seed %d group %s" % (seed, name))
        # Nothing leaked and nothing was over-billed at any point.
        stats = sess.stats()
        self.assertLessEqual(stats["peak_billed"], quota)
        self.assertEqual(stats["billed"], 0, "seed %d: %r" % (seed, stats))
        self.assertEqual(stats["staged"], 0)
        self.assertIn(sess.finish(), ("end", "source_error"))

    def test_random_schedules(self):
        for seed in range(60):
            with self.subTest(seed=seed):
                self._one(seed)


class PrefixOnExitTests(unittest.TestCase):
    def test_exit_leaves_exactly_a_prefix(self):
        for seed in range(20):
            rng = random.Random(1000 + seed)
            specs = [Spec(i + 1, rng.choice("abc"), rng.randint(1, 8))
                     for i in range(60)]
            max_len = max(s.length for s in specs)
            src = MockSource(specs, max_len=max_len)
            cats = {"a": ["A"], "b": ["B"], "c": ["C"]}
            sess = Session(src.link(), cats, max_len, max_len * 3)
            a, b = sess.group("A"), sess.group("B")
            got = []
            for _ in range(rng.randint(1, 8)):
                rec = a.fetch(block=True, timeout=1)
                if rec in (None, False):
                    break
                got.append(rec.seq)
                a.confirm(rec.ticket)
            a.exit()
            self.assertEqual(got, reference(specs, cats)["A"][:len(got)])


if __name__ == "__main__":
    unittest.main()
