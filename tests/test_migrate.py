"""Snapshot-to-snapshot alias migration: output shape, safety, failures, scale.

The contract under test:

* two ``FileSource`` snapshots in, JSON Lines out, one atomic batch per line;
* a changed account gets its *complete* new alias set (or is deleted), an
  unchanged account gets nothing, and an account appears at most once;
* from the old snapshot, after each committed batch every alias is on at most
  one account, and the end state is exactly the new snapshot;
* no batch exceeds ``max_ops``, no temporary account or intermediate alias is
  ever used;
* truncated feeds, invalid records, repeated ids/aliases and cross-account
  alias collisions raise ``MigrationError``, publish nothing and leave an
  earlier good output untouched;
* a hand-over cycle larger than ``max_ops`` is refused, and the refusal
  threshold is exactly the largest strongly connected component;
* only one batch is ever materialised, so memory stays bounded while inputs
  and outputs are much larger.
"""

import json
import os
import random
import shutil
import sys
import tempfile
import unittest

from shipper import (FeedWriter, Migration, MigrationError, MigrationResult,
                     migrate, parse_account)
from shipper.migrate_plan import iter_snapshot


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #
def write_snapshot(path, rows, max_len=None, seqs=None):
    """``rows`` is ``[(id, aliases), ...]`` (or raw byte payloads)."""
    payloads = []
    for row in rows:
        if isinstance(row, (bytes, bytearray)):
            payloads.append(bytes(row))
        else:
            account_id, aliases = row
            payloads.append(json.dumps({"id": account_id,
                                        "aliases": list(aliases)})
                            .encode("utf-8"))
    if not payloads:
        # A feed may legitimately hold zero records: a snapshot with no
        # accounts at all.
        max_len = max_len or 8
        with FeedWriter(path, max_len):
            pass
        return path
    if max_len is None:
        max_len = max(len(p) for p in payloads)
    with FeedWriter(path, max_len) as writer:
        if seqs is None:
            seqs = range(1, len(payloads) + 1)
        for seq, payload in zip(seqs, payloads):
            writer.append(seq, "account", payload)
    return path


def read_batches(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


class Replay:
    """Apply emitted batches to an in-memory table, checking the invariant."""

    def __init__(self, old, max_ops):
        self.table = {i: set(a) for i, a in old.items()}
        self.max_ops = max_ops
        self.seen = set()
        self.batches = []

    def apply(self, batch):
        self.batches.append(batch)
        self.assert_batch_shape(batch, len(self.batches))
        nxt = {i: set(a) for i, a in self.table.items()}
        for op in batch["ops"]:
            if op["id"] in self.seen:
                raise AssertionError("account %r appears twice" % op["id"])
            self.seen.add(op["id"])
            if op["op"] == "delete":
                del nxt[op["id"]]
            else:
                nxt[op["id"]] = set(op["aliases"])
        self.table = nxt
        self.assert_unique()

    def assert_batch_shape(self, batch, number):
        self.assert_ops_shape(batch)
        if batch["batch"] != number:
            raise AssertionError("batch %d numbered %r"
                                 % (number, batch["batch"]))
        if len(batch["ops"]) > self.max_ops:
            raise AssertionError("%d operations in a batch limited to %d"
                                 % (len(batch["ops"]), self.max_ops))

    def assert_ops_shape(self, batch):
        for op in batch["ops"]:
            if op["op"] == "upsert":
                aliases = op["aliases"]
                if not isinstance(aliases, list) or aliases != sorted(aliases):
                    raise AssertionError("aliases are not a sorted list: %r"
                                         % (op,))
                if len(set(aliases)) != len(aliases):
                    raise AssertionError("duplicate alias in %r" % (op,))
                if not all(isinstance(a, str) and a for a in aliases):
                    raise AssertionError("non-string alias in %r" % (op,))
            elif op["op"] == "delete":
                if set(op) != {"op", "id"}:
                    raise AssertionError("delete carries extra fields: %r"
                                         % (op,))
            else:
                raise AssertionError("unknown operation %r" % (op,))

    def assert_unique(self):
        owner = {}
        for account_id, aliases in self.table.items():
            for alias in aliases:
                if alias in owner:
                    raise AssertionError(
                        "alias %r is on both %r and %r after a batch"
                        % (alias, owner[alias], account_id))
                owner[alias] = account_id


class MigrationTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="shipper-migrate-")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.out = os.path.join(self.dir, "changes.jsonl")

    def snap(self, name, rows, **kw):
        return write_snapshot(os.path.join(self.dir, name), rows, **kw)

    def run_plan(self, old, new, max_ops=1000):
        return Migration(old, new).write(self.out, max_ops)

    def replay_file(self, old, max_ops):
        replay = Replay(dict(old), max_ops)
        for batch in read_batches(self.out):
            replay.apply(batch)
        return replay


class MigrateFunctionTests(MigrationTestCase):
    def test_the_module_level_helper(self):
        old = self.snap("old.bin", [("A", ["x"]), ("B", ["y"])])
        new = self.snap("new.bin", [("A", ["x", "y"])])
        result = migrate(old, new, self.out, 10)
        self.assertEqual((result.updates, result.deletes), (1, 1))
        self.assertEqual(self.replay_file({"A": ["x"], "B": ["y"]}, 10).table,
                         {"A": {"x", "y"}})

    def test_iter_ops_streams_the_same_operations(self):
        old = self.snap("old.bin", [("A", ["x"]), ("B", ["y"])])
        new = self.snap("new.bin", [("A", ["x"]), ("B", ["z"])])
        ops = list(Migration(old, new).iter_ops(1))
        self.assertEqual([op["id"] for op in ops], ["B"])
        self.assertEqual(ops[0]["aliases"], ["z"])


# --------------------------------------------------------------------- #
# Record parsing
# --------------------------------------------------------------------- #
class ParseAccountTests(unittest.TestCase):
    def test_aliases_are_a_sorted_set(self):
        _, aliases, canonical = parse_account(
            b'{"id": "A", "aliases": ["z", "a", "m"]}', 1)
        self.assertEqual(aliases, ["a", "m", "z"])
        _, aliases2, canonical2 = parse_account(
            b'{"id": "A", "aliases": ["a", "z", "m"]}', 2)
        self.assertEqual(canonical, canonical2)
        self.assertEqual(aliases, aliases2)

    def test_an_empty_alias_list_is_valid(self):
        self.assertEqual(parse_account(b'{"id": "A", "aliases": []}', 1)[1], [])

    def test_unicode_ids_and_aliases_round_trip(self):
        account_id, aliases, _ = parse_account(
            '{"id": "账号", "aliases": ["别名"]}'.encode("utf-8"), 1)
        self.assertEqual(account_id, "账号")
        self.assertEqual(aliases, ["别名"])

    def test_rejections(self):
        bad = [
            b"not json",
            b"",
            b"\xff\xfe",
            b"[]",
            b'"A"',
            b"123",
            b"null",
            b"{}",
            b'{"id": "A"}',
            b'{"aliases": []}',
            b'{"id": "A", "aliases": [], "x": 1}',
            b'{"id": 1, "aliases": []}',
            b'{"id": "", "aliases": []}',
            b'{"id": null, "aliases": []}',
            b'{"id": "A", "aliases": "x"}',
            b'{"id": "A", "aliases": null}',
            b'{"id": "A", "aliases": [1]}',
            b'{"id": "A", "aliases": [""]}',
            b'{"id": "A", "aliases": [null]}',
            b'{"id": "A", "aliases": ["x", "x"]}',
            b'{"id": "A", "id": "B", "aliases": []}',
            b'{"id": "A", "aliases": [], "aliases": []}',
        ]
        for data in bad:
            with self.subTest(data=data):
                with self.assertRaises(MigrationError):
                    parse_account(data, 7)

    def test_errors_name_the_record(self):
        with self.assertRaises(MigrationError) as ctx:
            parse_account(b'{"id": "", "aliases": []}', 42)
        self.assertIn("#42", str(ctx.exception))


class IterSnapshotTests(MigrationTestCase):
    def test_streams_valid_records_in_order(self):
        path = self.snap("s.bin", [("A", ["x"]), ("B", []), ("C", ["p", "q"])],
                         seqs=[10, 20, 30])
        self.assertEqual(list(iter_snapshot(path)),
                         [(10, "A", ["x"]), (20, "B", []),
                          (30, "C", ["p", "q"])])

    def test_a_bad_record_raises(self):
        path = self.snap("s.bin", [b'{"id": "A", "aliases": ['])
        with self.assertRaises(MigrationError):
            list(iter_snapshot(path))

    def test_a_truncated_feed_raises(self):
        path = self.snap("s.bin", [("A", ["x", "y", "z"])], max_len=64)
        with open(path, "rb") as f:
            blob = f.read()
        with open(path, "wb") as f:
            f.write(blob[:-70])
        with self.assertRaises(MigrationError):
            list(iter_snapshot(path))


# --------------------------------------------------------------------- #
# The change file
# --------------------------------------------------------------------- #
class ChangeFileTests(MigrationTestCase):
    def test_creates_updates_and_deletes(self):
        old = {"A": ["a1", "shared"], "B": ["b1"], "gone": ["g1"]}
        new = {"A": ["a1"], "B": ["b1", "shared"], "fresh": ["f1"]}
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", new.items()))
        self.assertIsInstance(result, MigrationResult)
        self.assertEqual((result.creates, result.updates, result.deletes),
                         (1, 2, 1))
        replay = self.replay_file(old, 1000)
        self.assertEqual(replay.table, {i: set(a) for i, a in new.items()})
        ops = {op["id"]: op for b in read_batches(self.out) for op in
               b["ops"]}
        # Full new sets, not deltas.
        self.assertEqual(ops["A"]["aliases"], ["a1"])
        self.assertEqual(ops["B"]["aliases"], ["b1", "shared"])
        self.assertEqual(ops["fresh"]["aliases"], ["f1"])
        self.assertEqual(ops["gone"]["op"], "delete")
        # An unchanged account produces nothing at all.
        self.assertNotIn("untouched", ops)

    def test_unchanged_snapshots_produce_an_empty_file(self):
        old = {"A": ["x"], "B": []}
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", old.items()))
        self.assertEqual((result.batches, result.ops, result.changed),
                         (0, 0, 0))
        self.assertEqual(os.path.getsize(self.out), 0)

    def test_alias_order_is_irrelevant(self):
        old = {"A": ["x", "y", "z"]}
        new = {"A": ["z", "x", "y"]}
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", new.items()))
        self.assertEqual(result.ops, 0)

    def test_accounts_may_be_in_any_order(self):
        old = {"A": ["a"], "B": ["b"], "C": ["c"]}
        new = {"C": ["c2"], "A": ["a2"], "B": ["b2"]}
        result = self.run_plan(self.snap("old.bin", list(old.items())[::-1]),
                               self.snap("new.bin", list(new.items())))
        self.assertEqual(result.ops, 3)
        self.assertEqual(self.replay_file(old, 1000).table,
                         {i: set(a) for i, a in new.items()})

    def test_an_account_whose_aliases_all_disappear_is_an_empty_upsert(self):
        old = {"A": ["x"]}
        new = {"A": []}
        self.run_plan(self.snap("old.bin", old.items()),
                      self.snap("new.bin", new.items()))
        ops = read_batches(self.out)[0]["ops"]
        self.assertEqual(ops, [{"op": "upsert", "id": "A", "aliases": []}])

    def test_migrating_to_and_from_an_empty_snapshot(self):
        old = {"A": ["x"], "B": ["y"]}
        empty = self.snap("empty.bin", [])
        nonempty = self.snap("nonempty.bin", list(old.items()))
        result = self.run_plan(nonempty, empty)
        self.assertEqual(result.deletes, 2)
        self.assertEqual(self.replay_file(old, 1000).table, {})
        result = self.run_plan(empty, nonempty)
        self.assertEqual(result.creates, 2)
        self.assertEqual(self.replay_file({}, 1000).table,
                         {i: set(a) for i, a in old.items()})

    def test_output_is_json_lines_with_stable_field_names(self):
        self.run_plan(self.snap("old.bin", [("A", ["x"])]),
                      self.snap("new.bin", [("A", ["y"])]))
        with open(self.out, encoding="utf-8") as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 1)
        doc = json.loads(lines[0])
        self.assertEqual(set(doc), {"batch", "ops"})
        self.assertEqual(set(doc["ops"][0]), {"op", "id", "aliases"})

    def test_the_file_is_refused_before_anything_is_published(self):
        # A batch limit that cannot work must leave no output at all.
        old = {"X": ["a"], "Y": ["b"]}
        new = {"X": ["b"], "Y": ["a"]}
        with self.assertRaises(MigrationError):
            self.run_plan(self.snap("old.bin", old.items()),
                          self.snap("new.bin", new.items()), max_ops=1)
        self.assertFalse(os.path.exists(self.out))

    def test_a_failed_run_does_not_overwrite_an_earlier_good_output(self):
        good = self.snap("g_old.bin", [("A", ["x"])])
        good_new = self.snap("g_new.bin", [("A", ["y"])])
        self.run_plan(good, good_new)
        with open(self.out, "rb") as f:
            before = f.read()
        bad = self.snap("bad.bin", [b'{"id": "A", "aliases": ['])
        with self.assertRaises(MigrationError):
            self.run_plan(bad, good_new)
        with open(self.out, "rb") as f:
            self.assertEqual(f.read(), before)
        # No stray temporary files are left next to the output either.
        self.assertEqual(sorted(os.listdir(self.dir)),
                         sorted(["changes.jsonl", "g_old.bin", "g_new.bin",
                                 "bad.bin"]))


# --------------------------------------------------------------------- #
# Invalid snapshots
# --------------------------------------------------------------------- #
class InvalidSnapshotTests(MigrationTestCase):
    def assert_refused(self, old_rows, new_rows, max_ops=100):
        old = self.snap("old.bin", old_rows)
        new = self.snap("new.bin", new_rows)
        with self.assertRaises(MigrationError):
            self.run_plan(old, new, max_ops)
        self.assertFalse(os.path.exists(self.out))

    def test_invalid_old_side(self):
        self.assert_refused([b'{"id": "A"}'], [("B", [])])

    def test_invalid_new_side(self):
        self.assert_refused([("A", [])], [b'{"id": "B", "aliases": [1]}'])

    def test_a_repeated_account_id_is_refused(self):
        self.assert_refused([("A", ["x"]), ("A", ["y"])], [("A", [])])

    def test_a_repeated_alias_in_one_account_is_refused(self):
        self.assert_refused([("A", ["x", "x"])], [("A", [])])

    def test_two_accounts_claiming_one_alias_are_refused(self):
        self.assert_refused([("A", ["x"]), ("B", ["x"])], [("A", [])])

    def test_the_same_alias_in_both_snapshots_is_fine(self):
        # Same owner in both is not a collision; only within one snapshot.
        old = self.snap("old.bin", [("A", ["x"])])
        new = self.snap("new.bin", [("A", ["x", "y"])])
        result = self.run_plan(old, new)
        self.assertEqual(result.ops, 1)

    def test_a_missing_file_is_refused(self):
        good = self.snap("good.bin", [("A", ["x"])])
        with self.assertRaises(MigrationError):
            self.run_plan(os.path.join(self.dir, "missing.bin"), good)
        self.assertFalse(os.path.exists(self.out))

    def test_a_truncated_feed_is_refused(self):
        good = self.snap("good.bin", [("A", ["x"])])
        bad = self.snap("bad.bin", [("B", ["y", "z"])], max_len=64)
        with open(bad, "rb") as f:
            blob = f.read()
        with open(bad, "wb") as f:
            f.write(blob[:-40])
        with self.assertRaises(MigrationError):
            self.run_plan(good, bad)
        self.assertFalse(os.path.exists(self.out))

    def test_a_tampered_record_count_is_refused(self):
        good = self.snap("good.bin", [("A", ["x"])])
        bad = self.snap("bad.bin", [("B", ["y"])], max_len=64)
        with open(bad, "r+b") as f:
            f.seek(16)                       # header's record-count field
            f.write(b"\x00\x00\x00\x09")
        with self.assertRaises(MigrationError):
            self.run_plan(good, bad)


# --------------------------------------------------------------------- #
# Batch limits and hand-over cycles
# --------------------------------------------------------------------- #
class BatchLimitTests(MigrationTestCase):
    def test_no_batch_exceeds_the_limit(self):
        rng = random.Random(5)
        old, new = {}, {}
        aliases = ["a%d" % i for i in range(40)]
        ids = ["i%02d" % i for i in range(30)]
        pool = aliases[:]
        rng.shuffle(pool)
        for i in ids:
            grab = pool[:rng.randint(0, 2)]
            old[i] = sorted(grab)
            pool = pool[len(grab):]
        leftover = [a for a in aliases if a not in {x for v in old.values()
                                                   for x in v}]
        rng.shuffle(leftover)
        for i in ids:
            take = leftover[:rng.randint(0, 2)]
            new[i] = sorted(take)
            leftover = leftover[len(take):]
        for max_ops in (1, 2, 3, 5, 1000):
            with self.subTest(max_ops=max_ops):
                try:
                    self.run_plan(self.snap("old.bin", old.items()),
                                  self.snap("new.bin", new.items()), max_ops)
                except MigrationError:
                    continue
                replay = self.replay_file(old, max_ops)
                self.assertEqual(replay.table,
                                 {i: set(a) for i, a in new.items()})

    def test_a_chain_never_needs_a_big_batch(self):
        # alias a_i moves from i to i+1: only an order is required.
        n = 25
        old = {"A%02d" % i: ["al%02d" % i] for i in range(n)}
        new = {"A%02d" % i: ([] if i == 0 else ["al%02d" % (i - 1)])
               for i in range(n)}
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", new.items()), max_ops=1)
        self.assertEqual(result.batches, n)
        self.assertEqual(self.replay_file(old, 1).table,
                         {i: set(a) for i, a in new.items()})

    def test_a_swap_forces_one_atomic_batch(self):
        old = {"X": ["a"], "Y": ["b"]}
        new = {"X": ["b"], "Y": ["a"]}
        with self.assertRaises(MigrationError):
            self.run_plan(self.snap("old.bin", old.items()),
                          self.snap("new.bin", new.items()), max_ops=1)
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", new.items()), max_ops=2)
        self.assertEqual(result.batches, 1)
        self.assertEqual(self.replay_file(old, 2).table,
                         {i: set(a) for i, a in new.items()})

    def test_the_refusal_threshold_is_the_largest_cycle(self):
        # A four-cycle and a separate two-cycle: max_ops 3 fails, 4 works.
        old = {"P": ["p"], "Q": ["q"], "R": ["r"], "S": ["s"],
               "U": ["u"], "V": ["v"]}
        new = {"P": ["q"], "Q": ["r"], "R": ["s"], "S": ["p"],
               "U": ["v"], "V": ["u"]}
        for max_ops, ok in ((1, False), (2, False), (3, False), (4, True),
                            (5, True)):
            with self.subTest(max_ops=max_ops):
                try:
                    self.run_plan(self.snap("old.bin", old.items()),
                                  self.snap("new.bin", new.items()), max_ops)
                except MigrationError:
                    if ok:
                        raise
                else:
                    if not ok:
                        raise AssertionError("max_ops=%d should have been "
                                             "refused" % max_ops)
                    self.assertEqual(self.replay_file(old, max_ops).table,
                                     {i: set(a) for i, a in new.items()})

    def test_an_unchanged_account_never_pads_a_cycle(self):
        # "Z" shares no alias with anyone; it must not enlarge the cycle.
        # Independent batches may come in either order, but X and Y are
        # together and Z is alone.
        old = {"X": ["a"], "Y": ["b"], "Z": ["z"]}
        new = {"X": ["b"], "Y": ["a"], "Z": ["z2"]}
        result = self.run_plan(self.snap("old.bin", old.items()),
                               self.snap("new.bin", new.items()), max_ops=2)
        self.assertEqual(result.ops, 3)          # X, Y together; Z alone
        batches = read_batches(self.out)
        self.assertEqual(
            sorted(sorted(op["id"] for op in b["ops"]) for b in batches),
            [["X", "Y"], ["Z"]])

    def test_no_temporary_account_or_intermediate_alias_is_used(self):
        # Every emitted id must exist in one of the snapshots, and every
        # emitted alias must be an alias of that account in the new snapshot.
        # Nothing is invented to "park" an alias along the way.
        old = {"X": ["a"], "Y": ["b"], "Z": ["c"]}
        new = {"X": ["b"], "Y": ["c"], "Z": ["a"]}
        known_ids = set(old) | set(new)
        new_aliases = {i: set(a) for i, a in new.items()}
        self.run_plan(self.snap("old.bin", old.items()),
                      self.snap("new.bin", new.items()), max_ops=3)
        for batch in read_batches(self.out):
            for op in batch["ops"]:
                self.assertIn(op["id"], known_ids)
                if op["op"] == "upsert":
                    self.assertEqual(set(op["aliases"]), new_aliases[op["id"]])

    def test_the_minimum_required_limit_is_exact(self):
        """Compare the accepted/rejected boundary against a straightforward
        reference implementation of the hand-over graph."""
        rng = random.Random(17)
        aliases = ["a%d" % i for i in range(10)]
        ids = ["i%d" % i for i in range(8)]

        def snapshot():
            rng.shuffle(ids)
            taken, out = set(), {}
            for account_id in ids:
                want = rng.sample(aliases, rng.randint(0, 2))
                out[account_id] = sorted(a for a in want if a not in taken)
                taken.update(out[account_id])
            return out

        for _ in range(40):
            old, new = snapshot(), snapshot()
            required = _largest_cycle(old, new)
            self.snap("old.bin", old.items())
            self.snap("new.bin", new.items())
            for max_ops in range(1, 6):
                with self.subTest(required=required, max_ops=max_ops):
                    try:
                        Migration(
                            os.path.join(self.dir, "old.bin"),
                            os.path.join(self.dir, "new.bin")
                        ).write(self.out, max_ops)
                    except MigrationError:
                        self.assertLess(max_ops, required)
                    else:
                        self.assertEqual(self.replay_file(old,
                                                          max_ops).table,
                                         {i: set(a) for i, a in new.items()})


def _largest_cycle(old, new):
    """Reference: the largest SCC of the alias hand-off relation."""
    old_owner = {a: i for i, al in old.items() for a in al}
    new_owner = {a: i for i, al in new.items() for a in al}
    changed = {i for i in set(old) | set(new)
               if set(old.get(i, ())) != set(new.get(i, ()))}
    edges = {i: set() for i in changed}
    for alias, src in old_owner.items():
        dst = new_owner.get(alias)
        if src in changed and dst in changed and dst != src:
            edges[src].add(dst)
    index, low, on, stack, sizes, counter = {}, {}, set(), [], [], [0]

    def visit(v):
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on.add(v)
        for w in edges[v]:
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            size, members = 0, []
            while True:
                w = stack.pop()
                on.discard(w)
                members.append(w)
                size += 1
                if w == v:
                    break
            sizes.append(size)

    sys.setrecursionlimit(10000)
    for v in edges:
        if v not in index:
            visit(v)
    return max(sizes, default=0)


# --------------------------------------------------------------------- #
# Scale
# --------------------------------------------------------------------- #
class ScaleTests(MigrationTestCase):
    def test_a_stream_far_larger_than_the_batch_is_handled(self):
        # 3000 accounts, 3000 hand-offs in a chain, max_ops 4: 750 batches.
        # The planner must not hold them all; it streams from disk.
        n = 3000
        old = {"a%05d" % i: ["x%05d" % i] for i in range(n)}
        new = {"a%05d" % i: ([] if i == 0 else ["x%05d" % (i - 1)])
               for i in range(n)}
        old_path = self.snap("old.bin", old.items())
        new_path = self.snap("new.bin", new.items())
        result = Migration(old_path, new_path).write(self.out, 4)
        self.assertEqual(result.batches, (n + 3) // 4)
        self.assertEqual(result.ops, n)
        # Spot-check rather than replay the whole 3000-account table.
        batches = read_batches(self.out)
        self.assertEqual(len(batches), result.batches)
        replay = Replay(dict(list(old.items())[:8]), 4)
        for batch in batches[:4]:
            replay.assert_ops_shape(batch)
            self.assertLessEqual(len(batch["ops"]), 4)
        last = batches[-1]
        self.assertEqual(sorted(op["id"] for op in last["ops"]),
                         ["a%05d" % i for i in range(n - 4, n)])

    def test_batch_numbering_is_dense_and_ordered(self):
        old = {"A%02d" % i: ["x%02d" % i] for i in range(40)}
        new = {"A%02d" % i: ["y%02d" % i] for i in range(40)}
        self.run_plan(self.snap("old.bin", old.items()),
                      self.snap("new.bin", new.items()), max_ops=6)
        numbers = [b["batch"] for b in read_batches(self.out)]
        self.assertEqual(numbers, list(range(1, len(numbers) + 1)))

    def test_disk_scratch_is_cleaned_up(self):
        work = os.path.join(self.dir, "work")
        os.makedirs(work)
        old = self.snap("old.bin", [("A", ["x"])])
        new = self.snap("new.bin", [("A", ["y"])])
        Migration(old, new, tmpdir=work).write(self.out, 10)
        self.assertEqual(os.listdir(work), [])

    def test_memory_does_not_grow_with_the_number_of_changes(self):
        """A run over an 8x larger input must not cost about 8x the memory.

        This is the requirement "fixed line size and batch limit, memory must
        not grow with accounts, aliases or changes".  Peak RSS includes the
        interpreter itself, so the comparison is on the delta rather than the
        absolute value; the two runs use a *fresh* subprocess each so neither
        inherits the other's peaks.
        """
        import subprocess

        if not hasattr(sys, "implementation") \
                or sys.implementation.name != "cpython":
            self.skipTest("RSS comparison is calibrated for CPython")
        probe = (
            "import json, resource, sys\n"
            "from shipper import FeedWriter, Migration\n"
            "n = int(sys.argv[1])\n"
            "def block(path, base):\n"
            "    with FeedWriter(path, 64) as w:\n"
            "        for i in range(n):\n"
            "            w.append(i + 1, 'a', json.dumps({\n"
            "                'id': 'acct%07d' % i,\n"
            "                'aliases': ['alias%07d' % (base + i)]}).encode())\n"
            "block('mo.bin', 0)\n"
            "block('mn.bin', 10000000)\n"
            "Migration('mo.bin', 'mn.bin').write('mo.jsonl', 50)\n"
            "print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)\n"
        )
        work = os.path.join(self.dir, "mem")
        os.makedirs(work)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # ``-I`` isolates the child from the parent's environment (including
        # PYTHONPATH), so the repository root is passed as an argument and put
        # on sys.path explicitly by the probe itself.
        probe = "import sys\nsys.path.insert(0, sys.argv[2])\n" + probe
        peaks = []
        for n in (4000, 32000):
            out = subprocess.run(
                [sys.executable, "-I", "-c", probe, str(n), root],
                cwd=work, capture_output=True, text=True, check=True).stdout
            peaks.append(int(out.strip()))
        small, large = peaks
        growth = large - small
        # 8x the accounts; allow well under the bytes-per-change a per-change
        # in-memory structure would cost (that would be tens of MB here).
        self.assertLess(growth, 12 * 1024,
                        "peak RSS grew by %d KiB from 4k to 32k accounts"
                        % growth)


if __name__ == "__main__":
    unittest.main()
