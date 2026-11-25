#!/usr/bin/env python3
"""Account-alias snapshot migration demo.

    python3 demo_migrate.py [--dir D] [--max-ops K]

Writes two full snapshots as ``FileSource`` feeds (the old and the new
account/alias table), generates the change file between them, then replays it
batch by batch against the old snapshot.  After every batch it checks the
central invariant -- no alias belongs to two accounts -- and at the end that
the table equals the new snapshot exactly.

The fixture deliberately contains the awkward case: three accounts rotate
aliases (``alice`` -> ``bob`` -> ``carol`` -> ``alice``), so all three must be
handed over in one atomic batch, and ``eve``/``frank`` swap two aliases, which
also forces them into one batch.  A batch limit of 1 or 2 can therefore only
be refused; 3 is enough.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile

from shipper import FeedWriter, Migration, MigrationError

# (id, aliases) -- old snapshot
OLD = [
    ("alice", ["teal", "rose", "alice-home"]),
    ("bob", ["cyan", "sea"]),
    ("carol", ["clay"]),
    ("dave", ["dave-main"]),
    ("eve", ["eve-a"]),
    ("frank", ["frank-b"]),
    ("gina", ["gina-x", "gina-y"]),
]
# (id, aliases) -- new snapshot.  "teal" alice->bob, "cyan" bob->carol,
# "clay" carol->alice: a three-cycle, so alice/bob/carol share one batch.
# eve<->frank swap: a two-cycle.  dave and gina are unchanged / deleted.
NEW = [
    ("alice", ["rose", "alice-home", "clay"]),
    ("bob", ["sea", "teal"]),
    ("carol", ["cyan"]),
    ("eve", ["frank-b"]),
    ("frank", ["eve-a"]),
    ("harriet", ["harriet-new"]),
]


def write_snapshot(path, rows):
    payloads = [json.dumps({"id": i, "aliases": sorted(a)}).encode("utf-8")
                for i, a in rows]
    with FeedWriter(path, max(len(p) for p in payloads)) as writer:
        for seq, payload in enumerate(payloads, 1):
            writer.append(seq, "account", payload)


def ownership(table):
    owner = {}
    for account_id, aliases in table.items():
        for alias in aliases:
            if alias in owner:
                raise AssertionError(
                    "alias %r is owned by both %r and %r"
                    % (alias, owner[alias], account_id))
            owner[alias] = account_id
    return owner


def apply_batch(table, ops):
    """Apply one atomic batch, then assert the invariant immediately."""
    pending = {account_id: set(aliases)
               for account_id, aliases in table.items()}
    for op in ops:
        if op["op"] == "delete":
            del pending[op["id"]]
        else:
            pending[op["id"]] = set(op["aliases"])
    ownership(pending)                    # raises if a batch was unsafe
    return pending


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=None,
                    help="working directory (a temp dir by default)")
    ap.add_argument("--max-ops", type=int, default=1,
                    help="maximum operations per batch (the fixture needs 3)")
    args = ap.parse_args()
    base = args.dir or tempfile.mkdtemp(prefix="shipper-migrate-demo-")
    os.makedirs(base, exist_ok=True)

    old_path = os.path.join(base, "old.bin")
    new_path = os.path.join(base, "new.bin")
    out_path = os.path.join(base, "changes.jsonl")
    write_snapshot(old_path, OLD)
    write_snapshot(new_path, NEW)

    print("== snapshots ==")
    print("old:", {i: a for i, a in OLD})
    print("new:", {i: a for i, a in NEW})

    print("\n== generating the change file (max_ops=%d) ==" % args.max_ops)
    try:
        result = Migration(old_path, new_path).write(out_path, args.max_ops)
    except MigrationError as exc:
        print("refused:", exc)
        print("\nThis fixture rotates aliases between alice/bob/carol and "
              "swaps eve/frank,")
        print("so those accounts cannot be split; re-run with --max-ops 3.")
        return
    print("batches=%d ops=%d creates=%d updates=%d deletes=%d"
          % (result.batches, result.ops, result.creates, result.updates,
             result.deletes))
    with open(out_path, encoding="utf-8") as f:
        for line in f:
            print("  " + line.rstrip())

    print("\n== replaying, checking the invariant after every batch ==")
    table = {i: set(a) for i, a in OLD}
    with open(out_path, encoding="utf-8") as f:
        for line in f:
            batch = json.loads(line)
            table = apply_batch(table, batch["ops"])
            print("  after batch %d: %d account(s), every alias unique"
                  % (batch["batch"], len(table)))
    expected = {i: set(a) for i, a in NEW}
    assert table == expected, (table, expected)
    print("\nfinal table equals the new snapshot.")
    print("OK: every committed batch kept each alias on at most one account, "
          "no temporary account was used, and the end state is exact.")


if __name__ == "__main__":
    main()
