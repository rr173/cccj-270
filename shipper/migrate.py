"""Account-alias snapshot migration: full snapshots in, safe batches out.

The shipper delivers a *full snapshot* of an account/alias table as one
:class:`~shipper.FileSource` feed, one JSON document per record::

    {"id": "<account id>", "aliases": ["<alias>", ...]}

``id`` is a non-empty string; ``aliases`` is a JSON array of non-empty strings
and may be empty.  Ids compare by exact text and an alias array is a *set*:
its order is irrelevant.  A valid snapshot never repeats an account id and
never declares one alias twice (whether inside one array or on two accounts).
Feed sequence numbers and categories take no part in the reconciliation: the
two snapshots are matched by ``id`` alone.

Downstream cannot stop and swap tables, so the move from *old* to *new* is
replayed batch by batch, and after every committed batch every alias belongs to
at most one account.  This module computes that migration and writes it as
JSON Lines.

Output
------
Each line is one **atomic batch**; each operation is one changed account::

    {"batch": 1, "ops": [
        {"op": "upsert", "id": "acct-7", "aliases": ["a", "b"]},
        {"op": "delete", "id": "acct-9"}
    ]}
    {"batch": 2, "ops": [
        {"op": "upsert", "id": "acct-3", "aliases": []}
    ]}

``batch``
    A 1-based batch number.  Batches are numbered in a safe order: a batch may
    be applied once every batch with a lower number has been committed.
``ops``
    The operations of this batch.  They take effect together or not at all.
``ops[].op``
    ``"upsert"`` or ``"delete"``; an account appears in at most one operation
    in the whole file.
``ops[].id``
    The account id, exactly as it appears in the snapshots.
``ops[].aliases``
    Only on ``"upsert"``: the account's **complete** new alias set, sorted.
    It replaces whatever the account had before; it is never a delta.  A
    changed account with no aliases left is emitted with ``[]``.

Unchanged accounts are not emitted.  Every batch holds at most ``max_ops``
operations.

Why batches are not just one-account-each
-----------------------------------------
An ``upsert``/``delete`` replaces one account atomically, so a batch is only
needed for two or more accounts when an alias moves between them.  If alias
``a`` goes from ``X`` to ``Y``, the operation dropping ``a`` from ``X`` must be
committed no later than the operation adding it to ``Y``; otherwise the batch
between them would leave ``a`` on both.  Those hand-offs form a directed graph
and every strongly connected component has to share one batch.  If a component
is larger than ``max_ops`` the migration cannot be produced within the limit
and :class:`~shipper.errors.MigrationError` is raised.  No temporary account or
intermediate alias is ever used to dodge that: the only operations are full
``upsert`` and ``delete``.

Failure and atomic publication
------------------------------
The output is written to a uniquely named temporary file next to the
destination, flushed, fsynced and only then renamed into place.  A truncated
feed, an unreadable snapshot, an invalid record, a duplicate id/alias, a
cross-account alias collision, an unreadable feed or a batch limit too small
for the hand-over all raise :class:`~shipper.errors.MigrationError` with the
offending record; the temporary file is removed and any previously successful
output stays exactly as it was.

Memory
------
Both snapshots are copied into a private SQLite database on disk and the change
log is streamed out of it a batch at a time.  Resident memory is bounded by the
largest batch (``max_ops`` operations, each an alias list from one feed
record, so bounded by the fixed maximum record length) plus fixed database
caches; it does not grow with the number of accounts, aliases or changes.
Scratch disk grows with the input.

API
---
::

    from shipper import Migration, MigrationError, migrate

    result = migrate("old.bin", "new.bin", "changes.jsonl", max_ops=1000)
    print(result.batches, result.ops, result.creates,
          result.updates, result.deletes)

    plan = Migration("old.bin", "new.bin")
    for batch_no, ops in plan.batches(max_ops=1000):
        apply(batch_no, ops)                       # no file needed

    for seq, account_id, aliases in iter_snapshot("old.bin"):
        ...                                        # validated streaming read
"""

from __future__ import annotations

import os

from .errors import MigrationError
from .migrate_plan import Planner, iter_snapshot, parse_account

__all__ = [
    "Migration", "MigrationError", "MigrationResult", "Planner", "iter_snapshot",
    "migrate", "parse_account",
]


class MigrationResult:
    """A summary of a completed migration."""

    __slots__ = ("path", "batches", "ops", "creates", "updates", "deletes")

    def __init__(self, path, batches, ops, creates, updates, deletes):
        self.path = path
        self.batches = batches
        self.ops = ops
        self.creates = creates
        self.updates = updates
        self.deletes = deletes

    @property
    def changed(self):
        """Number of accounts that differ between the two snapshots."""
        return self.creates + self.updates + self.deletes

    def as_dict(self):
        return {"path": self.path, "batches": self.batches, "ops": self.ops,
                "creates": self.creates, "updates": self.updates,
                "deletes": self.deletes, "changed_accounts": self.changed}

    def __repr__(self):  # pragma: no cover - debugging aid
        return ("<MigrationResult %r batches=%d ops=%d creates=%d updates=%d "
                "deletes=%d>" % (self.path, self.batches, self.ops,
                                 self.creates, self.updates, self.deletes))


class Migration:
    """Plan (and optionally write) the change log between two snapshots.

    ``old`` and ``new`` are :class:`~shipper.FileSource` feed paths.  Nothing
    is read in the constructor; :meth:`batches` and :meth:`write` each run a
    complete, independent plan, so the object can be reused.  The input files
    are assumed not to change while one plan runs.

    ``tmpdir`` chooses where the private staging database lives (the system
    temporary directory by default).  The output file, when written, is staged
    in the destination's own directory so the rename is atomic.
    """

    def __init__(self, old, new, *, tmpdir=None):
        self.old = os.fspath(old)
        self.new = os.fspath(new)
        self.tmpdir = None if tmpdir is None else os.fspath(tmpdir)

    def _planner(self, tmpdir=None):
        return Planner(self.old, self.new,
                       tmpdir=self.tmpdir if tmpdir is None else tmpdir)

    def write(self, path, max_ops):
        """Write the migration to ``path`` as JSON Lines, publishing it
        atomically.  Returns a :class:`MigrationResult`."""
        path = os.fspath(path)
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        with self._planner(directory) as planner:
            return planner.write(path, max_ops)

    def batches(self, max_ops):
        """Yield ``(batch_number, ops)`` tuples in a safe order."""
        with self._planner() as planner:
            for item in planner.batches(max_ops):
                yield item

    def iter_ops(self, max_ops):
        """Yield operation dicts one at a time, in batch order."""
        for _, ops in self.batches(max_ops):
            for op in ops:
                yield op


def migrate(old, new, path, max_ops, *, tmpdir=None):
    """Two snapshot paths -> one atomically published JSON Lines file."""
    return Migration(old, new, tmpdir=tmpdir).write(path, max_ops)
