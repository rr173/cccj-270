"""Staged, bounded-memory engine behind :mod:`shipper.migrate`.

The two snapshots are copied into a private SQLite database on disk; the
change log is then streamed out of it one batch at a time.  No Python
structure grows with the number of accounts, aliases or changes: the account
table, the alias-ownership map, the hand-over graph, the depth-first search
stacks and the batch order all live in the database.  Only the batch currently
being written is materialised, and its size is bounded by ``max_ops`` times
the feed's fixed maximum record length.

Why a graph
-----------
Give each changed account ``X`` exactly one operation that atomically replaces
its whole alias set (or deletes the account).  Let ``t(X)`` be the batch in
which that operation is applied.  For every alias ``a`` that moves from ``X``
in the old snapshot to ``Y`` in the new one, a batch boundary with
``t(Y) < t(X)`` would leave both accounts owning ``a``; the only safe order is
``t(X) <= t(Y)``.  So the alias hand-offs form a directed graph ``X -> Y`` and
a valid plan is an assignment of batch numbers that never goes against an
edge.  Every strongly connected component must therefore sit entirely in one
batch; if one component is larger than ``max_ops`` the hand-over cannot be
done within the limit and the planner refuses instead of emitting a wrong
file.

There is never a temporary account or an intermediate alias: the only
operations are ``upsert`` (the complete new alias set) and ``delete``.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

try:                                    # pragma: no cover - stdlib on CPython
    import sqlite3
except ImportError:                     # pragma: no cover
    sqlite3 = None

from .errors import MigrationError, SourceError
from .feed import FileSource

_FIELDS = ("aliases", "id")
_CHUNK = 256                            # fixed, so paging does not scale


# --------------------------------------------------------------------- #
# Snapshot records
# --------------------------------------------------------------------- #
def _reject_duplicate_keys(pairs):
    seen = set()
    for key, _ in pairs:
        if key in seen:
            raise ValueError("duplicate JSON key %r" % (key,))
        seen.add(key)
    return dict(pairs)


def _encodable(value):
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def parse_account(data, seq):
    """Decode and validate one snapshot record.

    ``data`` is the record's payload bytes and ``seq`` its source sequence
    number (used only in error messages).  Returns ``(id, aliases, canonical)``
    where ``aliases`` is the sorted set and ``canonical`` its stable JSON text
    (equal alias sets produce equal text).

    Raises :class:`~shipper.errors.MigrationError` for anything that is not a
    ``{"id": <non-empty str>, "aliases": [<non-empty str>, ...]}`` object with
    no duplicate keys, no empty strings and no alias declared twice.
    """
    try:
        text = bytes(data).decode("utf-8")
    except (TypeError, UnicodeDecodeError):
        raise MigrationError(
            "snapshot record #%d is not UTF-8 text" % seq) from None
    try:
        doc = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except ValueError as exc:
        raise MigrationError(
            "snapshot record #%d is not valid JSON: %s" % (seq, exc)) from None
    if not isinstance(doc, dict):
        raise MigrationError(
            "snapshot record #%d is not a JSON object" % seq)
    keys = set(doc)
    missing = [k for k in _FIELDS if k not in keys]
    if missing:
        raise MigrationError(
            "snapshot record #%d is missing field(s): %s"
            % (seq, ", ".join(missing)))
    extra = sorted(k for k in keys if k not in _FIELDS)
    if extra:
        raise MigrationError(
            "snapshot record #%d has unexpected field(s): %s"
            % (seq, ", ".join(extra)))
    account_id = doc["id"]
    if not isinstance(account_id, str) or not account_id:
        raise MigrationError(
            "snapshot record #%d has an id that is not a non-empty string"
            % seq)
    if not _encodable(account_id):
        raise MigrationError(
            "snapshot record #%d has an id that is not valid UTF-8 text" % seq)
    raw = doc["aliases"]
    if not isinstance(raw, list):
        raise MigrationError(
            "snapshot record #%d (id %r) has a non-array aliases field"
            % (seq, account_id))
    seen = set()
    for alias in raw:
        if not isinstance(alias, str) or not alias:
            raise MigrationError(
                "snapshot record #%d (id %r) has an alias that is not a "
                "non-empty string" % (seq, account_id))
        if not _encodable(alias):
            raise MigrationError(
                "snapshot record #%d (id %r) has an alias that is not valid "
                "UTF-8 text" % (seq, account_id))
        if alias in seen:
            raise MigrationError(
                "snapshot record #%d (id %r) declares alias %r twice"
                % (seq, account_id, alias))
        seen.add(alias)
    aliases = sorted(seen)
    canonical = json.dumps(aliases, ensure_ascii=True, separators=(",", ":"))
    return account_id, aliases, canonical


class _Snapshot:
    """A read-only streaming view of one feed, usable as a context manager."""

    def __init__(self, path):
        self.path = os.fspath(path)
        self._source = None
        self._link = None

    def __enter__(self):
        try:
            self._source = FileSource(self.path)
            self._link = self._source.open(None)
        except (SourceError, OSError) as exc:
            self.__exit__(None, None, None)
            raise MigrationError("snapshot %r is unreadable: %s"
                                 % (self.path, exc)) from exc
        return self._link

    def __exit__(self, *exc):
        for close in (self._link, self._source):
            if close is not None:
                try:
                    close.close()
                except Exception:       # pragma: no cover - defensive
                    pass
        self._link = self._source = None
        return False


def _pull(link, path):
    """Return the next :class:`Pulled`, or ``None`` at end of stream."""
    try:
        return link.take()
    except StopIteration:
        return None
    except SourceError as exc:
        raise MigrationError(
            "snapshot %r is truncated or invalid: %s" % (path, exc)) from exc


def iter_snapshot(path):
    """Yield ``(seq, id, aliases)`` for every record of one snapshot feed.

    Streams the feed and validates it exactly as the planner does, staging
    nothing.  An unreadable or truncated feed, or an invalid record, raises
    :class:`~shipper.errors.MigrationError`.
    """
    path = os.fspath(path)
    with _Snapshot(path) as link:
        while True:
            pulled = _pull(link, path)
            if pulled is None:
                return
            account_id, aliases, _ = parse_account(pulled.data, pulled.seq)
            yield pulled.seq, account_id, aliases


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #
def _check_max_ops(max_ops):
    if (not isinstance(max_ops, int) or isinstance(max_ops, bool)
            or max_ops < 1):
        raise ValueError("max_ops must be a positive integer, got %r"
                         % (max_ops,))


def _fsync_dir(path):
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        fd = os.open(path or ".", os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _rmtree(path):
    shutil.rmtree(path, ignore_errors=True)


# --------------------------------------------------------------------- #
# The planner
# --------------------------------------------------------------------- #
class Planner:
    """Stage two snapshots and emit the migration, batch by batch."""

    def __init__(self, old, new, *, tmpdir=None):
        if sqlite3 is None:             # pragma: no cover - CPython always has
            raise MigrationError("sqlite3 is required to plan a migration")
        self.old = os.fspath(old)
        self.new = os.fspath(new)
        self.tmpdir = None if tmpdir is None else os.fspath(tmpdir)
        self._workdir = None
        self._conn = None
        self._prepared = False
        self.counts = {"create": 0, "update": 0, "delete": 0}

    # -- lifecycle ---------------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
        if self._workdir is not None:
            _rmtree(self._workdir)
            self._workdir = None
        self._prepared = False

    def _connect(self):
        self._workdir = tempfile.mkdtemp(prefix="shipper-migrate-",
                                         dir=self.tmpdir)
        db = os.path.join(self._workdir, "plan.sqlite3")
        conn = sqlite3.connect(db, isolation_level=None)
        for pragma in ("journal_mode=OFF", "synchronous=OFF",
                       "temp_store=FILE", "cache_size=-2048",
                       "locking_mode=EXCLUSIVE"):
            conn.execute("PRAGMA " + pragma)
        self._conn = conn
        return conn

    # -- prepare -----------------------------------------------------------
    def prepare(self):
        """Stage both snapshots, build the graph and compute the batch order."""
        if self._prepared:
            return self
        conn = self._connect()
        try:
            self._create_tables()
            conn.execute("BEGIN")
            self._stage(self.old, "old")
            self._stage(self.new, "new")
            self._build_changes()
            self._build_edges()
            self._tarjan()
            conn.execute("COMMIT")
            self.counts = dict(conn.execute(
                "SELECT kind, COUNT(*) FROM changed GROUP BY kind").fetchall())
            self._prepared = True
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:       # pragma: no cover - best effort
                pass
            raise
        return self

    def _create_tables(self):
        statements = [
            "CREATE TABLE old_acct(id TEXT PRIMARY KEY, aliases TEXT NOT NULL)"
            " WITHOUT ROWID",
            "CREATE TABLE old_alias(alias TEXT PRIMARY KEY, id TEXT NOT NULL)"
            " WITHOUT ROWID",
            "CREATE TABLE new_acct(id TEXT PRIMARY KEY, aliases TEXT NOT NULL)"
            " WITHOUT ROWID",
            "CREATE TABLE new_alias(alias TEXT PRIMARY KEY, id TEXT NOT NULL)"
            " WITHOUT ROWID",
            "CREATE TABLE changed(id TEXT PRIMARY KEY, kind TEXT NOT NULL)"
            " WITHOUT ROWID",
            "CREATE TABLE op(id TEXT PRIMARY KEY, kind TEXT NOT NULL, "
            "aliases TEXT) WITHOUT ROWID",
            "CREATE TABLE edge(src TEXT NOT NULL, dst TEXT NOT NULL, "
            "PRIMARY KEY(src, dst)) WITHOUT ROWID",
            "CREATE TABLE node(id TEXT PRIMARY KEY, idx INTEGER, low INTEGER, "
            "onstack INTEGER NOT NULL, comp INTEGER) WITHOUT ROWID",
            "CREATE TABLE unvisited(id TEXT PRIMARY KEY) WITHOUT ROWID",
            "CREATE TABLE frame(depth INTEGER PRIMARY KEY, v TEXT NOT NULL, "
            "last TEXT)",
            "CREATE TABLE sccstack(pos INTEGER PRIMARY KEY AUTOINCREMENT, "
            "v TEXT NOT NULL UNIQUE)",
            "CREATE TABLE csize(cid INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
            "CREATE TABLE comp_order(seq INTEGER PRIMARY KEY AUTOINCREMENT, "
            "cid INTEGER NOT NULL)",
        ]
        for statement in statements:
            self._conn.execute(statement)

    def _stage(self, path, prefix):
        conn = self._conn
        acct_insert = ("INSERT INTO %s_acct(id, aliases) VALUES (?, ?)"
                       % prefix)
        alias_insert = ("INSERT OR IGNORE INTO %s_alias(alias, id) "
                        "VALUES (?, ?)" % prefix)
        alias_owner = "SELECT id FROM %s_alias WHERE alias = ?" % prefix
        try:
            source = FileSource(path)
        except (SourceError, OSError) as exc:
            raise MigrationError("snapshot %r is unreadable: %s"
                                 % (path, exc)) from exc
        link = None
        try:
            try:
                link = source.open(None)
            except SourceError as exc:
                raise MigrationError("snapshot %r is unreadable: %s"
                                     % (path, exc)) from exc
            while True:
                pulled = _pull(link, path)
                if pulled is None:
                    break
                try:
                    account_id, aliases, canonical = parse_account(
                        pulled.data, pulled.seq)
                except MigrationError as exc:
                    raise MigrationError("%s (in %r)" % (exc, path)) from None
                try:
                    conn.execute(acct_insert, (account_id, canonical))
                except sqlite3.IntegrityError:
                    raise MigrationError(
                        "snapshot %r declares account %r more than once "
                        "(record #%d)" % (path, account_id, pulled.seq)) \
                        from None
                for alias in aliases:
                    if conn.execute(alias_insert,
                                    (alias, account_id)).rowcount == 0:
                        owner = conn.execute(alias_owner,
                                             (alias,)).fetchone()[0]
                        raise MigrationError(
                            "snapshot %r lets more than one account claim "
                            "alias %r (record #%d, account %r; already on %r)"
                            % (path, alias, pulled.seq, account_id, owner))
        finally:
            if link is not None:
                link.close()
            source.close()

    def _build_changes(self):
        statements = [
            "INSERT INTO changed(id, kind) "
            "SELECT n.id, 'create' FROM new_acct n "
            "LEFT JOIN old_acct o ON o.id = n.id WHERE o.id IS NULL",
            "INSERT INTO changed(id, kind) "
            "SELECT n.id, 'update' FROM new_acct n "
            "JOIN old_acct o ON o.id = n.id WHERE n.aliases <> o.aliases",
            "INSERT INTO changed(id, kind) "
            "SELECT o.id, 'delete' FROM old_acct o "
            "LEFT JOIN new_acct n ON n.id = o.id WHERE n.id IS NULL",
            "INSERT INTO op(id, kind, aliases) "
            "SELECT c.id, "
            "CASE c.kind WHEN 'delete' THEN 'delete' ELSE 'upsert' END, "
            "n.aliases FROM changed c "
            "LEFT JOIN new_acct n ON n.id = c.id",
            "INSERT INTO node(id, idx, low, onstack, comp) "
            "SELECT id, NULL, NULL, 0, NULL FROM changed",
            "INSERT INTO unvisited(id) SELECT id FROM changed",
        ]
        for statement in statements:
            self._conn.execute(statement)

    def _build_edges(self):
        self._conn.execute(
            "INSERT OR IGNORE INTO edge(src, dst) "
            "SELECT oa.id, na.id FROM old_alias oa "
            "JOIN new_alias na ON na.alias = oa.alias "
            "JOIN changed c1 ON c1.id = oa.id "
            "JOIN changed c2 ON c2.id = na.id "
            "WHERE oa.id <> na.id")

    # -- strongly connected components ------------------------------------
    def _tarjan(self):
        """Tarjan's algorithm with node state and both stacks stored on disk.

        Components are appended to ``comp_order`` in the order they are
        emitted, i.e. reverse topological order of the condensation, so
        reading ``comp_order`` newest-first gives a safe batch order.
        """
        conn = self._conn
        counter = [0]
        components = [0]
        get_low = "SELECT low FROM node WHERE id = ?"
        get_idx = "SELECT idx FROM node WHERE id = ?"
        set_low = "UPDATE node SET low = ? WHERE id = ?"

        def enter(node):
            start = counter[0]
            conn.execute(
                "UPDATE node SET idx = ?, low = ?, onstack = 1 WHERE id = ?",
                (start, start, node))
            conn.execute("INSERT INTO sccstack(v) VALUES (?)", (node,))
            counter[0] += 1

        def push_frame(depth, node):
            conn.execute(
                "INSERT INTO frame(depth, v, last) VALUES (?, ?, NULL)",
                (depth, node))

        def frame(depth):
            return conn.execute(
                "SELECT v, last FROM frame WHERE depth = ?",
                (depth,)).fetchone()

        def next_neighbour(node, last):
            if last is None:
                row = conn.execute(
                    "SELECT dst FROM edge WHERE src = ? ORDER BY dst LIMIT 1",
                    (node,)).fetchone()
            else:
                row = conn.execute(
                    "SELECT dst FROM edge WHERE src = ? AND dst > ? "
                    "ORDER BY dst LIMIT 1", (node, last)).fetchone()
            return None if row is None else row[0]

        def close_component(root):
            components[0] += 1
            cid = components[0]
            size = 0
            while True:
                pos, top = conn.execute(
                    "SELECT pos, v FROM sccstack ORDER BY pos DESC LIMIT 1"
                ).fetchone()
                conn.execute("DELETE FROM sccstack WHERE pos = ?", (pos,))
                conn.execute(
                    "UPDATE node SET comp = ?, onstack = 0 WHERE id = ?",
                    (cid, top))
                size += 1
                if top == root:
                    break
            conn.execute("INSERT INTO csize(cid, n) VALUES (?, ?)",
                         (cid, size))
            conn.execute("INSERT INTO comp_order(cid) VALUES (?)", (cid,))

        conn.execute("CREATE INDEX node_by_comp ON node(comp, id)")

        while True:
            row = conn.execute(
                "SELECT id FROM unvisited ORDER BY id LIMIT 1").fetchone()
            if row is None:
                break
            root = row[0]
            conn.execute("DELETE FROM unvisited WHERE id = ?", (root,))
            enter(root)
            depth = 0
            push_frame(depth, root)
            while depth >= 0:
                node, last = frame(depth)
                neighbour = next_neighbour(node, last)
                if neighbour is not None:
                    conn.execute(
                        "UPDATE frame SET last = ? WHERE depth = ?",
                        (neighbour, depth))
                    nidx, nonstack = conn.execute(
                        "SELECT idx, onstack FROM node WHERE id = ?",
                        (neighbour,)).fetchone()
                    if nidx is None:
                        conn.execute("DELETE FROM unvisited WHERE id = ?",
                                     (neighbour,))
                        enter(neighbour)
                        depth += 1
                        push_frame(depth, neighbour)
                    elif nonstack:
                        low_node = conn.execute(get_low,
                                                (node,)).fetchone()[0]
                        if nidx < low_node:
                            conn.execute(set_low, (nidx, node))
                    continue
                low_node = conn.execute(get_low, (node,)).fetchone()[0]
                idx_node = conn.execute(get_idx, (node,)).fetchone()[0]
                if low_node == idx_node:
                    close_component(node)
                conn.execute("DELETE FROM frame WHERE depth = ?", (depth,))
                depth -= 1
                if depth >= 0:
                    parent = frame(depth)[0]
                    low_parent = conn.execute(get_low,
                                              (parent,)).fetchone()[0]
                    if low_node < low_parent:
                        conn.execute(set_low, (low_node, parent))

    # -- validation --------------------------------------------------------
    def check(self, max_ops):
        """Ensure every indivisible component fits in one batch."""
        _check_max_ops(max_ops)
        self.prepare()
        biggest = self._conn.execute("SELECT MAX(n) FROM csize").fetchone()[0]
        if biggest is not None and biggest > max_ops:
            raise MigrationError(
                "the migration cannot be handed over in batches of at most %d "
                "operation(s): an alias hand-over cycle forces %d accounts "
                "into one atomic batch" % (max_ops, biggest))
        return self

    # -- emission ----------------------------------------------------------
    def _load_ops(self, cid):
        """Materialise one component's operations (at most ``max_ops``)."""
        conn = self._conn
        get_op = "SELECT kind, aliases FROM op WHERE id = ?"
        ops = []
        last = None
        while True:
            if last is None:
                rows = conn.execute(
                    "SELECT id FROM node WHERE comp = ? ORDER BY id LIMIT ?",
                    (cid, _CHUNK)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id FROM node WHERE comp = ? AND id > ? "
                    "ORDER BY id LIMIT ?", (cid, last, _CHUNK)).fetchall()
            if not rows:
                break
            for (account_id,) in rows:
                kind, aliases = conn.execute(get_op,
                                             (account_id,)).fetchone()
                if kind == "delete":
                    ops.append({"op": "delete", "id": account_id})
                else:
                    ops.append({"op": "upsert", "id": account_id,
                                "aliases": json.loads(aliases)})
            last = rows[-1][0]
        return ops

    def _component_ids(self):
        """Yield component ids newest-first (= a topological order)."""
        conn = self._conn
        last = None
        while True:
            if last is None:
                rows = conn.execute(
                    "SELECT seq, cid FROM comp_order ORDER BY seq DESC "
                    "LIMIT ?", (_CHUNK,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT seq, cid FROM comp_order WHERE seq < ? "
                    "ORDER BY seq DESC LIMIT ?", (last, _CHUNK)).fetchall()
            if not rows:
                return
            for seq, cid in rows:
                yield cid
            last = rows[-1][0]

    def batches(self, max_ops):
        """Yield ``(batch_number, ops)`` in a safe order."""
        self.check(max_ops)
        current = []
        number = 0
        for cid in self._component_ids():
            ops = self._load_ops(cid)
            if current and len(current) + len(ops) > max_ops:
                number += 1
                current.sort(key=lambda op: op["id"])
                yield number, tuple(current)
                current = []
            current.extend(ops)
        if current:
            number += 1
            current.sort(key=lambda op: op["id"])
            yield number, tuple(current)

    def write(self, path, max_ops):
        """Stream the JSON Lines output and publish it atomically."""
        from .migrate import MigrationResult

        path = os.fspath(path)
        self.check(max_ops)
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + ".tmp.",
                                   dir=directory)
        batches = ops_written = 0
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as out:
                for number, ops in self.batches(max_ops):
                    out.write(json.dumps(
                        {"batch": number, "ops": list(ops)},
                        ensure_ascii=False, separators=(",", ":")))
                    out.write("\n")
                    batches += 1
                    ops_written += len(ops)
                out.flush()
                os.fsync(out.fileno())
            # mkstemp creates 0600; publish with the usual umask-adjusted
            # permissions so downstream readers can open it like any file.
            umask = os.umask(0)
            os.umask(umask)
            os.chmod(tmp, 0o666 & ~umask)
            os.replace(tmp, path)
            _fsync_dir(directory)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        counts = self.counts
        return MigrationResult(path, batches, ops_written,
                               counts.get("create", 0),
                               counts.get("update", 0),
                               counts.get("delete", 0))
