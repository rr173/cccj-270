"""Persistent jobs: recovery points that survive the process.

A *job* binds one delivery session to the local disk.  The configuration
(categories, ``M``, ``B``) and the source identities are fixed when the job
is created; :meth:`Job.save` then commits a *recovery point*: one consistent
cut of source positions, per-group acknowledgement progress, unconfirmed
payloads, ticket numbers, the byte ledger and the end/error state, all taken
under the session lock.  After the process exits, opening the job resumes
from the last committed point: confirmed records are not delivered again,
records still owed stay owed to the same group, and the stream continues
from exactly the saved position.

Layout on disk::

    <root>/<job>/job.json                 config + source identities
    <root>/<job>/HEAD                     id of the latest committed point
    <root>/<job>/checkpoints/NNNNNN.chk   recovery points

Guarantees:

* **Visibility** -- a recovery point exists only once it is complete: the
  file is staged under a temporary name, fsynced and renamed into place,
  and only then is ``HEAD`` advanced (also by rename).  A crash anywhere in
  that sequence leaves the previously committed point untouched and the new
  one invisible.
* **Binding** -- a point is loadable only with the job's own configuration
  and the very sources it was created with (checked by content-bound
  identity tokens and a configuration fingerprint); anything else is
  refused loudly, never silently adopted.
* **Explicit failure** -- loading validates magic, length, a SHA-256 of the
  body, the fingerprint, the identities and the state's internal
  consistency.  Any damage, incompatibility or incompleteness raises and
  yields no session at all, so a half-restored session can never be used;
  previously committed points stay openable.

Only one live handle per job is allowed at a time (an advisory file lock),
so two processes cannot resume -- and doubly deliver -- the same job.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shutil
import time

from .errors import (CheckpointCorrupt, JobError, JobExists, JobMismatch,
                     JobNotFound)
from .session import Session

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None

CHK_MAGIC = "SHPCHK1"
JOB_FORMAT = 1
META_FILE = "job.json"
HEAD_FILE = "HEAD"
CHK_DIR = "checkpoints"
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


# --------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------- #
def config_fingerprint(config):
    """A stable fingerprint of the delivery configuration."""
    canon = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canon.encode("utf-8")).hexdigest()


def _utcnow():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _check_name(name):
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ValueError("job names must match %s, got %r"
                         % (_NAME_RE.pattern, name))


def _resumable_sources(sources):
    if not isinstance(sources, (list, tuple)):
        sources = [sources]
    if not sources:
        raise ValueError("at least one source is required")
    out = []
    for s in sources:
        if not (hasattr(s, "identity") and hasattr(s, "open")):
            raise TypeError(
                "a job's sources must be resumable (identity() and "
                "open(position)); got %r" % (s,))
        out.append(s)
    return out


def _normalise_categories(categories):
    if not isinstance(categories, dict) or not categories:
        raise ValueError("categories must be a non-empty "
                         "{category: [group]} mapping")
    out = {}
    for cat, names in categories.items():
        if not isinstance(cat, str):
            raise ValueError("category names must be strings, got %r"
                             % (cat,))
        names = list(dict.fromkeys(names))
        if not names:
            continue
        for n in names:
            if not isinstance(n, str):
                raise ValueError("group names must be strings, got %r"
                                 % (n,))
        out[cat] = names
    if not out:
        raise ValueError("no category is received by any group")
    return out


def _atomic_write(path, data):
    """Write ``data`` to ``path`` so a crash never leaves a partial file."""
    tmp = "%s.tmp.%d" % (path, os.getpid())
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(os.path.dirname(path))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


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


def _read_file(path, what):
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        raise CheckpointCorrupt("%s is missing" % what) from None
    except OSError as exc:
        raise CheckpointCorrupt("%s cannot be read: %s" % (what, exc)) \
            from exc


def _lock_dir(dirpath, name):
    fd = os.open(os.path.join(dirpath, ".lock"), os.O_RDWR | os.O_CREAT,
                 0o644)
    if fcntl is not None:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise JobError("job %r is already open" % (name,)) from None
    return fd


def _unlock(fd):
    if fd is None:
        return
    try:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _quiet_close(link):
    try:
        link.close()
    except Exception:
        pass


# --------------------------------------------------------------------- #
# Recovery point encoding
# --------------------------------------------------------------------- #
def _encode_checkpoint(doc):
    body = json.dumps(doc, sort_keys=True, separators=(",", ":")) \
        .encode("utf-8")
    head = "%s %d %s\n" % (CHK_MAGIC, len(body),
                           hashlib.sha256(body).hexdigest())
    return head.encode("ascii") + body


def _decode_checkpoint(blob, what):
    line, sep, body = blob.partition(b"\n")
    if not sep:
        raise CheckpointCorrupt("%s: missing header" % what)
    try:
        magic, length, digest = line.decode("ascii").split(" ")
        length = int(length)
    except (ValueError, UnicodeDecodeError):
        raise CheckpointCorrupt("%s: damaged header" % what) from None
    if magic != CHK_MAGIC:
        raise CheckpointCorrupt("%s: not a shipper recovery point" % what)
    if length != len(body):
        raise CheckpointCorrupt("%s: incomplete file or trailing data"
                                % what)
    if hashlib.sha256(body).hexdigest() != digest:
        raise CheckpointCorrupt("%s: checksum mismatch" % what)
    try:
        doc = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise CheckpointCorrupt("%s: body is not valid JSON" % what) \
            from None
    if not isinstance(doc, dict) or doc.get("format") != JOB_FORMAT:
        raise CheckpointCorrupt("%s: unsupported format" % what)
    return doc


def _encode_state(snap):
    out = dict(snap)
    payloads = []
    for p in snap["payloads"]:
        data = p["data"]
        if not isinstance(data, (bytes, bytearray)) or len(data) != p["length"]:
            raise JobError("a staged payload is missing its bytes; "
                           "cannot save")
        q = dict(p)
        q["data"] = base64.b64encode(bytes(data)).decode("ascii")
        payloads.append(q)
    out["payloads"] = payloads
    return out


def _decode_state(state, what):
    if not isinstance(state, dict):
        raise CheckpointCorrupt("%s: missing session state" % what)
    payloads = state.get("payloads")
    if not isinstance(payloads, list):
        raise CheckpointCorrupt("%s: damaged payload section" % what)
    out = dict(state)
    decoded = []
    for p in payloads:
        if not isinstance(p, dict) or not isinstance(p.get("data"), str):
            raise CheckpointCorrupt("%s: damaged payload entry" % what)
        try:
            data = base64.b64decode(p["data"].encode("ascii"), validate=True)
        except (binascii.Error, ValueError, UnicodeEncodeError):
            raise CheckpointCorrupt("%s: undecodable payload bytes" % what) \
                from None
        q = dict(p)
        q["data"] = data
        decoded.append(q)
    out["payloads"] = decoded
    return out


# Re-exported for tests and tooling that need to inspect or rebuild a single
# recovery point while keeping the file format in one place.
encode_checkpoint = _encode_checkpoint
decode_checkpoint = _decode_checkpoint


# --------------------------------------------------------------------- #
# The job handle
# --------------------------------------------------------------------- #
class Job:
    """A live handle on a persistent job.

    ``job.session`` is an ordinary :class:`shipper.session.Session`;
    delivery, fetch/confirm, exit, quota, reconnect and concurrency all
    behave exactly as they do without persistence.  :meth:`save` commits
    the session's whole state as the next recovery point.
    """

    def __init__(self, store, name, meta, session, sources, links,
                 identities, head, checkpoint_id, lock_fd):
        self._store = store
        self._name = name
        self._meta = meta
        self._session = session
        self._sources = sources
        self._links = links
        self._identities = identities
        self._head = head
        self._checkpoint_id = checkpoint_id
        self._lock_fd = lock_fd
        self._closed = False

    def __repr__(self):  # pragma: no cover
        return "<Job %r at recovery point %d>" % (self._name,
                                                  self._checkpoint_id)

    @property
    def name(self):
        return self._name

    @property
    def session(self):
        """The live delivery session."""
        return self._session

    @property
    def head(self):
        """The id of the latest committed recovery point."""
        return self._head

    @property
    def checkpoint_id(self):
        """The point this handle resumed from or last saved."""
        return self._checkpoint_id

    @property
    def closed(self):
        return self._closed

    def checkpoints(self):
        """The committed recovery point ids, oldest first."""
        return self._store.checkpoints(self._name)

    def save(self):
        """Commit the current session state as the next recovery point.

        Returns the new point's id.  The point becomes visible only once it
        is completely written; a failure raises and leaves every previously
        committed point untouched.
        """
        if self._closed:
            raise JobError("job %r is closed" % (self._name,))
        snap = self._session._snapshot()
        state = _encode_state(snap)
        chk_id = self._head + 1
        doc = {"format": JOB_FORMAT,
               "job": self._name,
               "checkpoint": chk_id,
               "created_at": _utcnow(),
               "config_fingerprint": self._meta["config_fingerprint"],
               "identities": list(self._identities),
               "state": state}
        blob = _encode_checkpoint(doc)
        try:
            _atomic_write(self._store._chk_path(self._name, chk_id), blob)
            _atomic_write(self._store._job_path(self._name, HEAD_FILE),
                          ("%d\n" % chk_id).encode("ascii"))
        except OSError as exc:
            raise JobError("could not commit recovery point %d of job %r: "
                           "%s" % (chk_id, self._name, exc)) from exc
        self._head = chk_id
        self._checkpoint_id = chk_id
        return chk_id

    def close(self):
        """Release the job: close its links and the exclusive lock."""
        if self._closed:
            return
        self._closed = True
        for link in self._links:
            _quiet_close(link)
        _unlock(self._lock_fd)
        self._lock_fd = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __del__(self):  # pragma: no cover - best effort at GC time
        try:
            self.close()
        except Exception:
            pass


# --------------------------------------------------------------------- #
# The store
# --------------------------------------------------------------------- #
class JobStore:
    """A directory of persistent jobs on local disk."""

    def __init__(self, root):
        self.root = os.fspath(root)
        os.makedirs(self.root, exist_ok=True)

    # -- paths -------------------------------------------------------------
    def _job_path(self, name, *parts):
        return os.path.join(self.root, name, *parts)

    def _chk_path(self, name, chk_id):
        return self._job_path(name, CHK_DIR, "%06d.chk" % chk_id)

    # -- introspection -------------------------------------------------------
    def jobs(self):
        """The names of all jobs that have their metadata in place."""
        if not os.path.isdir(self.root):
            return ()
        return tuple(sorted(
            n for n in os.listdir(self.root)
            if os.path.isdir(self._job_path(n))
            and os.path.exists(self._job_path(n, META_FILE))))

    def checkpoints(self, name):
        """The committed recovery point ids of ``name``, oldest first."""
        head = self._read_head(name)
        chkdir = self._job_path(name, CHK_DIR)
        ids = []
        if head is not None and os.path.isdir(chkdir):
            for f in os.listdir(chkdir):
                stem, dot, ext = f.partition(".")
                if ext == "chk" and stem.isdigit() and int(stem) <= head:
                    ids.append(int(stem))
        return tuple(sorted(ids))

    # -- creation ------------------------------------------------------------
    def create(self, name, sources, categories, max_len, quota,
               event_window=None):
        """Create a job and open it, at a fresh initial recovery point.

        The configuration and the source identities are fixed here; every
        later open of this job must present matching ones.  Raises
        :class:`JobExists` if the name is taken.  ``event_window``, when set,
        puts the session in event mode (JSON event records, time-window
        rollups) and is part of the fixed configuration.
        """
        _check_name(name)
        srcs = _resumable_sources(sources)
        cats = _normalise_categories(categories)
        config = {"categories": cats,
                  "max_len": int(max_len),
                  "quota": int(quota),
                  "event_window": (None if event_window is None
                                   else int(event_window))}
        links = [s.open(None) for s in srcs]
        identities = [str(s.identity()) for s in srcs]
        try:
            session = Session(links, cats, max_len, quota,
                              event_window=event_window)
        except Exception:
            for link in links:
                _quiet_close(link)
            raise
        dirpath = self._job_path(name)
        try:
            os.mkdir(dirpath)
        except FileExistsError:
            for link in links:
                _quiet_close(link)
            raise JobExists("job %r already exists" % (name,)) from None
        lock_fd = None
        try:
            lock_fd = _lock_dir(dirpath, name)
            os.mkdir(self._job_path(name, CHK_DIR))
            meta = {"format": JOB_FORMAT,
                    "name": name,
                    "created_at": _utcnow(),
                    "config": config,
                    "config_fingerprint": config_fingerprint(config),
                    "sources": [{"identity": i} for i in identities]}
            _atomic_write(self._job_path(name, META_FILE),
                          json.dumps(meta, indent=2, sort_keys=True)
                          .encode("utf-8"))
            job = Job(self, name, meta, session, srcs, links, identities,
                      head=0, checkpoint_id=0, lock_fd=lock_fd)
            job.save()                    # the initial recovery point
            return job
        except Exception:
            _unlock(lock_fd)
            for link in links:
                _quiet_close(link)
            shutil.rmtree(dirpath, ignore_errors=True)
            raise

    # -- opening -------------------------------------------------------------
    def open(self, name, sources, checkpoint=None):
        """Open a job at a committed recovery point (the latest by default).

        ``sources`` must be the very sources the job was created with.
        Any damage, incompatibility or incompleteness raises and yields no
        session at all; previously committed points stay openable.
        """
        _check_name(name)
        if checkpoint is not None:
            if (not isinstance(checkpoint, int)
                    or isinstance(checkpoint, bool) or checkpoint < 1):
                raise ValueError("checkpoint must be a positive integer")
        dirpath = self._job_path(name)
        if not os.path.isdir(dirpath):
            raise JobNotFound("no job named %r" % (name,))
        lock_fd = _lock_dir(dirpath, name)
        links = []
        try:
            meta = self._read_meta(name)
            srcs = _resumable_sources(sources)
            expected = [s["identity"] for s in meta["sources"]]
            given = [str(s.identity()) for s in srcs]
            if given != expected:
                raise JobMismatch(
                    "job %r is bound to its original source(s); the "
                    "provided one(s) do not match" % (name,))
            head = self._read_head(name)
            if head is None:
                raise CheckpointCorrupt(
                    "job %r has no committed recovery point" % (name,))
            chk_id = head if checkpoint is None else checkpoint
            if chk_id > head:
                raise JobError("recovery point %d of job %r was never "
                               "committed (head is %d)" % (chk_id, name,
                                                           head))
            what = "recovery point %d of job %r" % (chk_id, name)
            doc = _decode_checkpoint(
                _read_file(self._chk_path(name, chk_id), what), what)
            self._validate_doc(doc, name, chk_id)
            if doc["config_fingerprint"] != meta["config_fingerprint"]:
                raise JobMismatch(
                    "recovery point %d was made with a different "
                    "configuration" % chk_id)
            if doc["identities"] != expected:
                raise JobMismatch(
                    "recovery point %d belongs to different source(s)"
                    % chk_id)
            state = _decode_state(doc["state"], what)
            for s, sd in zip(srcs, state["sources"]):
                links.append(s.open(sd["position"]))
            cfg = meta["config"]
            try:
                session = Session(links, cfg["categories"], cfg["max_len"],
                                  cfg["quota"],
                                  event_window=cfg.get("event_window"))
                session._load_snapshot(state)
            except Exception:
                for link in links:
                    _quiet_close(link)
                raise
            return Job(self, name, meta, session, srcs, links, expected,
                       head=head, checkpoint_id=chk_id, lock_fd=lock_fd)
        except Exception:
            _unlock(lock_fd)
            raise

    # -- metadata --------------------------------------------------------------
    def _read_meta(self, name):
        blob = _read_file(self._job_path(name, META_FILE),
                          "metadata of job %r" % (name,))
        try:
            meta = json.loads(blob.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise CheckpointCorrupt("metadata of job %r is not valid JSON"
                                    % (name,)) from None
        ok = (isinstance(meta, dict)
              and meta.get("format") == JOB_FORMAT
              and meta.get("name") == name
              and isinstance(meta.get("config"), dict)
              and isinstance(meta.get("config_fingerprint"), str)
              and isinstance(meta.get("sources"), list)
              and all(isinstance(s, dict) and isinstance(s.get("identity"), str)
                      for s in meta["sources"]))
        if not ok:
            raise CheckpointCorrupt("metadata of job %r is damaged" % (name,))
        if config_fingerprint(meta["config"]) != meta["config_fingerprint"]:
            raise CheckpointCorrupt(
                "metadata of job %r does not match its fingerprint" % (name,))
        return meta

    def _read_head(self, name):
        path = self._job_path(name, HEAD_FILE)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="ascii") as f:
                head = int(f.read().strip())
        except (ValueError, OSError, UnicodeDecodeError):
            raise CheckpointCorrupt("HEAD of job %r is damaged" % (name,)) \
                from None
        if head < 1:
            raise CheckpointCorrupt("HEAD of job %r is damaged" % (name,))
        return head

    @staticmethod
    def _validate_doc(doc, name, chk_id):
        if doc.get("job") != name:
            raise JobMismatch("the recovery point belongs to job %r, not %r"
                              % (doc.get("job"), name))
        if doc.get("checkpoint") != chk_id:
            raise CheckpointCorrupt(
                "recovery point %d of job %r is mislabelled"
                % (chk_id, name))
        if not isinstance(doc.get("config_fingerprint"), str) \
                or not isinstance(doc.get("identities"), list) \
                or not all(isinstance(i, str) for i in doc["identities"]) \
                or not isinstance(doc.get("state"), dict):
            raise CheckpointCorrupt(
                "recovery point %d of job %r is damaged" % (chk_id, name))
