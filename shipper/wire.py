"""Wire protocol between a session and a source, plus an in-memory link.

A session drives a source only through a :class:`SourceLink`, the same
interface a socket implementation would export.  That makes the delivery
guarantees a property of the protocol rather than of object sharing.
``MemoryLink`` implements the protocol over a shared :class:`SourceStore`,
which is what :class:`shipper.MockSource` uses.

What the source guarantees, and cannot be talked out of:

* :meth:`announce` is non-consuming and repeatable -- asking twice yields the
  same promise, and never touches the stream.
* :meth:`take` consumes exactly one record and checks it against any live
  promise at that position.  A mismatch, a record outside ``1..M`` bytes, or a
  non-increasing sequence number is a **source error**; from then on ``take``
  and ``announce`` raise :class:`SourceError`.
* :meth:`skip` consumes a record nobody receives without materialising or
  billing its bytes.  It validates exactly like :meth:`take`.
* At normal end of stream ``announce`` returns ``None`` and ``take`` raises
  :class:`StopIteration`, so "finished" and "broke" are never confused.
"""

from __future__ import annotations

import threading

from .errors import JobMismatch, LinkDown, SourceError


def check_position(position):
    """Normalise a resume token to ``(consumed, last_seq)``.

    A position token is ``{"consumed": n, "last_seq": s}``: how many records
    of the stream were consumed, and the sequence number of the last one
    (``0`` when none was).  Anything else cannot describe a recovery point,
    so it is refused with :class:`JobMismatch`.
    """
    if position is None:
        return 0, 0
    if not isinstance(position, dict):
        raise JobMismatch("a resume position must be a mapping, got %r"
                          % (position,))
    consumed = position.get("consumed")
    last = position.get("last_seq")
    ok = (isinstance(consumed, int) and not isinstance(consumed, bool)
          and isinstance(last, int) and not isinstance(last, bool)
          and consumed >= 0 and last >= 0 and (consumed > 0 or last == 0))
    if not ok:
        raise JobMismatch("damaged resume position: %r" % (position,))
    return consumed, last


class Announcement:
    """A non-consuming promise about the record at ``pos``."""

    __slots__ = ("category", "length", "pos")

    def __init__(self, category, length, pos):
        self.category = category
        self.length = length
        self.pos = pos

    def __repr__(self):  # pragma: no cover
        return "<Announcement %r %dB @%d>" % (
            self.category, self.length, self.pos)


class Pulled:
    """A consumed record.  ``data`` is ``None`` for a skipped record."""

    __slots__ = ("category", "length", "data", "seq")

    def __init__(self, category, length, data, seq):
        self.category = category
        self.length = length
        self.data = data
        self.seq = seq

    def __repr__(self):  # pragma: no cover
        return "<Pulled %r %dB #%d>" % (self.category, self.length, self.seq)


class SourceLink:
    """One ordered source as a session sees it.

    Implementations may raise :class:`LinkDown` from any call to mean "the
    transport failed, the stream is still fine".
    """

    def announce(self):
        """Promise about the next record, or ``None`` at end of stream."""
        raise NotImplementedError

    def take(self):
        """Consume one record, bytes included.  Raises ``StopIteration`` at end."""
        raise NotImplementedError

    def skip(self):
        """Consume one record, without producing its bytes."""
        raise NotImplementedError

    def close(self):
        """Release the transport.  Idempotent."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class Spec:
    """One record of a stream: sequence number, category, length."""

    __slots__ = ("seq", "category", "length")

    def __init__(self, seq, category, length):
        self.seq = seq
        self.category = category
        self.length = length

    def __repr__(self):  # pragma: no cover
        return "Spec(#%d, %r, %d)" % (self.seq, self.category, self.length)


class SourceStore:
    """Server-side state of one ordered stream; shared by every link to it.

    The lock covers only short mutations.  It is never held while a caller
    processes a record, so a slow consumer cannot stall another link.
    """

    def __init__(self, specs, max_len, strict=True):
        self.specs = specs
        self.max_len = int(max_len)
        self.strict = bool(strict)
        self.pos = 0
        self.error = None
        self.total_bytes = sum(s.length for s in specs)
        self._lock = threading.RLock()
        # At most one live promise exists at a time, for the record at
        # ``pos``.  Repeating announce() must not grow anything (a source is
        # allowed to be asked again and again), and a promise for a consumed
        # record is worthless, so it is dropped the moment the stream moves.
        self._promise = None

    # -- contract ---------------------------------------------------------
    def _raise_if_bad(self):
        if self.error is not None:
            raise SourceError(self.error)

    def _bad(self, message):
        if self.strict and self.error is None:
            self.error = message
        raise SourceError(self.error or message)

    # -- protocol ---------------------------------------------------------
    def peek(self):
        with self._lock:
            self._raise_if_bad()
            if self.pos >= len(self.specs):
                self._promise = None
                return None
            s = self.specs[self.pos]
            ann = Announcement(s.category, s.length, self.pos)
            self._promise = ann
            return ann

    def consume(self, materialise=True):
        with self._lock:
            self._raise_if_bad()
            if self.pos >= len(self.specs):
                self._promise = None
                raise StopIteration
            s = self.specs[self.pos]
            if not 0 < s.length <= self.max_len:
                self._bad("record #%d is %d bytes, outside 1..%d"
                          % (s.seq, s.length, self.max_len))
            ann = self._promise
            if ann is not None and ann.pos == self.pos:
                if s.category != ann.category or s.length != ann.length:
                    self._bad(
                        "announced %r/%dB at position %d; the stream "
                        "delivered %r/%dB" % (ann.category, ann.length,
                                              self.pos, s.category, s.length))
            if self.pos and s.seq <= self.specs[self.pos - 1].seq:
                self._bad("record #%d follows #%d: stream is not ordered"
                          % (s.seq, self.specs[self.pos - 1].seq))
            self.pos += 1
            self._promise = None
            data = bytes(s.length) if materialise else None
            return Pulled(s.category, s.length, data, s.seq)

    def void(self, announcement):
        """Forget a promise that will never be honoured."""
        with self._lock:
            if self._promise is announcement:
                self._promise = None

    # -- diagnostics ------------------------------------------------------
    def position(self):
        """A resume token: records consumed so far, and the last record's seq."""
        with self._lock:
            last = self.specs[self.pos - 1].seq if self.pos else 0
            return {"consumed": self.pos, "last_seq": last}

    @property
    def remaining(self):
        with self._lock:
            return len(self.specs) - self.pos

    @property
    def consumed(self):
        with self._lock:
            return self.pos


class MemoryLink(SourceLink):
    """A link that is just method calls on a :class:`SourceStore`."""

    __slots__ = ("store", "_closed")

    def __init__(self, store):
        self.store = store
        self._closed = False

    def _usable(self):
        if self._closed:
            raise LinkDown("link closed")

    def announce(self):
        self._usable()
        return self.store.peek()

    def take(self):
        self._usable()
        return self.store.consume(materialise=True)

    def skip(self):
        self._usable()
        return self.store.consume(materialise=False)

    def position(self):
        """The store's resume token; see :meth:`SourceStore.position`."""
        return self.store.position()

    def close(self):
        self._closed = True


class FlakyLink(SourceLink):
    """A link that can be told to drop, for reconnect tests."""

    def __init__(self, link):
        self._link = link
        self.down = False
        self.calls = 0

    def _guard(self):
        if self.down:
            raise LinkDown("injected transport failure")
        self.calls += 1

    def announce(self):
        self._guard()
        return self._link.announce()

    def take(self):
        self._guard()
        return self._link.take()

    def skip(self):
        self._guard()
        return self._link.skip()

    def close(self):
        self._link.close()
