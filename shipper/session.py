"""One delivery session: ordered pull, per-group staging, acknowledgement.

The design in one paragraph
---------------------------
A *source* is strictly ordered.  The session consumes it front to back.  When
the next record's category has a live receiver it is pulled once into a single
shared :class:`~shipper.model.Payload` and claimed by every such group's queue;
the payload is billed once when pulled and released once, when the last group
owing it has acknowledged or left.  When nobody alive receives the category the
record is consumed without its bytes being produced, so it never touches ``B``.

Each group's queue is in ticket order; the group only ever sees its head.
``confirm(ticket)`` settles exactly that head: tickets are session-wide and
never reused, re-confirming is harmless, and any other number is refused
without changing state, so a wrong number can never skip a record.  ``exit``
abandons everything the group still owes and the group is gone for good.

Pulling is refused *before the source produces anything* when the record would
not fit under ``B``; acknowledging or leaving releases bytes and the pull path
resumes.  No operation waits with the lock held, so a group starved on credit
never stops another group from reading, confirming or leaving.  Operations run
under one lock, so the concurrent interface is equivalent to *some* sequential
order consistent with the order the calls were made in.

A normal end of stream reports ``"end"``; a contract violation reports
``"source_error"``.  In both cases records already staged stay fetchable and
acknowledgeable -- the tail can still be drained.
"""

from __future__ import annotations

import time

from .accounting import ByteLedger, TicketIndex
from .errors import (GroupGone, LinkDown, ProtocolError, SourceError,
                     UnknownGroup)
from .hub import Hub
from .model import Behind, Payload, Record
from .wire import SourceLink

PULLED = "pulled"
SKIPPED = "skipped"
NO_DATA = "no-data"
NO_CREDIT = "no-credit"
BROKE = "broke"
LINK_DOWN = "link-down"

FINISH_RUNNING = "running"
FINISH_END = "end"
FINISH_ERROR = "source_error"
FINISH_PENDING = "pending"

END_OF_GROUP = "end-of-group"
STARVED = "starved"

MAX_LEN_CEILING = 1 << 30


class _SourceState:
    """Everything about one source that survives a reconnect."""

    __slots__ = ("link", "done", "error", "down", "last_seq", "pulled",
                 "skipped")

    def __init__(self, link):
        self.link = link
        self.done = False
        self.error = None
        self.down = None
        self.last_seq = 0
        self.pulled = 0
        self.skipped = 0


class _GroupState:
    """Everything a group owns besides the bytes themselves."""

    __slots__ = ("name", "categories", "settled_hi", "delivery",
                 "staging", "gone_by")

    def __init__(self, name, categories):
        self.name = name
        self.categories = frozenset(categories)
        self.settled_hi = 0
        self.delivery = 0
        self.staging = []          # [Behind] in ticket order
        self.gone_by = None        # None, "exit", or "source_error"

    @property
    def gone(self):
        return self.gone_by is not None


class Group:
    """A receiver handle.  Bound to one session and one declared name."""

    __slots__ = ("_session", "name")

    def __init__(self, session, name):
        self._session = session
        self.name = name

    def __repr__(self):  # pragma: no cover
        return "<Group %r>" % (self.name,)

    @property
    def categories(self):
        return self._session._state(self.name).categories

    @property
    def delivered(self):
        """How many records this group has acknowledged."""
        return self._session._state(self.name).delivery

    @property
    def pending(self):
        """How many pulled records are staged for this group."""
        return len(self._session._state(self.name).staging)

    def peek(self):
        """The unacknowledged head, or ``None``.  Never confirms anything."""
        with self._session.hub.lock:
            g = self._session._state(self.name)
            return _view(g) if g.staging else None

    def fetch(self, block=False, timeout=None):
        """Return this group's oldest unacknowledged record.

        ``Record``  -- the head; fetching never confirms it, so the same
                       record and ticket come back until it is confirmed;
        ``False``   -- the stream ended and this group owes nothing more;
        ``None``    -- nothing is available right now (``block=False``, or the
                       ``timeout`` expired).  Nothing has been lost; retry.

        Raises :class:`GroupGone` after :meth:`exit`, and :class:`SourceError`
        when the stream is broken and the group has nothing left to take.
        """
        return self._session._fetch(self.name, block, timeout)

    def announce(self, source=0):
        """Non-consuming ``(category, length)`` promise about the next record.

        ``None`` at end of stream.  Repeating it repeats the promise; it never
        consumes and never bills.  If a later pull delivers something else,
        the source is in error.
        """
        cat, length, src = self._session.announce(source)
        return (cat, length) if cat is not None else None

    def confirm(self, ticket):
        """Acknowledge the head by ticket number.

        * the head's ticket       -> settled, bytes released, ``True``;
        * an already-settled one  -> no effect, ``True`` (harmless retry);
        * any other number        -> :class:`ProtocolError`, nothing changes.

        Confirmation, not fetching, is what advances a group.
        """
        return self._session._confirm(self.name, ticket)

    def exit(self):
        """Abandon this group for good.

        Drops everything it still owes (releasing shared bytes at their last
        reference) and forgets it.  Returns the number of records dropped.
        Records of its categories go to other live groups or are skipped; this
        group can never be used again.
        """
        return self._session._exit(self.name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._session._exit(self.name, missing_ok=True)
        return False


def _view(g):
    return Record(g.name, g.delivery, g.staging[0])


class Session:
    """A delivery session over one or more ordered sources."""

    def __init__(self, sources, categories, max_len, quota):
        """``sources``    -- a SourceLink / SourceStore, or a list of them;
        ``categories`` -- ``{category: [group names]}`` declaring every group;
        ``max_len``    -- M, the largest record;
        ``quota``      -- B, the shared payload quota (``B >= M``).
        """
        if not isinstance(sources, (list, tuple)):
            sources = [sources]
        if not sources:
            raise ValueError("at least one source is required")
        if not 1 <= int(max_len) <= MAX_LEN_CEILING:
            raise ValueError("max_len (M) must be in 1..%d" % MAX_LEN_CEILING)
        if int(quota) < int(max_len):
            raise ValueError("quota (B) must be >= max_len (M)")

        self.max_len = int(max_len)
        self.quota = int(quota)
        self.hub = Hub()
        self.ledger = ByteLedger(self.quota)
        self._sources = [_SourceState(_as_link(s)) for s in sources]

        by_category = {}
        for cat, names in categories.items():
            names = tuple(dict.fromkeys(names))
            if names:
                by_category[cat] = names
        if not by_category:
            raise ValueError("no category is received by any group")
        self._by_category = by_category
        self._groups = {}
        for names in by_category.values():
            for name in names:
                if name not in self._groups:
                    self._groups[name] = _GroupState(
                        name, (c for c, ns in by_category.items()
                               if name in ns))
        self._accepted = frozenset(by_category)

        # Tickets are session-wide and never reused, so a number handed to one
        # group can never collide with another's -- "confirm by a non-reusable
        # serial" is then true globally, not just per group.
        self._tickets = TicketIndex()
        self._finished = None
        self._error_message = None
        self._epoch = 0

    # ------------------------------------------------------------------ #
    # Public surface
    # ------------------------------------------------------------------ #
    def group(self, name):
        """The handle for a declared group."""
        self._state(name)
        return Group(self, name)

    def groups(self):
        """The declared group names."""
        return tuple(sorted(self._groups))

    @property
    def state(self):
        """``"running"``, ``"end"`` (normal end) or ``"source_error"``."""
        with self.hub.lock:
            return self._state_name()

    @property
    def error(self):
        """The source contract violation's message, if any."""
        return self._error_message

    def announce(self, source=0):
        """Non-consuming ``(category, length, source)`` for the next record.

        ``None`` at normal end.  Never consumes and never bills.  The promise
        is checked against the delivered record; a mismatch is a source error.
        """
        with self.hub.lock:
            src = self._sources[source]
            if src.down is not None:
                raise LinkDown(src.down)
            if src.error is not None:
                raise SourceError(src.error)
            try:
                ann = src.link.announce()
            except StopIteration:
                self._mark_done(src)
                return None
            except LinkDown as exc:
                self._mark_down(src, exc)
                raise LinkDown(str(exc))
            except SourceError as exc:
                self._fail(src, str(exc))
                raise
            if ann is None:
                self._mark_done(src)
                return None
            if not 0 < ann.length <= self.max_len:
                self._fail(src, "announced %dB, outside 1..%d"
                           % (ann.length, self.max_len))
                raise SourceError(self._error_message)
            return ann.category, ann.length, source

    def finish(self):
        """Read the rest of every source, then report how it ended.

        Returns ``"end"`` for a normal end, ``"source_error"`` for a broken
        stream, or ``"pending"`` when the quota is full and the stream is not
        exhausted -- acknowledge or exit, then call again.  Whatever is staged
        remains fetchable in every case.
        """
        with self.hub.lock:
            self._flush_locked()
            if self._finished == FINISH_ERROR:
                return FINISH_ERROR
            if all(s.done for s in self._sources):
                return FINISH_END
            return FINISH_PENDING

    def drain(self, max_records=None):
        """Pull and acknowledge everything reachable, then :meth:`finish`.

        Convenience for the tail: it confirms on the groups' behalf.  Returns
        ``(acknowledged, state)`` where ``state`` is a finish state, or
        ``"more"`` when ``max_records`` stopped it early.
        """
        done = 0
        with self.hub.lock:
            while True:
                g = self._next_group_with_head()
                if g is not None:
                    if max_records is not None and done >= max_records:
                        return done, "more"
                    self._settle(g, g.staging[0])
                    done += 1
                    continue
                pulled = False
                for src in self._sources:
                    if src.done or src.down is not None:
                        continue
                    if self._pull_one(src) in (PULLED, SKIPPED):
                        pulled = True
                if pulled:
                    continue
                break
            if self._finished == FINISH_ERROR:
                return done, FINISH_ERROR
            if all(s.done for s in self._sources):
                return done, FINISH_END
            return done, FINISH_PENDING

    def pending(self):
        """``{group: [Record, ...]}`` for everything still unacknowledged."""
        with self.hub.lock:
            return {name: [Record(name, g.delivery, b) for b in g.staging]
                    for name, g in self._groups.items()}

    def stats(self):
        """Accounting snapshot, for tests and operators."""
        with self.hub.lock:
            return {
                "state": self._state_name(),
                "quota": self.quota,
                "billed": self.ledger.billed,
                "peak_billed": self.ledger.peak,
                "staged": sum(len(g.staging) for g in self._groups.values()),
                "groups": {n: {"pending": len(g.staging),
                               "delivered": g.delivery,
                               "gone": g.gone,
                               "tickets_live": len(g.staging),
                               "tickets_issued": self._tickets.high_water}
                           for n, g in self._groups.items()},
                "sources": [{"done": s.done, "error": s.error,
                             "down": s.down, "pulled": s.pulled,
                             "skipped": s.skipped} for s in self._sources],
            }

    def reconnect(self, link=None, source=0):
        """Replace one source's link; nothing is replayed and nothing is lost.

        Already confirmed records stay confirmed, staged records stay staged,
        and the next pull resumes where the stream actually is.  The new link
        must reach the same stream; that is checked when it can be.
        """
        with self.hub.lock:
            src = self._sources[source]
            new_link = _as_link(link)
            old_store = getattr(src.link, "store", None)
            new_store = getattr(new_link, "store", None)
            if (old_store is not None and new_store is not None
                    and old_store is not new_store):
                raise ValueError("reconnect() must target the same stream")
            src.link = new_link
            src.down = None
            self._bump()

    def down(self):
        """True while some source's transport is unusable."""
        with self.hub.lock:
            return any(s.down is not None and not s.done for s in self._sources)

    def wait(self, timeout=None):
        """Block until any state change, then return the current state."""
        with self.hub.lock:
            seen = self._epoch
            self.hub.wait_until(lambda: self._epoch != seen, timeout)
            return self._state_name()

    # ------------------------------------------------------------------ #
    # Read path
    # ------------------------------------------------------------------ #
    def _fetch(self, name, block, timeout):
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.hub.lock:
            while True:
                g = self._state(name)
                if g.staging:
                    return _view(g)
                verdict = self._advance_for(g)
                if g.staging:
                    return _view(g)
                if verdict == END_OF_GROUP:
                    return False
                if not block:
                    return None
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                seen = self._epoch
                if not self.hub.wait_until(
                        lambda: bool(g.staging) or g.gone
                        or self._finished is not None or self._epoch != seen,
                        remaining):
                    return None

    def _advance_for(self, g):
        """Pull until ``g`` has a head, the stream ends, or credit runs out.

        Sources are independent: a fault on one marks only that source dead.
        A group keeps draining every healthy source and only learns of a source
        error once nothing can deliver to it any more -- so records valid before
        the fault, and everything on the other sources, stay obtainable.
        """
        while True:
            if g.staging:
                return "head"
            if g.gone:
                raise GroupGone(g.name)
            progressed = starved = False
            for src in self._sources:
                r = self._pull_one(src)
                if r in (PULLED, SKIPPED):
                    progressed = True
                elif r is NO_CREDIT:
                    starved = True
                if g.staging:
                    return "head"
            if g.gone:
                raise GroupGone(g.name)
            if progressed:
                continue
            if starved:
                return STARVED
            if all(s.done for s in self._sources):
                if any(s.error for s in self._sources):
                    raise SourceError(self._first_error())
                self._finish_end()
                return END_OF_GROUP
            return STARVED

    def _first_error(self):
        for s in self._sources:
            if s.error:
                return s.error
        return self._error_message or "source error"

    def _flush_locked(self):
        """Offer pulls to every source while they still have room."""
        for src in self._sources:
            while not src.done and src.down is None:
                if self._pull_one(src) not in (PULLED, SKIPPED):
                    break

    def _next_group_with_head(self):
        for name in sorted(self._groups):
            g = self._groups[name]
            if g.staging and not g.gone:
                return g
        return None

    # ------------------------------------------------------------------ #
    # One pull
    # ------------------------------------------------------------------ #
    def _pull_one(self, src):
        """Consume at most one record from ``src``; report what happened."""
        if src.done:
            return NO_DATA
        if src.down is not None:
            return LINK_DOWN
        try:
            ann = src.link.announce()
        except StopIteration:
            self._mark_done(src)
            return NO_DATA
        except LinkDown as exc:
            self._mark_down(src, exc)
            return LINK_DOWN
        except SourceError as exc:
            self._fail(src, str(exc))
            return BROKE
        if ann is None:
            self._mark_done(src)
            return NO_DATA
        if not 0 < ann.length <= self.max_len:
            self._fail(src, "announced %dB, outside 1..%d"
                       % (ann.length, self.max_len))
            return BROKE

        receivers = self._live_receivers(ann.category)
        if not receivers:
            try:
                src.link.skip()
            except StopIteration:
                self._mark_done(src)
                return NO_DATA
            except LinkDown as exc:
                self._mark_down(src, exc)
                return LINK_DOWN
            except SourceError as exc:
                self._fail(src, str(exc))
                return BROKE
            src.skipped += 1
            return SKIPPED

        # Credit is checked before anything is consumed: nothing is pulled
        # that cannot be billed, so "no room means do not pull" is literal.
        if not self.ledger.fits(ann.length):
            return NO_CREDIT

        try:
            pulled = src.link.take()
        except StopIteration:
            self._mark_done(src)
            return NO_DATA
        except LinkDown as exc:
            self._mark_down(src, exc)
            return LINK_DOWN
        except SourceError as exc:
            self._fail(src, str(exc))
            return BROKE

        problem = self._validate_pulled(src, ann, pulled)
        if problem is not None:
            self._fail(src, problem)
            return BROKE

        self.ledger.charge(pulled.length)
        payload = Payload(pulled.seq, pulled.category, pulled.length,
                          pulled.data)
        payload.billed = True
        for name in receivers:
            gs = self._groups[name]
            behind = Behind(payload, 0)
            behind.ticket = self._tickets.issue(behind)
            gs.staging.append(behind)
            payload.refs += 1
        src.pulled += 1
        src.last_seq = pulled.seq
        self._bump()
        return PULLED

    # ------------------------------------------------------------------ #
    # Acknowledgement and leaving
    # ------------------------------------------------------------------ #
    def _confirm(self, name, ticket):
        with self.hub.lock:
            g = self._state(name)
            if isinstance(ticket, bool) or not isinstance(ticket, int):
                raise ProtocolError("ticket must be an integer, got %r"
                                    % (ticket,))
            if not self._tickets.known(ticket):
                raise ProtocolError("ticket %d was never issued" % (ticket,))
            behind = self._tickets.get(ticket)
            if behind is not None:
                if g.staging and g.staging[0] is behind:
                    self._settle(g, behind)
                    return True
                raise ProtocolError(
                    "ticket %d would skip the head (ticket %d) of %r"
                    % (ticket, g.staging[0].ticket if g.staging else -1,
                       name))
            return True                    # already settled: harmless retry

    def _settle(self, g, behind):
        if not g.staging or g.staging[0] is not behind:
            raise AssertionError("settling a non-head record")
        g.staging.pop(0)
        g.settled_hi = behind.ticket
        g.delivery += 1
        self._tickets.drop(behind.ticket)
        self._release(behind.payload)
        self._bump()

    def _release(self, payload):
        """Drop one claim; free the bytes when the last claim is gone."""
        if payload.refs > 0:
            payload.refs -= 1
        if payload.refs == 0:
            if payload.billed:
                self.ledger.release(payload.length)
                payload.billed = False
            payload.data = None

    def _exit(self, name, missing_ok=False):
        with self.hub.lock:
            g = self._groups.get(name)
            if g is None:
                if missing_ok:
                    return 0
                raise UnknownGroup(name)
            if g.gone:
                if missing_ok:
                    return 0
                raise GroupGone(name)
            dropped = len(g.staging)
            for behind in g.staging:
                self._tickets.drop(behind.ticket)
                self._release(behind.payload)
            g.staging = []
            g.gone_by = "exit"
            self._bump()
            return dropped

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _state(self, name):
        g = self._groups.get(name)
        if g is None:
            raise UnknownGroup(name)
        if g.gone_by is not None:
            raise GroupGone(name)
        return g

    def _live_receivers(self, category):
        return tuple(n for n in self._by_category.get(category, ())
                     if not self._groups[n].gone)

    def _state_name(self):
        if self._finished == FINISH_ERROR:
            return FINISH_ERROR
        if all(s.done for s in self._sources):
            return FINISH_END
        return FINISH_RUNNING

    def _finish_end(self):
        if self._finished is None:
            self._finished = FINISH_END
            self._bump()

    def _validate_pulled(self, src, ann, pulled):
        if pulled.category != ann.category or pulled.length != ann.length:
            return ("announced %r/%dB but the source delivered %r/%dB"
                    % (ann.category, ann.length, pulled.category,
                       pulled.length))
        if not 0 < pulled.length <= self.max_len:
            return ("delivered %dB, outside 1..%d"
                    % (pulled.length, self.max_len))
        if pulled.seq <= src.last_seq:
            return ("record #%d follows #%d: the stream is not strictly "
                    "ordered" % (pulled.seq, src.last_seq))
        return None

    def _mark_done(self, src):
        if not src.done:
            src.done = True
            self._bump()

    def _mark_down(self, src, exc):
        src.down = str(exc) or "link down"
        self._bump()

    def _fail(self, src, message):
        src.done = True
        if src.error is None:
            src.error = message
        if self._finished != FINISH_ERROR:
            self._finished = FINISH_ERROR
            self._error_message = message
        self._bump()

    def _bump(self):
        self._epoch += 1
        self.hub.wake()


def _as_link(source):
    """Accept a link, or a bare store, for convenience."""
    if isinstance(source, SourceLink):
        return source
    if hasattr(source, "consume") and hasattr(source, "peek"):
        return _StoreLink(source)
    raise TypeError("expected a SourceLink or SourceStore, got %r" % (source,))


class _StoreLink(SourceLink):
    __slots__ = ("store",)

    def __init__(self, store):
        self.store = store

    def announce(self):
        return self.store.peek()

    def take(self):
        return self.store.consume(materialise=True)

    def skip(self):
        return self.store.consume(materialise=False)
