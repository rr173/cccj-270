"""Byte quota and the auxiliary index.

``ByteLedger``
    The single authority on ``B``.  A record's bytes are charged once, when
    pulled, and released once, when the last group still owing them has
    acknowledged or left.  Bytes that have been pulled but are not yet staged
    are charged too: the charge happens *before* the bytes are moved, so no
    state ever exists in which pulled bytes are outside the ledger.  Credit is
    tested before consuming, which is what makes "no room, do not pull"
    literal rather than best effort.

``TicketIndex``
    The auxiliary index: a per-group ``ticket -> entry`` map of records that
    are pulled but not yet settled.  It is **not** charged against ``B`` (it is
    bookkeeping, not payload), and its size is bounded by the number of
    outstanding records, which the ledger already bounds.  Entries are removed
    the moment a record is settled or abandoned, so it never accumulates
    state for records that are long gone.

    It exists so acknowledgement is O(1) and so "this number is not my head"
    can be decided without searching the staging queue: it can never grow with
    the length of the stream, only with the records currently in flight.
"""

from __future__ import annotations

import itertools


class ByteLedger:
    """Net bytes in flight against ``B``."""

    __slots__ = ("cap", "billed", "peak")

    def __init__(self, cap):
        if cap < 1:
            raise ValueError("byte quota B must be positive")
        self.cap = int(cap)
        self.billed = 0
        self.peak = 0

    @property
    def room(self):
        """Bytes that may still be charged."""
        return self.cap - self.billed

    def fits(self, n):
        """Whether ``n`` more bytes would keep the total within ``B``."""
        return self.billed + n <= self.cap

    def charge(self, n):
        if not self.fits(n):
            raise AssertionError(
                "quota exceeded: %d + %d > %d" % (self.billed, n, self.cap))
        self.billed += n
        if self.billed > self.peak:
            self.peak = self.billed

    def release(self, n):
        if n > self.billed:
            raise AssertionError("underflow: %d - %d" % (self.billed, n))
        self.billed -= n

    def __repr__(self):  # pragma: no cover
        return "<ByteLedger %d/%d>" % (self.billed, self.cap)


class TicketIndex:
    """Per-group, never-reused ticket numbers and the live entries."""

    __slots__ = ("_counter", "_max", "_live")

    def __init__(self):
        self._counter = itertools.count(1)
        self._max = 0
        self._live = {}            # ticket -> entry (a Behind)

    @classmethod
    def restore(cls, high_water, live):
        """Rebuild an index from a recovery point.

        ``high_water`` is the largest ticket ever issued (new tickets start
        after it, so a number is never reused across a recovery) and ``live``
        maps the still-outstanding tickets to their entries.
        """
        ti = cls()
        ti._counter = itertools.count(high_water + 1)
        ti._max = high_water
        ti._live = dict(live)
        return ti

    def issue(self, entry):
        """Allocate the next ticket for ``entry`` and index it."""
        t = next(self._counter)
        self._max = t
        self._live[t] = entry
        return t

    def get(self, ticket):
        return self._live.get(ticket)

    def drop(self, ticket):
        self._live.pop(ticket, None)

    def known(self, ticket):
        """True for any integer ticket this group was ever issued."""
        return isinstance(ticket, int) and 1 <= ticket <= self._max

    def live(self, ticket):
        return ticket in self._live

    def __len__(self):
        return len(self._live)

    @property
    def high_water(self):
        return self._max
