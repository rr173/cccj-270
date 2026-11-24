"""Records, the shared payload, and the ticket that settles it.

Design decision that keeps the byte quota honest: **one record is billed
once**.  When several groups receive the same category, they do not get
private copies of the bytes; they share one :class:`Payload` through
references.  ``Payload.length`` is charged to the ledger exactly once, and
released exactly once, when the last group that still needs those bytes has
settled them (or left).  Any other reading would make the accounting depend on
how many groups happened to share a record, which the brief's "B is the shared
payload quota" does not ask for.

Tickets are per group and never reused.  A group's queue is in ticket order, so
its head is exactly the record it may acknowledge next; prefetching further
into the stream just adds tickets behind the head.
"""

from __future__ import annotations


class Payload:
    """One record's bytes, shared by every group that receives it."""

    __slots__ = ("seq", "category", "length", "data", "billed", "refs")

    def __init__(self, seq, category, length, data):
        self.seq = seq
        self.category = category
        self.length = length
        self.data = data
        self.billed = False        # charged against B right now
        self.refs = 0              # groups still owing this record

    def __repr__(self):  # pragma: no cover - debugging aid
        return "<Payload #%d %r %dB refs=%d%s>" % (
            self.seq, self.category, self.length, self.refs,
            " billed" if self.billed else "")


class Behind:
    """One group's claim on a payload: it cannot pass it without this ticket."""

    __slots__ = ("payload", "ticket")

    def __init__(self, payload, ticket=0):
        self.payload = payload
        self.ticket = ticket

    @property
    def length(self):
        return self.payload.length


class Record:
    """What a group sees.

    ``data`` is the shared bytes; it is not a second copy.  Once the record is
    acknowledged the bytes may be released (if no other group still owes
    them), and ``data`` becomes ``None`` rather than keeping them alive.

    Fetching a head never settles it: ``confirm(ticket)`` is the only operation
    that advances a group, which is why a slow group can never be made to
    "re-send" a record its fast peer already confirmed.
    """

    __slots__ = ("group", "index", "ticket", "category", "length", "seq",
                 "_payload")

    def __init__(self, group, index, behind):
        self.group = group
        self.index = index
        self.ticket = behind.ticket
        self.category = behind.payload.category
        self.length = behind.payload.length
        self.seq = behind.payload.seq
        self._payload = behind.payload

    @property
    def data(self):
        return self._payload.data

    @property
    def settled(self):
        return self._payload.data is None

    def __repr__(self):  # pragma: no cover
        return "<Record %s#%d %r %dB ticket=%d%s>" % (
            self.group, self.index, self.category, self.length, self.ticket,
            " settled" if self.settled else "")
