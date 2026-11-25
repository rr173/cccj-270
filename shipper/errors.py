"""Exception types reported by the shipper.

The taxonomy is deliberately narrow:

``SourceError``
    The ordered stream violated its contract: an announcement that did not
    match the record delivered, a record outside ``1..M`` bytes, or a
    non-increasing sequence number.  This is a property of the *source*, so it
    is sticky at the source and is reported separately from a normal end.

``LinkDown``
    The transport failed.  This says nothing about the stream: the session
    keeps its position, staged bytes and acknowledgements, and may be
    reconnected with :meth:`Session.reconnect`.

``GroupGone`` / ``UnknownGroup``
    The receiver left (and may never rejoin), or was never declared.

``ProtocolError``
    The caller named an acknowledgement that is not the group's head.  It is
    refused and changes nothing, so a wrong number can never skip a record.
"""


class ShipperError(Exception):
    """Base class for the shipper's failure kinds."""


class SourceError(ShipperError):
    """The source violated its contract."""


class LinkDown(ShipperError):
    """The transport to a source failed.  The stream itself is still valid."""


class GroupGone(ShipperError):
    """The group has left.  A group that left can never rejoin."""


class UnknownGroup(ShipperError):
    """No group with that name was declared."""


class ProtocolError(ShipperError):
    """An acknowledgement was not the group's unacknowledged head."""


class JobError(ShipperError):
    """A persistent job could not be created, opened or saved."""


class JobExists(JobError):
    """The job name is already taken."""


class JobNotFound(JobError):
    """No job with that name."""


class JobMismatch(JobError):
    """The source or configuration does not match the recovery point."""


class CheckpointCorrupt(JobError):
    """A recovery point or the job metadata is damaged, incomplete or foreign."""
