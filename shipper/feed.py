"""A local file-backed source: the "far more than memory" stream.

A *feed* is one file of fixed-size frames behind a small header, written
once by :class:`FeedWriter` and read by :class:`FileSource`.  Because every
frame is the same size, a link can seek to any record in O(1) and reads
exactly one frame at a time: the stream as a whole is never materialised in
memory, which is what makes it usable for recovery points over arbitrarily
large inputs.

Layout::

    header (64 bytes):  magic, version, category cap, max record length M,
                        record count, stream id, frame size
    frame i (at 64 + i * frame_size):
                        seq u64, length u32, category-length u8,
                        category (padded to the cap), data (padded to M)

:class:`FileLink` honours the same contract as
:class:`shipper.wire.SourceStore`: announcements are non-consuming promises,
a promise/record mismatch, an out-of-range length or a non-increasing
sequence number is a sticky :class:`SourceError`, and end of stream is
``None`` / ``StopIteration``.
"""

from __future__ import annotations

import os
import struct
import uuid

from .errors import JobMismatch, LinkDown, SourceError
from .wire import Announcement, Pulled, SourceLink, check_position

MAGIC = b"SHPFEED1"
VERSION = 1
HEADER_SIZE = 64
HEADER = struct.Struct(">8sIIIIQ16sI")   # magic, version, cat_cap, max_len,
                                         # reserved, count, stream_id, frame
FRAME_HEAD = struct.Struct(">QIB")       # seq, length, category length
CAT_CAP_DEFAULT = 32


class FeedWriter:
    """Writes a feed file once, record by record.  Not thread-safe."""

    def __init__(self, path, max_len, stream_id=None, cat_cap=CAT_CAP_DEFAULT):
        if not 1 <= int(max_len):
            raise ValueError("max_len must be positive")
        if not 1 <= int(cat_cap) <= 255:
            raise ValueError("cat_cap must be in 1..255")
        self.path = path
        self.max_len = int(max_len)
        self.cat_cap = int(cat_cap)
        self.stream_id = stream_id or uuid.uuid4().hex
        self.frame_size = FRAME_HEAD.size + self.cat_cap + self.max_len
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        self._count = 0
        self._last = 0
        self._closed = False
        self._write_header(0)

    def _write_header(self, count):
        buf = HEADER.pack(MAGIC, VERSION, self.cat_cap, self.max_len, 0,
                          count, uuid.UUID(self.stream_id).bytes,
                          self.frame_size)
        os.pwrite(self._fd, buf + b"\0" * (HEADER_SIZE - len(buf)), 0)

    @property
    def count(self):
        return self._count

    def append(self, seq, category, data):
        """Append one record: strictly increasing ``seq``, ``1..M`` bytes."""
        if self._closed:
            raise ValueError("the writer is closed")
        cat = category.encode("utf-8") if isinstance(category, str) \
            else bytes(category)
        if len(cat) > self.cat_cap:
            raise ValueError("category %r is longer than %d bytes"
                             % (category, self.cat_cap))
        if not isinstance(seq, int) or seq <= self._last:
            raise ValueError("record seqs must be strictly increasing")
        if not 0 < len(data) <= self.max_len:
            raise ValueError("record #%d is %d bytes, outside 1..%d"
                             % (seq, len(data), self.max_len))
        frame = FRAME_HEAD.pack(seq, len(data), len(cat)) + cat
        frame += b"\0" * (self.cat_cap - len(cat))
        frame += data + b"\0" * (self.max_len - len(data))
        os.pwrite(self._fd, frame,
                  HEADER_SIZE + self._count * self.frame_size)
        self._last = seq
        self._count += 1

    def close(self):
        """Fix the record count in the header and flush to disk."""
        if self._closed:
            return
        self._closed = True
        try:
            self._write_header(self._count)
            os.fsync(self._fd)
        finally:
            os.close(self._fd)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class FileSource:
    """A resumable source over a feed file.

    ``identity()`` names the stream (a random id fixed when the feed was
    written), and ``open(position)`` hands out a link placed exactly at a
    recovery point's position -- or refuses, when the position does not lie
    on this stream.
    """

    def __init__(self, path):
        self.path = path
        self._fd = os.open(path, os.O_RDONLY)
        self._closed = False
        try:
            buf = os.pread(self._fd, HEADER_SIZE, 0)
            if len(buf) < HEADER_SIZE:
                raise SourceError("feed header is incomplete: %r" % path)
            (magic, version, cat_cap, max_len, _reserved, count, stream_id,
             frame_size) = HEADER.unpack(buf[:HEADER.size])
            if magic != MAGIC:
                raise SourceError("not a shipper feed: %r" % path)
            if version != VERSION:
                raise SourceError("unsupported feed version %d" % version)
            if max_len < 1 or cat_cap < 1 \
                    or frame_size != FRAME_HEAD.size + cat_cap + max_len:
                raise SourceError("feed header is inconsistent: %r" % path)
            size = os.fstat(self._fd).st_size
            if size < HEADER_SIZE + count * frame_size:
                raise SourceError("feed file is truncated: %r" % path)
        except Exception:
            os.close(self._fd)
            self._closed = True
            raise
        self.max_len = max_len
        self.cat_cap = cat_cap
        self.count = count
        self.frame_size = frame_size
        self.stream_id = uuid.UUID(bytes=stream_id).hex

    def identity(self):
        return "feed:" + self.stream_id

    def open(self, position=None):
        """A link placed at ``position`` (a resume token, or ``None``)."""
        consumed, last = check_position(position)
        if consumed > self.count:
            raise JobMismatch(
                "position %d is beyond the end of the feed (%d records)"
                % (consumed, self.count))
        if consumed and self._seq_at(consumed - 1) != last:
            raise JobMismatch(
                "record %d of the feed is seq %d, but the recovery point "
                "expects seq %d" % (consumed, self._seq_at(consumed - 1),
                                    last))
        return FileLink(self, consumed, last)

    def link(self):
        """A link at the start of the feed (parity with ``MockSource``)."""
        return self.open(None)

    def _seq_at(self, i):
        buf = os.pread(self._fd, FRAME_HEAD.size,
                       HEADER_SIZE + i * self.frame_size)
        if len(buf) < FRAME_HEAD.size:
            raise JobMismatch("the feed ends before the recovery point's "
                              "position")
        return FRAME_HEAD.unpack(buf)[0]

    def close(self):
        if not self._closed:
            self._closed = True
            os.close(self._fd)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class FileLink(SourceLink):
    """One link over a feed: reads a single frame at a time, never more."""

    __slots__ = ("src", "_fd", "pos", "_last", "_promise", "error", "_closed")

    def __init__(self, src, pos, last_seq):
        self.src = src
        self._fd = os.open(src.path, os.O_RDONLY)
        self.pos = pos
        self._last = last_seq
        self._promise = None
        self.error = None
        self._closed = False

    # -- contract -----------------------------------------------------------
    def _bad(self, message):
        if self.error is None:
            self.error = message
        raise SourceError(self.error)

    def _usable(self):
        if self._closed:
            raise LinkDown("link closed")
        if self.error is not None:
            raise SourceError(self.error)

    def _head_and_cat(self, i):
        need = FRAME_HEAD.size + self.src.cat_cap
        buf = os.pread(self._fd, need, HEADER_SIZE + i * self.src.frame_size)
        if len(buf) < need:
            self._bad("feed is truncated at record %d" % i)
        seq, length, cat_len = FRAME_HEAD.unpack(buf[:FRAME_HEAD.size])
        if cat_len > self.src.cat_cap:
            self._bad("record %d has a corrupt category" % i)
        try:
            cat = buf[FRAME_HEAD.size:FRAME_HEAD.size + cat_len].decode("utf-8")
        except UnicodeDecodeError:
            self._bad("record %d has a corrupt category" % i)
        return seq, length, cat

    def _check(self, seq, length, cat, i):
        if not 0 < length <= self.src.max_len:
            self._bad("record %d is %d bytes, outside 1..%d"
                      % (i, length, self.src.max_len))
        ann = self._promise
        if ann is not None and ann.pos == i \
                and (ann.category != cat or ann.length != length):
            self._bad("announced %r/%dB at position %d; the stream "
                      "delivered %r/%dB" % (ann.category, ann.length, i,
                                            cat, length))
        if seq <= self._last:
            self._bad("record #%d follows #%d: stream is not ordered"
                      % (seq, self._last))

    # -- protocol ------------------------------------------------------------
    def announce(self):
        self._usable()
        if self.pos >= self.src.count:
            self._promise = None
            return None
        seq, length, cat = self._head_and_cat(self.pos)
        if not 0 < length <= self.src.max_len:
            self._bad("record %d is %d bytes, outside 1..%d"
                      % (self.pos, length, self.src.max_len))
        ann = Announcement(cat, length, self.pos)
        self._promise = ann
        return ann

    def take(self):
        self._usable()
        if self.pos >= self.src.count:
            self._promise = None
            raise StopIteration
        i = self.pos
        seq, length, cat = self._head_and_cat(i)
        self._check(seq, length, cat, i)
        off = HEADER_SIZE + i * self.src.frame_size \
            + FRAME_HEAD.size + self.src.cat_cap
        data = os.pread(self._fd, length, off)
        if len(data) < length:
            self._bad("feed is truncated at record %d" % i)
        self.pos += 1
        self._last = seq
        self._promise = None
        return Pulled(cat, length, data, seq)

    def skip(self):
        self._usable()
        if self.pos >= self.src.count:
            self._promise = None
            raise StopIteration
        i = self.pos
        seq, length, cat = self._head_and_cat(i)
        self._check(seq, length, cat, i)
        self.pos += 1
        self._last = seq
        self._promise = None
        return Pulled(cat, length, None, seq)

    def position(self):
        """The resume token for this exact spot of the stream."""
        return {"consumed": self.pos, "last_seq": self._last}

    def close(self):
        if not self._closed:
            self._closed = True
            os.close(self._fd)
