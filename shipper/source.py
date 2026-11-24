"""The simulated sources.

:class:`MockSource`
    One finite, strictly ordered stream, validated as it is built and again as
    it is consumed.  ``link()`` hands out one link per session.
    ``on_produce`` fires for every record whose bytes are actually
    materialised, which is how tests prove that credit pressure *prevented* a
    pull rather than merely delayed it.

``SyntheticLink`` / :func:`stream`
    A generator-backed source for "much more data than memory": nothing is
    stored up front, so a test can run millions of records through the same
    code paths without building them all first.
"""

from __future__ import annotations

import random

from .errors import SourceError
from .wire import Announcement, MemoryLink, Pulled, SourceLink, SourceStore, Spec


def _validate(specs, max_len):
    if not specs:
        raise ValueError("a source must have at least one record")
    prev = None
    for s in specs:
        if not 0 < s.length <= max_len:
            raise ValueError("record #%d is %d bytes, outside 1..%d"
                             % (s.seq, s.length, max_len))
        if prev is not None and s.seq <= prev:
            raise ValueError("record #%d is not after #%d" % (s.seq, prev))
        prev = s.seq


def uniform_specs(n, categories, min_len=1, max_len=16, seed=None):
    """``n`` records with uniformly drawn categories and lengths."""
    rng = random.Random(seed)
    cats = list(categories)
    return [Spec(i + 1, rng.choice(cats), rng.randint(min_len, max_len))
            for i in range(n)]


class MockSource:
    """A strictly ordered stream with observable production."""

    def __init__(self, specs, max_len=None, *, strict=True):
        specs = list(specs)
        max_len = max_len if max_len is not None else max(s.length for s in specs)
        _validate(specs, max_len)
        self.specs = specs
        self.max_len = max_len
        self.store = SourceStore(specs, max_len, strict=strict)
        self.produced = []          # seqs whose bytes were materialised
        self.on_produce = None

    @classmethod
    def uniform(cls, n, categories, min_len=1, max_len=16, seed=None, **kw):
        return cls(uniform_specs(n, categories, min_len, max_len, seed), **kw)

    def link(self):
        return _ObservedLink(self)

    # -- test controls ----------------------------------------------------
    def corrupt(self, seq, *, category=None, length=None):
        """Change one record's truth, keeping its position.

        The store validates against its own spec list, so changing it builds
        a source that announces one thing and delivers another.
        """
        for i, s in enumerate(self.specs):
            if s.seq == seq:
                new = Spec(s.seq,
                           s.category if category is None else category,
                           s.length if length is None else length)
                self.specs[i] = new
                self.store.specs[i] = new
                return
        raise KeyError(seq)

    def mark_produce(self, seq):
        self.produced.append(seq)
        if self.on_produce is not None:
            self.on_produce(seq)

    # -- introspection ----------------------------------------------------
    @property
    def total_bytes(self):
        return self.store.total_bytes

    @property
    def consumed(self):
        return self.store.consumed

    @property
    def remaining(self):
        return self.store.remaining

    @property
    def error(self):
        return self.store.error


class _ObservedLink(MemoryLink):
    __slots__ = ("mock",)

    def __init__(self, mock):
        super().__init__(mock.store)
        self.mock = mock

    def take(self):
        pulled = super().take()
        self.mock.mark_produce(pulled.seq)
        return pulled

    def skip(self):
        return super().skip()


class SyntheticLink(SourceLink):
    """A link over a generator of :class:`Spec`, for unbounded streams.

    Nothing is prebuilt: the stream exists only as far as the consumer has
    pulled it, which is exactly the "total can far exceed memory" case.  A
    generator cannot be rewound, so this link is deliberately not
    reconnectable -- :class:`SourceStore`-backed links are.
    """

    def __init__(self, gen):
        self._gen = gen
        self._next = None
        self._have = False
        self.pos = 0
        self.error = None
        self.done = False

    def _fill(self):
        if self._have:
            return True
        if self.done:
            return False
        try:
            self._next = next(self._gen)
        except StopIteration:
            self.done = True
            return False
        if self.error is not None:
            raise SourceError(self.error)
        self._have = True
        return True

    def announce(self):
        if self.error is not None:
            raise SourceError(self.error)
        if not self._fill():
            return None
        return Announcement(self._next.category, self._next.length, self.pos)

    def take(self):
        if self.error is not None:
            raise SourceError(self.error)
        if not self._fill():
            raise StopIteration
        spec = self._next
        self._have = False
        self.pos += 1
        if not 0 < spec.length <= 1 << 40:
            msg = "record #%d is %d bytes" % (spec.seq, spec.length)
            self.error = msg
            raise SourceError(msg)
        return Pulled(spec.category, spec.length, bytes(spec.length), spec.seq)

    def skip(self):
        if not self._fill():
            raise StopIteration
        spec = self._next
        self._have = False
        self.pos += 1
        return Pulled(spec.category, spec.length, None, spec.seq)


def stream(n, categories, min_len=1, max_len=16, seed=None):
    """A :class:`SyntheticLink` over ``n`` generated records."""
    rng = random.Random(seed)
    cats = list(categories)

    def gen():
        for i in range(1, n + 1):
            yield Spec(i, rng.choice(cats), rng.randint(min_len, max_len))

    return SyntheticLink(gen())
