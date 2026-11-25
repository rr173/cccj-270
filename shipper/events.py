"""Event payloads and the per-group time-window rollup.

An *event* is one JSON object carried as a record's payload::

    {"event_time": 1700000123, "metric": "clicks", "value": 42}

``event_time`` is UTC seconds (an integer), ``metric`` is a string and
``value`` is an integer.  Parsing is deliberately strict: anything else --
malformed JSON, a missing or extra field, a non-object document, a duplicate
key, or a field of the wrong type (booleans are not integers, floats are not
integers) -- raises :class:`~shipper.errors.EventError` and names the problem.

The :class:`EventRollup` is the *only* aggregate state.  Events are folded in
as their records are confirmed and are never retained: the structure grows
with the number of distinct
``(window, category, metric)`` keys, never with the number of events, so a
stream far larger than memory is summarised without caching its raw events.

Windows are fixed-width, left-closed/right-open intervals aligned to the
epoch: window ``w`` covers ``[w*width, (w+1)*width)``.  Integer floor
division puts negative timestamps in the window they belong to as well.

The rollup is plain data in a session recovery point (see
:meth:`EventRollup.snapshot` / :meth:`EventRollup.restore`), so the totals
survive a process restart exactly as the confirmed records do.
"""

from __future__ import annotations

import json

from .errors import CheckpointCorrupt, EventError

DEFAULT_WINDOW = 300
_EVENT_FIELDS = ("event_time", "metric", "value")


def window_of(event_time, width):
    """Index of the left-closed/right-open window holding ``event_time``."""
    return event_time // width


def window_bounds(window, width):
    """The ``[start, end)`` UTC-second bounds of ``window``."""
    return window * width, (window + 1) * width


def parse_event(data):
    """Decode and validate one event payload.

    ``data`` is the record's bytes.  Returns
    ``(event_time, metric, value)``.  Raises :class:`EventError` on any
    malformed payload; the caller's record is then left unsettled.
    """
    if isinstance(data, str):
        text = data
    elif isinstance(data, (bytes, bytearray)):
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EventError("payload is not UTF-8 text: %s" % exc) from None
    else:
        raise EventError("event payload must be bytes or text, got %r"
                         % (type(data).__name__,))
    try:
        doc = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except EventError:
        raise
    except ValueError as exc:
        raise EventError("payload is not valid JSON: %s" % exc) from None
    if not isinstance(doc, dict):
        raise EventError("event payload must be a JSON object, got %s"
                         % (type(doc).__name__,))
    keys = set(doc)
    missing = [f for f in _EVENT_FIELDS if f not in keys]
    if missing:
        raise EventError("event is missing field(s): %s"
                         % ", ".join(sorted(missing)))
    extra = sorted(k for k in keys if k not in _EVENT_FIELDS)
    if extra:
        raise EventError("event has unexpected field(s): %s"
                         % ", ".join(extra))
    event_time = doc["event_time"]
    if not isinstance(event_time, int) or isinstance(event_time, bool):
        raise EventError("event_time must be an integer of UTC seconds, "
                         "got %r" % (event_time,))
    metric = doc["metric"]
    if not isinstance(metric, str):
        raise EventError("metric must be a string, got %r" % (metric,))
    value = doc["value"]
    if not isinstance(value, int) or isinstance(value, bool):
        raise EventError("value must be an integer, got %r" % (value,))
    return event_time, metric, value


def _reject_duplicate_keys(pairs):
    seen = set()
    for key, _ in pairs:
        if key in seen:
            raise EventError("duplicate JSON key %r in event payload" % (key,))
        seen.add(key)
    return dict(pairs)


class EventWindow:
    """A read-only view of one ``(window, category, metric)`` aggregate."""

    __slots__ = ("window", "start", "end", "category", "metric", "total",
                 "events")

    def __init__(self, window, width, category, metric, total, events):
        self.window = window
        self.start, self.end = window_bounds(window, width)
        self.category = category
        self.metric = metric
        self.total = total
        self.events = events

    def as_tuple(self):
        return (self.window, self.category, self.metric, self.total,
                self.events)

    def __eq__(self, other):
        if not isinstance(other, EventWindow):
            return NotImplemented
        return self.as_tuple() == other.as_tuple()

    def __repr__(self):  # pragma: no cover - debugging aid
        return ("<EventWindow w=%d [%d,%d) %r/%r total=%d events=%d>"
                % (self.window, self.start, self.end, self.category,
                   self.metric, self.total, self.events))


class EventRollup:
    """Per-group totals over fixed time windows.

    For every group it keeps, keyed by ``(window, category, metric)``, the
    running sum of ``value`` and the number of folded events.  Nothing about
    an individual event is retained after it is folded in.
    """

    __slots__ = ("width", "_totals", "_counts")

    def __init__(self, width=DEFAULT_WINDOW):
        if not isinstance(width, int) or isinstance(width, bool) or width < 1:
            raise ValueError("window width must be a positive integer, "
                             "got %r" % (width,))
        self.width = width
        # group -> {(window, category, metric): summed value}
        self._totals = {}
        # group -> {(window, category, metric): event count}
        self._counts = {}

    # ------------------------------------------------------------------ #
    # Mutation: one confirmed record at a time
    # ------------------------------------------------------------------ #
    def add(self, group, category, event_time, metric, value):
        """Fold one validated event into ``group``'s totals."""
        key = (window_of(event_time, self.width), category, metric)
        bucket = self._totals.setdefault(group, {})
        bucket[key] = bucket.get(key, 0) + value
        counts = self._counts.setdefault(group, {})
        counts[key] = counts.get(key, 0) + 1

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def groups(self):
        """The groups that have folded at least one event, sorted."""
        return tuple(sorted(self._totals))

    def windows(self, group, category=None, metric=None):
        """Aggregate rows for one group, sorted by window/category/metric.

        Optional ``category`` and ``metric`` filters narrow the rows.
        """
        if group not in self._totals:
            return ()
        rows = []
        for key in sorted(self._totals[group]):
            window, cat, met = key
            if category is not None and cat != category:
                continue
            if metric is not None and met != metric:
                continue
            rows.append(EventWindow(window, self.width, cat, met,
                                    self._totals[group][key],
                                    self._counts[group][key]))
        return tuple(rows)

    def total(self, group, category=None, metric=None):
        """Sum of every value folded for ``group`` (optionally filtered)."""
        if group not in self._totals:
            return 0
        return sum(v for (_, cat, met), v in self._totals[group].items()
                   if (category is None or cat == category)
                   and (metric is None or met == metric))

    def event_count(self, group):
        """How many events have been folded for ``group``."""
        return sum(self._counts.get(group, {}).values())

    def key_count(self):
        """Distinct ``(group, window, category, metric)`` keys held."""
        return sum(len(b) for b in self._totals.values())

    # ------------------------------------------------------------------ #
    # Persistence: plain data, validated on the way back
    # ------------------------------------------------------------------ #
    def snapshot(self):
        """A JSON-serialisable cut of the whole rollup."""
        groups = []
        for name in sorted(self._totals):
            rows = []
            for key in sorted(self._totals[name]):
                window, category, metric = key
                rows.append({"window": window, "category": category,
                             "metric": metric, "total": self._totals[name][key],
                             "events": self._counts[name][key]})
            groups.append({"group": name, "rows": rows})
        return {"width": self.width, "groups": groups}

    @classmethod
    def restore(cls, doc):
        """Rebuild a rollup from :meth:`snapshot` data, validating every field.

        Raises :class:`CheckpointCorrupt` on any structural damage so a bad
        recovery point can never produce a silently wrong aggregate.
        """
        if not isinstance(doc, dict):
            raise CheckpointCorrupt("the event rollup section is damaged")
        width = doc.get("width")
        if not isinstance(width, int) or isinstance(width, bool) or width < 1:
            raise CheckpointCorrupt("the event rollup has a bad window width")
        gdocs = doc.get("groups")
        if not isinstance(gdocs, list):
            raise CheckpointCorrupt("the event rollup's group list is damaged")
        rollup = cls(width)
        seen_groups = set()
        for gd in gdocs:
            if not isinstance(gd, dict) or not isinstance(gd.get("group"), str):
                raise CheckpointCorrupt("a rollup group entry is damaged")
            name = gd["group"]
            if name in seen_groups:
                raise CheckpointCorrupt("rollup group %r appears twice"
                                        % (name,))
            seen_groups.add(name)
            rows = gd.get("rows")
            if not isinstance(rows, list):
                raise CheckpointCorrupt("rollup rows for %r are damaged"
                                        % (name,))
            totals, counts, seen_keys = {}, {}, set()
            for rd in rows:
                if not isinstance(rd, dict):
                    raise CheckpointCorrupt("a rollup row is damaged")
                window = rd.get("window")
                category = rd.get("category")
                metric = rd.get("metric")
                total = rd.get("total")
                events = rd.get("events")
                if (not isinstance(window, int) or isinstance(window, bool)
                        or not isinstance(category, str)
                        or not isinstance(metric, str)):
                    raise CheckpointCorrupt(
                        "a rollup row of %r has a bad key" % (name,))
                if (not isinstance(total, int) or isinstance(total, bool)
                        or not isinstance(events, int)
                        or isinstance(events, bool) or events < 1):
                    raise CheckpointCorrupt(
                        "a rollup row of %r has bad totals" % (name,))
                key = (window, category, metric)
                if key in seen_keys:
                    raise CheckpointCorrupt(
                        "duplicate rollup key %r for group %r" % (key, name))
                seen_keys.add(key)
                totals[key] = total
                counts[key] = events
            rollup._totals[name] = totals
            rollup._counts[name] = counts
        return rollup
