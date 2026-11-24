"""The session lock and its wake-up discipline.

One lock guards every byte of session state, so an operation is linearised the
moment it runs: the concurrent interface is equivalent to *some* sequential
order consistent with the real-time order of the calls.  Waiting releases the
lock, which is what keeps a slow group from blocking others.

Every mutation that may unblock a waiter goes through :meth:`Hub.wake`; the
condition variable rules out lost wake-ups without polling.
"""

from __future__ import annotations

import threading


class Hub:
    __slots__ = ("lock", "_cond")

    def __init__(self):
        self.lock = threading.RLock()
        self._cond = threading.Condition(self.lock)

    def wait_until(self, pred, timeout=None):
        """Block until ``pred()``; the lock must be held on entry."""
        if pred():
            return True
        return self._cond.wait_for(pred, timeout)

    def wake(self):
        """Wake every thread that may now find its predicate true."""
        self._cond.notify_all()
