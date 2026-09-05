"""Generic FIFO ticket lane.

A condition-variable-based admission primitive: any number of holders
may queue for a bounded number of simultaneous leases, admission is
strictly FIFO, and an abandoned lease is reclaimed automatically once
its time-to-live lapses rather than needing an external watchdog.
`gateway.py` instantiates exactly one `Lane` per active provider, sized
to that provider's own `concurrency_limit` (see `registry.py`) — this
class is not currently used for anything else. An earlier revision of
this docstring described a second, per-flow admission use built by
instantiating this class with a different capacity; that per-flow
reservation was removed (see `interface/v1/README.md`'s "no renewal
operation" note) and may not return without a new major version, so
that description was vestigial and has been removed here too.

This module knows nothing about providers, flows, or credentials —
`holder` is an opaque caller-supplied string — so it satisfies the
"AALP core contains no provider-specific branching" invariant (§22) by
construction: any provider-specific meaning is applied entirely by the
caller.
"""
from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


class LaneTimeout(TimeoutError):
    """Raised when acquire() could not obtain a lease before its deadline."""


@dataclass
class Lease:
    holder: str
    token: str
    expires_at: float


class Lane:
    """One fair FIFO resource lane with bounded concurrent leases."""

    def __init__(
        self,
        capacity: int,
        lease_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.capacity = capacity
        self.lease_seconds = lease_seconds
        self._clock = clock
        self.condition = threading.Condition()
        self.leases: dict[str, Lease] = {}
        self.waiters: list[tuple[str, str]] = []
        self.last_activity = clock()

    def _expire(self) -> None:
        now = self._clock()
        expired = [token for token, lease in self.leases.items()
                   if lease.expires_at <= now]
        for token in expired:
            del self.leases[token]
        if expired:
            self.last_activity = now
            self.condition.notify_all()

    def acquire(
        self,
        holder: str,
        timeout_seconds: float,
        token: str | None = None,
    ) -> str:
        """FIFO-admit `holder`, blocking until a slot frees or timeout.

        Passing an existing valid `token` renews that lease in place
        instead of queuing a new ticket — used by callers (e.g. AALP's
        flow admission) whose continuation requests must not risk being
        overtaken by another waiter's ticket.

        Raises LaneTimeout if no slot became available in time; in that
        case nothing is held and no cleanup is required by the caller.
        """
        if not holder:
            raise ValueError("holder is required")
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        deadline = self._clock() + timeout_seconds
        with self.condition:
            self._expire()
            if token:
                lease = self.leases.get(token)
                if lease and lease.holder == holder:
                    lease.expires_at = self._clock() + self.lease_seconds
                    self.last_activity = self._clock()
                    return token
                raise ValueError("invalid reentry lease")

            ticket = secrets.token_urlsafe(18)
            self.waiters.append((ticket, holder))
            try:
                while True:
                    self._expire()
                    position = next(
                        index for index, item in enumerate(self.waiters)
                        if item[0] == ticket
                    )
                    available = self.capacity - len(self.leases)
                    if available > 0 and position < available:
                        self.waiters.pop(position)
                        lease_token = secrets.token_urlsafe(32)
                        self.leases[lease_token] = Lease(
                            holder,
                            lease_token,
                            self._clock() + self.lease_seconds,
                        )
                        self.last_activity = self._clock()
                        self.condition.notify_all()
                        return lease_token
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        raise LaneTimeout("lane acquire timed out")
                    self.condition.wait(min(remaining, 0.25))
            finally:
                self.waiters = [item for item in self.waiters
                                if item[0] != ticket]
                self.condition.notify_all()

    def heartbeat(self, holder: str, token: str) -> bool:
        """Renew a held lease's expiry. Returns False if not held by holder."""
        with self.condition:
            self._expire()
            lease = self.leases.get(token)
            if not lease or lease.holder != holder:
                return False
            lease.expires_at = self._clock() + self.lease_seconds
            self.last_activity = self._clock()
            return True

    def release(self, holder: str, token: str) -> bool:
        """Free a held lease immediately. Returns False if not held by holder.

        Deliberately not called by a caller that cannot confirm the
        underlying operation actually stopped (§19 quarantine): leaving
        a lease unreleased is safe because _expire() reclaims it once
        lease_seconds elapses, never sooner.
        """
        with self.condition:
            self._expire()
            lease = self.leases.get(token)
            if not lease or lease.holder != holder:
                return False
            del self.leases[token]
            self.last_activity = self._clock()
            self.condition.notify_all()
            return True

    def status(self) -> dict[str, Any]:
        with self.condition:
            self._expire()
            return {
                "capacity": self.capacity,
                "leased": len(self.leases),
                "queued": len(self.waiters),
                "idle": not self.leases and not self.waiters,
                "idle_seconds": max(0.0, self._clock() - self.last_activity),
            }
