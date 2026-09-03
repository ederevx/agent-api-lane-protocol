"""Per-flow admission.

At most one flow's requests may be in flight through AALP at a time.
`FlowAdmission` wraps a single shared `Lane(capacity=1, ...)` — that
lane's own FIFO ticket order *is* the request queue required by
agent_protocols_v1_metadata_v1.md §24, and reusing one lease across a
flow's own sequence of requests (A, then B, then C) rather than
re-queueing on each one is what satisfies per-flow locking (§25)
without letting a later-arrived flow cut in front of an already-active
one's continuation.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from .lane import Lane, LaneTimeout

__all__ = ["FlowAdmission", "LaneTimeout"]


class FlowAdmission:
    """Serializes access across flows using one shared FIFO lane."""

    def __init__(
        self,
        lease_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lane = Lane(capacity=1, lease_seconds=lease_seconds, clock=clock)

    def admit(self, flow_id: str, timeout_seconds: float) -> str:
        """FIFO-block until `flow_id` is the sole active flow; return a token.

        Raises LaneTimeout if no slot opened up in time.
        """
        return self._lane.acquire(flow_id, timeout_seconds=timeout_seconds)

    def renew(self, flow_id: str, token: str) -> str:
        """Keep an already-admitted flow's lease alive for its next request.

        Raises ValueError if `token` is not currently held by `flow_id` —
        including when its TTL already lapsed, in which case the caller
        must fall back to admit() and re-queue rather than assume it is
        still the active flow.
        """
        return self._lane.acquire(flow_id, timeout_seconds=0, token=token)

    def close(self, flow_id: str, token: str) -> bool:
        """Release a finished flow's lease immediately.

        Without this, the next flow still gets admitted eventually — the
        lease's TTL reclaims it on its own — but calling close() as soon
        as ACP signals the flow is done avoids making the next flow wait
        out that TTL for no reason.
        """
        return self._lane.release(flow_id, token)

    def status(self) -> dict[str, Any]:
        return self._lane.status()
