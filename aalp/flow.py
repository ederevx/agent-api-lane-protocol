"""Per-request admission.

At most one request may be in flight through AALP at a time.
`FlowAdmission` wraps a single shared `Lane(capacity=1, ...)` — that
lane's own FIFO ticket order *is* the request queue required by
agent_protocols_v1_metadata_v1.md §24. Admission is strictly
request-scoped: every request admits fresh, in submission order, and
is closed the moment that request ends. A flow's own later request
gets no special treatment beyond the ordering its submission time
naturally gives it in the shared FIFO — this is what keeps submitted
requests (e.g. A1, B1, A2) executing in exactly submission order
regardless of which flow they belong to (§25), rather than letting one
flow reserve the lane across its own requests and cut ahead of another
flow's earlier-submitted one.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from .lane import Lane, LaneTimeout

__all__ = ["FlowAdmission", "LaneTimeout"]


class FlowAdmission:
    """Admits one request at a time, in submission order, via one shared
    FIFO lane. Request-scoped: each call to admit() takes a fresh ticket
    and must be paired with a close() once that single request ends —
    there is no cross-request renewal, so a flow's own next request
    re-queues like anyone else's, taking its place strictly by
    submission order rather than by flow identity.
    """

    def __init__(
        self,
        lease_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lane = Lane(capacity=1, lease_seconds=lease_seconds, clock=clock)

    def admit(self, flow_id: str, timeout_seconds: float) -> str:
        """FIFO-block until this request is the sole active one; return a token.

        Raises LaneTimeout if no slot opened up in time.
        """
        return self._lane.acquire(flow_id, timeout_seconds=timeout_seconds)

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
