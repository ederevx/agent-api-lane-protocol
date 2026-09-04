"""AALP's queue-generation state: request.queue's OPEN/READY/IN_FLIGHT/DONE
lifecycle
(agent_protocols_v1_queue_coalescing_adjustment_metadata_v1.md §6-§7).

Stage 1 scope only: `Gateway.handle_queue()` currently always builds a
generation of exactly one member and seals it immediately -- real
multi-member accumulation while a provider is occupied (§6/§8) and
mechanical payload-train assembly across separately-submitted members
(§11) are Stage 3 scope, gated on this singleton path first proving
equivalent to the pre-existing `request.forward` behavior it wraps around
(the adjustment's own §31 migration gate). This module is already shaped
for that: `members` is a list from day one and `append()` is the single
entry point Stage 3 will drive concurrently from multiple submitting
threads, rather than a singleton-only shortcut that would need
re-designing later.

Deliberately socket-free and provider-blind, like `aalp/lane.py`: what
`queue_key` compatibility means is entirely the caller's (ACP's, per §9),
this module owns only the generation object and its state transitions.
"""
from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class QueueGenerationState(Enum):
    OPEN = "open"
    READY = "ready"
    IN_FLIGHT = "in_flight"
    DONE = "done"


class QueueGenerationSealed(ValueError):
    """Raised by `append()`/a state transition attempted out of order.

    Membership is immutable once a generation leaves OPEN (§7, §35) --
    there is no code path that mutates `QueueGeneration.members` outside
    the OPEN state.
    """


@dataclass
class QueueMember:
    """One logical member of a generation.

    `member_id` and `payload` are opaque to AALP (§9) -- ACP defines what
    they mean. `output_budget` is the one payload-adjacent field AALP is
    allowed to read numerically, for mechanical aggregation only (§16);
    Stage 1 accepts and stores it but never sums it (nothing to sum in a
    generation of one).
    """

    member_id: str
    payload: Any = None
    output_budget: int | None = None


@dataclass
class QueueGeneration:
    """One provider physical-request unit: one or more FIFO-compatible
    members sharing one `queue_key`, ultimately admitted through exactly
    one `Lane` ticket (§6, §24)."""

    generation_id: str
    provider_id: str
    queue_key: str
    shared: dict[str, Any] = field(default_factory=dict)
    members: list[QueueMember] = field(default_factory=list)
    state: QueueGenerationState = QueueGenerationState.OPEN

    def append(self, member: QueueMember) -> None:
        if self.state is not QueueGenerationState.OPEN:
            raise QueueGenerationSealed(
                f"generation {self.generation_id} is {self.state.value}, "
                "not OPEN -- membership is immutable once sealed")
        self.members.append(member)

    def seal(self) -> None:
        """OPEN -> READY: stop accepting new members, still waiting for
        provider capacity (§7)."""
        if self.state is not QueueGenerationState.OPEN:
            raise QueueGenerationSealed(
                f"cannot seal generation {self.generation_id} from state "
                f"{self.state.value}")
        self.state = QueueGenerationState.READY

    def mark_in_flight(self) -> None:
        """READY -> IN_FLIGHT, or OPEN -> IN_FLIGHT directly when normal
        provider release happens before any bound forced an explicit
        READY seal first (§7: "Normal provider release may transition
        OPEN -> IN_FLIGHT if the queue had not already reached a
        limit.")."""
        if self.state not in (QueueGenerationState.OPEN, QueueGenerationState.READY):
            raise QueueGenerationSealed(
                f"cannot start generation {self.generation_id} from state "
                f"{self.state.value}")
        self.state = QueueGenerationState.IN_FLIGHT

    def mark_done(self) -> None:
        if self.state is not QueueGenerationState.IN_FLIGHT:
            raise QueueGenerationSealed(
                f"cannot complete generation {self.generation_id} from "
                f"state {self.state.value}")
        self.state = QueueGenerationState.DONE

    @property
    def member_count(self) -> int:
        return len(self.members)


def new_generation_id() -> str:
    return secrets.token_hex(8)
