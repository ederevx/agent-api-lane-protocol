"""AALP's queue-generation state: request.queue's OPEN/READY/IN_FLIGHT/DONE
lifecycle
(agent_protocols_v1_queue_coalescing_adjustment_metadata_v1.md §6-§7).

Stage 3 scope: real multi-member accumulation while a provider is occupied
(§6/§8) and mechanical payload-train assembly across separately-submitted
members (§11, §12) build on the Stage 1 singleton path, which already
proved equivalent to the pre-existing `request.forward` behavior it wraps
around (the adjustment's own §31 migration gate). `members` has been a
list from day one and `append()` the single entry point Stage 3 now
drives concurrently from multiple submitting threads.

Deliberately socket-free and provider-blind, like `aalp/lane.py`: what
`queue_key` compatibility means is entirely the caller's (ACP's, per §9),
this module owns only the generation object, its state transitions, and
the mechanical (structure-blind) assembly of the physical request body
from each member's opaque payload envelope.
"""
from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class QueueEnvelopeError(ValueError):
    """A member's `payload` doesn't carry the envelope shape
    `build_physical_body()` needs (§11: "structured builder, serialize
    once"). Always a caller-side (ACP) protocol bug, never a provider or
    transport failure -- callers are expected to map this to the same
    `Outcome.UNAVAILABLE` shape used for other malformed-request cases.
    """


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

    def build_physical_body(self) -> bytes:
        """Assemble the one physical request body for this generation
        (§10-§12): static-shared-config-first, most-volatile-(the member
        train)-last, by deep-setting the joined member train into the
        leader's (first member's) `shared` envelope at `content_path`.

        Every member's `payload` is expected to be the same self-describing
        envelope shape ACP builds (see `acp/queue_codec.py`):
        `{"shared": {...sentinel at content_path...}, "content_path": [...],
        "member_block": "...", "member_join": "...", "count_template": "..."}`.
        Only the leader's `shared`/`content_path`/`member_join`/
        `count_template` are used -- they are identical across members by
        construction (same `queue_key`) -- but every member contributes its
        own `member_block`, in append (FIFO) order. AALP never interprets
        `shared` or `member_block`; it only joins strings and deep-sets one
        path, so this stays correct for any ACP payload schema.
        """
        if not self.members:
            raise QueueEnvelopeError(
                f"generation {self.generation_id} has no members to assemble")

        try:
            leader = self.members[0].payload
            shared = leader["shared"]
            content_path = leader["content_path"]
            member_join = leader["member_join"]
            count_template = leader["count_template"]
            blocks = [member.payload["member_block"] for member in self.members]
        except (TypeError, KeyError) as exc:
            raise QueueEnvelopeError(
                f"generation {self.generation_id}: member payload missing "
                f"required envelope field {exc!r}") from exc

        train = member_join.join(blocks) + member_join + count_template.format(
            member_count=len(self.members))
        _deep_set(shared, content_path, train)
        return json.dumps(shared).encode("utf-8")


def _deep_set(obj: Any, path: list, value: Any) -> None:
    """Generic path traversal/assignment, blind to whether each step is a
    dict key or list index (`obj[key]` works for both) -- AALP never needs
    to know ACP's payload schema, only where to splice the member train."""
    if not path:
        raise QueueEnvelopeError("content_path must not be empty")
    try:
        for key in path[:-1]:
            obj = obj[key]
        obj[path[-1]] = value
    except (KeyError, IndexError, TypeError) as exc:
        raise QueueEnvelopeError(f"content_path {path!r} not found in shared envelope") from exc


def new_generation_id() -> str:
    return secrets.token_hex(8)
