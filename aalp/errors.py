"""Outcome classification shared across AALP's pipeline stages.

A single closed set of outcomes (agent_protocols_v1_metadata_v1.md
§18) lets every stage — flow admission, provider admission, forwarding
— report failure the same way, so the gateway and audit log don't need
per-stage-specific error types.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Outcome(Enum):
    SUCCESS = "success"
    UNAVAILABLE = "unavailable"
    QUEUE_TIMEOUT = "queue_timeout"
    COMPRESSION_TIMEOUT = "compression_timeout"
    TOTAL_TIMEOUT = "total_timeout"
    INVALID_RESPONSE = "invalid_response"
    UPSTREAM_ERROR = "upstream_error"


@dataclass
class AalpResult:
    """What a pipeline stage hands back to its caller."""

    outcome: Outcome
    status_code: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.SUCCESS
