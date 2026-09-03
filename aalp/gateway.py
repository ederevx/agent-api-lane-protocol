"""AALP's composition root: wires lane, flow, registry, credential,
forwarder, audit, migrate_ci and ingress into one working pipeline.

Every other module in this package is deliberately provider-agnostic
and (except forwarder.py) socket-free; `Gateway` is where those pieces
are actually assembled into the request path a caller drives. It is
also where this pass makes its one unavoidable, otherwise-unspecified
design choice: the concrete wire protocol `as_ingress_handler()`
exposes over `aalp.ingress.Ingress`. Nothing upstream of this module
pins that down, so it is pinned down here, plainly: the inbound HTTP
path's first `/`-delimited segment names the provider (stripped before
forwarding), and flow identity travels as the `X-Aalp-Flow-Id` header.
A different ingress adapter could make different choices without
touching `Gateway.handle()` at all.

Flow admission is request-scoped: `handle()` admits fresh on every
call and releases unconditionally before returning, so submitted
requests execute in strict submission-order FIFO (agent_protocols_v1_
metadata_v1.md §24/§25) regardless of which flow they belong to. There
is no renewal mechanism a caller can use to keep a flow "active" across
requests — `X-Aalp-Flow-Id` is purely an audit/grouping label with no
scheduling authority.
"""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Callable

from . import audit
from . import credential as credential_module
from . import forwarder
from . import migrate_ci
from . import registry
from .errors import AalpResult, Outcome
from .flow import FlowAdmission
from .ingress import Handler
from .lane import Lane, LaneTimeout
from .registry import ProviderDefinition

_STATUS_BY_OUTCOME: dict[Outcome, int] = {
    Outcome.UNAVAILABLE: 503,
    Outcome.QUEUE_TIMEOUT: 504,
    Outcome.TOTAL_TIMEOUT: 504,
    Outcome.COMPRESSION_TIMEOUT: 504,
    Outcome.INVALID_RESPONSE: 502,
    Outcome.UPSTREAM_ERROR: 502,
}

# The sole definition of interface v1's capability list — must match
# interface/v1/contract.json's top-level "capabilities" array verbatim.
# service.capabilities (below) is the only reader of this constant.
INTERFACE_V1_CAPABILITIES: tuple[str, ...] = (
    "request.forward",
    "provider.status",
    "provider.concurrency",
    "request.timeout_outcomes",
)

_DISCOVERY_PATH_PREFIX = "_aalp"


def _resolve_timeout(
    provider: ProviderDefinition | None,
    key: str,
    env_var: str,
    default: float,
) -> float:
    """Resolve one named timeout budget: provider override > env var >
    hardcoded default, in that order. `provider=None` (an unknown
    provider id) simply skips the override lookup."""
    if provider is not None:
        override = provider.timeout_overrides.get(key)
        if override is not None:
            return float(override)
    configured = os.environ.get(env_var)
    if configured is not None:
        return float(configured)
    return float(default)


def _header(headers: dict[str, str], name: str) -> str | None:
    """Case-insensitive header lookup that works on a plain dict too —
    `http.server`'s own header object is already case-insensitive, but
    a caller (e.g. a test) may hand this a bare dict instead."""
    folded = name.casefold()
    for key, value in headers.items():
        if key.casefold() == folded:
            return value
    return None


class Gateway:
    """Owns the shared lane/flow state and drives one request through
    admission, forwarding, and audit."""

    def __init__(
        self,
        providers_dir: Path,
        root: str | Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        lease_seconds: float = 30.0,
        connection_factory: forwarder.ConnectionFactory | None = None,
    ) -> None:
        self.providers_dir = providers_dir
        self.root = root
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.connection_factory = connection_factory

        self.providers = registry.load_providers(providers_dir)
        self.flows = FlowAdmission(lease_seconds=lease_seconds, clock=clock)
        self.provider_lanes: dict[str, Lane] = {
            provider_id: Lane(
                capacity=provider.concurrency_limit,
                lease_seconds=lease_seconds,
                clock=clock,
            )
            for provider_id, provider in self.providers.items()
            if provider.active
        }

        self.migration_status: migrate_ci.MigrationStatus | None = None
        # Generic over provider id everywhere else in this module; this
        # is the one narrow, deliberate exception — "ci" is the only
        # provider with a legacy ADP credential to migrate, and a
        # NEEDS_PROMPT result here is not a startup failure, only
        # something worth surfacing for observability.
        if "ci" in self.providers:
            self.migration_status = migrate_ci.migrate_ci(providers_dir, root)

    def _audit_and_return(
        self,
        provider_id: str,
        flow_id: str,
        path: str,
        result: AalpResult,
        start: float,
        queue_wait_ms: float,
    ) -> AalpResult:
        elapsed_ms = (self.clock() - start) * 1000
        audit.append(
            provider_id,
            flow_id,
            path,
            result.outcome,
            result.status_code,
            queue_wait_ms=queue_wait_ms,
            elapsed_ms=elapsed_ms,
            root=self.root,
        )
        return result

    def handle(
        self,
        flow_id: str,
        provider_id: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> AalpResult:
        start = self.clock()
        # Looked up now purely so a per-provider timeout_overrides entry
        # can apply to the two deadlines below; an unknown/inactive
        # provider still goes through flow admission — only provider
        # lane acquisition (step 5) is skipped, in favor of this result.
        provider = self.providers.get(provider_id)
        total_deadline = start + _resolve_timeout(
            provider, "total_timeout_seconds", "ACP_TOTAL_TIMEOUT", 120)
        queue_deadline = start + _resolve_timeout(
            provider, "queue_timeout_seconds", "ACP_QUEUE_TIMEOUT", 30)

        provider_available = provider is not None and provider.active
        unavailable_result = None
        if not provider_available:
            unavailable_result = AalpResult(
                Outcome.UNAVAILABLE,
                message=f"provider {provider_id!r} is not available")

        remaining = queue_deadline - self.clock()
        if remaining <= 0:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        try:
            flow_token = self.flows.admit(flow_id, timeout_seconds=remaining)
        except LaneTimeout:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        # Admission succeeded: every return path from here on must
        # release this request's flow lease, so the next FIFO waiter
        # (regardless of flow identity) is never kept waiting on it.
        try:
            queue_wait_ms = (self.clock() - start) * 1000

            if self.clock() >= total_deadline:
                result = AalpResult(Outcome.TOTAL_TIMEOUT)
                return self._audit_and_return(
                    provider_id, flow_id, path, result, start, queue_wait_ms)

            if unavailable_result is not None:
                return self._audit_and_return(
                    provider_id, flow_id, path, unavailable_result,
                    start, queue_wait_ms)

            lane = self.provider_lanes[provider_id]
            remaining = queue_deadline - self.clock()
            if remaining <= 0:
                result = AalpResult(Outcome.QUEUE_TIMEOUT)
                return self._audit_and_return(
                    provider_id, flow_id, path, result, start, queue_wait_ms)

            try:
                provider_token = lane.acquire(
                    flow_id, timeout_seconds=remaining)
            except LaneTimeout:
                result = AalpResult(Outcome.QUEUE_TIMEOUT)
                return self._audit_and_return(
                    provider_id, flow_id, path, result, start, queue_wait_ms)

            if self.clock() >= total_deadline:
                # Confirmed-idle slot (no network attempt happened yet),
                # so an explicit release here is safe — unlike the
                # quarantine case below, there is nothing to leave the
                # TTL to clean up.
                lane.release(flow_id, provider_token)
                result = AalpResult(Outcome.TOTAL_TIMEOUT)
                return self._audit_and_return(
                    provider_id, flow_id, path, result, start, queue_wait_ms)

            stop_heartbeat = threading.Event()
            interval = self.lease_seconds / 3

            def _heartbeat_loop() -> None:
                while not stop_heartbeat.wait(interval):
                    lane.heartbeat(flow_id, provider_token)

            heartbeat_thread = threading.Thread(
                target=_heartbeat_loop, daemon=True)
            heartbeat_thread.start()
            try:
                credential = credential_module.read_credential(
                    provider_id, root=self.root)
                compression_timeout = _resolve_timeout(
                    provider, "compression_timeout_seconds",
                    "ACP_COMPRESSION_TIMEOUT", 60)
                try:
                    result, closed = forwarder.forward(
                        provider, credential, method, path, headers, body,
                        timeout_seconds=compression_timeout,
                        connection_factory=self.connection_factory,
                    )
                except ValueError as error:
                    # Bad `path` — a config/caller bug, never a real
                    # network attempt, so the slot is confirmed idle.
                    result = AalpResult(Outcome.UNAVAILABLE, message=str(error))
                    closed = True
            finally:
                stop_heartbeat.set()
                heartbeat_thread.join()

            if closed:
                lane.release(flow_id, provider_token)
            # else: leave the lease in place. This *is* §19 quarantine — an
            # unconfirmed close means we cannot prove the upstream operation
            # actually stopped, so the slot is left held until its own TTL
            # (lease_seconds) reclaims it, rather than building a second
            # mechanism for the same guarantee Lane already provides.

            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)
        finally:
            self.flows.close(flow_id, flow_token)

    def _provider_status_object(self, provider: ProviderDefinition) -> dict[str, Any]:
        lane = self.provider_lanes.get(provider.id)
        if lane is not None:
            # Lane.status() already calls its own (private) _expire()
            # before counting, so this is always a fresh read — no
            # separate expiry trigger is needed here. A quarantined
            # (unconfirmed-close) lease is intentionally still counted
            # in in_flight until its TTL reclaims it; that matches the
            # contract's provider_status_object description verbatim.
            lane_status = lane.status()
            in_flight = lane_status["leased"]
            queued = lane_status["queued"]
            idle = lane_status["idle"]
            idle_seconds = lane_status["idle_seconds"]
        else:
            # Inactive providers have no Lane at all; report the same
            # zero/idle defaults a never-used active lane would show.
            in_flight = 0
            queued = 0
            idle = True
            idle_seconds = 0.0
        return {
            "id": provider.id,
            "display_name": provider.display_name,
            "active": provider.active,
            "concurrency_limit": provider.concurrency_limit,
            "in_flight": in_flight,
            "queued": queued,
            "idle": idle,
            "idle_seconds": idle_seconds,
            "accepted_paths": list(provider.request_shape.get("paths", [])),
        }

    def _handle_discovery(
        self, method: str, segments: list[str]
    ) -> tuple[int, dict[str, str], bytes]:
        """Serve service.capabilities and provider.status, both pure
        read-only introspection reached under the reserved /_aalp/v1/...
        prefix (see interface/v1/contract.json's transport_binding)."""
        headers = {"Content-Type": "application/json"}
        if method != "GET":
            return 404, {}, b""

        if segments == ["v1", "capabilities"]:
            body = json.dumps({
                "service": "aalp",
                "interface_version": 1,
                "capabilities": list(INTERFACE_V1_CAPABILITIES),
            }).encode("utf-8")
            return 200, headers, body

        if len(segments) == 2 and segments[0] == "v1" and segments[1] == "providers":
            providers = [
                self._provider_status_object(provider)
                for provider in self.providers.values()
            ]
            body = json.dumps({"providers": providers}).encode("utf-8")
            return 200, headers, body

        if len(segments) == 3 and segments[0] == "v1" and segments[1] == "providers":
            provider_id = segments[2]
            provider = self.providers.get(provider_id)
            if provider is None:
                body = json.dumps({
                    "error": "provider_not_found",
                    "provider_id": provider_id,
                }).encode("utf-8")
                return 404, headers, body
            body = json.dumps(self._provider_status_object(provider)).encode("utf-8")
            return 200, headers, body

        return 404, {}, b""

    def as_ingress_handler(self) -> Handler:
        """Build the closure `aalp.ingress.Ingress` calls per request.

        See the module docstring: the path-prefixed provider id and the
        `X-Aalp-Flow-Id` header are this pass's own concrete choice of
        wire protocol, made here and nowhere upstream. `X-Aalp-Flow-Id`
        carries no scheduling authority — it is passed through to
        `flow_id` purely as an audit/grouping label; every request is
        admitted fresh, in submission order, regardless of its value.
        """

        def _handler(
            method: str,
            path: str,
            headers: dict[str, str],
            body: bytes,
        ) -> tuple[int, dict[str, str], bytes]:
            segments = [segment for segment in path.split("/") if segment]
            if segments and segments[0] == _DISCOVERY_PATH_PREFIX:
                return self._handle_discovery(method, segments[1:])

            provider_id, _, rest = path.lstrip("/").partition("/")
            forwarded_path = "/" + rest

            # interface/v1/contract.json's scheduling_model.flow_id is
            # optional: it is a pure audit/grouping label with no
            # scheduling authority, so a caller that omits it must still
            # be served — synthesize an opaque one rather than reject
            # the request. This only affects the audit-log grouping
            # value; admission/ordering never depended on it even when
            # the header was supplied.
            flow_id = _header(headers, "X-Aalp-Flow-Id") or secrets.token_hex(16)

            result = self.handle(
                flow_id, provider_id, method, forwarded_path, headers, body)

            response_headers = dict(result.headers)
            response_headers["X-Aalp-Outcome"] = result.outcome.value

            if result.outcome is Outcome.SUCCESS:
                return result.status_code, response_headers, result.body

            status = _STATUS_BY_OUTCOME.get(result.outcome, 502)
            response_body = result.body
            if not response_body:
                # interface/v1/contract.json's on_other_outcome body is
                # required, not optional; a pipeline stage's AalpResult
                # never populates one (only SUCCESS carries a body), so
                # it is synthesized here, at the one place a response
                # actually leaves the process.
                response_headers["Content-Type"] = "application/json"
                response_body = json.dumps({
                    "outcome": result.outcome.value,
                    "message": result.message,
                }).encode("utf-8")
            return status, response_headers, response_body

        return _handler
