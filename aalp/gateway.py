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
forwarding), and flow identity travels as the `X-Aalp-Flow-Id` /
`X-Aalp-Flow-Token` headers. A different ingress adapter could make
different choices without touching `Gateway.handle()` at all.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Callable

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
        resolved_flow_token: str | None,
        start: float,
        queue_wait_ms: float,
    ) -> tuple[AalpResult, str | None]:
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
        return result, resolved_flow_token

    def handle(
        self,
        flow_id: str,
        provider_id: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        flow_token: str | None = None,
    ) -> tuple[AalpResult, str | None]:
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

        resolved_flow_token = flow_token
        remaining = queue_deadline - self.clock()
        if remaining <= 0:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

        try:
            if flow_token:
                try:
                    resolved_flow_token = self.flows.renew(
                        flow_id, flow_token)
                except ValueError:
                    resolved_flow_token = self.flows.admit(
                        flow_id, timeout_seconds=remaining)
            else:
                resolved_flow_token = self.flows.admit(
                    flow_id, timeout_seconds=remaining)
        except LaneTimeout:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            resolved_flow_token = flow_token  # no lease was ever obtained
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

        queue_wait_ms = (self.clock() - start) * 1000

        if self.clock() >= total_deadline:
            # The flow lease is still legitimately held here — this flow
            # may continue with a later request, so it is not released.
            result = AalpResult(Outcome.TOTAL_TIMEOUT)
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

        if unavailable_result is not None:
            return self._audit_and_return(
                provider_id, flow_id, path, unavailable_result,
                resolved_flow_token, start, queue_wait_ms)

        lane = self.provider_lanes[provider_id]
        remaining = queue_deadline - self.clock()
        if remaining <= 0:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

        try:
            provider_token = lane.acquire(flow_id, timeout_seconds=remaining)
        except LaneTimeout:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

        if self.clock() >= total_deadline:
            # Confirmed-idle slot (no network attempt happened yet), so
            # an explicit release here is safe — unlike the quarantine
            # case below, there is nothing to leave the TTL to clean up.
            lane.release(flow_id, provider_token)
            result = AalpResult(Outcome.TOTAL_TIMEOUT)
            return self._audit_and_return(
                provider_id, flow_id, path, result, resolved_flow_token,
                start, queue_wait_ms)

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
                # Bad `path` — a config/caller bug, never a real network
                # attempt, so the slot is confirmed idle.
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
            provider_id, flow_id, path, result, resolved_flow_token,
            start, queue_wait_ms)

    def close_flow(self, flow_id: str, token: str) -> bool:
        """Release a finished flow's lease once ACP signals the whole
        flow — not just one request — is done."""
        return self.flows.close(flow_id, token)

    def as_ingress_handler(self) -> Handler:
        """Build the closure `aalp.ingress.Ingress` calls per request.

        See the module docstring: the path-prefixed provider id and
        `X-Aalp-Flow-*` headers are this pass's own concrete choice of
        wire protocol, made here and nowhere upstream.
        """

        def _handler(
            method: str,
            path: str,
            headers: dict[str, str],
            body: bytes,
        ) -> tuple[int, dict[str, str], bytes]:
            provider_id, _, rest = path.lstrip("/").partition("/")
            forwarded_path = "/" + rest

            flow_id = _header(headers, "X-Aalp-Flow-Id")
            if not flow_id:
                return 400, {}, b"missing X-Aalp-Flow-Id header"
            flow_token = _header(headers, "X-Aalp-Flow-Token")

            result, resolved_flow_token = self.handle(
                flow_id, provider_id, method, forwarded_path, headers,
                body, flow_token=flow_token)

            if result.outcome is Outcome.SUCCESS:
                status = result.status_code
            else:
                status = _STATUS_BY_OUTCOME.get(result.outcome, 502)

            response_headers = dict(result.headers)
            if resolved_flow_token is not None:
                response_headers["X-Aalp-Flow-Token"] = resolved_flow_token

            return status, response_headers, result.body

        return _handler
