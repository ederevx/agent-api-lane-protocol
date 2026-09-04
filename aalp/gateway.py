"""AALP's composition root: wires lane, registry, credential,
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

There is no separate global admission gate. Each active provider owns
exactly one `Lane` whose capacity is that provider's own declared
`concurrency_limit` (registry.py) — this is the single pool referred
to throughout this module: it already scales with `concurrency_limit`
with no code change required (adding a provider or raising its limit
is a config change, not a new lane to wire up), and it is the only
thing that gates both a provider's request order (its own FIFO ticket
queue, agent_protocols_v1_metadata_v1.md §24/§25) and its concurrency
ceiling. Different providers — and, once `concurrency_limit` > 1, a
single provider's own requests — genuinely execute concurrently;
nothing outside `Lane` itself imposes system-wide single-flight.
`X-Aalp-Flow-Id` is purely an audit/grouping label with no scheduling
authority and no bearing on lane admission.
"""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Callable

from dataclasses import dataclass, field

from . import audit
from . import credential as credential_module
from . import forwarder
from . import migrate_ci
from . import registry
from .errors import AalpResult, Outcome
from .ingress import Handler
from .lane import Lane, LaneTimeout
from .queue import (
    QueueEnvelopeError,
    QueueGeneration,
    QueueGenerationState,
    QueueMember,
    new_generation_id,
)
from .registry import ProviderDefinition

# §22's primary Stage 3 bound: a generation seals once it holds this many
# members, whether or not the provider is still occupied. The byte/output-
# budget bounds §22 also describes are deliberately deferred -- member
# count is the only one with an unambiguous, payload-blind measurement.
_DEFAULT_MAX_QUEUE_MEMBERS = 4

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
    "request.queue",
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


@dataclass
class _QueueSlot:
    """Gateway-private coordination state for one OPEN-or-later
    generation -- kept out of `aalp/queue.py` because `done_event`/
    `result` are concurrency plumbing, not part of the generation's own
    state machine. The leader (the thread that created this slot) drives
    `generation` to DONE and publishes `result` here exactly once; every
    joiner blocks on `done_event` and then reads `result`."""

    generation: QueueGeneration
    done_event: threading.Event = field(default_factory=threading.Event)
    result: AalpResult | None = None


class Gateway:
    """Owns each provider's lane state and drives one request through
    admission, forwarding, and audit."""

    def __init__(
        self,
        providers_dir: Path,
        root: str | Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        lease_seconds: float = 30.0,
        connection_factory: forwarder.ConnectionFactory | None = None,
        max_queue_members: int | None = None,
    ) -> None:
        self.providers_dir = providers_dir
        self.root = root
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.connection_factory = connection_factory
        self.max_queue_members = max_queue_members or int(
            os.environ.get("AALP_MAX_QUEUE_MEMBERS", _DEFAULT_MAX_QUEUE_MEMBERS))

        self.providers = registry.load_providers(providers_dir)
        self.provider_lanes: dict[str, Lane] = {
            provider_id: Lane(
                capacity=provider.concurrency_limit,
                lease_seconds=lease_seconds,
                clock=clock,
            )
            for provider_id, provider in self.providers.items()
            if provider.active
        }
        # Keyed by (provider_id, queue_key) -- the only OPEN generation a
        # new arrival may still join. Removed the moment a generation
        # seals (bound reached, or its leader was admitted to the Lane),
        # never mutated concurrently without holding `_queue_lock`.
        self._queue_lock = threading.Lock()
        self._open_queue_slots: dict[tuple[str, str], _QueueSlot] = {}

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
        # can apply to the two deadlines below.
        provider = self.providers.get(provider_id)
        total_deadline = start + _resolve_timeout(
            provider, "total_timeout_seconds", "ACP_TOTAL_TIMEOUT", 120)
        queue_deadline = start + _resolve_timeout(
            provider, "queue_timeout_seconds", "ACP_QUEUE_TIMEOUT", 30)

        if self.clock() >= total_deadline:
            result = AalpResult(Outcome.TOTAL_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        provider_available = provider is not None and provider.active
        if not provider_available:
            result = AalpResult(
                Outcome.UNAVAILABLE,
                message=f"provider {provider_id!r} is not available")
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        # This provider's own Lane is the single pool that gates it —
        # both its FIFO request order and its concurrency ceiling
        # (capacity == provider.concurrency_limit, set in __init__).
        # Nothing wider is held here, so a different provider's request
        # — or, once concurrency_limit > 1, this same provider's own
        # next request — is never blocked behind this one.
        lane = self.provider_lanes[provider_id]
        remaining = queue_deadline - self.clock()
        if remaining <= 0:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        try:
            provider_token = lane.acquire(flow_id, timeout_seconds=remaining)
        except LaneTimeout:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
            queue_wait_ms = (self.clock() - start) * 1000
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        queue_wait_ms = (self.clock() - start) * 1000

        if self.clock() >= total_deadline:
            # Confirmed-idle slot (no network attempt happened yet), so
            # an explicit release here is safe — unlike the quarantine
            # case below, there is nothing to leave the TTL to clean up.
            lane.release(flow_id, provider_token)
            result = AalpResult(Outcome.TOTAL_TIMEOUT)
            return self._audit_and_return(
                provider_id, flow_id, path, result, start, queue_wait_ms)

        result = self._execute_admitted(
            provider, provider_id, lane, flow_id, provider_token,
            method, path, headers, body)

        return self._audit_and_return(
            provider_id, flow_id, path, result, start, queue_wait_ms)

    def _execute_admitted(
        self,
        provider: ProviderDefinition,
        provider_id: str,
        lane: Lane,
        holder: str,
        token: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> AalpResult:
        """Shared body of the post-admission pipeline: heartbeat while
        forwarding, then closed/quarantine release (§19). `holder`/
        `token` are whatever identity was used to `lane.acquire()` this
        slot -- a flow id from `handle()`'s per-flow path, or a
        generation id from `handle_queue()`'s per-generation leader path
        -- so this one implementation serves both without duplicating
        the credential-read/heartbeat/forward/quarantine logic.
        """
        stop_heartbeat = threading.Event()
        interval = self.lease_seconds / 3

        def _heartbeat_loop() -> None:
            while not stop_heartbeat.wait(interval):
                lane.heartbeat(holder, token)

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
            lane.release(holder, token)
        # else: leave the lease in place. This *is* §19 quarantine — an
        # unconfirmed close means we cannot prove the upstream operation
        # actually stopped, so the slot is left held until its own TTL
        # (lease_seconds) reclaims it, rather than building a second
        # mechanism for the same guarantee Lane already provides.
        return result

    def _immediate_queue_failure(
        self,
        provider_id: str,
        queue_key: str,
        flow_id: str,
        path: str,
        start: float,
        result: AalpResult,
    ) -> tuple[AalpResult, QueueGeneration]:
        """A logical member that never joined any generation at all --
        its own deadline was already past, or its payload envelope was
        malformed. Still returns a (terminal, empty) generation so every
        caller gets the same response shape regardless of outcome."""
        generation = QueueGeneration(
            generation_id=new_generation_id(),
            provider_id=provider_id,
            queue_key=queue_key,
        )
        generation.seal()
        generation.mark_in_flight()
        generation.mark_done()
        queue_wait_ms = (self.clock() - start) * 1000
        audited = self._audit_and_return(
            provider_id, flow_id, path, result, start, queue_wait_ms)
        return audited, generation

    def _finish_leader(
        self,
        slot: _QueueSlot,
        result: AalpResult,
        provider_id: str,
        flow_id: str,
        path: str,
        start: float,
        queue_wait_ms: float,
    ) -> tuple[AalpResult, QueueGeneration]:
        """Leader-only: drive the shared generation the rest of the way
        to DONE (whatever state a failure path left it in), publish the
        physical result for every joiner, and wake them."""
        generation = slot.generation
        if generation.state is QueueGenerationState.OPEN:
            generation.seal()
        if generation.state is QueueGenerationState.READY:
            generation.mark_in_flight()
        if generation.state is QueueGenerationState.IN_FLIGHT:
            generation.mark_done()
        slot.result = result
        slot.done_event.set()
        audited = self._audit_and_return(
            provider_id, flow_id, path, result, start, queue_wait_ms)
        return audited, generation

    def _close_open_slot(self, provider_id: str, queue_key: str, slot: _QueueSlot) -> None:
        with self._queue_lock:
            key = (provider_id, queue_key)
            if self._open_queue_slots.get(key) is slot:
                del self._open_queue_slots[key]
            if slot.generation.state is QueueGenerationState.OPEN:
                slot.generation.seal()

    def _run_leader(
        self,
        slot: _QueueSlot,
        provider_id: str,
        queue_key: str,
        method: str,
        path: str,
        headers: dict[str, str],
        flow_id: str,
        start: float,
        total_deadline: float,
        queue_deadline: float,
    ) -> tuple[AalpResult, QueueGeneration]:
        """The thread that opened `slot.generation` immediately queues
        for the provider's Lane under the generation's own id (so an
        idle provider incurs zero added latency, §5, and Lane's own FIFO
        ticket order -- not a separate scheduler -- is what keeps
        generation dispatch order correct, §8). Once admitted, it seals
        membership (preventing further joins, §7), mechanically
        assembles the physical body (§10-§12), executes it, and wakes
        every joiner that appended while it waited.
        """
        generation = slot.generation
        provider = self.providers.get(provider_id)
        if provider is None or not provider.active:
            self._close_open_slot(provider_id, queue_key, slot)
            result = AalpResult(
                Outcome.UNAVAILABLE, message=f"provider {provider_id!r} is not available")
            queue_wait_ms = (self.clock() - start) * 1000
            return self._finish_leader(
                slot, result, provider_id, flow_id, path, start, queue_wait_ms)

        lane = self.provider_lanes[provider_id]
        remaining = queue_deadline - self.clock()
        provider_token: str | None = None
        result: AalpResult | None = None
        if remaining <= 0:
            result = AalpResult(Outcome.QUEUE_TIMEOUT)
        else:
            try:
                provider_token = lane.acquire(
                    generation.generation_id, timeout_seconds=remaining)
            except LaneTimeout:
                result = AalpResult(Outcome.QUEUE_TIMEOUT)

        self._close_open_slot(provider_id, queue_key, slot)
        queue_wait_ms = (self.clock() - start) * 1000

        if provider_token is not None and self.clock() >= total_deadline:
            lane.release(generation.generation_id, provider_token)
            provider_token = None
            result = AalpResult(Outcome.TOTAL_TIMEOUT)

        if provider_token is not None:
            try:
                physical_body = generation.build_physical_body()
            except QueueEnvelopeError as error:
                lane.release(generation.generation_id, provider_token)
                result = AalpResult(Outcome.UNAVAILABLE, message=str(error))
            else:
                generation.mark_in_flight()
                result = self._execute_admitted(
                    provider, provider_id, lane, generation.generation_id,
                    provider_token, method, path, headers, physical_body)

        return self._finish_leader(
            slot, result, provider_id, flow_id, path, start, queue_wait_ms)

    def _wait_as_joiner(
        self,
        slot: _QueueSlot,
        provider_id: str,
        path: str,
        flow_id: str,
        start: float,
        total_deadline: float,
    ) -> tuple[AalpResult, QueueGeneration]:
        """Blocks on the leader's completion signal, bounded by this
        member's own total deadline (§21: coalescing must not reset or
        extend a logical request's own deadline) -- never on the Lane
        itself, which only the leader ever touches."""
        remaining = total_deadline - self.clock()
        if remaining > 0:
            slot.done_event.wait(remaining)
        if slot.done_event.is_set():
            result = slot.result
        else:
            result = AalpResult(Outcome.TOTAL_TIMEOUT)
        queue_wait_ms = (self.clock() - start) * 1000
        audited = self._audit_and_return(
            provider_id, flow_id, path, result, start, queue_wait_ms)
        return audited, slot.generation

    def handle_queue(
        self,
        flow_id: str,
        provider_id: str,
        queue_key: str,
        member_id: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[AalpResult, QueueGeneration]:
        """`request.queue` entry point: real multi-member coalescing
        (§6-§13). The first caller to open a generation for
        `(provider_id, queue_key)` becomes its "leader" (see
        `_run_leader`); any other caller whose `(provider_id, queue_key)`
        matches an already-OPEN generation becomes a "joiner" (see
        `_wait_as_joiner`), appending its member and then blocking on
        that generation's own completion signal instead of the Lane.
        `body` is expected to be the JSON-encoded self-describing
        envelope `acp/queue_codec.py` builds (`shared`/`content_path`/
        `member_block`/`member_join`/`count_template`) -- AALP never
        interprets its contents beyond that shape (§9, §11).
        """
        start = self.clock()
        provider = self.providers.get(provider_id)
        total_deadline = start + _resolve_timeout(
            provider, "total_timeout_seconds", "ACP_TOTAL_TIMEOUT", 120)
        queue_deadline = start + _resolve_timeout(
            provider, "queue_timeout_seconds", "ACP_QUEUE_TIMEOUT", 30)

        if self.clock() >= total_deadline:
            return self._immediate_queue_failure(
                provider_id, queue_key, flow_id, path, start,
                AalpResult(Outcome.TOTAL_TIMEOUT))

        try:
            payload = json.loads(body) if body else {}
            if not isinstance(payload, dict):
                raise ValueError("queue member payload must be a JSON object")
        except (json.JSONDecodeError, ValueError) as error:
            result = AalpResult(
                Outcome.UNAVAILABLE,
                message=f"malformed queue member payload: {error}")
            return self._immediate_queue_failure(
                provider_id, queue_key, flow_id, path, start, result)

        member = QueueMember(member_id=member_id, payload=payload)
        slot_key = (provider_id, queue_key)
        is_leader = False
        with self._queue_lock:
            slot = self._open_queue_slots.get(slot_key)
            if slot is None:
                generation = QueueGeneration(
                    generation_id=new_generation_id(),
                    provider_id=provider_id,
                    queue_key=queue_key,
                )
                generation.append(member)
                slot = _QueueSlot(generation=generation)
                is_leader = True
                # §22: a generation seals as soon as it holds
                # max_queue_members, even if that happens on its very
                # first (leader) member -- max_queue_members=1 must mean
                # every member gets its own singleton generation, never
                # silently waiting for a joiner that would push it over
                # the configured bound. Only register the slot as OPEN
                # (joinable) if it is genuinely still under the cap.
                if generation.member_count >= self.max_queue_members:
                    generation.seal()
                else:
                    self._open_queue_slots[slot_key] = slot
            else:
                slot.generation.append(member)
                if slot.generation.member_count >= self.max_queue_members:
                    slot.generation.seal()
                    del self._open_queue_slots[slot_key]

        if is_leader:
            return self._run_leader(
                slot, provider_id, queue_key, method, path, headers, flow_id,
                start, total_deadline, queue_deadline)
        return self._wait_as_joiner(slot, provider_id, path, flow_id, start, total_deadline)

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

            queue_key = _header(headers, "X-Aalp-Queue-Key")
            response_headers: dict[str, str]
            if queue_key is not None:
                # request.queue path: Stage 1 always builds a singleton
                # generation (see handle_queue()'s docstring) -- the
                # member id has no meaning of its own yet, so it is
                # synthesized the same way an omitted flow_id is above.
                member_id = _header(
                    headers, "X-Aalp-Queue-Member-Id") or secrets.token_hex(16)
                result, generation = self.handle_queue(
                    flow_id, provider_id, queue_key, member_id, method,
                    forwarded_path, headers, body)
                response_headers = dict(result.headers)
                response_headers["X-Aalp-Outcome"] = result.outcome.value
                response_headers["X-Aalp-Queue-Generation-Id"] = generation.generation_id
                response_headers["X-Aalp-Queue-Member-Count"] = str(generation.member_count)
            else:
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
