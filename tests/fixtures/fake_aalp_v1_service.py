"""A conforming, standalone fake of AALP interface v1 (interface/v1/contract.json).

Reimplements the *interface shape* described in contract.json from scratch,
using only the Python standard library. It never imports the `aalp` package
or reads any AALP-private on-disk state, so a client test built against this
fake stays correct even if AALP's real internals (Gateway, Lane, ...) are
completely rewritten.

Two layers are provided:

- `FakeAalpV1Service` — the in-process core: capabilities/provider-status
  lookups and request.forward's outcome/scheduling logic, unit-testable
  without any socket.
- `FakeAalpV1Server` — wraps the core in a real loopback HTTP server
  (127.0.0.1, ephemeral port) implementing contract.json's http_binding,
  for tests that want to drive it the same way a real ACP client would.

Test setup, in order:

1. `service.set_providers([...])` to declare the provider table
   (`provider.status` reads this).
2. `service.program_response(provider_id, path, outcome=..., ...)` once per
   forward() call you expect a test to make against that (provider_id, path)
   pair -- programmed responses are consumed FIFO per pair. Calling
   request.forward for a pair with nothing programmed raises `LookupError`
   (surfaced over HTTP as a 500 with a diagnostic body) rather than guessing
   an outcome, since a fake with unprogrammed default behavior would hide
   test-fixture bugs.
"""
from __future__ import annotations

import json
import secrets
import threading
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Mirrors contract.json's top-level "capabilities" array verbatim.
CAPABILITIES = [
    "request.forward",
    "provider.status",
    "provider.concurrency",
    "request.timeout_outcomes",
    "service.maintenance",
    "request.queue",
]

# Mirrors contract.json's outcomes.values.<outcome>.response_status_code.
OUTCOME_STATUS = {
    "unavailable": 503,
    "queue_timeout": 504,
    "compression_timeout": 504,
    "total_timeout": 504,
    "invalid_response": 502,
    "upstream_error": 502,
    "maintenance": 503,
}

# Per contract.json's outcome meanings: these three ("An upstream network
# attempt was made ...") and "success" are the only outcomes that occupy a
# provider concurrency-lane slot. "unavailable", "queue_timeout" and
# "total_timeout" are documented as "no upstream network attempt was made"
# (total_timeout: "may occur ... before an upstream attempt started"), so
# they never touch the lane.
NETWORK_ATTEMPT_OUTCOMES = frozenset({"success", "compression_timeout", "invalid_response", "upstream_error"})


@dataclass
class FakeProviderConfig:
    id: str
    display_name: str
    active: bool = True
    concurrency_limit: int = 1
    accepted_paths: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.id.startswith("_"):
            # contract.json transport_binding.reserved_path_prefix_rule:
            # "No provider id may begin with '_'."
            raise ValueError(f"provider id {self.id!r} may not begin with '_' (reserved for /_aalp/... discovery)")
        if self.concurrency_limit < 1:
            raise ValueError("concurrency_limit must be >= 1")


@dataclass
class ProgrammedResponse:
    """One canned request.forward result, consumed once."""

    outcome: str = "success"
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    message: str | None = None
    delay: float = 0.0

    def __post_init__(self) -> None:
        if self.outcome != "success" and self.outcome not in OUTCOME_STATUS:
            raise ValueError(f"unknown outcome {self.outcome!r}")


@dataclass
class ForwardResult:
    outcome: str
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass
class QueueForwardResult(ForwardResult):
    """request.queue's result: identical to ForwardResult plus the two
    generation-metadata headers contract.json requires on its responses.
    `generation_id`/`member_count` reflect real coalescing (§6-§13):
    every logical member that lands in the same generation gets the same
    `generation_id` and the true `member_count`, matching the real
    Gateway's `handle_queue()`."""

    generation_id: str = ""
    member_count: int = 1


class QueueEnvelopeError(ValueError):
    """A member's body didn't carry the envelope shape
    `_build_physical_body()` needs. Deliberately duplicated (in miniature)
    from `aalp/queue.py`'s real `QueueEnvelopeError`/`build_physical_body`/
    `_deep_set` rather than imported -- this fake never imports the `aalp`
    package (see module docstring)."""


def _deep_set(obj: object, path: list, value: object) -> None:
    if not path:
        raise QueueEnvelopeError("content_path must not be empty")
    try:
        for key in path[:-1]:
            obj = obj[key]  # type: ignore[index]
        obj[path[-1]] = value  # type: ignore[index]
    except (KeyError, IndexError, TypeError) as exc:
        raise QueueEnvelopeError(f"content_path {path!r} not found in shared envelope") from exc


def _build_physical_body(members: list[dict]) -> bytes:
    """Mechanical (payload-blind) assembly of one physical request body
    from each member's self-describing envelope -- mirrors
    `aalp.queue.QueueGeneration.build_physical_body()`: only the leader's
    (first member's) `shared`/`content_path`/`member_join`/
    `count_template` are used, but every member contributes its own
    `member_block`, in append (FIFO) order."""
    if not members:
        raise QueueEnvelopeError("generation has no members to assemble")
    try:
        leader = members[0]
        shared = leader["shared"]
        content_path = leader["content_path"]
        member_join = leader["member_join"]
        count_template = leader["count_template"]
        blocks = [member["member_block"] for member in members]
    except (KeyError, TypeError) as exc:
        raise QueueEnvelopeError(f"member payload missing required envelope field {exc!r}") from exc
    train = member_join.join(blocks) + member_join + count_template.format(member_count=len(members))
    _deep_set(shared, content_path, train)
    return json.dumps(shared).encode("utf-8")


@dataclass
class _QueueSlot:
    """One OPEN-or-later generation's coordination state: the leader (the
    caller that created this slot) drives it to completion and publishes
    `result` here exactly once; every joiner blocks on `done_event`."""

    generation_id: str = field(default_factory=lambda: secrets.token_hex(8))
    members: list[dict] = field(default_factory=list)
    done_event: threading.Event = field(default_factory=threading.Event)
    result: QueueForwardResult | None = None


class _ProviderLane:
    """Strict submitted-order FIFO admission bounded by concurrency_limit.

    Tickets are handed out in acquire() call order; only the head of the
    waiting line may take a free slot, so N callers blocked on acquire()
    are admitted in exactly the order they called it -- independent of any
    X-Aalp-Flow-Id, which this fake never reads for scheduling.
    """

    def __init__(self, concurrency_limit: int) -> None:
        self.concurrency_limit = concurrency_limit
        self._cv = threading.Condition()
        self._in_flight = 0
        self._waiting: deque[int] = deque()
        self._next_ticket = 0
        self._became_idle_at = time.monotonic()

    def acquire(self) -> None:
        with self._cv:
            ticket = self._next_ticket
            self._next_ticket += 1
            self._waiting.append(ticket)
            while not (self._waiting[0] == ticket and self._in_flight < self.concurrency_limit):
                self._cv.wait()
            self._waiting.popleft()
            self._in_flight += 1

    def release(self) -> None:
        with self._cv:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._became_idle_at = time.monotonic()
            self._cv.notify_all()

    @property
    def in_flight(self) -> int:
        with self._cv:
            return self._in_flight

    @property
    def queued(self) -> int:
        with self._cv:
            return len(self._waiting)

    def idle_seconds(self) -> float:
        with self._cv:
            if self._in_flight != 0:
                return 0.0
            return time.monotonic() - self._became_idle_at


class FakeAalpV1Service:
    """In-process core: capabilities, provider.status, request.forward."""

    def __init__(
        self, providers: list[FakeProviderConfig] | None = None, max_queue_members: int = 4
    ) -> None:
        self._lock = threading.RLock()
        self._providers: dict[str, FakeProviderConfig] = {}
        self._lanes: dict[str, _ProviderLane] = {}
        self._programmed: dict[tuple[str, str], deque[ProgrammedResponse]] = {}
        self._maintenance = False
        self.max_queue_members = max_queue_members
        self._open_queue_slots: dict[tuple[str, str], _QueueSlot] = {}
        if providers:
            self.set_providers(providers)

    # -- service.maintenance -------------------------------------------

    def set_maintenance(self, active: bool) -> None:
        with self._lock:
            self._maintenance = active

    @property
    def is_maintenance(self) -> bool:
        with self._lock:
            return self._maintenance

    # -- configuration -----------------------------------------------

    def set_providers(self, providers: list[FakeProviderConfig | dict]) -> None:
        normalized = [p if isinstance(p, FakeProviderConfig) else FakeProviderConfig(**p) for p in providers]
        with self._lock:
            self._providers = {p.id: p for p in normalized}
            self._lanes = {p.id: _ProviderLane(p.concurrency_limit) for p in normalized}

    def add_provider(self, provider: FakeProviderConfig | dict) -> None:
        p = provider if isinstance(provider, FakeProviderConfig) else FakeProviderConfig(**provider)
        with self._lock:
            self._providers[p.id] = p
            self._lanes[p.id] = _ProviderLane(p.concurrency_limit)

    def program_response(
        self,
        provider_id: str,
        path: str,
        *,
        outcome: str = "success",
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        message: str | None = None,
        delay: float = 0.0,
    ) -> None:
        resp = ProgrammedResponse(
            outcome=outcome, status=status, headers=dict(headers or {}), body=body, message=message, delay=delay
        )
        with self._lock:
            self._programmed.setdefault((provider_id, path), deque()).append(resp)

    # -- service.capabilities -----------------------------------------

    def capabilities(self) -> dict:
        return {"service": "aalp", "interface_version": 1, "capabilities": list(CAPABILITIES)}

    # -- provider.status ------------------------------------------------

    def list_providers(self) -> dict:
        with self._lock:
            providers = list(self._providers.values())
        return {"providers": [self._status_obj(p) for p in providers]}

    def get_provider_status(self, provider_id: str) -> dict | None:
        with self._lock:
            provider = self._providers.get(provider_id)
        if provider is None:
            return None
        return self._status_obj(provider)

    def _status_obj(self, provider: FakeProviderConfig) -> dict:
        lane = self._lanes[provider.id]
        in_flight = lane.in_flight
        return {
            "id": provider.id,
            "display_name": provider.display_name,
            "active": provider.active,
            "concurrency_limit": provider.concurrency_limit,
            "in_flight": in_flight,
            "queued": lane.queued,
            "idle": in_flight == 0,
            "idle_seconds": lane.idle_seconds(),
            "accepted_paths": list(provider.accepted_paths),
        }

    # -- request.forward --------------------------------------------------

    def forward(
        self, provider_id: str, method: str, path: str, headers: dict[str, str] | None = None, body: bytes = b""
    ) -> ForwardResult:
        del method, headers, body  # unused by this fake's outcome logic; accepted for shape fidelity

        with self._lock:
            if self._maintenance:
                return self._maintenance_result()
            provider = self._providers.get(provider_id)

        if provider_id.startswith("_") or provider is None or not provider.active:
            return self._unavailable(f"provider {provider_id!r} unknown or inactive")
        if path not in provider.accepted_paths:
            return self._unavailable(f"path {path!r} not accepted by provider {provider_id!r}")

        key = (provider_id, path)
        with self._lock:
            queue = self._programmed.get(key)
            if not queue:
                raise LookupError(
                    f"fake_aalp_v1_service: no response programmed for provider={provider_id!r} path={path!r}; "
                    "call program_response(...) before exercising request.forward for this (provider_id, path)"
                )
            programmed = queue.popleft()

        if programmed.outcome not in NETWORK_ATTEMPT_OUTCOMES:
            if programmed.delay:
                time.sleep(programmed.delay)
            return self._synthetic_outcome(programmed)

        lane = self._lanes[provider_id]
        lane.acquire()
        try:
            if programmed.delay:
                time.sleep(programmed.delay)
            if programmed.outcome == "success":
                out_headers = dict(programmed.headers)
                out_headers["X-Aalp-Outcome"] = "success"
                return ForwardResult("success", programmed.status, out_headers, programmed.body)
            return self._synthetic_outcome(programmed)
        finally:
            lane.release()

    # -- request.queue ----------------------------------------------------

    def submit_queue_member(
        self,
        provider_id: str,
        queue_key: str,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        member_id: str | None = None,
    ) -> QueueForwardResult:
        """Real multi-member coalescing (§6-§13), mirroring the real
        Gateway's leader/joiner split: the first caller to open a
        generation for `(provider_id, queue_key)` becomes its "leader"
        and drives it to completion by calling `forward()` directly
        (which already provides FIFO/concurrency admission via this
        provider's own `_ProviderLane`); any other caller whose
        `(provider_id, queue_key)` matches an already-OPEN generation
        becomes a "joiner", appending its member and blocking on that
        generation's own completion signal instead. `body` is expected
        to be the JSON-encoded self-describing envelope
        `acp/queue_codec.py` builds.
        """
        del member_id  # opaque to this fake, same as the real Gateway (§9)
        try:
            payload = json.loads(body) if body else {}
            if not isinstance(payload, dict):
                raise ValueError("queue member payload must be a JSON object")
        except (json.JSONDecodeError, ValueError) as exc:
            result = self._unavailable(f"malformed queue member payload: {exc}")
            generation_id = secrets.token_hex(8)
            out_headers = dict(result.headers)
            out_headers["X-Aalp-Queue-Generation-Id"] = generation_id
            out_headers["X-Aalp-Queue-Member-Count"] = "0"
            return QueueForwardResult(
                outcome=result.outcome, status=result.status, headers=out_headers,
                body=result.body, generation_id=generation_id, member_count=0)

        slot_key = (provider_id, queue_key)
        is_leader = False
        with self._lock:
            slot = self._open_queue_slots.get(slot_key)
            if slot is None:
                slot = _QueueSlot()
                self._open_queue_slots[slot_key] = slot
                is_leader = True
            slot.members.append(payload)
            if len(slot.members) >= self.max_queue_members:
                self._open_queue_slots.pop(slot_key, None)

        if is_leader:
            return self._run_queue_leader(slot, slot_key, provider_id, method, path, headers)
        return self._wait_queue_joiner(slot)

    def _run_queue_leader(
        self,
        slot: _QueueSlot,
        slot_key: tuple[str, str],
        provider_id: str,
        method: str,
        path: str,
        headers: dict[str, str] | None,
    ) -> QueueForwardResult:
        # Mirrors the real Gateway's leader ordering exactly: acquire this
        # provider's own admission lane *first* (so a caller stuck behind
        # an occupied provider stays OPEN for joins the whole time it
        # waits, and Lane's own FIFO ticket order -- not a separate
        # scheduler -- is what keeps physical dispatch order correct
        # across different queue_key generations), and only seal (remove
        # from `_open_queue_slots`) once actually admitted.
        del method, headers  # unused by this fake's outcome logic, same as forward()
        with self._lock:
            provider = self._providers.get(provider_id)

        if provider_id.startswith("_") or provider is None or not provider.active:
            with self._lock:
                self._open_queue_slots.pop(slot_key, None)
            result: ForwardResult = self._unavailable(
                f"provider {provider_id!r} unknown or inactive")
        elif path not in provider.accepted_paths:
            with self._lock:
                self._open_queue_slots.pop(slot_key, None)
            result = self._unavailable(f"path {path!r} not accepted by provider {provider_id!r}")
        else:
            lane = self._lanes[provider_id]
            lane.acquire()
            try:
                with self._lock:
                    self._open_queue_slots.pop(slot_key, None)
                try:
                    physical_body = _build_physical_body(slot.members)
                except QueueEnvelopeError as exc:
                    result = self._unavailable(str(exc))
                else:
                    result = self._pop_and_execute_programmed(provider_id, path, physical_body)
            finally:
                lane.release()

        out_headers = dict(result.headers)
        out_headers["X-Aalp-Queue-Generation-Id"] = slot.generation_id
        out_headers["X-Aalp-Queue-Member-Count"] = str(len(slot.members))
        queue_result = QueueForwardResult(
            outcome=result.outcome, status=result.status, headers=out_headers, body=result.body,
            generation_id=slot.generation_id, member_count=len(slot.members))
        slot.result = queue_result
        slot.done_event.set()
        return queue_result

    def _wait_queue_joiner(self, slot: _QueueSlot) -> QueueForwardResult:
        slot.done_event.wait(60)
        if slot.result is None:
            # Leader never finished within a generous bound -- surfaced
            # the same way an unprogrammed forward() call is: a fixture
            # bug, not a real outcome to synthesize.
            raise LookupError(
                f"fake_aalp_v1_service: queue generation {slot.generation_id!r} "
                "never completed (leader did not signal done_event)")
        return slot.result

    def _pop_and_execute_programmed(self, provider_id: str, path: str, body: bytes) -> ForwardResult:
        """Queue-leader-only counterpart to `forward()`'s programmed-
        response consumption -- called with the provider's lane already
        held, so (unlike `forward()`) every outcome is treated uniformly
        rather than skipping the lane for non-network-attempt outcomes;
        no current queue test relies on that distinction."""
        del body  # this fake's canned responses never depend on the physical body's content
        key = (provider_id, path)
        with self._lock:
            queue = self._programmed.get(key)
            if not queue:
                raise LookupError(
                    f"fake_aalp_v1_service: no response programmed for provider={provider_id!r} "
                    f"path={path!r}; call program_response(...) before exercising request.queue "
                    "for this (provider_id, path)"
                )
            programmed = queue.popleft()

        if programmed.delay:
            time.sleep(programmed.delay)
        if programmed.outcome == "success":
            out_headers = dict(programmed.headers)
            out_headers["X-Aalp-Outcome"] = "success"
            return ForwardResult("success", programmed.status, out_headers, programmed.body)
        return self._synthetic_outcome(programmed)

    @staticmethod
    def _unavailable(message: str) -> ForwardResult:
        body = json.dumps({"outcome": "unavailable", "message": message}).encode()
        headers = {"X-Aalp-Outcome": "unavailable", "Content-Type": "application/json"}
        return ForwardResult("unavailable", OUTCOME_STATUS["unavailable"], headers, body)

    @staticmethod
    def _maintenance_result() -> ForwardResult:
        body = json.dumps({"outcome": "maintenance", "message": "AALP is in maintenance mode"}).encode()
        headers = {"X-Aalp-Outcome": "maintenance", "Content-Type": "application/json"}
        return ForwardResult("maintenance", OUTCOME_STATUS["maintenance"], headers, body)

    @staticmethod
    def _synthetic_outcome(programmed: ProgrammedResponse) -> ForwardResult:
        body = json.dumps({"outcome": programmed.outcome, "message": programmed.message or ""}).encode()
        headers = {"X-Aalp-Outcome": programmed.outcome, "Content-Type": "application/json"}
        return ForwardResult(programmed.outcome, OUTCOME_STATUS[programmed.outcome], headers, body)


def _make_handler(service: FakeAalpV1Service) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - stdlib signature
            pass

        def __getattr__(self, name: str):
            if name.startswith("do_"):
                return self._dispatch
            raise AttributeError(name)

        def _dispatch(self) -> None:
            parsed = urllib.parse.urlsplit(self.path)
            path = parsed.path
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""
            req_headers = {k: v for k, v in self.headers.items()}

            try:
                if self.command == "GET" and path == "/_aalp/v1/capabilities":
                    self._write_json(200, service.capabilities())
                    return
                if self.command == "GET" and path == "/_aalp/v1/providers":
                    self._write_json(200, service.list_providers())
                    return
                if self.command == "GET" and path == "/_aalp/v1/maintenance":
                    self._write_json(200, {"maintenance": service.is_maintenance})
                    return
                if self.command == "GET" and path.startswith("/_aalp/v1/providers/"):
                    provider_id = path[len("/_aalp/v1/providers/") :]
                    status = service.get_provider_status(provider_id)
                    if status is None:
                        self._write_json(404, {"error": "provider_not_found", "provider_id": provider_id})
                    else:
                        self._write_json(200, status)
                    return
                if path.startswith("/_aalp/"):
                    self._write_json(404, {"error": "not_found"})
                    return

                segment, _, rest = path.lstrip("/").partition("/")
                upstream_path = "/" + rest if rest else ""
                result = service.forward(segment, self.command, upstream_path, headers=req_headers, body=body)
                self._write_raw(result.status, result.headers, result.body)
            except LookupError as exc:
                self._write_raw(500, {"Content-Type": "text/plain"}, str(exc).encode())

        def _write_json(self, status: int, obj: dict) -> None:
            self._write_raw(status, {"Content-Type": "application/json"}, json.dumps(obj).encode())

        def _write_raw(self, status: int, headers: dict[str, str], body: bytes) -> None:
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

    return _Handler


class FakeAalpV1Server:
    """Loopback HTTP binding of FakeAalpV1Service, per contract.json's http_binding."""

    def __init__(self, service: FakeAalpV1Service | None = None, host: str = "127.0.0.1") -> None:
        self.service = service or FakeAalpV1Service()
        self._httpd = ThreadingHTTPServer((host, 0), _make_handler(self.service))
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> "FakeAalpV1Server":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> "FakeAalpV1Server":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
