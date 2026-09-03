"""Cross-backend conformance tests (agent adjustment §9, §35 acceptance
tests #7/#8): the *same* assertion bodies run once against the real
`Gateway.as_ingress_handler()` and once against the standalone
`FakeAalpV1Service`, proving both satisfy interface/v1/contract.json
identically rather than each backend's own idiosyncrasies. If a future
ACP client test only ever calls the interface-shaped operations exercised
here and gets contract-conformant behavior from both, internal refactors
to either side genuinely can't break that client.

Two small in-process "driver" adapters below give both backends the same
narrow surface (`set_providers`, `program_success`, `capabilities`,
`provider_status`, `forward`); `ConformanceMixin` holds the shared test
bodies and is deliberately not a `unittest.TestCase` subclass so it is
never collected/run on its own.

"""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from collections import deque
from pathlib import Path

from aalp.credential import write_credential
from aalp.gateway import Gateway

from tests.fixtures.fake_aalp_v1_service import FakeAalpV1Service, FakeProviderConfig

_CONTRACT_PATH = (
    Path(__file__).resolve().parent.parent / "interface" / "v1" / "contract.json")
_CONTRACT = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
_CONTRACT_CAPABILITIES = list(_CONTRACT["capabilities"])
_PROVIDER_STATUS_REQUIRED_FIELDS = set(
    _CONTRACT["operations"]["provider.status"]["provider_status_object"]["required"])


def _write_provider_file(
    providers_dir: Path, provider_id: str, *, display_name: str, active: bool,
    concurrency_limit: int, accepted_paths: list[str],
) -> None:
    data = {
        "id": provider_id,
        "display_name": display_name,
        "endpoint": "https://example.invalid/api",
        "concurrency_limit": concurrency_limit,
        "client": "python-http.client",
        "active": active,
        "request_shape": {
            "paths": list(accepted_paths),
            "passthrough": True,
            "auth_header": "Authorization",
            "auth_scheme": "Bearer",
        },
        "timeout_overrides": {},
    }
    (providers_dir / f"{provider_id}.json").write_text(
        json.dumps(data), encoding="utf-8")


class _CannedResponse:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status = status
        self._headers = headers
        self._body = body

    def getheaders(self):
        return list(self._headers.items())

    def read(self) -> bytes:
        return self._body


class _CannedConnection:
    """One programmed response for RealBackendDriver's injected connection_factory."""

    def __init__(self, response: _CannedResponse, delay: float = 0.0) -> None:
        self._response = response
        self._delay = delay

    def request(self, method, path, body=None, headers=None) -> None:
        pass

    def getresponse(self) -> _CannedResponse:
        if self._delay:
            time.sleep(self._delay)
        return self._response

    def close(self) -> None:
        pass


class RealBackendDriver:
    """Drives the real Gateway.as_ingress_handler() in-process -- same
    temp-providers-dir/temp-credential-root/injected-connection_factory
    construction used by test_interface_endpoints.py and
    test_gateway_pipeline.py, just wrapped behind ConformanceMixin's
    small backend-agnostic surface."""

    def __init__(self) -> None:
        self._providers_tmp = tempfile.TemporaryDirectory()
        self._root_tmp = tempfile.TemporaryDirectory()
        self.providers_dir = Path(self._providers_tmp.name)
        self.root = Path(self._root_tmp.name)
        self._connection_queue: deque[_CannedConnection] = deque()
        self._queue_lock = threading.Lock()
        self.gateway: Gateway | None = None
        self.adapter = None
        # capabilities() must work with no providers configured at all
        # (contract.json: service.capabilities takes no request fields).
        self.set_providers([])

    def _connection_factory(self, provider, timeout):
        with self._queue_lock:
            return self._connection_queue.popleft()

    def set_providers(self, configs: list[dict]) -> None:
        for cfg in configs:
            provider_id = cfg["id"]
            _write_provider_file(
                self.providers_dir, provider_id,
                display_name=cfg.get("display_name", provider_id),
                active=cfg.get("active", True),
                concurrency_limit=cfg.get("concurrency_limit", 1),
                accepted_paths=cfg.get("accepted_paths", []))
            write_credential(provider_id, "fake-token", root=self.root)
        self.gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=self._connection_factory)
        self.adapter = self.gateway.as_ingress_handler()

    def program_success(
        self, provider_id: str, path: str, *, status: int = 200,
        headers: dict[str, str] | None = None, body: bytes = b"",
        delay: float = 0.0,
    ) -> None:
        # provider_id/path aren't used for routing here: the injected
        # connection_factory takes neither, so programmed responses are
        # consumed strictly FIFO in forward-attempt order -- which, for
        # the single-provider/single-path scenarios this driver is used
        # in, is exactly the order each test cares about.
        del provider_id, path
        response = _CannedResponse(status, dict(headers or {}), body)
        with self._queue_lock:
            self._connection_queue.append(_CannedConnection(response, delay=delay))

    def capabilities(self) -> dict:
        status, _headers, body = self.adapter("GET", "/_aalp/v1/capabilities", {}, b"")
        assert status == 200
        return json.loads(body)

    def provider_status(self, provider_id: str | None = None) -> tuple[int, dict]:
        path = (
            "/_aalp/v1/providers" if provider_id is None
            else f"/_aalp/v1/providers/{provider_id}")
        status, _headers, body = self.adapter("GET", path, {}, b"")
        return status, json.loads(body)

    def forward(
        self, provider_id: str, method: str, path: str, *,
        headers: dict[str, str] | None = None, body: bytes = b"",
        flow_id: str | None = "conformance-flow",
    ) -> tuple[int, dict[str, str], bytes]:
        request_headers = dict(headers or {})
        if flow_id is not None:
            request_headers["X-Aalp-Flow-Id"] = flow_id
        return self.adapter(method, f"/{provider_id}{path}", request_headers, body)

    def close(self) -> None:
        self._providers_tmp.cleanup()
        self._root_tmp.cleanup()


class FakeBackendDriver:
    """Drives FakeAalpV1Service's in-process core directly -- no `import
    aalp`, matching how a real ACP client test is meant to use this fake."""

    def __init__(self) -> None:
        self.service = FakeAalpV1Service()

    def set_providers(self, configs: list[dict]) -> None:
        self.service.set_providers([
            FakeProviderConfig(
                id=cfg["id"],
                display_name=cfg.get("display_name", cfg["id"]),
                active=cfg.get("active", True),
                concurrency_limit=cfg.get("concurrency_limit", 1),
                accepted_paths=list(cfg.get("accepted_paths", [])),
            )
            for cfg in configs
        ])

    def program_success(
        self, provider_id: str, path: str, *, status: int = 200,
        headers: dict[str, str] | None = None, body: bytes = b"",
        delay: float = 0.0,
    ) -> None:
        self.service.program_response(
            provider_id, path, outcome="success", status=status,
            headers=headers or {}, body=body, delay=delay)

    def capabilities(self) -> dict:
        return self.service.capabilities()

    def provider_status(self, provider_id: str | None = None) -> tuple[int, dict]:
        if provider_id is None:
            return 200, self.service.list_providers()
        result = self.service.get_provider_status(provider_id)
        if result is None:
            return 404, {"error": "provider_not_found", "provider_id": provider_id}
        return 200, result

    def forward(
        self, provider_id: str, method: str, path: str, *,
        headers: dict[str, str] | None = None, body: bytes = b"",
        flow_id: str | None = "conformance-flow",
    ) -> tuple[int, dict[str, str], bytes]:
        # contract.json: X-Aalp-Flow-Id is audit/grouping only and is
        # never read for scheduling by a conforming service, so a
        # conforming fake has no reason to accept it as a parameter here.
        del flow_id
        result = self.service.forward(
            provider_id, method, path, headers=headers or {}, body=body)
        return result.status, result.headers, result.body

    def close(self) -> None:
        pass


class ConformanceMixin:
    """Shared assertion bodies, run unchanged against both backends.

    Not a `unittest.TestCase` subclass itself (plain mixin) so it is never
    collected/run directly; `driver` is set by each concrete subclass's
    `setUp()`.
    """

    driver: RealBackendDriver | FakeBackendDriver

    # -- service.capabilities ---------------------------------------------

    def test_capabilities_matches_contract_exactly(self) -> None:
        payload = self.driver.capabilities()
        self.assertEqual(payload, {
            "service": "aalp",
            "interface_version": 1,
            "capabilities": _CONTRACT_CAPABILITIES,
        })

    # -- provider.status ----------------------------------------------------

    def test_provider_status_list_includes_active_and_inactive(self) -> None:
        self.driver.set_providers([
            {"id": "ci", "display_name": "CI", "active": True,
             "concurrency_limit": 2, "accepted_paths": ["/v1/messages"]},
            {"id": "retired", "display_name": "Retired", "active": False,
             "concurrency_limit": 1, "accepted_paths": ["/v1/messages"]},
        ])

        status, payload = self.driver.provider_status()

        self.assertEqual(status, 200)
        providers = {p["id"]: p for p in payload["providers"]}
        self.assertEqual(set(providers), {"ci", "retired"})
        for provider in providers.values():
            self.assertEqual(set(provider), _PROVIDER_STATUS_REQUIRED_FIELDS)
        self.assertTrue(providers["ci"]["active"])
        self.assertFalse(providers["retired"]["active"])

    def test_provider_status_single_known(self) -> None:
        self.driver.set_providers([
            {"id": "ci", "display_name": "CI", "concurrency_limit": 3,
             "accepted_paths": ["/v1/messages", "/v1/messages/count_tokens"]},
        ])

        status, payload = self.driver.provider_status("ci")

        self.assertEqual(status, 200)
        self.assertEqual(set(payload), _PROVIDER_STATUS_REQUIRED_FIELDS)
        self.assertEqual(payload["id"], "ci")
        self.assertEqual(payload["concurrency_limit"], 3)
        self.assertEqual(
            payload["accepted_paths"], ["/v1/messages", "/v1/messages/count_tokens"])

    def test_provider_status_single_unknown_is_404(self) -> None:
        self.driver.set_providers([
            {"id": "ci", "display_name": "CI", "accepted_paths": ["/v1/messages"]},
        ])

        status, payload = self.driver.provider_status("nonexistent")

        self.assertEqual(status, 404)
        self.assertEqual(
            payload, {"error": "provider_not_found", "provider_id": "nonexistent"})

    # -- request.forward: X-Aalp-Outcome header + body shape ----------------

    def test_forward_success_carries_outcome_header_and_passthrough_body(self) -> None:
        self.driver.set_providers([
            {"id": "ci", "display_name": "CI", "concurrency_limit": 1,
             "accepted_paths": ["/v1/messages"]},
        ])
        self.driver.program_success(
            "ci", "/v1/messages", status=201, body=b'{"ok":true}')

        status, headers, body = self.driver.forward("ci", "POST", "/v1/messages")

        self.assertEqual(status, 201)
        self.assertEqual(headers["X-Aalp-Outcome"], "success")
        self.assertEqual(body, b'{"ok":true}')

    def test_forward_failure_carries_outcome_header_and_shaped_body(self) -> None:
        self.driver.set_providers([
            {"id": "ci", "display_name": "CI", "concurrency_limit": 1,
             "accepted_paths": ["/v1/messages"]},
        ])

        # Unknown provider id -- 'unavailable' outcome, on both backends,
        # with no need to program a canned upstream response at all.
        status, headers, body = self.driver.forward(
            "unknown-provider", "POST", "/v1/messages")

        self.assertEqual(status, 503)
        self.assertEqual(headers["X-Aalp-Outcome"], "unavailable")
        payload = json.loads(body)
        self.assertEqual(payload["outcome"], "unavailable")
        self.assertIn("message", payload)

    # -- scheduling: submitted-request FIFO ----------------------------------

    def _fifo_completion_order(
        self, provider_id: str, path: str, submissions: list[tuple[str, str]],
    ) -> list[str]:
        """`submissions`: [(marker, flow_id), ...] in intended submission
        order. Programs the first response with a real delay so a
        concurrency_limit=1 provider genuinely can't start the next one
        early, submits every request on its own thread with a small
        stagger, and returns the markers in the order they finished."""
        for index, (marker, _flow_id) in enumerate(submissions):
            self.driver.program_success(
                provider_id, path, body=f"{marker}-body".encode(),
                delay=0.15 if index == 0 else 0.0)

        finished: list[str] = []
        lock = threading.Lock()

        def submit(marker: str, flow_id: str) -> None:
            self.driver.forward(provider_id, "POST", path, flow_id=flow_id)
            with lock:
                finished.append(marker)

        threads = []
        for marker, flow_id in submissions:
            thread = threading.Thread(target=submit, args=(marker, flow_id))
            thread.start()
            threads.append(thread)
            # Ensures this submission is admitted before the next one is
            # even sent, so submission order is deterministic.
            time.sleep(0.05)
        for thread in threads:
            thread.join(timeout=5)

        return finished

    def test_concurrency_limit_one_serializes_submitted_requests_in_order(self) -> None:
        # §35 acceptance #1/#3, proven identically against both backends:
        # a concurrency_limit=1 provider genuinely serializes two
        # submitted requests in the order they were submitted.
        self.driver.set_providers([
            {"id": "prov", "display_name": "Prov", "concurrency_limit": 1,
             "accepted_paths": ["/v1/messages"]},
        ])

        order = self._fifo_completion_order(
            "prov", "/v1/messages", [("A", "flow-1"), ("B", "flow-2")])

        self.assertEqual(order, ["A", "B"])

    def test_concurrency_limit_above_one_allows_genuine_concurrent_execution(
        self,
    ) -> None:
        # A provider's own concurrency_limit is the only thing that
        # should gate how many of its requests run at once (contract.json
        # scheduling_model.concurrency_bound) -- proven identically
        # against both backends by observing both requests actually
        # in flight together, not serialized behind anything wider.
        self.driver.set_providers([
            {"id": "prov", "display_name": "Prov", "concurrency_limit": 2,
             "accepted_paths": ["/v1/messages"]},
        ])
        self.driver.program_success("prov", "/v1/messages", body=b"A", delay=0.2)
        self.driver.program_success("prov", "/v1/messages", body=b"B", delay=0.2)

        results = {}

        def submit(marker: str) -> None:
            results[marker] = self.driver.forward(
                "prov", "POST", "/v1/messages", flow_id=f"flow-{marker}")

        threads = [
            threading.Thread(target=submit, args=(marker,))
            for marker in ("A", "B")
        ]
        for thread in threads:
            thread.start()

        deadline = time.monotonic() + 2.0
        observed_both_in_flight = False
        while time.monotonic() < deadline:
            _, payload = self.driver.provider_status("prov")
            if payload["in_flight"] == 2:
                observed_both_in_flight = True
                break
            time.sleep(0.01)

        for thread in threads:
            thread.join(timeout=5)

        self.assertTrue(
            observed_both_in_flight,
            "expected both concurrency_limit=2 requests to run at the "
            "same time instead of being serialized")
        self.assertEqual(results["A"][0], 200)
        self.assertEqual(results["B"][0], 200)

    def test_completing_earlier_flows_request_leaves_no_idle_reservation(self) -> None:
        # §35 acceptance #2, explicit: completion of A1 (flow-A) must
        # leave no idle flow-A scheduling reservation that could block or
        # delay B1 -- a different flow's already-submitted, earlier-queued
        # request -- and once both are done, no reservation should be left
        # occupying the provider either.
        self.driver.set_providers([
            {"id": "prov", "display_name": "Prov", "concurrency_limit": 1,
             "accepted_paths": ["/v1/messages"]},
        ])

        order = self._fifo_completion_order(
            "prov", "/v1/messages", [("A1", "flow-A"), ("B1", "flow-B")])
        self.assertEqual(order, ["A1", "B1"])

        status, payload = self.driver.provider_status("prov")
        self.assertEqual(status, 200)
        self.assertEqual(payload["in_flight"], 0)
        self.assertEqual(payload["queued"], 0)
        self.assertTrue(payload["idle"])


class RealBackendConformanceTest(ConformanceMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.driver = RealBackendDriver()
        self.addCleanup(self.driver.close)


class FakeBackendConformanceTest(ConformanceMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.driver = FakeBackendDriver()
        self.addCleanup(self.driver.close)


if __name__ == "__main__":
    unittest.main()
