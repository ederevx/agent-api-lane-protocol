import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from aalp import migrate_ci as migrate_ci_module
from aalp.audit import read_entries
from aalp.credential import write_credential
from aalp.errors import AalpResult, Outcome
from aalp.gateway import Gateway

REAL_PROVIDERS_DIR = Path(__file__).resolve().parent.parent / "providers"


class FakeClock:
    """A controllable clock so timeout tests never real-sleep."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not met before timeout")


def _write_provider(providers_dir: Path, provider_id: str, **overrides) -> None:
    data = {
        "id": provider_id,
        "display_name": f"Test Provider {provider_id}",
        "endpoint": "https://example.invalid/api",
        "concurrency_limit": 1,
        "client": "python-http.client",
        "active": True,
        "request_shape": {
            "paths": ["/v1/messages"],
            "passthrough": True,
            "auth_header": "Authorization",
            "auth_scheme": "Bearer",
        },
        "timeout_overrides": {},
    }
    data.update(overrides)
    (providers_dir / f"{provider_id}.json").write_text(
        json.dumps(data), encoding="utf-8")


class FakeResponse:
    def __init__(self, status=200, headers=None, body=b""):
        self.status = status
        self._headers = headers or {}
        self._body = body

    def getheaders(self):
        return list(self._headers.items())

    def read(self):
        return self._body


class FakeConnection:
    """Stands in for forwarder's connection seam — mirrors the fake used
    in tests/test_forwarder.py, plus optional getresponse()-blocking and
    close()-failure knobs this suite needs."""

    def __init__(self, response=None, block_event=None, close_exception=None):
        self.response = response if response is not None else FakeResponse()
        self.block_event = block_event
        self.close_exception = close_exception
        self.requests: list[tuple] = []

    def request(self, method, path, body=None, headers=None):
        self.requests.append((method, path, body, headers))

    def getresponse(self):
        if self.block_event is not None:
            self.block_event.wait()
        return self.response

    def close(self):
        if self.close_exception is not None:
            raise self.close_exception


class _TempGatewayCase(unittest.TestCase):
    """Common temp providers_dir/root scaffolding for direct-provider tests."""

    def setUp(self) -> None:
        self._providers_tmp = tempfile.TemporaryDirectory()
        self._root_tmp = tempfile.TemporaryDirectory()
        self.providers_dir = Path(self._providers_tmp.name)
        self.root = Path(self._root_tmp.name)

    def tearDown(self) -> None:
        self._providers_tmp.cleanup()
        self._root_tmp.cleanup()


class BoundedConcurrencyTest(_TempGatewayCase):
    def test_peak_leased_never_exceeds_concurrency_limit(self) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        block_event = threading.Event()
        fake_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"ok"),
            block_event=block_event)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn,
            lease_seconds=0.2)

        peak = {"value": 0}
        stop_sampling = threading.Event()

        def sample() -> None:
            while not stop_sampling.is_set():
                leased = gateway.provider_lanes["prov"].status()["leased"]
                if leased > peak["value"]:
                    peak["value"] = leased
                time.sleep(0.005)

        sampler = threading.Thread(target=sample)
        sampler.start()

        results = {}

        def first_call() -> None:
            results["first"] = gateway.handle(
                "flow-A", "prov", "POST", "/v1/messages", {}, b"{}")

        thread_first = threading.Thread(target=first_call)
        thread_first.start()
        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 1)

        second_done = threading.Event()

        def second_call() -> None:
            results["second"] = gateway.handle(
                "flow-B", "prov", "POST", "/v1/messages", {}, b"{}")
            second_done.set()

        thread_second = threading.Thread(target=second_call)
        thread_second.start()
        time.sleep(0.05)
        # flow-A's lease (0.2s TTL) has not lapsed yet: flow-B must still
        # be queued behind it.
        self.assertFalse(second_done.is_set())

        block_event.set()
        thread_first.join(timeout=2)
        _wait_until(lambda: second_done.is_set(), timeout=2)
        thread_second.join(timeout=2)

        stop_sampling.set()
        sampler.join(timeout=2)

        self.assertTrue(results["first"][0].ok)
        self.assertTrue(results["second"][0].ok)
        self.assertLessEqual(peak["value"], 1)


class QueueTimeoutTest(_TempGatewayCase):
    def test_second_flow_times_out_while_first_holds_the_flow_lease(self) -> None:
        _write_provider(
            self.providers_dir, "prov", concurrency_limit=1,
            timeout_overrides={"queue_timeout_seconds": 0.05})
        write_credential("prov", "fake-token", root=self.root)

        block_event = threading.Event()
        fake_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"ok"),
            block_event=block_event)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn)

        def first_call() -> None:
            gateway.handle("flow-A", "prov", "POST", "/v1/messages", {}, b"{}")

        thread_first = threading.Thread(target=first_call)
        thread_first.start()
        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 1)

        result, token = gateway.handle(
            "flow-B", "prov", "POST", "/v1/messages", {}, b"{}")

        self.assertEqual(result.outcome, Outcome.QUEUE_TIMEOUT)
        self.assertIsNone(token)

        block_event.set()
        thread_first.join(timeout=2)


class TotalTimeoutTest(_TempGatewayCase):
    def test_total_timeout_before_forward_leaves_provider_lane_untouched(self) -> None:
        _write_provider(
            self.providers_dir, "prov", concurrency_limit=1,
            timeout_overrides={"total_timeout_seconds": 0})
        write_credential("prov", "fake-token", root=self.root)

        clock = FakeClock()

        def never_called_connection_factory(provider, timeout):
            raise AssertionError("forward() must not be reached")

        gateway = Gateway(
            self.providers_dir, root=self.root, clock=clock.now,
            connection_factory=never_called_connection_factory)

        result, token = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages", {}, b"{}")

        self.assertEqual(result.outcome, Outcome.TOTAL_TIMEOUT)
        # The flow lease is still legitimately held (the flow may
        # continue with a later request), so it must not be None.
        self.assertIsNotNone(token)
        self.assertEqual(
            gateway.provider_lanes["prov"].status()["leased"], 0)


class QuarantineTest(_TempGatewayCase):
    def test_unconfirmed_close_leaves_lease_held_until_ttl_reclaims_it(self) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        clock = FakeClock()
        fake_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"ok"),
            close_exception=RuntimeError("socket already gone"))
        gateway = Gateway(
            self.providers_dir, root=self.root, clock=clock.now,
            connection_factory=lambda provider, timeout: fake_conn,
            lease_seconds=5.0)

        result, _token = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages", {}, b"{}")

        self.assertTrue(result.ok)
        self.assertEqual(
            gateway.provider_lanes["prov"].status()["leased"], 1)

        clock.advance(5.1)
        result2, _token2 = gateway.handle(
            "flow-B", "prov", "POST", "/v1/messages", {}, b"{}")

        self.assertTrue(result2.ok)


class ConfirmedCloseTest(_TempGatewayCase):
    def test_confirmed_close_releases_the_lane_slot_immediately(self) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        clock = FakeClock()
        fake_conn = FakeConnection(response=FakeResponse(status=200, body=b"ok"))
        gateway = Gateway(
            self.providers_dir, root=self.root, clock=clock.now,
            connection_factory=lambda provider, timeout: fake_conn,
            lease_seconds=5.0)

        result, _token = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages", {}, b"{}")

        self.assertTrue(result.ok)
        self.assertEqual(
            gateway.provider_lanes["prov"].status()["leased"], 0)


class UnavailableProviderTest(_TempGatewayCase):
    def test_unknown_provider_is_unavailable_but_flow_admission_still_happens(
        self,
    ) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)

        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: FakeConnection())

        result, token = gateway.handle(
            "flow-A", "unknown-provider", "POST", "/v1/messages", {}, b"{}")

        self.assertEqual(result.outcome, Outcome.UNAVAILABLE)
        self.assertIsNotNone(token)


class AuditBlindnessTest(_TempGatewayCase):
    def test_audit_entries_never_contain_credential_headers_or_body(self) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        fake_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"response-secret-body"))
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn)

        result, _token = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages",
            {"X-Marker": "header-secret-value"},
            b"request-secret-body")

        self.assertTrue(result.ok)

        entries = read_entries(root=self.root)
        self.assertEqual(len(entries), 1)
        serialized = json.dumps(entries[0])
        for forbidden in (
            "fake-token", "header-secret-value",
            "request-secret-body", "response-secret-body",
        ):
            self.assertNotIn(forbidden, serialized)


class FlowContinuationTest(_TempGatewayCase):
    def test_second_request_in_same_flow_reuses_token_without_requeue(self) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        fake_conn = FakeConnection(response=FakeResponse(status=200, body=b"ok"))
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn)

        result1, token1 = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages", {}, b"{}")
        self.assertTrue(result1.ok)
        self.assertIsNotNone(token1)

        result2, token2 = gateway.handle(
            "flow-A", "prov", "POST", "/v1/messages", {}, b"{}",
            flow_token=token1)

        self.assertTrue(result2.ok)
        # renew() returns the identical token; a fresh admit() (i.e. a
        # re-queue) would mint a brand-new one, so equality here proves
        # the second call never had to queue behind anything.
        self.assertEqual(token2, token1)
        self.assertEqual(gateway.flows.status()["queued"], 0)


class IngressAdapterTest(_TempGatewayCase):
    def setUp(self) -> None:
        super().setUp()
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)
        self.gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: FakeConnection())

    def test_path_prefix_strips_provider_segment_before_handle(self) -> None:
        calls = []

        def fake_handle(flow_id, provider_id, method, path, headers, body,
                         flow_token=None):
            calls.append((flow_id, provider_id, method, path, flow_token))
            return (
                AalpResult(Outcome.SUCCESS, status_code=200, body=b"ok"),
                "next-token",
            )

        self.gateway.handle = fake_handle
        adapter = self.gateway.as_ingress_handler()

        status, headers, body = adapter(
            "POST", "/prov/v1/messages",
            {"X-Aalp-Flow-Id": "flow-1"}, b"{}")

        self.assertEqual(status, 200)
        self.assertEqual(body, b"ok")
        self.assertEqual(headers.get("X-Aalp-Flow-Token"), "next-token")
        self.assertEqual(len(calls), 1)
        flow_id, provider_id, _method, path, flow_token = calls[0]
        self.assertEqual(flow_id, "flow-1")
        self.assertEqual(provider_id, "prov")
        self.assertEqual(path, "/v1/messages")
        self.assertIsNone(flow_token)

    def test_missing_flow_id_header_returns_400_without_calling_handle(self) -> None:
        def never_called(*args, **kwargs):
            raise AssertionError("handle() must not be called")

        self.gateway.handle = never_called
        adapter = self.gateway.as_ingress_handler()

        status, _headers, _body = adapter(
            "POST", "/prov/v1/messages", {}, b"{}")

        self.assertEqual(status, 400)

    def test_non_success_outcome_maps_to_ingress_status_with_flow_token_header(
        self,
    ) -> None:
        def fake_handle(flow_id, provider_id, method, path, headers, body,
                         flow_token=None):
            return AalpResult(Outcome.UNAVAILABLE), "carry-token"

        self.gateway.handle = fake_handle
        adapter = self.gateway.as_ingress_handler()

        # Lowercase header name exercises the case-insensitive lookup.
        status, headers, _body = adapter(
            "POST", "/prov/v1/messages",
            {"x-aalp-flow-id": "flow-2"}, b"{}")

        self.assertEqual(status, 503)
        self.assertEqual(headers.get("X-Aalp-Flow-Token"), "carry-token")


class MigrateCiWiringTest(unittest.TestCase):
    def test_migrate_ci_invoked_when_ci_provider_present(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            calls = []

            def fake_migrate_ci(providers_dir, root=None, provider_id="ci"):
                calls.append((providers_dir, root, provider_id))
                return migrate_ci_module.MigrationStatus.NEEDS_PROMPT

            with patch(
                "aalp.gateway.migrate_ci.migrate_ci", fake_migrate_ci,
            ):
                gateway = Gateway(REAL_PROVIDERS_DIR, root=root)

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0], REAL_PROVIDERS_DIR)
            self.assertEqual(calls[0][1], root)
            self.assertEqual(calls[0][2], "ci")
            self.assertEqual(
                gateway.migration_status,
                migrate_ci_module.MigrationStatus.NEEDS_PROMPT)


if __name__ == "__main__":
    unittest.main()
