import json
import tempfile
import unittest
from pathlib import Path

from aalp.credential import write_credential
from aalp.errors import Outcome
from aalp.gateway import INTERFACE_V1_CAPABILITIES, Gateway


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
    def __init__(self, response=None):
        self.response = response if response is not None else FakeResponse()

    def request(self, method, path, body=None, headers=None):
        pass

    def getresponse(self):
        return self.response

    def close(self):
        pass


class _TempGatewayCase(unittest.TestCase):
    def setUp(self) -> None:
        self._providers_tmp = tempfile.TemporaryDirectory()
        self._root_tmp = tempfile.TemporaryDirectory()
        self.providers_dir = Path(self._providers_tmp.name)
        self.root = Path(self._root_tmp.name)

    def tearDown(self) -> None:
        self._providers_tmp.cleanup()
        self._root_tmp.cleanup()


class CapabilitiesEndpointTest(_TempGatewayCase):
    def test_returns_exact_capability_list(self) -> None:
        _write_provider(self.providers_dir, "ci", concurrency_limit=1)
        write_credential("ci", "fake-token", root=self.root)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: FakeConnection())
        adapter = gateway.as_ingress_handler()

        status, headers, body = adapter(
            "GET", "/_aalp/v1/capabilities", {}, b"")

        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload, {
            "service": "aalp",
            "interface_version": 1,
            "capabilities": list(INTERFACE_V1_CAPABILITIES),
        })
        self.assertEqual(
            payload["capabilities"],
            ["request.forward", "provider.status",
             "provider.concurrency", "request.timeout_outcomes",
             "request.queue"])


class ProviderStatusListTest(_TempGatewayCase):
    def test_includes_every_loaded_provider_active_and_inactive(self) -> None:
        _write_provider(self.providers_dir, "ci", concurrency_limit=1)
        _write_provider(
            self.providers_dir, "retired", concurrency_limit=2, active=False)
        write_credential("ci", "fake-token", root=self.root)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: FakeConnection())
        adapter = gateway.as_ingress_handler()

        status, _headers, body = adapter(
            "GET", "/_aalp/v1/providers", {}, b"")

        self.assertEqual(status, 200)
        providers = {p["id"]: p for p in json.loads(body)["providers"]}
        self.assertEqual(set(providers), {"ci", "retired"})

        ci = providers["ci"]
        self.assertEqual(ci["display_name"], "Test Provider ci")
        self.assertTrue(ci["active"])
        self.assertEqual(ci["concurrency_limit"], 1)
        self.assertEqual(ci["in_flight"], 0)
        self.assertEqual(ci["queued"], 0)
        self.assertTrue(ci["idle"])
        self.assertGreaterEqual(ci["idle_seconds"], 0)
        self.assertEqual(ci["accepted_paths"], ["/v1/messages"])

        retired = providers["retired"]
        self.assertFalse(retired["active"])
        self.assertEqual(retired["in_flight"], 0)
        self.assertEqual(retired["queued"], 0)
        self.assertTrue(retired["idle"])
        self.assertEqual(retired["idle_seconds"], 0.0)
        self.assertEqual(retired["accepted_paths"], ["/v1/messages"])


class ProviderStatusSingleTest(_TempGatewayCase):
    def setUp(self) -> None:
        super().setUp()
        _write_provider(self.providers_dir, "ci", concurrency_limit=3)
        write_credential("ci", "fake-token", root=self.root)
        self.gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: FakeConnection())
        self.adapter = self.gateway.as_ingress_handler()

    def test_known_provider_returns_status_object(self) -> None:
        status, _headers, body = self.adapter(
            "GET", "/_aalp/v1/providers/ci", {}, b"")

        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload["id"], "ci")
        self.assertEqual(payload["concurrency_limit"], 3)
        self.assertEqual(
            set(payload),
            {"id", "display_name", "active", "concurrency_limit",
             "in_flight", "queued", "idle", "idle_seconds",
             "accepted_paths"})

    def test_unknown_provider_returns_404(self) -> None:
        status, _headers, body = self.adapter(
            "GET", "/_aalp/v1/providers/nonexistent", {}, b"")

        self.assertEqual(status, 404)
        payload = json.loads(body)
        self.assertEqual(payload, {
            "error": "provider_not_found",
            "provider_id": "nonexistent",
        })


class OutcomeHeaderTest(_TempGatewayCase):
    def test_success_response_carries_outcome_header(self) -> None:
        _write_provider(self.providers_dir, "ci", concurrency_limit=1)
        write_credential("ci", "fake-token", root=self.root)
        fake_conn = FakeConnection(response=FakeResponse(status=200, body=b"ok"))
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn)
        adapter = gateway.as_ingress_handler()

        status, headers, _body = adapter(
            "POST", "/ci/v1/messages", {"X-Aalp-Flow-Id": "flow-1"}, b"{}")

        self.assertEqual(status, 200)
        self.assertEqual(headers["X-Aalp-Outcome"], Outcome.SUCCESS.value)

    def test_non_success_outcome_carries_outcome_header(self) -> None:
        _write_provider(
            self.providers_dir, "ci", concurrency_limit=1,
            timeout_overrides={"total_timeout_seconds": 0})
        write_credential("ci", "fake-token", root=self.root)

        def never_called_connection_factory(provider, timeout):
            raise AssertionError("forward() must not be reached")

        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=never_called_connection_factory)
        adapter = gateway.as_ingress_handler()

        status, headers, body = adapter(
            "POST", "/ci/v1/messages", {"X-Aalp-Flow-Id": "flow-1"}, b"{}")

        self.assertEqual(status, 504)
        self.assertEqual(headers["X-Aalp-Outcome"], Outcome.TOTAL_TIMEOUT.value)
        self.assertEqual(headers["Content-Type"], "application/json")
        payload = json.loads(body)
        self.assertEqual(payload["outcome"], Outcome.TOTAL_TIMEOUT.value)
        self.assertIn("message", payload)


if __name__ == "__main__":
    unittest.main()
