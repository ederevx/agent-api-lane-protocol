import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from tests.fixtures.fake_aalp_v1_service import (
    CAPABILITIES,
    FakeAalpV1Server,
    FakeAalpV1Service,
    FakeProviderConfig,
)


def make_ci_provider(**overrides):
    fields = dict(
        id="ci",
        display_name="CheapestInference",
        active=True,
        concurrency_limit=1,
        accepted_paths=["/v1/messages", "/v1/messages/count_tokens"],
    )
    fields.update(overrides)
    return FakeProviderConfig(**fields)


def http_request(url, method="GET", headers=None, body=None):
    data = body if body is None else (body if isinstance(body, bytes) else body.encode())
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers.items()), exc.read()


class InProcessServiceTests(unittest.TestCase):
    """Exercises FakeAalpV1Service's own logic, no socket involved."""

    def test_capabilities_matches_contract(self):
        service = FakeAalpV1Service()
        self.assertEqual(
            service.capabilities(),
            {"service": "aalp", "interface_version": 1, "capabilities": list(CAPABILITIES)},
        )

    def test_provider_status_list_and_single(self):
        service = FakeAalpV1Service([make_ci_provider()])
        listing = service.list_providers()
        self.assertEqual(len(listing["providers"]), 1)
        obj = listing["providers"][0]
        for key in ("id", "display_name", "active", "concurrency_limit", "in_flight", "queued", "idle",
                    "idle_seconds", "accepted_paths"):
            self.assertIn(key, obj)
        self.assertEqual(obj["id"], "ci")
        self.assertTrue(obj["idle"])
        self.assertEqual(obj["in_flight"], 0)
        self.assertEqual(obj["queued"], 0)

        single = service.get_provider_status("ci")
        self.assertAlmostEqual(single.pop("idle_seconds"), obj.pop("idle_seconds"), delta=0.5)
        self.assertEqual(single, obj)

    def test_provider_status_not_found(self):
        service = FakeAalpV1Service([make_ci_provider()])
        self.assertIsNone(service.get_provider_status("nope"))

    def test_forward_requires_programmed_response(self):
        service = FakeAalpV1Service([make_ci_provider()])
        with self.assertRaises(LookupError):
            service.forward("ci", "POST", "/v1/messages")

    def test_forward_success_passthrough(self):
        service = FakeAalpV1Service([make_ci_provider()])
        service.program_response(
            "ci", "/v1/messages", outcome="success", status=201,
            headers={"X-Upstream": "yes"}, body=b'{"ok":true}',
        )
        result = service.forward("ci", "POST", "/v1/messages")
        self.assertEqual(result.outcome, "success")
        self.assertEqual(result.status, 201)
        self.assertEqual(result.headers["X-Aalp-Outcome"], "success")
        self.assertEqual(result.headers["X-Upstream"], "yes")
        self.assertEqual(result.body, b'{"ok":true}')

    def test_forward_unavailable_unknown_provider(self):
        service = FakeAalpV1Service([make_ci_provider()])
        result = service.forward("nope", "POST", "/v1/messages")
        self.assertEqual(result.outcome, "unavailable")
        self.assertEqual(result.status, 503)
        self.assertEqual(result.headers["X-Aalp-Outcome"], "unavailable")

    def test_forward_unavailable_inactive_provider(self):
        service = FakeAalpV1Service([make_ci_provider(active=False)])
        result = service.forward("ci", "POST", "/v1/messages")
        self.assertEqual(result.outcome, "unavailable")
        self.assertEqual(result.status, 503)

    def test_forward_unavailable_bad_path(self):
        service = FakeAalpV1Service([make_ci_provider()])
        result = service.forward("ci", "POST", "/v1/not-accepted")
        self.assertEqual(result.outcome, "unavailable")
        self.assertEqual(result.status, 503)

    def test_forward_queue_timeout(self):
        service = FakeAalpV1Service([make_ci_provider()])
        service.program_response("ci", "/v1/messages", outcome="queue_timeout", message="admission budget elapsed")
        result = service.forward("ci", "POST", "/v1/messages")
        self.assertEqual(result.outcome, "queue_timeout")
        self.assertEqual(result.status, 504)
        body = json.loads(result.body)
        self.assertEqual(body["outcome"], "queue_timeout")

    def test_forward_upstream_error(self):
        service = FakeAalpV1Service([make_ci_provider()])
        service.program_response("ci", "/v1/messages", outcome="upstream_error", message="connection refused")
        result = service.forward("ci", "POST", "/v1/messages")
        self.assertEqual(result.outcome, "upstream_error")
        self.assertEqual(result.status, 502)

    def test_provider_id_leading_underscore_rejected_at_config(self):
        with self.assertRaises(ValueError):
            FakeProviderConfig(id="_reserved", display_name="x", accepted_paths=["/v1/messages"])

    def test_reserved_prefix_provider_id_is_unavailable_not_forwarded(self):
        service = FakeAalpV1Service([make_ci_provider()])
        result = service.forward("_aalp", "GET", "/v1/capabilities")
        self.assertEqual(result.outcome, "unavailable")

    def test_concurrency_limit_one_serializes_two_requests_in_submission_order(self):
        service = FakeAalpV1Service([make_ci_provider(concurrency_limit=1)])
        service.program_response("ci", "/v1/messages", outcome="success", body=b"first", delay=0.15)
        service.program_response("ci", "/v1/messages", outcome="success", body=b"second", delay=0.0)

        finished_order = []

        def run(tag):
            result = service.forward("ci", "POST", "/v1/messages")
            finished_order.append((tag, result.body))

        t1 = threading.Thread(target=run, args=("A",))
        t2 = threading.Thread(target=run, args=("B",))
        t1.start()
        time.sleep(0.05)  # ensure A is admitted into the lane strictly before B is submitted
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        # A must be admitted first (it was submitted first) and must fully finish
        # (holding the concurrency_limit=1 slot for its delay) before B's result appears.
        self.assertEqual(finished_order, [("A", b"first"), ("B", b"second")])

    def test_provider_status_reflects_in_flight_during_concurrent_request(self):
        service = FakeAalpV1Service([make_ci_provider(concurrency_limit=1)])
        service.program_response("ci", "/v1/messages", outcome="success", body=b"slow", delay=0.2)
        service.program_response("ci", "/v1/messages", outcome="success", body=b"fast", delay=0.0)

        observed = {}

        def occupy():
            service.forward("ci", "POST", "/v1/messages")

        t = threading.Thread(target=occupy)
        t.start()
        time.sleep(0.05)
        observed["mid_flight"] = service.get_provider_status("ci")

        def queue_second():
            observed["second_result"] = service.forward("ci", "POST", "/v1/messages")

        t2 = threading.Thread(target=queue_second)
        t2.start()
        time.sleep(0.05)
        observed["mid_queue"] = service.get_provider_status("ci")

        t.join(timeout=5)
        t2.join(timeout=5)

        self.assertEqual(observed["mid_flight"]["in_flight"], 1)
        self.assertFalse(observed["mid_flight"]["idle"])
        self.assertEqual(observed["mid_queue"]["queued"], 1)

        final_status = service.get_provider_status("ci")
        self.assertEqual(final_status["in_flight"], 0)
        self.assertTrue(final_status["idle"])


class HttpServerTests(unittest.TestCase):
    def setUp(self):
        self.service = FakeAalpV1Service([make_ci_provider()])
        self.server = FakeAalpV1Server(self.service).start()
        self.addCleanup(self.server.stop)

    def test_capabilities_endpoint(self):
        status, headers, body = http_request(f"{self.server.base_url}/_aalp/v1/capabilities")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), self.service.capabilities())

    def test_providers_list_endpoint(self):
        status, headers, body = http_request(f"{self.server.base_url}/_aalp/v1/providers")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertEqual(payload["providers"][0]["id"], "ci")

    def test_providers_single_endpoint(self):
        status, headers, body = http_request(f"{self.server.base_url}/_aalp/v1/providers/ci")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["id"], "ci")

    def test_providers_single_not_found(self):
        status, headers, body = http_request(f"{self.server.base_url}/_aalp/v1/providers/nope")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "provider_not_found", "provider_id": "nope"})

    def test_request_forward_success_over_http(self):
        self.service.program_response(
            "ci", "/v1/messages", outcome="success", status=200,
            headers={"Content-Type": "application/json"}, body=b'{"reply":"hi"}',
        )
        status, headers, body = http_request(
            f"{self.server.base_url}/ci/v1/messages", method="POST", body=b"{}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("X-Aalp-Outcome"), "success")
        self.assertEqual(body, b'{"reply":"hi"}')

    def test_request_forward_flow_id_header_accepted_and_ignored(self):
        self.service.program_response("ci", "/v1/messages", outcome="success", body=b"ok")
        status, headers, body = http_request(
            f"{self.server.base_url}/ci/v1/messages",
            method="POST", body=b"{}", headers={"X-Aalp-Flow-Id": "flow-1"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("X-Aalp-Outcome"), "success")

    def test_request_forward_queue_timeout_over_http(self):
        self.service.program_response("ci", "/v1/messages", outcome="queue_timeout")
        status, headers, body = http_request(
            f"{self.server.base_url}/ci/v1/messages", method="POST", body=b"{}"
        )
        self.assertEqual(status, 504)
        self.assertEqual(headers.get("X-Aalp-Outcome"), "queue_timeout")
        self.assertEqual(json.loads(body)["outcome"], "queue_timeout")

    def test_request_forward_unavailable_over_http(self):
        status, headers, body = http_request(
            f"{self.server.base_url}/unknown-provider/v1/messages", method="POST", body=b"{}"
        )
        self.assertEqual(status, 503)
        self.assertEqual(headers.get("X-Aalp-Outcome"), "unavailable")

    def test_request_forward_upstream_error_over_http(self):
        self.service.program_response("ci", "/v1/messages", outcome="upstream_error", message="dns failure")
        status, headers, body = http_request(
            f"{self.server.base_url}/ci/v1/messages", method="POST", body=b"{}"
        )
        self.assertEqual(status, 502)
        self.assertEqual(headers.get("X-Aalp-Outcome"), "upstream_error")


if __name__ == "__main__":
    unittest.main()
