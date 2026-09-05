import socket
import threading
import time
import unittest
from typing import Any

from aalp.errors import Outcome
from aalp.forwarder import build_connection, forward, probe
from aalp.registry import ProviderDefinition
from tests.fixtures.fake_upstream import FakeUpstream


def make_provider(**overrides: Any) -> ProviderDefinition:
    fields = dict(
        id="ci",
        display_name="CheapestInference",
        endpoint="https://api.cheapestinference.com/anthropic",
        concurrency_limit=1,
        client="python-http.client",
        active=True,
        request_shape={
            "paths": ["/v1/messages", "/v1/messages/count_tokens"],
            "passthrough": True,
            "auth_header": "Authorization",
            "auth_scheme": "Bearer",
        },
        timeout_overrides={"total_timeout_seconds": 3600},
    )
    fields.update(overrides)
    return ProviderDefinition(**fields)


class FakeResponse:
    def __init__(self, status=200, headers=None, body=b"", read_exception=None):
        self.status = status
        self._headers = headers or {}
        self._body = body
        self._read_exception = read_exception

    def getheaders(self):
        return list(self._headers.items())

    def read(self):
        if self._read_exception is not None:
            raise self._read_exception
        return self._body


class FakeConnection:
    def __init__(
        self,
        request_exception=None,
        getresponse_exception=None,
        response=None,
        close_exception=None,
    ):
        self.request_exception = request_exception
        self.getresponse_exception = getresponse_exception
        self.response = response if response is not None else FakeResponse()
        self.close_exception = close_exception
        self.requests: list[tuple] = []
        self.closed = False

    def request(self, method, path, body=None, headers=None):
        self.requests.append((method, path, body, headers))
        if self.request_exception is not None:
            raise self.request_exception

    def getresponse(self):
        if self.getresponse_exception is not None:
            raise self.getresponse_exception
        return self.response

    def close(self):
        self.closed = True
        if self.close_exception is not None:
            raise self.close_exception


class PathValidationTest(unittest.TestCase):
    def test_unknown_path_raises_value_error(self):
        provider = make_provider()
        with self.assertRaisesRegex(ValueError, "/v1/not-a-real-path"):
            forward(
                provider, "secret-token", "POST", "/v1/not-a-real-path",
                {}, b"{}", 10.0,
                connection_factory=lambda p, t: FakeConnection(),
            )


class HeaderInjectionTest(unittest.TestCase):
    def test_auth_header_injected_and_inbound_one_stripped(self):
        provider = make_provider()
        fake = FakeConnection()
        result, closed = forward(
            provider, "secret-token", "POST", "/v1/messages",
            {"authorization": "Bearer stolen-token", "X-Other": "keep-me"},
            b'{"hello": true}', 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertTrue(closed)
        self.assertTrue(result.ok)
        self.assertEqual(len(fake.requests), 1)
        _method, _path, _body, headers = fake.requests[0]
        auth_values = [
            value for name, value in headers.items()
            if name.lower() == "authorization"
        ]
        self.assertEqual(auth_values, ["Bearer secret-token"])
        self.assertEqual(headers.get("X-Other"), "keep-me")

    def test_inbound_connection_specific_headers_are_not_forwarded(self):
        # A real inbound ingress request's headers (aalp/ingress.py hands
        # `forward()` every header off the request it received) name the
        # *loopback* connection, not the upstream one. Forwarding them
        # verbatim broke real traffic to the 'ci' provider: a stale
        # `Host: 127.0.0.1:<port>` header made Cloudflare 403 the request
        # with an HTML error page before it ever reached the backend.
        provider = make_provider()
        fake = FakeConnection()
        forward(
            provider, "tok", "POST", "/v1/messages",
            {
                "Host": "127.0.0.1:54321",
                "Content-Length": "2",
                "Connection": "keep-alive",
                "X-Other": "keep-me",
            },
            b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        _method, _path, _body, headers = fake.requests[0]
        lowered = {name.lower() for name in headers}
        self.assertNotIn("host", lowered)
        self.assertNotIn("content-length", lowered)
        self.assertNotIn("connection", lowered)
        self.assertEqual(headers.get("X-Other"), "keep-me")

    def test_upstream_path_prefixes_endpoint_path(self):
        provider = make_provider()
        fake = FakeConnection()
        forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        _method, path, _body, _headers = fake.requests[0]
        self.assertEqual(path, "/anthropic/v1/messages")


class ClassificationTest(unittest.TestCase):
    def test_success(self):
        provider = make_provider()
        fake = FakeConnection(
            response=FakeResponse(status=200, body=b"ok-body"))
        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertEqual(result.outcome, Outcome.SUCCESS)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.body, b"ok-body")
        self.assertTrue(closed)

    def test_getresponse_timeout_is_compression_timeout(self):
        provider = make_provider()
        fake = FakeConnection(getresponse_exception=socket.timeout("timed out"))
        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertEqual(result.outcome, Outcome.COMPRESSION_TIMEOUT)
        self.assertTrue(closed)

    def test_request_os_error_is_upstream_error(self):
        provider = make_provider()
        fake = FakeConnection(request_exception=ConnectionResetError("reset"))
        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertEqual(result.outcome, Outcome.UPSTREAM_ERROR)
        self.assertTrue(closed)

    def test_read_error_is_invalid_response(self):
        provider = make_provider()
        fake = FakeConnection(
            response=FakeResponse(read_exception=ValueError("bad chunked encoding")))
        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertEqual(result.outcome, Outcome.INVALID_RESPONSE)
        self.assertTrue(closed)


class ClosedBoolTest(unittest.TestCase):
    def test_close_success_reports_true(self):
        provider = make_provider()
        fake = FakeConnection()
        _result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertTrue(closed)

    def test_close_failure_reports_false_without_masking_result(self):
        provider = make_provider()
        fake = FakeConnection(
            response=FakeResponse(status=200, body=b"fine"),
            close_exception=RuntimeError("socket already gone"),
        )
        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
        )
        self.assertFalse(closed)
        self.assertEqual(result.outcome, Outcome.SUCCESS)
        self.assertEqual(result.body, b"fine")


class _SlowFakeResponse:
    """A response body that never raises its own socket.timeout, no
    matter how long read() blocks -- standing in for a real upstream
    that keeps trickling a few bytes every couple of seconds, which
    resets http.client's per-read timeout without ever finishing. Only
    unblocks when the test itself releases it, well after forward()
    should already have given up."""

    status = 200

    def __init__(self, release: threading.Event):
        self._release = release

    def getheaders(self):
        return []

    def read(self):
        self._release.wait(timeout=5.0)
        return b'{"content": []}'


class SlowFakeConnection:
    """Confirmed via a live activation run: a real upstream response
    delivered as slowly-trickling chunks never trips http.client's own
    timeout= (which only bounds gaps between reads), so a plain blocking
    forward() implementation would block far longer than
    `timeout_seconds` in aggregate. This fake reproduces that shape
    without any real sockets or real time: its read() only returns once
    the test explicitly releases it, long after forward()'s own
    deadline -- proving forward() stops *waiting* on schedule rather
    than depending on the connection itself being interrupted."""

    def __init__(self, release: threading.Event):
        self.requests: list[tuple] = []
        self.closed = False
        self._response = _SlowFakeResponse(release)

    def request(self, method, path, body=None, headers=None):
        self.requests.append((method, path, body, headers))

    def getresponse(self):
        return self._response

    def close(self):
        self.closed = True


class DeadlineWatchdogTest(unittest.TestCase):
    def test_slow_trickling_response_returns_promptly_as_compression_timeout(self):
        provider = make_provider()
        release = threading.Event()
        fake = SlowFakeConnection(release)
        started = time.monotonic()
        try:
            result, closed = forward(
                provider, "tok", "POST", "/v1/messages", {}, b"{}",
                timeout_seconds=0.2,
                connection_factory=lambda p, t: fake,
            )
            elapsed = time.monotonic() - started
        finally:
            # Let the abandoned background thread's read() return so it
            # doesn't linger past this test.
            release.set()

        self.assertEqual(result.outcome, Outcome.COMPRESSION_TIMEOUT)
        # Unconfirmed close -- forward() gave up waiting, it never
        # learned whether the connection actually stopped.
        self.assertFalse(closed)
        self.assertLess(elapsed, 2.0)


class OnLateCompletionTest(unittest.TestCase):
    def test_fires_with_real_outcome_after_caller_already_gave_up(self):
        provider = make_provider()
        release = threading.Event()
        fake = SlowFakeConnection(release)
        late_calls: list[tuple] = []
        late_call_seen = threading.Event()

        def on_late_completion(result, closed):
            late_calls.append((result, closed))
            late_call_seen.set()

        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}",
            timeout_seconds=0.2,
            connection_factory=lambda p, t: fake,
            on_late_completion=on_late_completion,
        )
        self.assertEqual(result.outcome, Outcome.COMPRESSION_TIMEOUT)
        self.assertFalse(closed)
        self.assertEqual(late_calls, [])

        # Let the abandoned background thread's read() finish now.
        release.set()
        self.assertTrue(late_call_seen.wait(timeout=5.0))

        self.assertEqual(len(late_calls), 1)
        late_result, late_closed = late_calls[0]
        self.assertEqual(late_result.outcome, Outcome.SUCCESS)
        self.assertTrue(late_closed)

    def test_not_called_when_caller_never_gives_up(self):
        provider = make_provider()
        fake = FakeConnection(response=FakeResponse(status=200, body=b"ok"))
        late_calls: list[tuple] = []

        result, closed = forward(
            provider, "tok", "POST", "/v1/messages", {}, b"{}", 10.0,
            connection_factory=lambda p, t: fake,
            on_late_completion=lambda r, c: late_calls.append((r, c)),
        )

        self.assertEqual(result.outcome, Outcome.SUCCESS)
        self.assertTrue(closed)
        self.assertEqual(late_calls, [])


class BuildConnectionTest(unittest.TestCase):
    def test_unknown_client_raises_value_error(self):
        provider = make_provider(client="some-other-client")
        with self.assertRaisesRegex(ValueError, "some-other-client"):
            build_connection(provider, 10.0)


class EndToEndFakeUpstreamTest(unittest.TestCase):
    def test_real_loopback_round_trip(self):
        with FakeUpstream() as upstream:
            upstream.set_response(status=200, body=b'{"ok": true}')
            provider = make_provider(endpoint=f"{upstream.base_url}/anthropic")

            result, closed = forward(
                provider, "real-secret", "POST", "/v1/messages",
                {"Authorization": "Bearer stolen"}, b'{"hi": 1}', 10.0,
            )

            self.assertTrue(closed)
            self.assertEqual(result.outcome, Outcome.SUCCESS)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.body, b'{"ok": true}')

            recorded = upstream.last_request
            self.assertEqual(recorded.method, "POST")
            self.assertEqual(recorded.path, "/anthropic/v1/messages")
            self.assertEqual(recorded.headers.get("Authorization"),
                              "Bearer real-secret")


class ProbeTest(unittest.TestCase):
    def test_success_returns_true(self):
        provider = make_provider()
        fake = FakeConnection(response=FakeResponse(status=200, body=b"{}"))
        self.assertTrue(
            probe(provider, "tok", connection_factory=lambda p, t: fake))

    def test_unauthorized_returns_false(self):
        provider = make_provider()
        fake = FakeConnection(response=FakeResponse(status=401, body=b""))
        self.assertFalse(
            probe(provider, "bad-tok", connection_factory=lambda p, t: fake))

    def test_transport_failure_returns_false(self):
        provider = make_provider()
        fake = FakeConnection(getresponse_exception=socket.timeout("timed out"))
        self.assertFalse(
            probe(provider, "tok", connection_factory=lambda p, t: fake))


if __name__ == "__main__":
    unittest.main()
