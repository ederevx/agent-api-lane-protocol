import http.client
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from aalp.ingress import Ingress, IngressError, load_or_create_secret

_TIMEOUT = 2.0


class RecordingHandler:
    """A call-recording fake injected handler."""

    def __init__(self, response=(200, {}, b"ok"), raise_error=False):
        self.calls = []
        self.response = response
        self.raise_error = raise_error

    def __call__(self, method, path, headers, body):
        self.calls.append((method, path, headers, body))
        if self.raise_error:
            raise RuntimeError("boom: sensitive detail")
        return self.response


class IngressRequestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = self.tempdir.name
        self.secret = "test-secret-token"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _start_ingress(self, handler, max_request_bytes=10 * 1024 * 1024):
        ingress = Ingress(
            handler,
            root=self.root,
            max_request_bytes=max_request_bytes,
            secret=self.secret,
        )
        ingress.start()
        self.addCleanup(ingress.stop)
        return ingress

    def _connection(self, ingress) -> http.client.HTTPConnection:
        conn = http.client.HTTPConnection("127.0.0.1", ingress.port, timeout=_TIMEOUT)
        self.addCleanup(conn.close)
        return conn

    def test_authorized_request_reaches_handler_and_echoes_response(self):
        handler = RecordingHandler(
            response=(201, {"X-Custom": "yes"}, b"created-body"))
        ingress = self._start_ingress(handler)
        conn = self._connection(ingress)

        conn.request(
            "POST",
            "/some/path",
            body=b"hello",
            headers={
                "Authorization": f"Bearer {self.secret}",
                "Content-Type": "text/plain",
            },
        )
        response = conn.getresponse()
        body = response.read()

        self.assertEqual(response.status, 201)
        self.assertEqual(body, b"created-body")
        self.assertEqual(response.getheader("X-Custom"), "yes")
        self.assertEqual(len(handler.calls), 1)
        method, path, headers, received_body = handler.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/some/path")
        self.assertEqual(received_body, b"hello")

    def test_missing_authorization_header_returns_401_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)
        conn = self._connection(ingress)

        conn.request("POST", "/x", body=b"payload")
        response = conn.getresponse()
        response.read()

        self.assertEqual(response.status, 401)
        self.assertEqual(handler.calls, [])

    def test_wrong_bearer_token_returns_401_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)
        conn = self._connection(ingress)

        conn.request(
            "POST", "/x", body=b"payload",
            headers={"Authorization": "Bearer wrong-token"},
        )
        response = conn.getresponse()
        response.read()

        self.assertEqual(response.status, 401)
        self.assertEqual(handler.calls, [])

    def test_oversized_content_length_returns_413_without_reading_body(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler, max_request_bytes=100)
        conn = self._connection(ingress)

        oversized_body = b"x" * 1000
        conn.request(
            "POST", "/x", body=oversized_body,
            headers={"Authorization": f"Bearer {self.secret}"},
        )
        response = conn.getresponse()
        response.read()

        self.assertEqual(response.status, 413)
        self.assertEqual(handler.calls, [])

    def test_missing_content_length_returns_400_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)
        conn = self._connection(ingress)

        # Send a raw request line + headers with no Content-Length, no body.
        conn.putrequest("POST", "/x", skip_host=False, skip_accept_encoding=False)
        conn.putheader("Authorization", f"Bearer {self.secret}")
        conn.endheaders()
        response = conn.getresponse()
        response.read()

        self.assertEqual(response.status, 400)
        self.assertEqual(handler.calls, [])

    def test_handler_exception_returns_500_without_leaking_traceback_and_server_survives(self):
        handler = RecordingHandler(raise_error=True)
        ingress = self._start_ingress(handler)
        conn = self._connection(ingress)

        conn.request(
            "POST", "/x", body=b"payload",
            headers={"Authorization": f"Bearer {self.secret}"},
        )
        response = conn.getresponse()
        body = response.read()

        self.assertEqual(response.status, 500)
        self.assertNotIn(b"boom", body)
        self.assertNotIn(b"sensitive detail", body)
        self.assertNotIn(b"Traceback", body)

        # A second, well-formed request afterward still succeeds.
        handler.raise_error = False
        conn2 = self._connection(ingress)
        conn2.request(
            "POST", "/y", body=b"payload2",
            headers={"Authorization": f"Bearer {self.secret}"},
        )
        response2 = conn2.getresponse()
        body2 = response2.read()

        self.assertEqual(response2.status, 200)
        self.assertEqual(body2, b"ok")


class LoadOrCreateSecretTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = self.tempdir.name

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_first_call_creates_secret_file_with_strict_perms(self):
        secret = load_or_create_secret(self.root)

        secret_path = Path(self.root) / ".aalp" / "state" / "ingress.secret"
        state_dir = Path(self.root) / ".aalp" / "state"
        self.assertTrue(secret_path.is_file())
        self.assertTrue(secret)

        file_mode = stat.S_IMODE(secret_path.stat().st_mode)
        dir_mode = stat.S_IMODE(state_dir.stat().st_mode)
        self.assertEqual(file_mode, 0o600)
        self.assertEqual(dir_mode, 0o700)

    def test_second_call_returns_identical_persisted_secret(self):
        first = load_or_create_secret(self.root)
        second = load_or_create_secret(self.root)
        self.assertEqual(first, second)

    def test_tampered_permissions_raise_ingress_error(self):
        load_or_create_secret(self.root)
        secret_path = Path(self.root) / ".aalp" / "state" / "ingress.secret"
        os.chmod(secret_path, 0o644)

        with self.assertRaises(IngressError):
            load_or_create_secret(self.root)


class IngressDescriptorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = self.tempdir.name

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_start_writes_descriptor_with_bound_port_and_stop_shuts_down(self):
        handler = RecordingHandler()
        ingress = Ingress(handler, root=self.root, secret="s3cr3t")
        ingress.start()

        descriptor_path = Path(self.root) / ".aalp" / "state" / "ingress.json"
        self.assertTrue(descriptor_path.is_file())
        descriptor = json.loads(descriptor_path.read_text())
        self.assertEqual(descriptor["host"], "127.0.0.1")
        self.assertEqual(descriptor["port"], ingress.port)
        self.assertIn("secret_file", descriptor)

        port = ingress.port
        ingress.stop()

        with self.assertRaises((ConnectionRefusedError, OSError)):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=_TIMEOUT)
            conn.connect()


if __name__ == "__main__":
    unittest.main()
