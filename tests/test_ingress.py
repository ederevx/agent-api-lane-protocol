import base64
import json
import os
import socket
import stat
import struct
import tempfile
import unittest
from pathlib import Path

from aalp.ingress import Ingress, IngressError, load_or_create_secret

_TIMEOUT = 2.0
_LENGTH_PREFIX = struct.Struct(">I")


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


def _recv_exact(sock, n):
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer closed before sending all expected bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class _Response:
    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


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

    def _raw_send(self, ingress, payload: bytes) -> _Response:
        """Send a raw, already-framed payload and read back one framed
        response. Used by tests that need to send something other than a
        well-formed envelope (e.g. an oversized or malformed frame)."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(_TIMEOUT)
        self.addCleanup(sock.close)
        sock.connect(str(ingress.socket_path))
        sock.sendall(payload)
        header = _recv_exact(sock, _LENGTH_PREFIX.size)
        (length,) = _LENGTH_PREFIX.unpack(header)
        response_payload = _recv_exact(sock, length)
        response = json.loads(response_payload.decode("utf-8"))
        raw_body = response.get("body") or ""
        body = base64.b64decode(raw_body) if raw_body else b""
        return _Response(response["status"], dict(response.get("headers") or {}), body)

    def _call(self, ingress, method, path, headers=None, body=b"") -> _Response:
        envelope = json.dumps({
            "method": method,
            "path": path,
            "headers": headers or {},
            "body": base64.b64encode(body).decode("ascii") if body else "",
        }).encode("utf-8")
        return self._raw_send(ingress, _LENGTH_PREFIX.pack(len(envelope)) + envelope)

    def test_authorized_request_reaches_handler_and_echoes_response(self):
        handler = RecordingHandler(
            response=(201, {"X-Custom": "yes"}, b"created-body"))
        ingress = self._start_ingress(handler)

        response = self._call(
            ingress, "POST", "/some/path",
            headers={
                "Authorization": f"Bearer {self.secret}",
                "Content-Type": "text/plain",
            },
            body=b"hello",
        )

        self.assertEqual(response.status, 201)
        self.assertEqual(response.body, b"created-body")
        self.assertEqual(response.getheader("X-Custom"), "yes")
        self.assertEqual(len(handler.calls), 1)
        method, path, headers, received_body = handler.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/some/path")
        self.assertEqual(received_body, b"hello")

    def test_missing_authorization_header_returns_401_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)

        response = self._call(ingress, "POST", "/x", body=b"payload")

        self.assertEqual(response.status, 401)
        self.assertEqual(handler.calls, [])

    def test_wrong_bearer_token_returns_401_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)

        response = self._call(
            ingress, "POST", "/x", body=b"payload",
            headers={"Authorization": "Bearer wrong-token"},
        )

        self.assertEqual(response.status, 401)
        self.assertEqual(handler.calls, [])

    def test_oversized_frame_returns_413_without_reading_body(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler, max_request_bytes=100)

        oversized_body = b"x" * 1000
        response = self._call(
            ingress, "POST", "/x", body=oversized_body,
            headers={"Authorization": f"Bearer {self.secret}"},
        )

        self.assertEqual(response.status, 413)
        self.assertEqual(handler.calls, [])

    def test_malformed_envelope_returns_400_and_skips_handler(self):
        handler = RecordingHandler()
        ingress = self._start_ingress(handler)

        payload = b"not valid json"
        response = self._raw_send(ingress, _LENGTH_PREFIX.pack(len(payload)) + payload)

        self.assertEqual(response.status, 400)
        self.assertEqual(handler.calls, [])

    def test_handler_exception_returns_500_without_leaking_traceback_and_server_survives(self):
        handler = RecordingHandler(raise_error=True)
        ingress = self._start_ingress(handler)

        response = self._call(
            ingress, "POST", "/x", body=b"payload",
            headers={"Authorization": f"Bearer {self.secret}"},
        )

        self.assertEqual(response.status, 500)
        self.assertNotIn(b"boom", response.body)
        self.assertNotIn(b"sensitive detail", response.body)
        self.assertNotIn(b"Traceback", response.body)

        # A second, well-formed request afterward still succeeds.
        handler.raise_error = False
        response2 = self._call(
            ingress, "POST", "/y", body=b"payload2",
            headers={"Authorization": f"Bearer {self.secret}"},
        )

        self.assertEqual(response2.status, 200)
        self.assertEqual(response2.body, b"ok")


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

    def test_start_writes_descriptor_with_bound_socket_and_stop_shuts_down(self):
        handler = RecordingHandler()
        ingress = Ingress(handler, root=self.root, secret="s3cr3t")
        ingress.start()

        descriptor_path = Path(self.root) / ".aalp" / "state" / "ingress.json"
        self.assertTrue(descriptor_path.is_file())
        descriptor = json.loads(descriptor_path.read_text())
        self.assertEqual(descriptor["socket_path"], str(ingress.socket_path))
        self.assertIn("secret_file", descriptor)

        socket_path = ingress.socket_path
        ingress.stop()

        self.assertFalse(socket_path.exists())
        with self.assertRaises(OSError):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(str(socket_path))
            finally:
                sock.close()


if __name__ == "__main__":
    unittest.main()
