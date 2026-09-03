import base64
import json
import socket
import struct
import tempfile
import unittest
from pathlib import Path

from aalp.serve import build_ingress

_TIMEOUT = 2.0
_LENGTH_PREFIX = struct.Struct(">I")


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


class ServeEndToEndTest(unittest.TestCase):
    """Exercises aalp.serve.build_ingress as a real, out-of-process-shaped
    listener: a real Gateway wired to a real loopback Ingress, reachable
    only through interface v1's own length-prefixed-JSON-over-`AF_UNIX`
    surface -- the same path a genuine external client (ACP) would take."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.providers_dir = self.root / "providers"
        self.providers_dir.mkdir()
        (self.providers_dir / "test-provider.json").write_text(json.dumps({
            "id": "test-provider",
            "display_name": "Test Provider",
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
        }))
        self.ingress = build_ingress(
            providers_dir=self.providers_dir, root=self.root)
        self.ingress.start()
        self.addCleanup(self.ingress.stop)
        self.addCleanup(self.tempdir.cleanup)

    def _get(self, path: str) -> tuple[int, dict]:
        envelope = json.dumps({
            "method": "GET",
            "path": path,
            "headers": {"Authorization": f"Bearer {self.ingress.secret}"},
            "body": "",
        }).encode("utf-8")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(_TIMEOUT)
        try:
            sock.connect(str(self.ingress.socket_path))
            sock.sendall(_LENGTH_PREFIX.pack(len(envelope)) + envelope)
            header = _recv_exact(sock, _LENGTH_PREFIX.size)
            (length,) = _LENGTH_PREFIX.unpack(header)
            payload = _recv_exact(sock, length)
        finally:
            sock.close()
        response = json.loads(payload.decode("utf-8"))
        raw_body = response.get("body") or ""
        body = base64.b64decode(raw_body) if raw_body else b""
        return response["status"], json.loads(body)

    def test_capabilities_reachable_over_real_socket(self) -> None:
        status, body = self._get("/_aalp/v1/capabilities")
        self.assertEqual(status, 200)
        self.assertEqual(body["service"], "aalp")
        self.assertEqual(body["interface_version"], 1)

    def test_provider_status_reflects_loaded_provider(self) -> None:
        status, body = self._get("/_aalp/v1/providers/test-provider")
        self.assertEqual(status, 200)
        self.assertEqual(body["id"], "test-provider")
        self.assertTrue(body["active"])

    def test_ingress_descriptor_written_to_configured_root(self) -> None:
        descriptor_path = self.root / ".aalp" / "state" / "ingress.json"
        self.assertTrue(descriptor_path.exists())
        descriptor = json.loads(descriptor_path.read_text())
        self.assertEqual(descriptor["socket_path"], str(self.ingress.socket_path))


if __name__ == "__main__":
    unittest.main()
