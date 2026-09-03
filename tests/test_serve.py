import http.client
import json
import tempfile
import unittest
from pathlib import Path

from aalp.serve import build_ingress

_TIMEOUT = 2.0


class ServeEndToEndTest(unittest.TestCase):
    """Exercises aalp.serve.build_ingress as a real, out-of-process-shaped
    listener: a real Gateway wired to a real loopback Ingress, reachable
    only through interface v1's own HTTP surface -- the same path a
    genuine external client (ACP) would take."""

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
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.ingress.port, timeout=_TIMEOUT)
        try:
            connection.request(
                "GET", path,
                headers={
                    "Authorization": f"Bearer {self.ingress.secret}",
                    "Content-Length": "0",
                })
            response = connection.getresponse()
            body = response.read()
            return response.status, json.loads(body)
        finally:
            connection.close()

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
        self.assertEqual(descriptor["host"], "127.0.0.1")
        self.assertEqual(descriptor["port"], self.ingress.port)


if __name__ == "__main__":
    unittest.main()
