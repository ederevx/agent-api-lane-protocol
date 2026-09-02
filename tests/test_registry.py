import json
import tempfile
import unittest
from pathlib import Path

from aalp.registry import load_provider, load_providers

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_PROVIDERS_DIR = REPO_ROOT / "providers"


class LoadRealProvidersTest(unittest.TestCase):
    def test_loads_ci(self):
        providers = load_providers(REAL_PROVIDERS_DIR)
        self.assertEqual(set(providers), {"ci"})

    def test_ci_fields(self):
        ci = load_provider(REAL_PROVIDERS_DIR, "ci")
        self.assertEqual(ci.id, "ci")
        self.assertEqual(ci.display_name, "CheapestInference")
        self.assertEqual(ci.endpoint,
                          "https://api.cheapestinference.com/anthropic")
        self.assertEqual(ci.concurrency_limit, 1)
        self.assertIsInstance(ci.concurrency_limit, int)
        self.assertTrue(ci.active)
        self.assertIn("/v1/messages", ci.request_shape.get("paths", []))

    def test_missing_provider_raises_key_error(self):
        with self.assertRaises(KeyError):
            load_provider(REAL_PROVIDERS_DIR, "does-not-exist")


class RejectionTest(unittest.TestCase):
    def _write(self, directory: Path, filename: str, data: dict) -> None:
        (directory / filename).write_text(json.dumps(data), encoding="utf-8")

    def test_missing_required_field_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._write(tmp_path, "broken.json", {
                "id": "broken",
                "display_name": "Broken",
                "concurrency_limit": 1,
                "client": "test",
            })
            with self.assertRaisesRegex(ValueError, "endpoint"):
                load_providers(tmp_path)

    def test_non_positive_concurrency_limit_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._write(tmp_path, "zero.json", {
                "id": "zero",
                "display_name": "Zero",
                "endpoint": "https://example.invalid",
                "concurrency_limit": 0,
                "client": "test",
            })
            with self.assertRaisesRegex(ValueError, "concurrency_limit"):
                load_providers(tmp_path)

    def test_duplicate_id_across_files_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            definition = {
                "id": "dup",
                "display_name": "Dup",
                "endpoint": "https://example.invalid",
                "concurrency_limit": 1,
                "client": "test",
            }
            # Two distinctly named files that both declare id "dup".
            self._write(tmp_path, "dup-a.json", definition)
            self._write(tmp_path, "dup-b.json", definition)
            with self.assertRaisesRegex(ValueError, "duplicate provider id"):
                load_providers(tmp_path)


if __name__ == "__main__":
    unittest.main()
