import inspect
import json
import stat
import tempfile
import unittest
from pathlib import Path

from aalp.audit import append, read_entries
from aalp.errors import Outcome


class AuditTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def log_path(self) -> Path:
        return self.root / ".aalp" / "state" / "audit.log"


class AppendAndReadTest(AuditTestBase):
    def test_single_entry_round_trips_all_fields(self) -> None:
        append(
            "ci",
            "flow-1",
            "/v1/messages",
            Outcome.SUCCESS,
            200,
            12.5,
            340.25,
            root=self.root,
        )
        entries = read_entries(root=self.root)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["provider_id"], "ci")
        self.assertEqual(entry["flow_id"], "flow-1")
        self.assertEqual(entry["path"], "/v1/messages")
        self.assertEqual(entry["outcome"], "success")
        self.assertIsInstance(entry["outcome"], str)
        self.assertEqual(entry["upstream_status"], 200)
        self.assertEqual(entry["queue_wait_ms"], 12.5)
        self.assertEqual(entry["elapsed_ms"], 340.25)
        self.assertIn("timestamp", entry)

    def test_upstream_status_none_round_trips_as_null(self) -> None:
        append(
            "ci",
            "flow-2",
            "/v1/messages",
            Outcome.QUEUE_TIMEOUT,
            None,
            5000.0,
            5000.0,
            root=self.root,
        )
        entries = read_entries(root=self.root)
        self.assertEqual(len(entries), 1)
        self.assertIsNone(entries[0]["upstream_status"])

    def test_missing_log_file_reads_as_empty_list(self) -> None:
        self.assertEqual(read_entries(root=self.root), [])


class PermissionTest(AuditTestBase):
    def test_creates_state_dir_0700_and_log_0600(self) -> None:
        append(
            "ci", "flow-1", "/v1/messages", Outcome.SUCCESS, 200, 1.0, 2.0,
            root=self.root,
        )
        state_dir = self.root / ".aalp" / "state"
        dir_mode = stat.S_IMODE(state_dir.stat().st_mode)
        file_mode = stat.S_IMODE(self.log_path().stat().st_mode)
        self.assertEqual(dir_mode, 0o700)
        self.assertEqual(file_mode, 0o600)


class OrderingTest(AuditTestBase):
    def test_multiple_appends_preserve_call_order(self) -> None:
        for index in range(5):
            append(
                "ci", f"flow-{index}", "/v1/messages", Outcome.SUCCESS,
                200, float(index), float(index) * 10, root=self.root,
            )
        entries = read_entries(root=self.root)
        self.assertEqual([entry["flow_id"] for entry in entries],
                          [f"flow-{index}" for index in range(5)])

        with self.log_path().open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
        self.assertEqual(len(lines), 5)
        for line in lines:
            json.loads(line)


class RotationTest(AuditTestBase):
    def test_rotation_produces_backup_and_trims_live_log(self) -> None:
        tiny_max_bytes = 650
        for index in range(10):
            append(
                "ci", f"flow-{index}", "/v1/messages", Outcome.SUCCESS,
                200, float(index), float(index), root=self.root,
                max_bytes=tiny_max_bytes,
            )

        backup_path = self.log_path().with_suffix(
            self.log_path().suffix + ".1")
        self.assertTrue(backup_path.exists())
        # The backup holds whatever had accumulated right up to the
        # rotation trigger; the live log restarted empty after that
        # and so is smaller than the backup it was rotated out of.
        self.assertLess(self.log_path().stat().st_size,
                         backup_path.stat().st_size)

        entries = read_entries(root=self.root)
        seen_flow_ids = {entry["flow_id"] for entry in entries}
        # The rotated-out early entries must not appear via read_entries.
        self.assertNotIn("flow-0", seen_flow_ids)
        self.assertIn("flow-9", seen_flow_ids)


class SignatureTest(unittest.TestCase):
    def test_append_signature_has_no_content_or_secret_escape_hatch(self) -> None:
        signature = inspect.signature(append)
        forbidden_substrings = (
            "body", "header", "credential", "prompt", "secret",
        )
        for name, parameter in signature.parameters.items():
            folded = name.casefold()
            for forbidden in forbidden_substrings:
                self.assertNotIn(
                    forbidden, folded,
                    f"append() parameter {name!r} looks like a content/"
                    f"secret escape hatch (matches {forbidden!r})")
            self.assertNotEqual(
                parameter.kind, inspect.Parameter.VAR_KEYWORD,
                f"append() must not accept **kwargs (found {name!r})")


if __name__ == "__main__":
    unittest.main()
