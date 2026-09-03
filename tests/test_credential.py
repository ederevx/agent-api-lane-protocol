import os
import stat
import tempfile
import unittest
from pathlib import Path

from aalp.credential import (
    CredentialError,
    credential_path,
    read_credential,
    remove_credential,
    write_credential,
)


class CredentialTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()


class RoundTripTest(CredentialTestBase):
    def test_write_then_read_roundtrip(self) -> None:
        write_credential("ci", "sk-example-not-a-real-secret", root=self.root)
        self.assertEqual(
            read_credential("ci", root=self.root), "sk-example-not-a-real-secret")

    def test_write_overwrite_replaces_value_atomically(self) -> None:
        write_credential("ci", "first-value", root=self.root)
        write_credential("ci", "second-value", root=self.root)
        self.assertEqual(read_credential("ci", root=self.root), "second-value")

        parent = credential_path("ci", root=self.root).parent
        leftovers = [entry for entry in os.listdir(parent)
                     if entry != "ci"]
        self.assertEqual(leftovers, [])

    def test_write_rejects_invalid_provider_id(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("Ci/../etc", "value", root=self.root)

    def test_invalid_provider_id_leaves_no_directory(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("BAD ID", "value", root=self.root)
        self.assertFalse((self.root / ".aalp").exists())


class PermissionTest(CredentialTestBase):
    def test_write_sets_owner_only_permissions(self) -> None:
        path = write_credential("ci", "value", root=self.root)
        file_mode = stat.S_IMODE(path.stat().st_mode)
        dir_mode = stat.S_IMODE(path.parent.stat().st_mode)
        self.assertEqual(file_mode & 0o077, 0)
        self.assertEqual(dir_mode, 0o700)

    def test_read_rejects_group_or_other_readable_file(self) -> None:
        path = write_credential("ci", "value", root=self.root)
        os.chmod(path, 0o644)
        with self.assertRaises(CredentialError):
            read_credential("ci", root=self.root)

    def test_read_rejects_symlinked_credential(self) -> None:
        real = self.root / "elsewhere.txt"
        real.write_text("value\n", encoding="utf-8")
        path = credential_path("ci", root=self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to(real)
        with self.assertRaises(OSError):
            read_credential("ci", root=self.root)


class FormatValidationTest(CredentialTestBase):
    def test_rejects_multiline_value(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("ci", "line-one\nline-two", root=self.root)

    def test_rejects_empty_value(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("ci", "", root=self.root)

    def test_rejects_env_assignment_shaped_value(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("ci", "OPENAI_API_KEY=sk-example", root=self.root)

    def test_rejects_export_prefixed_assignment(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("ci", "export TOKEN=abc123", root=self.root)

    def test_allows_a_raw_token_containing_an_equals_sign(self) -> None:
        # Base64url-ish tokens can legitimately contain '=' padding; only
        # values that *look like* a NAME=value assignment are rejected.
        write_credential("ci", "abc123==", root=self.root)
        self.assertEqual(read_credential("ci", root=self.root), "abc123==")

    def test_invalid_write_leaves_no_file_behind(self) -> None:
        with self.assertRaises(CredentialError):
            write_credential("ci", "TOKEN=abc123", root=self.root)
        self.assertFalse(credential_path("ci", root=self.root).exists())


class RemoveCredentialTest(CredentialTestBase):
    def test_remove_deletes_and_returns_true(self) -> None:
        write_credential("ci", "value", root=self.root)
        self.assertTrue(remove_credential("ci", root=self.root))
        self.assertFalse(credential_path("ci", root=self.root).exists())

    def test_remove_missing_credential_returns_false(self) -> None:
        self.assertFalse(remove_credential("ci", root=self.root))

    def test_remove_rejects_symlink_without_deleting_target(self) -> None:
        real = self.root / "elsewhere.txt"
        real.write_text("value\n", encoding="utf-8")
        path = credential_path("ci", root=self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to(real)
        with self.assertRaises(CredentialError):
            remove_credential("ci", root=self.root)
        self.assertTrue(real.exists())


if __name__ == "__main__":
    unittest.main()
