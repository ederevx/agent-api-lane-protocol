import tempfile
import unittest
from pathlib import Path

from aalp.credential import CredentialError, read_credential, write_credential
from aalp.migrate_ci import (
    MigrationConflict,
    MigrationStatus,
    MigrationValidationError,
    migrate_ci,
)

PROVIDERS_DIR = Path(__file__).resolve().parent.parent / "providers"


def _always_true(provider, value):
    return True


def _always_false(provider, value):
    return False


def _never_called(*args, **kwargs):
    raise AssertionError("should not have been called")


class MigrateCiTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self._legacy_tmp = tempfile.TemporaryDirectory()
        self.legacy_dir = Path(self._legacy_tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()
        self._legacy_tmp.cleanup()

    def _legacy_file(self, name: str, content: str) -> Path:
        path = self.legacy_dir / name
        path.write_text(content, encoding="utf-8")
        return path


class NoCandidatesTest(MigrateCiTestBase):
    def test_zero_candidates_returns_needs_prompt(self) -> None:
        status = migrate_ci(
            PROVIDERS_DIR,
            root=self.root,
            discover=lambda: [],
            probe=_never_called,
        )
        self.assertEqual(status, MigrationStatus.NEEDS_PROMPT)
        from aalp.credential import credential_path
        self.assertFalse(credential_path("ci", root=self.root).exists())


class SingleCandidateTest(MigrateCiTestBase):
    def test_single_candidate_migrates_and_deletes_legacy_file(self) -> None:
        legacy = self._legacy_file("cheapestinference", "sk-legacy-value")

        status = migrate_ci(
            PROVIDERS_DIR,
            root=self.root,
            discover=lambda: [legacy],
            probe=_always_true,
        )

        self.assertEqual(status, MigrationStatus.MIGRATED)
        self.assertEqual(read_credential("ci", root=self.root), "sk-legacy-value")
        self.assertFalse(legacy.exists())


class MultipleIdenticalCandidatesTest(MigrateCiTestBase):
    def test_multiple_identical_candidates_migrate_and_delete_all(self) -> None:
        first = self._legacy_file("a", "same-value")
        second = self._legacy_file("b", "same-value")

        status = migrate_ci(
            PROVIDERS_DIR,
            root=self.root,
            discover=lambda: [first, second],
            probe=_always_true,
        )

        self.assertEqual(status, MigrationStatus.MIGRATED)
        self.assertEqual(read_credential("ci", root=self.root), "same-value")
        self.assertFalse(first.exists())
        self.assertFalse(second.exists())


class ConflictTest(MigrateCiTestBase):
    def test_differing_candidates_raise_conflict_without_writing_or_deleting(
        self,
    ) -> None:
        first = self._legacy_file("a", "value-one-secret")
        second = self._legacy_file("b", "value-two-secret")

        with self.assertRaises(MigrationConflict) as context:
            migrate_ci(
                PROVIDERS_DIR,
                root=self.root,
                discover=lambda: [first, second],
                probe=_never_called,
            )

        message = str(context.exception)
        self.assertNotIn("value-one-secret", message)
        self.assertNotIn("value-two-secret", message)
        self.assertEqual(set(context.exception.paths), {first, second})

        from aalp.credential import credential_path
        self.assertFalse(credential_path("ci", root=self.root).exists())
        self.assertTrue(first.exists())
        self.assertTrue(second.exists())


class FormatValidationTest(MigrateCiTestBase):
    def test_env_assignment_shaped_value_raises_credential_error(self) -> None:
        legacy = self._legacy_file("cheapestinference", "TOKEN=abc123")

        with self.assertRaises(CredentialError):
            migrate_ci(
                PROVIDERS_DIR,
                root=self.root,
                discover=lambda: [legacy],
                probe=_never_called,
            )

        from aalp.credential import credential_path
        self.assertFalse(credential_path("ci", root=self.root).exists())
        self.assertTrue(legacy.exists())


class ProbeFailureTest(MigrateCiTestBase):
    def test_probe_failure_leaves_both_copies_in_place(self) -> None:
        legacy = self._legacy_file("cheapestinference", "sk-legacy-value")

        with self.assertRaises(MigrationValidationError):
            migrate_ci(
                PROVIDERS_DIR,
                root=self.root,
                discover=lambda: [legacy],
                probe=_always_false,
            )

        # Copy happened before verify: the AALP credential exists and is
        # readable even though the probe failed.
        self.assertEqual(read_credential("ci", root=self.root), "sk-legacy-value")
        # Legacy file was not deleted on probe failure.
        self.assertTrue(legacy.exists())


class AlreadyPresentTest(MigrateCiTestBase):
    def test_already_present_short_circuits_without_discover_or_probe(
        self,
    ) -> None:
        write_credential("ci", "already-here", root=self.root)

        status = migrate_ci(
            PROVIDERS_DIR,
            root=self.root,
            discover=_never_called,
            probe=_never_called,
        )

        self.assertEqual(status, MigrationStatus.ALREADY_PRESENT)
        self.assertEqual(read_credential("ci", root=self.root), "already-here")


if __name__ == "__main__":
    unittest.main()
