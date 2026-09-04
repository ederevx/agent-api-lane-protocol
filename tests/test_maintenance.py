import tempfile
import unittest
from pathlib import Path

from aalp import maintenance


class MaintenanceFlagTest(unittest.TestCase):
    def setUp(self) -> None:
        self._root_tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._root_tmp.name)

    def tearDown(self) -> None:
        self._root_tmp.cleanup()

    def test_absent_by_default(self) -> None:
        self.assertFalse(maintenance.is_maintenance_mode(self.root))

    def test_enter_creates_flag_and_parent_dirs(self) -> None:
        maintenance.enter_maintenance(self.root)

        self.assertTrue(maintenance.is_maintenance_mode(self.root))
        self.assertTrue(
            (self.root / ".aalp" / "state" / "maintenance").is_file())

    def test_enter_is_idempotent(self) -> None:
        maintenance.enter_maintenance(self.root)
        maintenance.enter_maintenance(self.root)

        self.assertTrue(maintenance.is_maintenance_mode(self.root))

    def test_exit_removes_flag(self) -> None:
        maintenance.enter_maintenance(self.root)
        maintenance.exit_maintenance(self.root)

        self.assertFalse(maintenance.is_maintenance_mode(self.root))

    def test_exit_without_enter_is_a_no_op(self) -> None:
        maintenance.exit_maintenance(self.root)

        self.assertFalse(maintenance.is_maintenance_mode(self.root))

    def test_root_env_var_used_when_no_explicit_root_given(self) -> None:
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"AALP_HOME": str(self.root)}):
            maintenance.enter_maintenance()
            self.assertTrue(maintenance.is_maintenance_mode())
            self.assertTrue(maintenance.is_maintenance_mode(self.root))


if __name__ == "__main__":
    unittest.main()
