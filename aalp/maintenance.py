"""Maintenance-mode bypass: `.aalp/state/maintenance`.

A flag file, not a config value -- presence alone puts AALP into
maintenance mode, absence takes it out. An operator toggles it directly
(touch/rm) without restarting the service; `Gateway.handle()` checks it
fresh on every request (see gateway.py), so the effect is immediate in
both directions and needs no code deploy to flip.

Mirrors `aalp/credential.py`'s root-resolution convention (`AALP_HOME`
env var if set, else the caller's own `root`, else cwd) rather than
inventing a second one.
"""
from __future__ import annotations

import os
from pathlib import Path


def _default_root() -> Path:
    configured = os.environ.get("AALP_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.cwd()


def maintenance_flag_path(root: str | Path | None = None) -> Path:
    base = Path(root) if root is not None else _default_root()
    return base / ".aalp" / "state" / "maintenance"


def is_maintenance_mode(root: str | Path | None = None) -> bool:
    return maintenance_flag_path(root).exists()


def enter_maintenance(root: str | Path | None = None) -> None:
    """Create the flag file (and its parent dir) if not already present."""
    path = maintenance_flag_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def exit_maintenance(root: str | Path | None = None) -> None:
    """Remove the flag file if present; a no-op if already absent."""
    try:
        maintenance_flag_path(root).unlink()
    except FileNotFoundError:
        pass
