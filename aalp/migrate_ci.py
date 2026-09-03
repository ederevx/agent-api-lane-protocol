"""One-time migration of the `ci` (CheapestInference) credential from ADP.

agent-delegation-protocol ("ADP") already has a real, in-use credential
for this provider on disk, in ADP's own config location. Re-prompting an
operator to type it in again into AALP's separate store
(`aalp/credential.py`) would be needless friction and an extra chance to
transcribe it wrong, so this module locates the existing ADP credential
file(s), copies the value into AALP's store, and confirms the copy
actually authenticates before declaring the migration done
(agent_protocols_v1_metadata_v1.md §27).

Path discovery in `discover_adp_credential_paths` reimplements (does not
import) ADP's own `_config_root()`/`credential_path()` from
scripts/agents/managed_service.py. Unlike ADP's own resolution, which is
deterministic and returns a single path for the current environment,
this function deliberately probes every plausible historical location:
an operator's environment variables can change over time (e.g. XDG_CONFIG_HOME
set today but unset when the ADP credential was first written), and a
stale leftover file can sit at a location the current environment no
longer resolves to. Missing a stale file would silently leave it behind
after migration; checking a location that doesn't exist costs nothing.
"""
from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Callable

from . import credential as credential_module
from .registry import load_provider
from .forwarder import probe as default_probe

DiscoverFn = Callable[[], "list[Path]"]
ProbeFn = Callable[..., bool]

_LEGACY_DIR_NAME = "agent-delegation-protocol"


class MigrationStatus(Enum):
    ALREADY_PRESENT = "already_present"
    MIGRATED = "migrated"
    NEEDS_PROMPT = "needs_prompt"


class MigrationConflict(ValueError):
    """Discovered legacy candidates do not all share the same value.

    Carries only the candidate *paths*, never their contents — this
    exception's message and `.paths` must remain safe to log.
    """

    def __init__(self, paths: "list[Path]") -> None:
        self.paths = list(paths)
        super().__init__(
            f"multiple differing legacy credentials found: {self.paths}")


class MigrationValidationError(ValueError):
    """Raised when the copied credential failed forwarder.probe()."""


def discover_adp_credential_paths(
    reference: str = "cheapestinference",
) -> "list[Path]":
    """Return every plausible ADP credential file for `reference` that
    actually exists on disk, deduplicated by resolved absolute path.

    Read-only: never reads file *contents*, only checks existence.
    """
    candidates: list[Path] = []

    configured = os.environ.get("DELEGATION_CONFIG_HOME")
    if configured:
        candidates.append(
            Path(configured).expanduser() / "credentials" / reference)

    xdg_configured = os.environ.get("XDG_CONFIG_HOME")
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
    else:
        base = Path(xdg_configured or str(Path.home() / ".config"))
    candidates.append(base / _LEGACY_DIR_NAME / "credentials" / reference)

    if os.name != "nt" and xdg_configured:
        candidates.append(
            Path.home() / ".config" / _LEGACY_DIR_NAME
            / "credentials" / reference)

    seen: set[Path] = set()
    result: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        if candidate.is_file():
            seen.add(resolved)
            result.append(candidate)
    return result


def migrate_ci(
    providers_dir: Path,
    root: str | Path | None = None,
    provider_id: str = "ci",
    *,
    probe: ProbeFn | None = None,
    discover: DiscoverFn | None = None,
) -> MigrationStatus:
    """Copy the legacy ADP `ci` credential into AALP's store, once.

    A second call after a successful migration is a cheap no-op: the
    already-present check below returns immediately without invoking
    `discover` or `probe` again.
    """
    probe = probe if probe is not None else default_probe
    discover = discover if discover is not None else discover_adp_credential_paths

    if credential_module.credential_path(provider_id, root).exists():
        return MigrationStatus.ALREADY_PRESENT

    candidates = discover()
    if not candidates:
        return MigrationStatus.NEEDS_PROMPT

    values = [path.read_text(encoding="utf-8").rstrip("\n")
              for path in candidates]

    if any(value != values[0] for value in values):
        raise MigrationConflict(candidates)
    value = values[0]

    credential_module._validate_credential_value(value)

    credential_module.write_credential(provider_id, value, root=root)

    provider = load_provider(providers_dir, provider_id)
    if not probe(provider, value):
        # Deliberately leave both the just-written AALP credential and
        # every legacy ADP file untouched here. A probe failure can be a
        # transient network problem rather than proof the credential
        # itself is bad; deleting a possibly-correct copy over a flaky
        # check would destroy more than it protects. Both copies survive
        # so a retry (of migrate_ci, or of the probe alone) is possible.
        raise MigrationValidationError(
            f"copied credential for provider {provider_id!r} failed probe")

    # Safe only now: step above proved every candidate held the same
    # value, so deleting all of them loses nothing.
    for path in candidates:
        path.unlink()

    return MigrationStatus.MIGRATED
