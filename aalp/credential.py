"""Steady-state credential storage: `.aalp/credential/<provider id>`.

Ports agent-delegation-protocol's proven POSIX credential-file handling
(scripts/agents/managed_service.py: read_credential/write_credential/
credential_path) rather than inventing a new storage format: O_NOFOLLOW
open with a regular-file check, a strict permission check on read
(reject anything broader than 0600), an atomic temp-file + os.replace
write with 0600/0700 perms, and rejection of values that look like an
`export FOO=...` assignment rather than a raw token.

Generic over `provider_id` — no branch here ever inspects which
provider it is (agent_protocols_v1_metadata_v1.md §22, §28). Credential
values never appear in providers/*.json and are never accepted by
anything in this module as coming from that file.
"""
from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import Any

_REFERENCE_CHARSET = set("abcdefghijklmnopqrstuvwxyz0123456789._-")
_SECRET_SUFFIXES = (
    "_api_key", "_access_key", "_token", "_secret", "_password",
    "_credential",
)
_SECRET_NAMES = {
    "api_key", "access_key", "token", "secret", "password", "credential",
}


class CredentialError(ValueError):
    """A stable credential-store validation error."""


def _default_root() -> Path:
    configured = os.environ.get("AALP_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.cwd()


def credential_path(provider_id: str, root: str | Path | None = None) -> Path:
    if (not provider_id or len(provider_id) > 64
            or not provider_id[0].isalnum()
            or any(character not in _REFERENCE_CHARSET
                   for character in provider_id)):
        raise CredentialError("provider id is invalid")
    base = Path(root) if root is not None else _default_root()
    return base / ".aalp" / "credential" / provider_id


def _validate_credential_value(value: Any) -> str:
    """Require a raw, single-line credential rather than an env assignment."""
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise CredentialError("credential must contain exactly one non-empty line")
    name, separator, _remainder = value.partition("=")
    normalized = name.removeprefix("export ").strip()
    folded = normalized.casefold()
    likely_name = (
        normalized and normalized[0].isalpha() and
        all(character.isalnum() or character == "_" for character in normalized)
    )
    if (separator and likely_name and
            (folded in _SECRET_NAMES or folded.endswith(_SECRET_SUFFIXES))):
        raise CredentialError(
            "credential must be the raw token, not an environment assignment")
    return value


def write_credential(
    provider_id: str,
    value: str,
    root: str | Path | None = None,
) -> Path:
    value = _validate_credential_value(value)
    path = credential_path(provider_id, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        handle = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = -1
        with handle:
            handle.write(value + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        return path
    except BaseException:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    else:
        temporary_path.unlink(missing_ok=True)


def read_credential(provider_id: str, root: str | Path | None = None) -> str:
    """Read one protected credential without following the final symlink."""
    path = credential_path(provider_id, root)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise CredentialError("credential is not a regular file")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise CredentialError("credential permissions are broader than 0600")
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = -1
            value = handle.read().rstrip("\n")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return _validate_credential_value(value)


def remove_credential(provider_id: str, root: str | Path | None = None) -> bool:
    path = credential_path(provider_id, root)
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise CredentialError("credential is not a regular non-symlink file")
    # Validate protection before allowing the store operation to remove it.
    read_credential(provider_id, root)
    try:
        os.unlink(path)
    except FileNotFoundError:
        return False
    return True
