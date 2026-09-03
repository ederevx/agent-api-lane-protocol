"""Append-only, owner-private, size-bounded audit log.

Records what happened on each forwarded request — never what was in
it. The audit surface must be provably blind to secret/content data
(agent_protocols_v1_metadata_v1.md §34), and that's enforced here by
the shape of `append()` itself: its parameter list has no `body`,
`headers`, `credential`, `prompt`, or catch-all `**kwargs` slot for
such a value to travel through, and it must never gain one — the
absence of an escape hatch is the guarantee, not a convention layered
on top of it.

One JSON object per line at `<root>/.aalp/state/audit.log`, rotated to
a single `.1` backup generation once `max_bytes` would be exceeded —
enough to bound disk usage without building a log archiver.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .errors import Outcome

DEFAULT_MAX_BYTES = 10 * 1024 * 1024


def _default_root() -> Path:
    configured = os.environ.get("AALP_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.cwd()


def _log_path(root: str | Path | None) -> Path:
    base = Path(root) if root is not None else _default_root()
    return base / ".aalp" / "state" / "audit.log"


def append(
    provider_id: str,
    flow_id: str,
    path: str,
    outcome: Outcome,
    upstream_status: int | None,
    queue_wait_ms: float,
    elapsed_ms: float,
    root: str | Path | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> None:
    log_path = _log_path(root)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(log_path.parent, 0o700)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "provider_id": provider_id,
        "flow_id": flow_id,
        "path": path,
        "outcome": outcome.value,
        "upstream_status": upstream_status,
        "queue_wait_ms": queue_wait_ms,
        "elapsed_ms": elapsed_ms,
    }
    line = (json.dumps(entry, sort_keys=True) + "\n").encode("utf-8")

    if log_path.exists() and log_path.stat().st_size + len(line) > max_bytes:
        backup_path = log_path.with_suffix(log_path.suffix + ".1")
        os.replace(log_path, backup_path)

    descriptor = os.open(
        log_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, line)
    finally:
        os.close(descriptor)


def read_entries(root: str | Path | None = None) -> list[dict]:
    log_path = _log_path(root)
    if not log_path.exists():
        return []
    with log_path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]
