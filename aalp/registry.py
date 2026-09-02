"""Provider registry/loader for AALP.

Reads non-secret provider definitions from providers/*.json and turns
them into validated ProviderDefinition objects. Contains no
provider-specific branching, endpoints, or credential handling of its
own — those live entirely in each provider's own JSON file (schema:
agent_protocols_v1 metadata §23) and in the separate, gitignored
credential store under .aalp/credential/<provider id>. Adding a new
provider must only ever require a new JSON file here, never a change to
this module.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REQUIRED_STRING_FIELDS = ("id", "display_name", "endpoint", "client")


@dataclass(frozen=True)
class ProviderDefinition:
    id: str
    display_name: str
    endpoint: str
    concurrency_limit: int
    client: str
    active: bool = True
    request_shape: dict[str, Any] = field(default_factory=dict)
    timeout_overrides: dict[str, Any] = field(default_factory=dict)


def _validate(path: Path, data: Any) -> ProviderDefinition:
    if not isinstance(data, dict):
        raise ValueError(f"{path}: provider definition must be a JSON object")

    for name in _REQUIRED_STRING_FIELDS:
        value = data.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"{path}: field {name!r} must be a non-empty string")

    concurrency_limit = data.get("concurrency_limit")
    if (not isinstance(concurrency_limit, int)
            or isinstance(concurrency_limit, bool)
            or concurrency_limit <= 0):
        raise ValueError(
            f"{path}: field 'concurrency_limit' must be a positive integer")

    provider_id = data["id"]
    active = data.get("active", True)
    if not isinstance(active, bool):
        raise ValueError(f"{path}: field 'active' must be a boolean")

    request_shape = data.get("request_shape", {})
    if not isinstance(request_shape, dict):
        raise ValueError(f"{path}: field 'request_shape' must be an object")

    timeout_overrides = data.get("timeout_overrides", {})
    if not isinstance(timeout_overrides, dict):
        raise ValueError(
            f"{path}: field 'timeout_overrides' must be an object")

    return ProviderDefinition(
        id=provider_id,
        display_name=data["display_name"],
        endpoint=data["endpoint"],
        concurrency_limit=concurrency_limit,
        client=data["client"],
        active=active,
        request_shape=request_shape,
        timeout_overrides=timeout_overrides,
    )


def load_providers(providers_dir: Path) -> dict[str, ProviderDefinition]:
    """Load and validate every providers/*.json file.

    Raises ValueError naming the offending file on any malformed or
    duplicate-id definition — a broken provider file must never be
    silently skipped, since it may later gate real traffic.
    """
    providers_dir = Path(providers_dir)
    result: dict[str, ProviderDefinition] = {}
    for path in sorted(providers_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"{path}: invalid JSON: {error}") from error
        definition = _validate(path, raw)
        if definition.id in result:
            raise ValueError(
                f"{path}: duplicate provider id {definition.id!r} "
                f"(already defined by another file in {providers_dir})")
        result[definition.id] = definition
    return result


def load_provider(providers_dir: Path, provider_id: str) -> ProviderDefinition:
    """Load a single provider by id, for callers that need only one."""
    providers = load_providers(providers_dir)
    try:
        return providers[provider_id]
    except KeyError:
        raise KeyError(
            f"no provider {provider_id!r} found under {providers_dir}"
        ) from None
