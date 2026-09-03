"""The only module in AALP that speaks http.client to a real upstream.

Every other stage (lane admission, flow admission, registry, credential
store) is provider-agnostic and socket-free by construction; this module
is deliberately the single seam where that changes, so it is also the
single place dependency injection (`connection_factory`) is required to
keep the rest of the test suite free of real network calls. Classification
here is transport-level only (agent_protocols_v1_metadata_v1.md §18): a
4xx/5xx *from the upstream API* is still Outcome.SUCCESS, since the
gateway — not this module — interprets the passed-through status/body.
"""
from __future__ import annotations

import http.client
import socket
from typing import Any, Callable
from urllib.parse import urlsplit

from .errors import AalpResult, Outcome
from .registry import ProviderDefinition

ConnectionFactory = Callable[[ProviderDefinition, float], Any]


def build_connection(
    provider: ProviderDefinition,
    timeout_seconds: float,
) -> http.client.HTTPConnection:
    if provider.client != "python-http.client":
        raise ValueError(
            f"provider {provider.id!r} declares unsupported client "
            f"{provider.client!r}")
    split = urlsplit(provider.endpoint)
    connection_class = (
        http.client.HTTPSConnection if split.scheme == "https"
        else http.client.HTTPConnection
    )
    return connection_class(split.hostname, split.port, timeout=timeout_seconds)


def forward(
    provider: ProviderDefinition,
    credential: str,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes,
    timeout_seconds: float,
    connection_factory: ConnectionFactory | None = None,
) -> tuple[AalpResult, bool]:
    """Send one request upstream and classify the transport outcome.

    Raises ValueError if `path` is not one this provider declares —
    a caller/config bug, never a network outcome.
    """
    allowed_paths = provider.request_shape.get("paths", [])
    if path not in allowed_paths:
        raise ValueError(
            f"path {path!r} is not declared for provider {provider.id!r}")

    auth_header = provider.request_shape["auth_header"]
    auth_scheme = provider.request_shape["auth_scheme"]
    outgoing_headers = {
        name: value for name, value in headers.items()
        if name.lower() != auth_header.lower()
    }
    outgoing_headers[auth_header] = f"{auth_scheme} {credential}"

    upstream_path = urlsplit(provider.endpoint).path + path

    factory = connection_factory or build_connection
    connection = factory(provider, timeout_seconds)

    try:
        try:
            connection.request(
                method, upstream_path, body=body, headers=outgoing_headers)
            response = connection.getresponse()
        except (socket.timeout, TimeoutError) as error:
            result = AalpResult(
                outcome=Outcome.COMPRESSION_TIMEOUT, message=str(error))
        except (OSError, http.client.HTTPException) as error:
            result = AalpResult(
                outcome=Outcome.UPSTREAM_ERROR, message=str(error))
        else:
            try:
                response_body = response.read()
            except (socket.timeout, TimeoutError) as error:
                result = AalpResult(
                    outcome=Outcome.COMPRESSION_TIMEOUT, message=str(error))
            except Exception as error:
                result = AalpResult(
                    outcome=Outcome.INVALID_RESPONSE, message=str(error))
            else:
                result = AalpResult(
                    outcome=Outcome.SUCCESS,
                    status_code=response.status,
                    headers=dict(response.getheaders()),
                    body=response_body,
                )
    finally:
        try:
            connection.close()
            closed = True
        except Exception:
            closed = False

    return result, closed


def probe(
    provider: ProviderDefinition,
    credential: str,
    connection_factory: ConnectionFactory | None = None,
) -> bool:
    """Bounded check that `credential` actually authenticates against
    `provider`, for migration code validating a freshly-copied credential.

    Makes no assumption about response content beyond status code, and
    never raises on a bad credential — only a genuine transport or
    programming error propagates.
    """
    path = provider.request_shape["paths"][0]
    result, _closed = forward(
        provider, credential, "POST", path, {}, b"{}",
        timeout_seconds=10.0, connection_factory=connection_factory,
    )
    return result.ok and result.status_code not in (401, 403)
