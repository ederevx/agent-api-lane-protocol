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
import threading
from typing import Any, Callable
from urllib.parse import urlsplit

from .errors import AalpResult, Outcome
from .registry import ProviderDefinition

ConnectionFactory = Callable[[ProviderDefinition, float], Any]

# Headers that name the *inbound* (ACP-to-AALP loopback) connection, not
# the upstream one -- forwarded verbatim they describe the wrong
# destination. Confirmed via a live activation run: AALP's real ingress
# hands `forward()` every header off the request it received, Host
# included; forwarding that stale loopback Host header (e.g.
# "127.0.0.1:54321") to the real, Cloudflare-fronted 'ci' endpoint made
# Cloudflare reject the request with a 403 HTML page before it ever
# reached the backend, instead of the expected Messages-shaped JSON.
# Omitting Host/Content-Length here lets http.client compute correct
# ones for the actual upstream connection; the classic hop-by-hop set
# (RFC 2616 13.5.1) is stripped for the same reason.
_DO_NOT_FORWARD_HEADERS = frozenset({
    "host", "content-length",
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
})


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

    The actual socket work runs on a background thread while this call
    waits at most `timeout_seconds` for it. `http.client`'s own
    `timeout=` only bounds the gap *between* individual blocking reads,
    not a call's total duration -- confirmed via a live activation run
    against a real, slowly-chunked upstream response: a connection that
    keeps trickling some bytes every few seconds never trips its own
    per-read socket timeout, so a plain blocking call can run far longer
    in aggregate than the configured budget (an earlier attempt at this
    fix tried to force-close the connection from a timer instead, but
    closing/shutting down a socket from a different thread than the one
    blocked in recv() on it is not reliably interruptive for an SSL
    socket -- a live run still overran its budget with that approach).
    If the deadline passes before the background thread finishes, this
    returns Outcome.COMPRESSION_TIMEOUT with `closed=False`: the same
    unconfirmed-close quarantine path already used when close() itself
    fails (see Gateway.handle()'s handling of that flag). The abandoned
    thread is left to finish or fail on its own; its result, and the
    connection it holds, are simply never looked at again.
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
        and name.lower() not in _DO_NOT_FORWARD_HEADERS
    }
    outgoing_headers[auth_header] = f"{auth_scheme} {credential}"

    upstream_path = urlsplit(provider.endpoint).path + path

    factory = connection_factory or build_connection
    connection = factory(provider, timeout_seconds)

    outcome_box: list[tuple[AalpResult, bool]] = []

    def _do_call() -> None:
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
            outcome_box.append((result, closed))

    worker = threading.Thread(target=_do_call, daemon=True)
    worker.start()
    worker.join(timeout_seconds)

    if not outcome_box:
        return (
            AalpResult(
                outcome=Outcome.COMPRESSION_TIMEOUT,
                message=f"no response within {timeout_seconds}s",
            ),
            False,
        )

    return outcome_box[0]


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
