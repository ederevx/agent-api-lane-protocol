"""Authenticated local ingress: the loopback HTTP listener ACP talks to.

AALP has exactly one authorized client on the same host (ACP), and the
traffic it forwards is already HTTP-shaped, so this is deliberately NOT
a bespoke framed-JSON+HMAC+nonce-replay protocol (that pattern belongs
to a different, multi-tenant trust model elsewhere) — a plain stdlib
`http.server.ThreadingHTTPServer` bound to 127.0.0.1 with a single
bearer-token secret is the right-sized trust boundary here.

`Ingress` takes a caller-supplied handler callback rather than knowing
about any "Gateway" class — that composition-root module is built
separately and simply constructs an `Ingress`, passing its own request
handler as this callback. This module knows nothing about it beyond
the callback signature.
"""
from __future__ import annotations

import hmac
import json
import os
import secrets
import stat
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable

Handler = Callable[[str, str, dict[str, str], bytes], tuple[int, dict[str, str], bytes]]

_SECRET_FILENAME = "ingress.secret"
_DESCRIPTOR_FILENAME = "ingress.json"


class IngressError(ValueError):
    """A stable ingress secret/descriptor validation error."""


def _default_root() -> Path:
    configured = os.environ.get("AALP_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.cwd()


def _state_dir(root: str | Path | None) -> Path:
    base = Path(root) if root is not None else _default_root()
    return base / ".aalp" / "state"


def _atomic_write(path: Path, content: str) -> None:
    """Mirror aalp/credential.py's temp-file + os.replace pattern."""
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        handle = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = -1
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
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


def load_or_create_secret(root: str | Path | None = None) -> str:
    """Read the persisted ingress bearer secret, generating it on first use."""
    path = _state_dir(root) / _SECRET_FILENAME
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        secret = secrets.token_urlsafe(32)
        _atomic_write(path, secret + "\n")
        return secret
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise IngressError("ingress secret is not a regular file")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise IngressError("ingress secret permissions are broader than 0600")
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = -1
            return handle.read().strip()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def write_ingress_descriptor(
    root: str | Path | None, host: str, port: int, secret_path: Path
) -> Path:
    """Publish the actual bound port so ACP can discover it (port=0 binds ephemeral)."""
    path = _state_dir(root) / _DESCRIPTOR_FILENAME
    descriptor = {"host": host, "port": port, "secret_file": str(secret_path)}
    _atomic_write(path, json.dumps(descriptor) + "\n")
    return path


class Ingress:
    """One loopback HTTP listener authenticated by a single bearer secret."""

    def __init__(
        self,
        handler: Handler,
        root: str | Path | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        max_request_bytes: int = 10 * 1024 * 1024,
        secret: str | None = None,
    ) -> None:
        self.root = root
        self.host = host
        self.max_request_bytes = max_request_bytes
        self.secret = secret if secret is not None else load_or_create_secret(root)
        self.secret_path = _state_dir(root) / _SECRET_FILENAME
        self._handler = handler
        self._thread: threading.Thread | None = None

        outer = self

        class _RequestHandler(BaseHTTPRequestHandler):
            def _dispatch(self) -> None:
                outer._handle_request(self)

            def do_GET(self) -> None:  # noqa: N802 - stdlib naming
                self._dispatch()

            def do_POST(self) -> None:  # noqa: N802
                self._dispatch()

            def do_PUT(self) -> None:  # noqa: N802
                self._dispatch()

            def do_PATCH(self) -> None:  # noqa: N802
                self._dispatch()

            def do_DELETE(self) -> None:  # noqa: N802
                self._dispatch()

            def log_message(self, format: str, *args: object) -> None:
                pass  # silence default stderr access logging

        self._server = ThreadingHTTPServer((host, port), _RequestHandler)

    def _handle_request(self, request: BaseHTTPRequestHandler) -> None:
        length_header = request.headers.get("Content-Length")
        if length_header is None:
            self._respond(request, 400, b"missing Content-Length")
            return
        try:
            content_length = int(length_header)
        except ValueError:
            self._respond(request, 400, b"invalid Content-Length")
            return

        if content_length > self.max_request_bytes:
            self._respond(request, 413, b"request body too large")
            return

        authorization = request.headers.get("Authorization")
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization.removeprefix("Bearer ")
        if not token or not hmac.compare_digest(token, self.secret):
            self._respond(request, 401, b"unauthorized")
            return

        body = request.rfile.read(content_length)
        headers = {key: value for key, value in request.headers.items()}
        try:
            status, response_headers, response_body = self._handler(
                request.command, request.path, headers, body
            )
        except Exception:
            self._respond(request, 500, b"internal error")
            return
        self._respond(request, status, response_body, response_headers)

    def _respond(
        self,
        request: BaseHTTPRequestHandler,
        status: int,
        body: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        request.send_response(status)
        for key, value in (headers or {}).items():
            request.send_header(key, value)
        request.send_header("Content-Length", str(len(body)))
        request.end_headers()
        request.wfile.write(body)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        write_ingress_descriptor(self.root, self.host, self.port, self.secret_path)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join()

    @property
    def port(self) -> int:
        return self._server.server_address[1]
