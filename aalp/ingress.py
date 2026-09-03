"""Authenticated local ingress: the loopback Unix-socket listener ACP talks to.

AALP has exactly one authorized client on the same host (ACP), so this is
deliberately NOT a bespoke framed-JSON+HMAC+nonce-replay protocol built for
a multi-tenant network trust model -- a Unix domain socket, reachable only
by processes on this host with filesystem access to the socket path, plus
a single bearer-token secret, is the right-sized trust boundary here.

This replaces an earlier HTTP-over-TCP-loopback implementation. That
transport was retired after live activation testing found that
`http.client`/`http.server`'s framing made it easy to bound only a single
socket syscall with `settimeout()` rather than a whole multi-syscall
read/write -- the same defect class already found and fixed twice in
`aalp/forwarder.py` -- and a live case where AALP's own audit log proved a
response was ready in 6.2s while the client didn't find out for 30+
seconds. A custom, minimal length-prefixed JSON protocol over `AF_UNIX`
removes that whole state machine: every read loop below tracks its own
cumulative deadline explicitly (see `_recv_exact`), rather than trusting a
single `settimeout()` call to bound work that may take multiple `recv()`s.

Wire protocol: each connection carries exactly one request and one
response, then closes (mirroring the previous transport's HTTP/1.0-style
one-shot-per-connection behavior -- AALP's own ingress never set
`protocol_version = "HTTP/1.1"`, so no client here has ever relied on
keep-alive). A request or response is one length-prefixed frame: a 4-byte
big-endian unsigned length, followed by that many bytes of UTF-8-encoded
JSON:

    request:  {"method": str, "path": str, "headers": {str: str}, "body": <base64 str>}
    response: {"status": int, "headers": {str: str}, "body": <base64 str>}

`body` is base64-encoded because it is a passthrough upstream
request/response body and is not guaranteed to be valid UTF-8.

`Ingress` takes a caller-supplied handler callback rather than knowing
about any "Gateway" class -- that composition-root module is built
separately and simply constructs an `Ingress`, passing its own request
handler as this callback. This module knows nothing about it beyond the
callback signature, which is unchanged by this transport swap.
"""
from __future__ import annotations

import base64
import hmac
import json
import os
import secrets
import socket
import stat
import struct
import tempfile
import threading
from pathlib import Path
from typing import Callable

Handler = Callable[[str, str, dict[str, str], bytes], tuple[int, dict[str, str], bytes]]

_SECRET_FILENAME = "ingress.secret"
_DESCRIPTOR_FILENAME = "ingress.json"
_SOCKET_FILENAME = "ingress.sock"

_LENGTH_PREFIX = struct.Struct(">I")


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
    root: str | Path | None, socket_path: Path, secret_path: Path
) -> Path:
    """Publish the bound Unix socket path so ACP can discover it."""
    path = _state_dir(root) / _DESCRIPTOR_FILENAME
    descriptor = {"socket_path": str(socket_path), "secret_file": str(secret_path)}
    _atomic_write(path, json.dumps(descriptor) + "\n")
    return path


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly `n` bytes. The server side never imposes a per-read
    deadline here -- matching the previous HTTP-based ingress, which never
    set a socket timeout either. Budget enforcement is the *client's* job
    (see acp.aalp_client.AalpClient.forward's layered queue/compression/
    total timeouts); this server simply blocks until it has a complete
    frame or the peer goes away."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError(
                "peer closed the connection before sending all expected bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket, max_frame_bytes: int) -> bytes:
    """Read one length-prefixed frame's payload, rejecting an oversized
    frame before reading its body."""
    header = _recv_exact(sock, _LENGTH_PREFIX.size)
    (length,) = _LENGTH_PREFIX.unpack(header)
    if length > max_frame_bytes:
        raise ValueError(f"frame length {length} exceeds max_frame_bytes {max_frame_bytes}")
    return _recv_exact(sock, length)


def write_frame(sock: socket.socket, payload: bytes) -> None:
    """Write one length-prefixed frame."""
    sock.sendall(_LENGTH_PREFIX.pack(len(payload)) + payload)


class Ingress:
    """One loopback Unix-domain-socket listener authenticated by a single
    bearer secret. See the module docstring for the wire protocol."""

    # Frames are capped at a generous multiple of max_request_bytes to
    # allow for base64 inflation (~4/3) plus the JSON envelope's own
    # method/path/headers overhead around the body -- an
    # implementation-private budget, not part of interface v1's own
    # max_request_bytes contract.
    _FRAME_OVERHEAD_MULTIPLIER = 2

    # Closing a listening AF_UNIX socket from another thread does not
    # itself unblock a concurrent accept() on Linux, so the accept loop
    # polls with a short timeout and rechecks _stop_requested -- the same
    # strategy socketserver's own poll_interval-based shutdown() uses --
    # rather than relying on close() alone to interrupt it.
    _ACCEPT_POLL_SECONDS = 0.2

    def __init__(
        self,
        handler: Handler,
        root: str | Path | None = None,
        socket_path: str | Path | None = None,
        max_request_bytes: int = 10 * 1024 * 1024,
        secret: str | None = None,
    ) -> None:
        self.root = root
        self.socket_path = (
            Path(socket_path) if socket_path is not None
            else _state_dir(root) / _SOCKET_FILENAME
        )
        self.max_request_bytes = max_request_bytes
        self.secret = secret if secret is not None else load_or_create_secret(root)
        self.secret_path = _state_dir(root) / _SECRET_FILENAME
        self._handler = handler
        self._thread: threading.Thread | None = None
        self._server_socket: socket.socket | None = None
        self._stop_requested = threading.Event()

    def _max_frame_bytes(self) -> int:
        return self.max_request_bytes * self._FRAME_OVERHEAD_MULTIPLIER

    @staticmethod
    def _encode_response(status: int, headers: dict[str, str] | None, body: bytes) -> bytes:
        envelope = {
            "status": status,
            "headers": dict(headers or {}),
            "body": base64.b64encode(body or b"").decode("ascii"),
        }
        return json.dumps(envelope).encode("utf-8")

    def _handle_connection(self, connection: socket.socket) -> None:
        with connection:
            try:
                try:
                    payload = read_frame(connection, self._max_frame_bytes())
                except ValueError:
                    write_frame(
                        connection,
                        self._encode_response(413, {}, b"request body too large"))
                    return

                try:
                    envelope = json.loads(payload.decode("utf-8"))
                    method = envelope["method"]
                    path = envelope["path"]
                    headers = envelope["headers"]
                    raw_body = envelope.get("body") or ""
                    body = base64.b64decode(raw_body) if raw_body else b""
                except Exception:
                    write_frame(
                        connection,
                        self._encode_response(400, {}, b"malformed request envelope"))
                    return

                authorization = headers.get("Authorization")
                token = None
                if authorization and authorization.startswith("Bearer "):
                    token = authorization.removeprefix("Bearer ")
                if not token or not hmac.compare_digest(token, self.secret):
                    write_frame(connection, self._encode_response(401, {}, b"unauthorized"))
                    return

                try:
                    status, response_headers, response_body = self._handler(
                        method, path, headers, body)
                except Exception:
                    write_frame(
                        connection, self._encode_response(500, {}, b"internal error"))
                    return
                write_frame(
                    connection,
                    self._encode_response(status, response_headers, response_body))
            except (ConnectionError, OSError):
                pass  # peer went away mid-request/response; nothing more to do

    def _serve_forever(self) -> None:
        while not self._stop_requested.is_set():
            try:
                connection, _ = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            thread = threading.Thread(
                target=self._handle_connection, args=(connection,), daemon=True)
            thread.start()

    def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.socket_path.parent, 0o700)
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass

        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_socket.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        server_socket.listen(128)
        server_socket.settimeout(self._ACCEPT_POLL_SECONDS)
        self._server_socket = server_socket

        self._thread = threading.Thread(target=self._serve_forever, daemon=True)
        self._thread.start()
        write_ingress_descriptor(self.root, self.socket_path, self.secret_path)

    def stop(self) -> None:
        self._stop_requested.set()
        if self._thread is not None:
            self._thread.join()
        if self._server_socket is not None:
            self._server_socket.close()
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
