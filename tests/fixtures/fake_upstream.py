"""A loopback HTTP server standing in for CheapestInference in tests.

Lets tests exercise aalp.forwarder.forward()/build_connection() against a
real socket without ever reaching the actual upstream: the canned
response is configurable and every received request is recorded so a
test can assert exactly what went out on the wire (in particular, the
injected Authorization header and the absence of any leaked inbound one).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass
class RecordedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass
class _CannedResponse:
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        self.server.fake_upstream.last_request = RecordedRequest(
            method=self.command,
            path=self.path,
            headers=dict(self.headers.items()),
            body=body,
        )
        canned = self.server.fake_upstream.canned_response
        self.send_response(canned.status)
        for name, value in canned.headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(canned.body)))
        self.end_headers()
        if canned.body:
            self.wfile.write(canned.body)

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_DELETE = _handle
    do_PATCH = _handle

    def log_message(self, format: str, *args: object) -> None:
        pass


class FakeUpstream:
    """Ephemeral-port loopback server usable as a context manager."""

    def __init__(self) -> None:
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.canned_response = _CannedResponse()
        self.last_request: RecordedRequest | None = None

    def set_response(
        self,
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> None:
        self.canned_response = _CannedResponse(
            status=status, headers=headers or {}, body=body)

    def start(self) -> "FakeUpstream":
        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        server.fake_upstream = self
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever,
                                         daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "FakeUpstream":
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
