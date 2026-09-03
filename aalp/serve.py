"""Minimal standalone AALP server entrypoint.

Every other module in this package is exercised only in-process, by its
own test suite or by an embedding caller that builds a `Gateway` and
drives it directly. Nothing wires a `Gateway` to a real, listening
`Ingress` outside of tests — so, until this module, AALP could not
actually be run as a standalone process for a genuine out-of-process
client (interface/v1's real, intended consumer) to talk to over a real
socket. This module is that wiring, kept deliberately small: it owns no
policy of its own, it only constructs `Gateway` from `providers/` +
`AALP_HOME`/root, starts `Ingress` on `Gateway.as_ingress_handler()`, and
blocks until interrupted.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from pathlib import Path

from .gateway import Gateway
from .ingress import Ingress


def _default_providers_dir() -> Path:
    configured = os.environ.get("AALP_PROVIDERS_DIR")
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent.parent / "providers"


def _default_root() -> str | None:
    return os.environ.get("AALP_HOME")


def build_ingress(
    providers_dir: Path | None = None,
    root: str | Path | None = None,
    socket_path: str | Path | None = None,
) -> Ingress:
    """Construct a `Gateway` and the `Ingress` that serves it.

    Returned unstarted; the caller decides when to `start()`/`stop()`.
    """
    gateway = Gateway(
        providers_dir=providers_dir or _default_providers_dir(),
        root=root if root is not None else _default_root(),
    )
    return Ingress(
        gateway.as_ingress_handler(),
        root=root if root is not None else _default_root(),
        socket_path=socket_path,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m aalp", description=__doc__)
    parser.add_argument(
        "--providers-dir", type=Path, default=None,
        help="Directory of provider definitions (default: AALP_PROVIDERS_DIR "
             "env var, else the repository's providers/ directory).")
    parser.add_argument(
        "--root", type=str, default=None,
        help="AALP state root, i.e. where .aalp/ lives (default: AALP_HOME "
             "env var, else the current working directory).")
    parser.add_argument(
        "--socket-path", type=str, default=None,
        help="Unix socket path to bind (default: <root>/.aalp/state/"
             "ingress.sock, published via .aalp/state/ingress.json for "
             "clients to discover).")
    args = parser.parse_args(argv)

    ingress = build_ingress(
        providers_dir=args.providers_dir,
        root=args.root,
        socket_path=args.socket_path,
    )
    ingress.start()
    print(f"aalp: listening on {ingress.socket_path}", file=sys.stderr)

    stop_event = threading.Event()

    def _handle_signal(signum: int, frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    stop_event.wait()
    ingress.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
