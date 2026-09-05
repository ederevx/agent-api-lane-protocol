"""Minimal, dependency-free non-blocking exclusive file lock: `fcntl.flock`
on POSIX, `msvcrt.locking` on Windows, behind one interface.

Why this exists instead of the `filelock` PyPI package: it was tried
first, but this host's system Python is PEP 668 "externally managed",
so `pip install` doesn't work without extra ceremony -- and the entire
point of removing `aalp.service` is "clone the repo and run" on a
fresh machine (Windows included) with no supervised service and no
package-manager prerequisite. A third-party dependency reintroduces
exactly the install step this migration exists to remove, so this repo
declares no dependency manifest at all and this shim is stdlib-only.

Deliberately duplicated, not shared, with `acp/filelock_compat.py` in
the sibling `agent-compression-protocol` repo: AALP and ACP must stay
independently deployable, and extracting ~40 lines into a shared
package would buy nothing but a coupling neither protocol otherwise
needs. If you're reading this wondering whether to deduplicate the two
copies: don't -- it's deliberate, not an oversight.

POSIX and Windows do not offer the same primitive, so both paths are
implemented explicitly rather than papered over:

* POSIX `fcntl.flock(fd, LOCK_EX | LOCK_NB)` locks the *whole file*,
  advisory (a process that never calls `flock` on the same file isn't
  stopped by it), and is released by `LOCK_UN` or simply by every fd
  referencing it closing -- including on process death, which is
  exactly what gives crash-release for free with zero cleanup code.
* Windows `msvcrt.locking(fd, mode, nbytes)` locks a *byte range*
  starting at the file's current seek position, and it is mandatory,
  not advisory. Because the range is relative to the current position,
  this module always seeks to 0 and locks exactly 1 byte -- the same
  byte, every time, in acquire, release, and any retry. Skipping that
  seek (or locking a different byte count) would let two callers each
  lock a different range of the same file and both "succeed" at once,
  which would silently defeat the whole point of this module.

UNVERIFIED: no Windows machine was available in the session that wrote
this, so the `msvcrt.locking` branch below is implemented from
documented behavior only and has never actually been run on Windows.
Everything else in this module (the POSIX branch, and the
platform-independent surface both branches sit behind) was exercised
directly, including multi-process and SIGKILL tests.

Both platforms raise `OSError` on a failed non-blocking attempt, with
different errnos -- callers of this module never need to know either:
a failed `acquire()` here always raises `LockBusy`, on both platforms.
"""
from __future__ import annotations

import os
from pathlib import Path

# Selected once, at import time, per module docstring/design -- never
# re-checked per call.
_IS_WINDOWS = os.name == "nt"

if _IS_WINDOWS:
    import msvcrt
else:
    import fcntl

# The single byte this module locks on Windows, always at offset 0.
# Irrelevant on POSIX, where flock locks the whole file regardless.
_WINDOWS_LOCK_NBYTES = 1


class LockBusy(OSError):
    """Raised when a single non-blocking acquire attempt found the lock
    already held by someone else (this process or another).

    Not a timeout in the temporal sense -- this module never waits;
    retry/backoff/deadline policy belongs one layer up (see
    `file_lane.py`), so there is exactly one bounded wait in that
    admission path, not two nested ones.
    """


class FileLock:
    """One non-blocking exclusive lock on `path`.

    Usage is deliberately structural, via a context manager, so
    release happens even if the caller forgets or raises, rather than
    depending on every call site remembering an explicit `release()`::

        with FileLock(path).acquire():
            ...  # do work while holding the lock

    `acquire()` itself never blocks and raises `LockBusy` immediately
    if the lock is already held; only entering the `with` after a
    successful `acquire()` should be assumed to mean the lock is held.

    The underlying file handle is kept open for the lock's entire
    lifetime. On both platforms, closing that handle is what actually
    releases the lock -- explicitly, via `release()`, or implicitly,
    via the OS reclaiming every fd a killed process held -- so this
    class never closes it early, and callers must not either.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fh = None

    @property
    def is_locked(self) -> bool:
        return self._fh is not None

    def acquire(self) -> "FileLock":
        """Try exactly once, non-blocking.

        Raises `LockBusy` if some other holder already has it (nothing
        is left open or held in that case). Returns self on success,
        so this reads naturally as `with FileLock(path).acquire():`.
        Acquiring an already-held-by-self lock is a no-op success (not
        reentrant counting -- a second call before release() just
        confirms the same lock is still held).
        """
        if self.is_locked:
            return self
        # O_CREAT, deliberately never O_CREAT | O_EXCL: this file's
        # mere existence carries no meaning, only its lock state does,
        # so two processes racing to create it on a first run both
        # succeed here -- whichever then wins the actual lock below is
        # the only thing that matters.
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        fh = os.fdopen(fd, "r+b")
        try:
            if _IS_WINDOWS:
                fh.seek(0)
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, _WINDOWS_LOCK_NBYTES)
                except OSError as exc:
                    raise LockBusy(str(self.path)) from exc
            else:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise LockBusy(str(self.path)) from exc
        except LockBusy:
            fh.close()
            raise
        self._fh = fh
        return self

    def release(self) -> None:
        """Idempotent: releasing an already-unlocked FileLock is a
        no-op, matching `aalp.lane.Lane.release()`'s spirit of being
        safe to call defensively."""
        if not self.is_locked:
            return
        fh = self._fh
        self._fh = None
        try:
            if _IS_WINDOWS:
                fh.seek(0)
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, _WINDOWS_LOCK_NBYTES)
                except OSError:
                    pass
            else:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            # Closing releases the lock on both platforms even if the
            # explicit unlock above somehow failed -- belt and
            # suspenders around the same crash-release property this
            # module leans on elsewhere.
            fh.close()

    def __enter__(self) -> "FileLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
