"""Direct coverage of aalp.filelock_compat.FileLock -- the stdlib-only
POSIX/Windows lock shim `file_lane.py` is built on.

This is now our own ~40 lines rather than a third-party library, so
its own crash-release and mutual-exclusion properties get the same
real-multi-process scrutiny as file_lane.py's, one layer down: these
tests exercise the shim directly, with no retry/backoff/deadline logic
from FileLane in between, per team review after the filelock -> stdlib
shim swap ("we are the CI now").

Only the POSIX (`fcntl.flock`) branch is exercised here -- no Windows
machine is available in this session. The Windows (`msvcrt.locking`)
branch remains UNVERIFIED; see filelock_compat.py's module docstring.
"""
from __future__ import annotations

import multiprocessing
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path

from aalp.filelock_compat import FileLock, LockBusy

_CTX = multiprocessing.get_context("fork")


def _acquire_signal_and_hang(path, acquired_event, hang_seconds):
    lock = FileLock(path)
    lock.acquire()
    acquired_event.set()
    time.sleep(hang_seconds)  # only reached if never killed
    lock.release()


def _try_acquire_once(path, result_queue):
    lock = FileLock(path)
    try:
        lock.acquire()
    except LockBusy:
        result_queue.put(("busy", None))
        return
    result_queue.put(("acquired", time.time()))
    lock.release()


class FileLockMutualExclusionTest(unittest.TestCase):
    def test_second_acquire_fails_while_first_is_held_same_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            first = FileLock(path)
            first.acquire()
            try:
                second = FileLock(path)
                with self.assertRaises(LockBusy):
                    second.acquire()
                self.assertFalse(second.is_locked)
            finally:
                first.release()

    def test_release_actually_frees_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            first = FileLock(path)
            first.acquire()
            second = FileLock(path)
            with self.assertRaises(LockBusy):
                second.acquire()

            first.release()
            self.assertFalse(first.is_locked)

            # Now genuinely free: a fresh attempt must succeed.
            third = FileLock(path)
            third.acquire()
            self.assertTrue(third.is_locked)
            third.release()

    def test_second_acquire_fails_while_held_by_another_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_signal_and_hang, args=(str(path), acquired_event, 5.0))
            holder.start()
            self.addCleanup(lambda: (holder.terminate(), holder.join(timeout=2)))
            self.assertTrue(acquired_event.wait(timeout=5))

            results = _CTX.Queue()
            prober = _CTX.Process(target=_try_acquire_once, args=(str(path), results))
            prober.start()
            prober.join(timeout=5)
            self.assertFalse(prober.is_alive())
            outcome, _ = results.get(timeout=1)
            self.assertEqual(outcome, "busy")

    def test_double_release_is_a_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            lock = FileLock(path)
            lock.acquire()
            lock.release()
            lock.release()  # must not raise
            self.assertFalse(lock.is_locked)

    def test_context_manager_releases_on_normal_exit_and_on_exception(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            with FileLock(path).acquire() as lock:
                self.assertTrue(lock.is_locked)
            # Released -- a fresh acquire must succeed immediately.
            FileLock(path).acquire().release()

            with self.assertRaises(RuntimeError):
                with FileLock(path).acquire():
                    raise RuntimeError("boom")
            # Still released even though the body raised.
            FileLock(path).acquire().release()

    def test_creates_lock_file_that_does_not_exist_yet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "brand-new.lock"
            self.assertFalse(path.exists())
            lock = FileLock(path)
            lock.acquire()
            self.assertTrue(path.exists())
            lock.release()

    def test_reacquiring_already_held_by_self_is_a_no_op_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            lock = FileLock(path)
            lock.acquire()
            lock.acquire()  # must not raise, must not deadlock
            self.assertTrue(lock.is_locked)
            lock.release()


class FileLockCrashRecoveryTest(unittest.TestCase):
    def test_sigkilled_holder_releases_lock_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.lock"
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_signal_and_hang, args=(str(path), acquired_event, 30.0))
            holder.start()
            self.assertTrue(acquired_event.wait(timeout=5),
                             "holder never signalled that it acquired")

            os.kill(holder.pid, signal.SIGKILL)
            holder.join(timeout=5)
            self.assertFalse(holder.is_alive())
            # Holder was killed while sleeping in the "hang" branch, so
            # it never reached its own release() -- this is genuinely
            # exercising OS-level reclaim on process death.

            results = _CTX.Queue()
            prober = _CTX.Process(target=_try_acquire_once, args=(str(path), results))
            prober.start()
            prober.join(timeout=5)
            self.assertFalse(prober.is_alive())
            outcome, _ = results.get(timeout=1)
            # A single non-blocking probe succeeds right away -- no
            # retry loop needed at this layer to prove the release
            # already happened.
            self.assertEqual(outcome, "acquired")


if __name__ == "__main__":
    unittest.main()
