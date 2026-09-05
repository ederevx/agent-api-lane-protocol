"""Multi-process evidence for aalp.file_lane.FileLane.

This module replaces Lane's in-process condition variable with OS
advisory file locks specifically so admission works across independent
processes -- a single-process/threaded test would not exercise the
thing that actually changed. Every contention test here therefore uses
real `multiprocessing` child processes (fork on Linux: a genuine
separate PID, its own fd table, its own copy of the interpreter), never
threads.
"""
from __future__ import annotations

import multiprocessing
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path

from aalp.file_lane import FileLane, FileLaneTimeout, lock_path, state_dir

_CTX = multiprocessing.get_context("fork")


def _acquire_hold_release(directory, provider_id, capacity, timeout_seconds,
                           hold_seconds, counter, max_seen, counter_lock,
                           result_queue):
    """Acquire, bump a shared "currently held" counter, hold briefly,
    then release. Used to prove at-most-`capacity` concurrent holders."""
    lane = FileLane(provider_id, capacity, directory=Path(directory))
    try:
        lease = lane.acquire(timeout_seconds)
    except FileLaneTimeout:
        result_queue.put(("timeout", None))
        return
    with counter_lock:
        counter.value += 1
        if counter.value > max_seen.value:
            max_seen.value = counter.value
    time.sleep(hold_seconds)
    with counter_lock:
        counter.value -= 1
    lease.release()
    result_queue.put(("acquired", lease.slot))


def _acquire_signal_and_hang(directory, provider_id, capacity, acquired_event,
                              hang_seconds):
    """Acquire and then wedge (never release, never crash on its own) --
    simulates a live-but-stuck holder until the test SIGKILLs it."""
    lane = FileLane(provider_id, capacity, directory=Path(directory))
    lease = lane.acquire(timeout_seconds=5)
    acquired_event.set()
    time.sleep(hang_seconds)  # only reached if never killed
    lease.release()


def _acquire_and_report_time(directory, provider_id, capacity, timeout_seconds,
                              result_queue):
    lane = FileLane(provider_id, capacity, directory=Path(directory))
    try:
        lane.acquire(timeout_seconds)
    except FileLaneTimeout:
        result_queue.put(("timeout", time.time()))
        return
    result_queue.put(("acquired", time.time()))


def _acquire_and_hang_forever(directory, provider_id, capacity, acquired_event):
    """Acquire and hang until killed -- never releases on its own,
    unlike `_acquire_signal_and_hang`'s bounded sleep."""
    lane = FileLane(provider_id, capacity, directory=Path(directory))
    # Keep the lease referenced: FileLease holds the FileLock, and the
    # FileLock holds the open fd that the flock actually lives on --
    # letting the lease get garbage-collected would close that fd and
    # release the lock immediately, before this process ever hangs.
    lease = lane.acquire(timeout_seconds=5)  # noqa: F841
    acquired_event.set()
    while True:
        time.sleep(1.0)


def _acquire_do_work_and_release(directory, provider_id, capacity, acquired_event,
                                  counter, iterations, sleep_seconds, done_event):
    """Acquire a slot, then do visible ongoing "work" (incrementing a
    shared counter in a loop with short sleeps) before releasing. Used
    to prove FileLane.status()'s probing does not disturb a real
    holder."""
    lane = FileLane(provider_id, capacity, directory=Path(directory))
    lease = lane.acquire(timeout_seconds=5)
    acquired_event.set()
    for _ in range(iterations):
        with counter.get_lock():
            counter.value += 1
        time.sleep(sleep_seconds)
    lease.release()
    done_event.set()


class FileLaneMutualExclusionTest(unittest.TestCase):
    def test_two_processes_capacity_one_never_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            counter = _CTX.Value("i", 0)
            max_seen = _CTX.Value("i", 0)
            counter_lock = _CTX.Lock()
            results = _CTX.Queue()
            procs = [
                _CTX.Process(
                    target=_acquire_hold_release,
                    args=(tmp, "ci", 1, 5.0, 0.4, counter, max_seen,
                          counter_lock, results),
                )
                for _ in range(2)
            ]
            for p in procs:
                p.start()
            for p in procs:
                p.join(timeout=10)
                self.assertFalse(p.is_alive())

            outcomes = [results.get(timeout=1) for _ in procs]
            self.assertEqual([o[0] for o in outcomes], ["acquired", "acquired"])
            # The whole point of an exclusive lock: with capacity=1 and
            # two holders whose hold windows were made to overlap
            # (0.4s each), the shared counter must never have shown
            # more than 1 concurrently-held slot.
            self.assertEqual(max_seen.value, 1)

    def test_n_slots_admit_n_concurrently_and_no_more(self) -> None:
        capacity = 2
        worker_count = 6
        with tempfile.TemporaryDirectory() as tmp:
            counter = _CTX.Value("i", 0)
            max_seen = _CTX.Value("i", 0)
            counter_lock = _CTX.Lock()
            results = _CTX.Queue()
            procs = [
                _CTX.Process(
                    target=_acquire_hold_release,
                    args=(tmp, "ci", capacity, 10.0, 0.4, counter, max_seen,
                          counter_lock, results),
                )
                for _ in range(worker_count)
            ]
            for p in procs:
                p.start()
            for p in procs:
                p.join(timeout=15)
                self.assertFalse(p.is_alive())

            outcomes = [results.get(timeout=1) for _ in procs]
            self.assertEqual([o[0] for o in outcomes], ["acquired"] * worker_count)
            # Never more than `capacity` concurrent holders...
            self.assertLessEqual(max_seen.value, capacity)
            # ...but genuine concurrency did happen, not accidental
            # full serialization.
            self.assertEqual(max_seen.value, capacity)


class FileLaneCrashRecoveryTest(unittest.TestCase):
    def test_sigkilled_holder_releases_immediately_for_a_waiter(self) -> None:
        # This is the property the whole design rests on: OS-level lock
        # release on process death, not a TTL, is what lets a waiter
        # back in. Prove it directly rather than assuming it.
        with tempfile.TemporaryDirectory() as tmp:
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_signal_and_hang,
                args=(tmp, "ci", 1, acquired_event, 30.0),
            )
            holder.start()
            self.assertTrue(acquired_event.wait(timeout=5),
                             "holder never signalled that it acquired")

            results = _CTX.Queue()
            waiter = _CTX.Process(
                target=_acquire_and_report_time,
                args=(tmp, "ci", 1, 5.0, results),
            )
            waiter.start()
            # Give the waiter a moment to be actively polling (blocked
            # behind the still-alive, still-holding holder) before we
            # kill the holder out from under it.
            time.sleep(0.3)
            self.assertTrue(holder.is_alive(), "holder exited before being killed")

            kill_time = time.time()
            os.kill(holder.pid, signal.SIGKILL)
            holder.join(timeout=5)
            self.assertFalse(holder.is_alive())
            # The holder never reached its own release() call -- it was
            # killed while sleeping in the "hang" branch -- so this is
            # genuinely exercising OS-level reclaim, not a normal
            # release racing with the kill.

            outcome, acquired_wall_time = results.get(timeout=6)
            waiter.join(timeout=6)

            self.assertEqual(outcome, "acquired")
            elapsed_after_kill = acquired_wall_time - kill_time
            # Near-instant in practice (one poll interval); generous
            # bound here to stay non-flaky while still proving the
            # waiter did not sit out anything resembling the old
            # lease-based TTL (which would have been ~30s) or its own
            # full 5s acquire timeout.
            self.assertLess(elapsed_after_kill, 2.0)
            self.assertGreaterEqual(elapsed_after_kill, 0.0)


class FileLaneTimeoutTest(unittest.TestCase):
    def test_acquire_times_out_cleanly_without_hanging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_signal_and_hang,
                args=(tmp, "ci", 1, acquired_event, 5.0),
            )
            holder.start()
            self.addCleanup(lambda: (holder.terminate(), holder.join(timeout=2)))
            self.assertTrue(acquired_event.wait(timeout=5))

            lane = FileLane("ci", 1, directory=Path(tmp))
            started = time.monotonic()
            with self.assertRaises(FileLaneTimeout):
                lane.acquire(timeout_seconds=0.5)
            elapsed = time.monotonic() - started
            # Must return at (approximately) the requested deadline,
            # never hang past it and never immediately without trying.
            self.assertGreaterEqual(elapsed, 0.5)
            self.assertLess(elapsed, 2.0)

    def test_immediate_timeout_zero_does_not_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane_a = FileLane("ci", 1, directory=Path(tmp))
            lease = lane_a.acquire(timeout_seconds=1)
            try:
                lane_b = FileLane("ci", 1, directory=Path(tmp))
                started = time.monotonic()
                with self.assertRaises(FileLaneTimeout):
                    lane_b.acquire(timeout_seconds=0)
                self.assertLess(time.monotonic() - started, 0.5)
            finally:
                lease.release()


class FileLaneConcurrentFirstRunTest(unittest.TestCase):
    def test_concurrent_first_run_against_missing_state_dir(self) -> None:
        capacity = 3
        worker_count = 9
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fresh-root"
            # Deliberately do NOT create root or .aalp/state -- both
            # racing processes' FileLane.acquire() must create it.
            self.assertFalse(root.exists())

            counter = _CTX.Value("i", 0)
            max_seen = _CTX.Value("i", 0)
            counter_lock = _CTX.Lock()
            results = _CTX.Queue()

            def worker(root_str, result_q):
                lane = FileLane("ci", capacity, root=Path(root_str))
                try:
                    lease = lane.acquire(timeout_seconds=10)
                except Exception as exc:  # noqa: BLE001 -- want to see anything
                    result_q.put(("error", repr(exc)))
                    return
                with counter_lock:
                    counter.value += 1
                    if counter.value > max_seen.value:
                        max_seen.value = counter.value
                time.sleep(0.2)
                with counter_lock:
                    counter.value -= 1
                lease.release()
                result_q.put(("acquired", lease.slot))

            procs = [
                _CTX.Process(target=worker, args=(str(root), results))
                for _ in range(worker_count)
            ]
            for p in procs:
                p.start()
            for p in procs:
                p.join(timeout=15)
                self.assertFalse(p.is_alive())

            outcomes = [results.get(timeout=1) for _ in procs]
            self.assertEqual([o[0] for o in outcomes], ["acquired"] * worker_count,
                              f"unexpected outcomes: {outcomes}")
            self.assertLessEqual(max_seen.value, capacity)
            expected_dir = state_dir(root)
            self.assertTrue(expected_dir.is_dir())
            for slot in range(capacity):
                self.assertTrue(lock_path(expected_dir, "ci", slot).exists())


class FileLaneConstructionAndLeaseTest(unittest.TestCase):
    def test_rejects_non_positive_capacity(self) -> None:
        with self.assertRaises(ValueError):
            FileLane("ci", 0)

    def test_rejects_empty_provider_id(self) -> None:
        with self.assertRaises(ValueError):
            FileLane("", 1)

    def test_rejects_negative_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 1, directory=Path(tmp))
            with self.assertRaises(ValueError):
                lane.acquire(timeout_seconds=-1)

    def test_lock_filenames_follow_lane_provider_slot_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 3, directory=Path(tmp))
            names = sorted(p.name for p in lane._lock_paths)
            self.assertEqual(
                names, ["lane.ci.0.lock", "lane.ci.1.lock", "lane.ci.2.lock"])

    def test_release_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 1, directory=Path(tmp))
            lease = lane.acquire(timeout_seconds=1)
            lease.release()
            lease.release()  # must not raise

    def test_context_manager_releases_on_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 1, directory=Path(tmp))
            with lane.acquire(timeout_seconds=1):
                other = FileLane("ci", 1, directory=Path(tmp))
                with self.assertRaises(FileLaneTimeout):
                    other.acquire(timeout_seconds=0)
            # Released on __exit__: a fresh acquire must now succeed
            # immediately.
            other = FileLane("ci", 1, directory=Path(tmp))
            other.acquire(timeout_seconds=0).release()


class FileLaneStatusTest(unittest.TestCase):
    def _poll_until(self, predicate, timeout=5.0, interval=0.05):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = predicate()
            if last:
                return last
            time.sleep(interval)
        self.fail(f"condition never became true; last value: {last!r}")

    def test_fresh_lane_reports_zero_in_flight_and_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 3, directory=Path(tmp))
            status = lane.status()
            self.assertEqual(status, {"in_flight": 0, "idle": True})

    def test_partial_occupancy_reports_correct_in_flight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 3, directory=Path(tmp))
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_signal_and_hang,
                args=(tmp, "ci", 3, acquired_event, 30.0),
            )
            holder.start()
            self.addCleanup(lambda: (holder.terminate(), holder.join(timeout=2)))
            self.assertTrue(acquired_event.wait(timeout=5))

            status = lane.status()
            self.assertEqual(status["in_flight"], 1)
            self.assertFalse(status["idle"])

            os.kill(holder.pid, signal.SIGKILL)
            holder.join(timeout=5)
            self.assertFalse(holder.is_alive())

            self._poll_until(lambda: lane.status()["in_flight"] == 0)

    def test_full_occupancy_reports_capacity_in_flight(self) -> None:
        capacity = 3
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", capacity, directory=Path(tmp))
            events = [_CTX.Event() for _ in range(capacity)]
            holders = [
                _CTX.Process(
                    target=_acquire_signal_and_hang,
                    args=(tmp, "ci", capacity, events[i], 30.0),
                )
                for i in range(capacity)
            ]
            for h in holders:
                h.start()
            self.addCleanup(
                lambda: [(h.terminate(), h.join(timeout=2)) for h in holders])
            for e in events:
                self.assertTrue(e.wait(timeout=5))

            status = lane.status()
            self.assertEqual(status["in_flight"], capacity)
            self.assertFalse(status["idle"])

    def test_status_probing_does_not_disturb_a_real_holder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 2, directory=Path(tmp))
            acquired_event = _CTX.Event()
            done_event = _CTX.Event()
            counter = _CTX.Value("i", 0)
            iterations = 20
            sleep_seconds = 0.05  # ~1 second of total "work"

            holder = _CTX.Process(
                target=_acquire_do_work_and_release,
                args=(tmp, "ci", 2, acquired_event, counter, iterations,
                      sleep_seconds, done_event),
            )
            holder.start()
            self.addCleanup(lambda: (holder.terminate(), holder.join(timeout=2)))
            self.assertTrue(acquired_event.wait(timeout=5))

            deadline = time.time() + (iterations * sleep_seconds) + 1.0
            status_count = 0
            while not done_event.is_set() and time.time() < deadline:
                status = lane.status()
                self.assertGreaterEqual(status["in_flight"], 1)
                status_count += 1
                time.sleep(0.01)
            self.assertGreater(status_count, 0, "status loop never actually ran")

            holder.join(timeout=5)
            self.assertFalse(holder.is_alive())
            self.assertEqual(counter.value, iterations)

    def test_in_flight_drops_after_sigkill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lane = FileLane("ci", 1, directory=Path(tmp))
            acquired_event = _CTX.Event()
            holder = _CTX.Process(
                target=_acquire_and_hang_forever,
                args=(tmp, "ci", 1, acquired_event),
            )
            holder.start()
            self.addCleanup(lambda: (holder.terminate(), holder.join(timeout=2)))
            self.assertTrue(acquired_event.wait(timeout=5))

            self.assertEqual(lane.status()["in_flight"], 1)

            os.kill(holder.pid, signal.SIGKILL)
            holder.join(timeout=5)
            self.assertFalse(holder.is_alive())

            status = self._poll_until(
                lambda: (lambda s: s if s["in_flight"] == 0 else None)(lane.status()))
            self.assertTrue(status["idle"])


if __name__ == "__main__":
    unittest.main()
