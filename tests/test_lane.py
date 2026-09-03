import threading
import time
import unittest

from aalp.lane import Lane, LaneTimeout


class FakeClock:
    """A controllable clock so lease-expiry tests never real-sleep."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not met before timeout")


class LaneFifoOrderTest(unittest.TestCase):
    def test_fifo_order_under_concurrent_threads(self) -> None:
        lane = Lane(capacity=1, lease_seconds=10)
        token_a = lane.acquire("A", timeout_seconds=1)

        order: list[str] = []
        order_lock = threading.Lock()

        def waiter(holder: str) -> None:
            token = lane.acquire(holder, timeout_seconds=5)
            with order_lock:
                order.append(holder)
            lane.release(holder, token)

        thread_b = threading.Thread(target=waiter, args=("B",))
        thread_b.start()
        _wait_until(lambda: len(lane.waiters) == 1)

        thread_c = threading.Thread(target=waiter, args=("C",))
        thread_c.start()
        _wait_until(lambda: len(lane.waiters) == 2)

        lane.release("A", token_a)
        thread_b.join(timeout=2)
        thread_c.join(timeout=2)

        self.assertEqual(order, ["B", "C"])

    def test_timeout_does_not_disturb_other_waiters(self) -> None:
        lane = Lane(capacity=1, lease_seconds=10)
        token_a = lane.acquire("A", timeout_seconds=1)

        with self.assertRaises(LaneTimeout):
            lane.acquire("B", timeout_seconds=0.05)

        # B's ticket must be cleaned up, not left blocking future waiters.
        self.assertEqual(lane.waiters, [])

        result = {}

        def waiter() -> None:
            result["token"] = lane.acquire("C", timeout_seconds=2)

        thread_c = threading.Thread(target=waiter)
        thread_c.start()
        _wait_until(lambda: len(lane.waiters) == 1)
        lane.release("A", token_a)
        thread_c.join(timeout=2)

        self.assertIn("token", result)


class LaneExpiryTest(unittest.TestCase):
    def test_expired_lease_reclaimed_without_explicit_release(self) -> None:
        clock = FakeClock()
        lane = Lane(capacity=1, lease_seconds=5, clock=clock.now)
        lane.acquire("A", timeout_seconds=1)

        clock.advance(5.1)
        # A never released; the lease's TTL has lapsed, so a fresh
        # acquire must succeed immediately rather than block/timeout.
        token_b = lane.acquire("B", timeout_seconds=0)
        self.assertTrue(token_b)

    def test_reentrant_acquire_renews_instead_of_requeueing(self) -> None:
        clock = FakeClock()
        lane = Lane(capacity=1, lease_seconds=5, clock=clock.now)
        token = lane.acquire("A", timeout_seconds=1)

        clock.advance(4.0)
        renewed = lane.acquire("A", timeout_seconds=1, token=token)
        self.assertEqual(renewed, token)

        # Renewal reset the TTL clock, so it should not be reclaimed yet
        # even though 4.0 + 4.0 > the original 5-second lease_seconds.
        clock.advance(4.0)
        with self.assertRaises(LaneTimeout):
            lane.acquire("B", timeout_seconds=0)

    def test_reentrant_acquire_rejects_wrong_holder(self) -> None:
        lane = Lane(capacity=1, lease_seconds=5)
        token = lane.acquire("A", timeout_seconds=1)
        with self.assertRaises(ValueError):
            lane.acquire("B", timeout_seconds=1, token=token)


class LaneReleaseHeartbeatTest(unittest.TestCase):
    def test_release_by_non_holder_is_a_no_op(self) -> None:
        lane = Lane(capacity=1, lease_seconds=5)
        token = lane.acquire("A", timeout_seconds=1)
        self.assertFalse(lane.release("B", token))
        # Still held by A: a second acquire must time out immediately.
        with self.assertRaises(LaneTimeout):
            lane.acquire("C", timeout_seconds=0)

    def test_heartbeat_by_non_holder_is_a_no_op(self) -> None:
        lane = Lane(capacity=1, lease_seconds=5)
        token = lane.acquire("A", timeout_seconds=1)
        self.assertFalse(lane.heartbeat("B", token))

    def test_heartbeat_extends_expiry(self) -> None:
        clock = FakeClock()
        lane = Lane(capacity=1, lease_seconds=5, clock=clock.now)
        token = lane.acquire("A", timeout_seconds=1)
        clock.advance(4.0)
        self.assertTrue(lane.heartbeat("A", token))
        clock.advance(4.0)
        # 8.0 total elapsed but heartbeat reset the TTL at t=4.0, so the
        # lease is still valid.
        with self.assertRaises(LaneTimeout):
            lane.acquire("B", timeout_seconds=0)


class LaneConstructionTest(unittest.TestCase):
    def test_rejects_non_positive_capacity(self) -> None:
        with self.assertRaises(ValueError):
            Lane(capacity=0, lease_seconds=5)

    def test_rejects_non_positive_lease_seconds(self) -> None:
        with self.assertRaises(ValueError):
            Lane(capacity=1, lease_seconds=0)

    def test_status_reports_no_holder_identity(self) -> None:
        lane = Lane(capacity=2, lease_seconds=5)
        lane.acquire("A", timeout_seconds=1)
        status = lane.status()
        self.assertEqual(status["capacity"], 2)
        self.assertEqual(status["leased"], 1)
        self.assertEqual(status["queued"], 0)
        self.assertNotIn("owners", status)
        self.assertNotIn("holders", status)


if __name__ == "__main__":
    unittest.main()
