import threading
import time
import unittest

from aalp.flow import FlowAdmission, LaneTimeout


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


class FlowAdmissionOrderingTest(unittest.TestCase):
    def test_later_flow_blocked_while_one_is_active(self) -> None:
        flows = FlowAdmission(lease_seconds=10)
        flows.admit("100", timeout_seconds=1)

        with self.assertRaises(LaneTimeout):
            flows.admit("101", timeout_seconds=0.05)

    def test_admitted_immediately_after_close(self) -> None:
        flows = FlowAdmission(lease_seconds=10)
        token = flows.admit("100", timeout_seconds=1)
        flows.close("100", token)

        # No TTL wait required: close() frees the slot right away.
        token_101 = flows.admit("101", timeout_seconds=0)
        self.assertTrue(token_101)

    def test_second_flow_admitted_only_after_first_closes(self) -> None:
        flows = FlowAdmission(lease_seconds=10)
        token_100 = flows.admit("100", timeout_seconds=1)

        result = {}

        def waiter() -> None:
            result["token"] = flows.admit("101", timeout_seconds=2)

        thread = threading.Thread(target=waiter)
        thread.start()
        _wait_until(lambda: flows.status()["queued"] == 1)

        flows.close("100", token_100)
        thread.join(timeout=2)

        self.assertIn("token", result)


class FlowAdmissionExpiryTest(unittest.TestCase):
    def test_ttl_expiry_is_a_crash_safety_fallback(self) -> None:
        clock = FakeClock()
        flows = FlowAdmission(lease_seconds=5, clock=clock.now)
        flows.admit("100", timeout_seconds=1)

        # Flow 100 vanishes without calling close() (e.g. it crashed).
        clock.advance(5.1)
        token_101 = flows.admit("101", timeout_seconds=0)
        self.assertTrue(token_101)

    def test_renew_keeps_flow_alive_across_its_own_requests(self) -> None:
        clock = FakeClock()
        flows = FlowAdmission(lease_seconds=5, clock=clock.now)
        token = flows.admit("100", timeout_seconds=1)

        clock.advance(4.0)
        renewed = flows.renew("100", token)
        self.assertEqual(renewed, token)

        clock.advance(4.0)
        # Elapsed since admit() is 8.0s (> lease_seconds), but renew()
        # reset the TTL at t=4.0, so 100 must still hold the slot.
        with self.assertRaises(LaneTimeout):
            flows.admit("101", timeout_seconds=0)

    def test_renew_by_a_different_flow_is_rejected(self) -> None:
        flows = FlowAdmission(lease_seconds=5)
        token = flows.admit("100", timeout_seconds=1)
        with self.assertRaises(ValueError):
            flows.renew("101", token)

    def test_renew_after_expiry_raises_rather_than_silently_succeeding(self) -> None:
        clock = FakeClock()
        flows = FlowAdmission(lease_seconds=5, clock=clock.now)
        token = flows.admit("100", timeout_seconds=1)
        clock.advance(5.1)
        with self.assertRaises(ValueError):
            flows.renew("100", token)


if __name__ == "__main__":
    unittest.main()
