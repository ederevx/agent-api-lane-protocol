"""Stage 4 timeout/failure audit for `Gateway.handle_queue()`
(agent_protocols_v1_queue_coalescing_adjustment_metadata_v1.md §20-§21,
§24-§25).

Mirrors `test_gateway_pipeline.py`'s direct-Gateway, real-thread,
small-real-sleep style -- deliberately not the backend-agnostic
conformance suite, since `FakeAalpV1Service` never implements real
wall-clock deadline semantics at all (its outcomes are only ever
test-programmed, never derived from elapsed time). These tests exist
specifically to exercise the queue path's own timeout-phase separation
and failure fan-out against the real `Gateway`, extending
`test_gateway_pipeline.py`'s existing `QueueTimeoutTest`/
`TotalTimeoutTest`/`BoundedConcurrencyTest` coverage (all `handle()`-only)
to `handle_queue()`'s leader/joiner split, which no earlier stage's suite
covers under real elapsed time.

Small helper duplication from `test_gateway_pipeline.py`
(`FakeConnection`/`FakeResponse`/`_write_provider`/`_wait_until`/
`_TempGatewayCase`) is deliberate, matching this test suite's existing
convention (`_CannedResponse` is likewise duplicated between
`test_interface_conformance.py` and `test_gateway_pipeline.py`) rather
than a relative import -- `tests/` has no `__init__.py`, so it is not a
package `unittest discover` can import across.
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from aalp.credential import write_credential
from aalp.errors import Outcome
from aalp.gateway import Gateway
from aalp.queue import QueueGenerationState


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not met before timeout")


def _write_provider(providers_dir: Path, provider_id: str, **overrides) -> None:
    data = {
        "id": provider_id,
        "display_name": f"Test Provider {provider_id}",
        "endpoint": "https://example.invalid/api",
        "concurrency_limit": 1,
        "client": "python-http.client",
        "active": True,
        "request_shape": {
            "paths": ["/v1/messages"],
            "passthrough": True,
            "auth_header": "Authorization",
            "auth_scheme": "Bearer",
        },
        "timeout_overrides": {},
    }
    data.update(overrides)
    (providers_dir / f"{provider_id}.json").write_text(
        json.dumps(data), encoding="utf-8")


def _queue_envelope(member_block: str = "member-block") -> bytes:
    return json.dumps({
        "shared": {"content": "__SENTINEL__"},
        "content_path": ["content"],
        "member_block": member_block,
        "member_join": "\n\n",
        "count_template": "ACP-QUEUE-MEMBER-COUNT: {member_count}",
    }).encode("utf-8")


class FakeResponse:
    def __init__(self, status=200, headers=None, body=b""):
        self.status = status
        self._headers = headers or {}
        self._body = body

    def getheaders(self):
        return list(self._headers.items())

    def read(self):
        return self._body


class FakeConnection:
    """Stands in for forwarder's connection seam -- same shape as
    `test_gateway_pipeline.py`'s own, plus an optional `error` to raise
    from `getresponse()` (simulating a real upstream transport failure,
    §20)."""

    def __init__(self, response=None, block_event=None, error=None):
        self.response = response if response is not None else FakeResponse()
        self.block_event = block_event
        self.error = error
        self.requests: list[tuple] = []

    def request(self, method, path, body=None, headers=None):
        self.requests.append((method, path, body, headers))

    def getresponse(self):
        if self.block_event is not None:
            self.block_event.wait()
        if self.error is not None:
            raise self.error
        return self.response

    def close(self):
        pass


class _SequencedConnectionFactory:
    """Returns one connection per call, in order -- lets a test give the
    occupier's `handle()` call and the queue leader's post-admission
    `forward()` call independently controllable fakes, without either
    call needing to know about the other."""

    def __init__(self, connections: list[FakeConnection]) -> None:
        self._connections = list(connections)
        self._index = 0
        self.lock = threading.Lock()

    def __call__(self, provider, timeout):
        with self.lock:
            connection = self._connections[self._index]
            self._index += 1
            return connection


class _TempGatewayCase(unittest.TestCase):
    def setUp(self) -> None:
        self._providers_tmp = tempfile.TemporaryDirectory()
        self._root_tmp = tempfile.TemporaryDirectory()
        self.providers_dir = Path(self._providers_tmp.name)
        self.root = Path(self._root_tmp.name)

    def tearDown(self) -> None:
        self._providers_tmp.cleanup()
        self._root_tmp.cleanup()


class QueueLeaderQueueTimeoutTest(_TempGatewayCase):
    """§21: `queue_timeout` bounds a leader's wait for physical provider
    admission -- independent of `compression_timeout`/`total_timeout`."""

    def test_leader_times_out_waiting_for_admission_while_provider_occupied(
        self,
    ) -> None:
        _write_provider(
            self.providers_dir, "prov", concurrency_limit=1,
            timeout_overrides={"queue_timeout_seconds": 0.05})
        write_credential("prov", "fake-token", root=self.root)

        occupier_block = threading.Event()
        occupier_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"occupier-ok"),
            block_event=occupier_block)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: occupier_conn)

        def occupy() -> None:
            gateway.handle("occupier", "prov", "POST", "/v1/messages", {}, b"{}")

        occupier_thread = threading.Thread(target=occupy)
        occupier_thread.start()
        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 1)

        result, generation = gateway.handle_queue(
            "flow-leader", "prov", "queue-key", "member-1", "POST",
            "/v1/messages", {}, _queue_envelope())

        self.assertEqual(result.outcome, Outcome.QUEUE_TIMEOUT)
        self.assertEqual(generation.member_count, 1)

        occupier_block.set()
        occupier_thread.join(timeout=2)


class QueueLeaderTotalTimeoutTest(_TempGatewayCase):
    """§21: `total_timeout` bounds the whole logical request; a leader
    whose deadline has already passed must never reach the provider's
    Lane at all (mirrors `test_gateway_pipeline.py`'s
    `TotalTimeoutTest` for the plain `handle()` path)."""

    def test_total_timeout_before_admission_leaves_provider_lane_untouched(
        self,
    ) -> None:
        _write_provider(
            self.providers_dir, "prov", concurrency_limit=1,
            timeout_overrides={"total_timeout_seconds": 0})
        write_credential("prov", "fake-token", root=self.root)

        def never_called_connection_factory(provider, timeout):
            raise AssertionError("forward() must not be reached")

        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=never_called_connection_factory)

        result, generation = gateway.handle_queue(
            "flow-A", "prov", "queue-key", "member-1", "POST",
            "/v1/messages", {}, _queue_envelope())

        self.assertEqual(result.outcome, Outcome.TOTAL_TIMEOUT)
        self.assertEqual(gateway.provider_lanes["prov"].status()["leased"], 0)
        self.assertEqual(generation.state, QueueGenerationState.DONE)
        self.assertEqual(generation.member_count, 0)


class JoinerIndependentTotalDeadlineTest(_TempGatewayCase):
    """§21's central coalescing correction: "the total deadline must not
    reset when a member is coalesced." A joiner's own wait is bounded by
    its own `total_deadline`, computed from its own arrival -- not
    extended, reset, or otherwise influenced by how long the leader's
    physical execution actually takes. Proven here by making the leader's
    physical call run well past both members' shared (tiny)
    `total_timeout_seconds` budget: the joiner must time out on its own,
    while the leader -- already past admission, now bounded only by
    `compression_timeout` (§21: waiting behind another generation must
    not consume the budget meant for model execution) -- keeps running
    and still completes successfully once unblocked, undisturbed by the
    joiner having given up on it."""

    def test_joiner_times_out_independently_leader_still_succeeds(self) -> None:
        _write_provider(
            self.providers_dir, "prov", concurrency_limit=1,
            timeout_overrides={"total_timeout_seconds": 0.3})
        write_credential("prov", "fake-token", root=self.root)

        occupier_block = threading.Event()
        occupier_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"occupier-ok"),
            block_event=occupier_block)
        leader_block = threading.Event()
        leader_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"leader-ok"),
            block_event=leader_block)
        factory = _SequencedConnectionFactory([occupier_conn, leader_conn])
        gateway = Gateway(
            self.providers_dir, root=self.root, connection_factory=factory)

        def occupy() -> None:
            gateway.handle("occupier", "prov", "POST", "/v1/messages", {}, b"{}")

        occupier_thread = threading.Thread(target=occupy)
        occupier_thread.start()
        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 1)

        leader_result: dict = {}

        def submit_leader() -> None:
            leader_result["value"] = gateway.handle_queue(
                "flow-leader", "prov", "shared-key", "leader-member", "POST",
                "/v1/messages", {}, _queue_envelope("leader-block"))

        leader_thread = threading.Thread(target=submit_leader)
        leader_thread.start()
        # Give the leader time to register its OPEN generation and start
        # blocking on lane.acquire() behind the occupier.
        time.sleep(0.05)

        joiner_result: dict = {}

        def submit_joiner() -> None:
            joiner_result["value"] = gateway.handle_queue(
                "flow-joiner", "prov", "shared-key", "joiner-member", "POST",
                "/v1/messages", {}, _queue_envelope("joiner-block"))

        joiner_thread = threading.Thread(target=submit_joiner)
        joiner_thread.start()
        # Give the joiner time to append to the still-OPEN generation
        # before releasing the occupier -- otherwise the leader could
        # seal the generation (on admission) before the joiner arrives.
        time.sleep(0.03)

        occupier_block.set()
        occupier_thread.join(timeout=2)
        # The leader is admitted quickly from here (well inside its own
        # 0.3s total_timeout budget) and immediately starts blocking
        # inside its own forward() call via leader_block -- so it never
        # publishes a result while the joiner is still waiting.

        joiner_thread.join(timeout=2)
        self.assertFalse(joiner_thread.is_alive())

        # The joiner's own total_timeout_seconds (0.3s, counted from its
        # own arrival) elapsed while the leader was still genuinely
        # executing -- it must synthesize its own TOTAL_TIMEOUT rather
        # than wait indefinitely for a result that has not been
        # published yet, and must not be extended by having coalesced.
        self.assertEqual(joiner_result["value"][0].outcome, Outcome.TOTAL_TIMEOUT)

        # The leader must still be genuinely in flight at this point --
        # the joiner giving up must not have cancelled or otherwise
        # disturbed it.
        self.assertTrue(leader_thread.is_alive())
        self.assertNotIn("value", leader_result)

        leader_block.set()
        leader_thread.join(timeout=2)

        leader_outcome, leader_generation = leader_result["value"]
        self.assertEqual(leader_outcome.outcome, Outcome.SUCCESS)
        self.assertEqual(leader_outcome.body, b"leader-ok")
        self.assertEqual(leader_generation.member_count, 2)
        self.assertEqual(leader_generation.state, QueueGenerationState.DONE)


class SharedTransportFailureFanOutTest(_TempGatewayCase):
    """§20: a queue generation is one physical provider operation, so a
    genuine transport failure applies to every logical member in it --
    not just the leader that happened to make the call."""

    def test_upstream_failure_reaches_every_coalesced_member_identically(
        self,
    ) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

        occupier_block = threading.Event()
        occupier_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"occupier-ok"),
            block_event=occupier_block)
        leader_conn = FakeConnection(error=OSError("simulated upstream failure"))
        factory = _SequencedConnectionFactory([occupier_conn, leader_conn])
        gateway = Gateway(
            self.providers_dir, root=self.root, connection_factory=factory)

        def occupy() -> None:
            gateway.handle("occupier", "prov", "POST", "/v1/messages", {}, b"{}")

        occupier_thread = threading.Thread(target=occupy)
        occupier_thread.start()
        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 1)

        results: dict[str, tuple] = {}

        def submit(marker: str) -> None:
            results[marker] = gateway.handle_queue(
                f"flow-{marker}", "prov", "shared-key", f"member-{marker}",
                "POST", "/v1/messages", {}, _queue_envelope(f"{marker}-block"))

        threads = [
            threading.Thread(target=submit, args=(marker,))
            for marker in ("A", "B")
        ]
        for thread in threads:
            thread.start()
            time.sleep(0.03)

        occupier_block.set()
        occupier_thread.join(timeout=2)
        for thread in threads:
            thread.join(timeout=2)

        result_a, generation_a = results["A"]
        result_b, generation_b = results["B"]

        self.assertEqual(result_a.outcome, Outcome.UPSTREAM_ERROR)
        self.assertEqual(result_b.outcome, Outcome.UPSTREAM_ERROR)
        self.assertEqual(generation_a.generation_id, generation_b.generation_id)
        self.assertEqual(generation_a.member_count, 2)
        # Lane slot must be released, not quarantined -- a raised
        # exception is still a confirmed-idle failure (forward()'s own
        # `closed` bookkeeping), so the provider must be free for the
        # next request immediately.
        self.assertEqual(gateway.provider_lanes["prov"].status()["leased"], 0)


class QueueConcurrencyOversubscriptionTest(_TempGatewayCase):
    """§24: physical concurrency is counted in queue generations, not
    logical members -- a `concurrency_limit=2` provider must never run
    more than 2 generations' physical requests at once, no matter how
    many different `queue_key`s arrive concurrently."""

    def test_peak_in_flight_generations_never_exceeds_concurrency_limit(
        self,
    ) -> None:
        _write_provider(self.providers_dir, "prov", concurrency_limit=2)
        write_credential("prov", "fake-token", root=self.root)

        block_event = threading.Event()
        fake_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"ok"),
            block_event=block_event)
        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=lambda provider, timeout: fake_conn)

        peak = {"value": 0}
        stop_sampling = threading.Event()

        def sample() -> None:
            while not stop_sampling.is_set():
                leased = gateway.provider_lanes["prov"].status()["leased"]
                if leased > peak["value"]:
                    peak["value"] = leased
                time.sleep(0.005)

        sampler = threading.Thread(target=sample)
        sampler.start()

        results: dict[str, tuple] = {}

        def submit(marker: str) -> None:
            results[marker] = gateway.handle_queue(
                f"flow-{marker}", "prov", f"key-{marker}", f"member-{marker}",
                "POST", "/v1/messages", {}, _queue_envelope(f"{marker}-block"))

        markers = ["A", "B", "C", "D"]
        threads = [threading.Thread(target=submit, args=(marker,)) for marker in markers]
        for thread in threads:
            thread.start()

        _wait_until(
            lambda: gateway.provider_lanes["prov"].status()["leased"] == 2)
        # Give the 2 excess submissions (C, D) time to genuinely queue
        # behind the 2 already-leased generations before releasing.
        time.sleep(0.05)

        block_event.set()
        for thread in threads:
            thread.join(timeout=2)
        stop_sampling.set()
        sampler.join(timeout=2)

        for marker in markers:
            self.assertEqual(results[marker][0].outcome, Outcome.SUCCESS)
        self.assertLessEqual(peak["value"], 2)
        self.assertEqual(peak["value"], 2)


if __name__ == "__main__":
    unittest.main()
