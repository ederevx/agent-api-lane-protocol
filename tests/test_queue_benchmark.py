"""Stage 5 benchmark for `Gateway.handle_queue()`
(agent_protocols_v1_queue_coalescing_adjustment_metadata_v1.md §22, §34):
benchmark widths 1, 2, and 4; measure backlog drain time and provider-
request-count reduction; select the first production queue-width limit.

Same real-thread, real-wall-clock style as `test_queue_timeout_audit.py`
(direct `Gateway`, not the backend-agnostic conformance suite), plus one
addition: a fixed simulated per-physical-request latency
(`_LatencyConnection`), modeling that a real physical round-trip has a
roughly fixed per-request overhead independent of how many members are
mechanically joined into it. Under `concurrency_limit=1`, generations drain
strictly serially (FIFO, §23), so a backlog's total drain time is
approximately `generation_count * latency` -- this is what lets width
actually show up as a measurable wall-clock improvement here, not just a
call-count one.

Queue width has no separate fixed member-count cap -- it is driven purely
by `max_queue_input_bytes` (a `Gateway` constructor parameter,
`AALP_MAX_QUEUE_INPUT_BYTES` env var otherwise). Every backlog member's
`member_block` here is the same fixed size (`_MEMBER_BLOCK_BYTES`), so a
given width is forced deterministically by setting
`max_queue_input_bytes = width * _MEMBER_BLOCK_BYTES` -- provider files
themselves don't change between widths, so all three benchmark runs reuse
the same `providers_dir`/credential and just construct a fresh `Gateway`
per width.
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


# Every backlog member's own member_block is forced to exactly this many
# bytes, so a desired width can be forced deterministically by setting
# max_queue_input_bytes = width * _MEMBER_BLOCK_BYTES (see module docstring).
_MEMBER_BLOCK_BYTES = 1_000


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
    def __init__(self, response=None, block_event=None):
        self.response = response if response is not None else FakeResponse()
        self.block_event = block_event

    def request(self, method, path, body=None, headers=None):
        pass

    def getresponse(self):
        if self.block_event is not None:
            self.block_event.wait()
        return self.response

    def close(self):
        pass


class _LatencyConnection:
    """A fresh physical connection whose `getresponse()` costs a fixed,
    simulated round-trip latency -- stands in for a real provider's
    roughly-fixed per-request overhead, independent of how many mechanically
    joined members ride inside the one physical body."""

    def __init__(self, latency: float, body: bytes = b"ok"):
        self.latency = latency
        self.response = FakeResponse(status=200, body=body)

    def request(self, method, path, body=None, headers=None):
        pass

    def getresponse(self):
        time.sleep(self.latency)
        return self.response

    def close(self):
        pass


class QueueBenchmarkTest(unittest.TestCase):
    _BACKLOG_SIZE = 8
    _LATENCY = 0.08

    def setUp(self) -> None:
        self._providers_tmp = tempfile.TemporaryDirectory()
        self._root_tmp = tempfile.TemporaryDirectory()
        self.providers_dir = Path(self._providers_tmp.name)
        self.root = Path(self._root_tmp.name)
        _write_provider(self.providers_dir, "prov", concurrency_limit=1)
        write_credential("prov", "fake-token", root=self.root)

    def tearDown(self) -> None:
        self._providers_tmp.cleanup()
        self._root_tmp.cleanup()

    def _run_backlog_for_width(self, width: int) -> tuple[float, int, dict]:
        """Occupy the provider, accumulate `_BACKLOG_SIZE` same-key
        submissions behind it, release, and return
        `(drain_elapsed_seconds, physical_request_count, results)`."""
        physical_request_count = {"value": 0}
        count_lock = threading.Lock()
        occupier_block = threading.Event()
        occupier_conn = FakeConnection(
            response=FakeResponse(status=200, body=b"occupier-ok"),
            block_event=occupier_block)

        def factory(provider, timeout):
            with count_lock:
                is_first = physical_request_count["value"] == 0 and not occupier_block.is_set()
            # The very first physical call is the occupier's own -- it must
            # not count as (or share fate with) a backlog generation.
            if is_first and physical_request_count["value"] == 0:
                pass
            return None  # overwritten below; placeholder never used

        # Simpler than threading a flag through `factory`: the occupier is
        # submitted and admitted (and its connection consumed) strictly
        # before any backlog member is submitted, so a plain call counter
        # that skips call #0 is unambiguous and race-free.
        call_index = {"value": 0}

        def connection_factory(provider, timeout):
            with count_lock:
                index = call_index["value"]
                call_index["value"] += 1
            if index == 0:
                return occupier_conn
            physical_request_count["value"] += 1
            return _LatencyConnection(self._LATENCY, body=f"gen-{index}".encode())

        gateway = Gateway(
            self.providers_dir, root=self.root,
            connection_factory=connection_factory,
            max_queue_input_bytes=width * _MEMBER_BLOCK_BYTES)

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
                "POST", "/v1/messages", {},
                _queue_envelope(marker * _MEMBER_BLOCK_BYTES))

        markers = [str(i) for i in range(self._BACKLOG_SIZE)]
        threads = [threading.Thread(target=submit, args=(marker,)) for marker in markers]
        for thread in threads:
            thread.start()
            # Staggered registration, matching test_interface_conformance.
            # py's own convention -- each submission's leader/joiner role
            # and generation membership resolves under `_queue_lock` well
            # before the next thread starts, making accumulation order
            # deterministic instead of racing.
            time.sleep(0.03)

        drain_start = time.monotonic()
        occupier_block.set()
        occupier_thread.join(timeout=5)
        for thread in threads:
            thread.join(timeout=5)
        drain_elapsed = time.monotonic() - drain_start

        return drain_elapsed, physical_request_count["value"], results

    def test_widths_1_2_4_reduce_provider_requests_and_drain_time(self) -> None:
        import math

        widths = [1, 2, 4]
        drain_times: dict[int, float] = {}
        request_counts: dict[int, int] = {}

        for width in widths:
            with self.subTest(width=width):
                drain_elapsed, request_count, results = self._run_backlog_for_width(width)

                for marker, (result, generation) in results.items():
                    self.assertEqual(
                        result.outcome, Outcome.SUCCESS,
                        f"width={width} member={marker} outcome={result.outcome}")

                expected_generations = math.ceil(self._BACKLOG_SIZE / width)
                self.assertEqual(request_count, expected_generations)

                generation_ids = {
                    results[m][1].generation_id for m in results
                }
                self.assertEqual(len(generation_ids), expected_generations)

                drain_times[width] = drain_elapsed
                request_counts[width] = request_count

        # Provider-request reduction: strictly fewer physical requests as
        # width grows, for the same backlog.
        self.assertEqual(request_counts, {1: 8, 2: 4, 4: 2})

        # Backlog drain time: strictly faster as width grows, since fewer
        # serialized (concurrency_limit=1) physical round-trips are needed
        # to drain the same backlog. Generous tolerance around the
        # theoretical generation_count * latency floor absorbs thread-
        # scheduling jitter without masking the real, order-of-magnitude
        # difference between widths.
        for width in widths:
            expected_floor = math.ceil(self._BACKLOG_SIZE / width) * self._LATENCY
            self.assertGreaterEqual(drain_times[width], expected_floor - 0.05)
            self.assertLess(drain_times[width], expected_floor + 0.4)

        self.assertLess(drain_times[4], drain_times[2])
        self.assertLess(drain_times[2], drain_times[1])


if __name__ == "__main__":
    unittest.main()
