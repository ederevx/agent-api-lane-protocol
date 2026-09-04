"""Direct unit coverage of aalp/queue.py's generation state machine."""
from __future__ import annotations

import unittest

from aalp.queue import (
    QueueGeneration,
    QueueGenerationSealed,
    QueueGenerationState,
    QueueMember,
    new_generation_id,
)


class NewGenerationIdTest(unittest.TestCase):
    def test_returns_distinct_opaque_strings(self) -> None:
        a = new_generation_id()
        b = new_generation_id()
        self.assertIsInstance(a, str)
        self.assertNotEqual(a, b)


class QueueGenerationTest(unittest.TestCase):
    def _generation(self) -> QueueGeneration:
        return QueueGeneration(
            generation_id=new_generation_id(),
            provider_id="ci",
            queue_key="key-1",
        )

    def test_starts_open_and_empty(self) -> None:
        generation = self._generation()
        self.assertEqual(generation.state, QueueGenerationState.OPEN)
        self.assertEqual(generation.member_count, 0)

    def test_append_while_open(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="m1"))
        generation.append(QueueMember(member_id="m2"))
        self.assertEqual(generation.member_count, 2)

    def test_seal_transitions_open_to_ready(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="m1"))
        generation.seal()
        self.assertEqual(generation.state, QueueGenerationState.READY)

    def test_append_after_seal_raises(self) -> None:
        generation = self._generation()
        generation.seal()
        with self.assertRaises(QueueGenerationSealed):
            generation.append(QueueMember(member_id="late"))

    def test_seal_twice_raises(self) -> None:
        generation = self._generation()
        generation.seal()
        with self.assertRaises(QueueGenerationSealed):
            generation.seal()

    def test_mark_in_flight_directly_from_open(self) -> None:
        # §7: normal provider release may transition OPEN -> IN_FLIGHT
        # directly when no bound forced an explicit READY seal first.
        generation = self._generation()
        generation.append(QueueMember(member_id="m1"))
        generation.mark_in_flight()
        self.assertEqual(generation.state, QueueGenerationState.IN_FLIGHT)

    def test_mark_in_flight_from_ready(self) -> None:
        generation = self._generation()
        generation.seal()
        generation.mark_in_flight()
        self.assertEqual(generation.state, QueueGenerationState.IN_FLIGHT)

    def test_mark_in_flight_from_done_raises(self) -> None:
        generation = self._generation()
        generation.mark_in_flight()
        generation.mark_done()
        with self.assertRaises(QueueGenerationSealed):
            generation.mark_in_flight()

    def test_mark_done_requires_in_flight(self) -> None:
        generation = self._generation()
        with self.assertRaises(QueueGenerationSealed):
            generation.mark_done()

    def test_mark_done_from_in_flight(self) -> None:
        generation = self._generation()
        generation.mark_in_flight()
        generation.mark_done()
        self.assertEqual(generation.state, QueueGenerationState.DONE)

    def test_append_after_in_flight_raises(self) -> None:
        generation = self._generation()
        generation.mark_in_flight()
        with self.assertRaises(QueueGenerationSealed):
            generation.append(QueueMember(member_id="late"))

    def test_member_count_reflects_appended_members(self) -> None:
        generation = self._generation()
        self.assertEqual(generation.member_count, 0)
        generation.append(QueueMember(member_id="m1"))
        self.assertEqual(generation.member_count, 1)


if __name__ == "__main__":
    unittest.main()
