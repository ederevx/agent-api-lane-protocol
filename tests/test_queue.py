"""Direct unit coverage of aalp/queue.py's generation state machine."""
from __future__ import annotations

import unittest

import json

from aalp.queue import (
    QueueEnvelopeError,
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


def _envelope(member_block: str, *, content: str = "__SENTINEL__") -> dict:
    return {
        "shared": {
            "model": "claude-x",
            "messages": [{"role": "user", "content": content}],
        },
        "content_path": ["messages", 0, "content"],
        "member_block": member_block,
        "member_join": "\n\n",
        "count_template": "ACP-QUEUE-MEMBER-COUNT: {member_count}",
    }


class BuildPhysicalBodyTest(unittest.TestCase):
    def _generation(self) -> QueueGeneration:
        return QueueGeneration(
            generation_id=new_generation_id(), provider_id="ci", queue_key="key-1")

    def test_single_member_deep_sets_train_and_count(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="solo", payload=_envelope("ITEM: solo")))

        body = generation.build_physical_body()

        decoded = json.loads(body)
        self.assertEqual(
            decoded["messages"][0]["content"],
            "ITEM: solo\n\nACP-QUEUE-MEMBER-COUNT: 1")
        self.assertEqual(decoded["model"], "claude-x")

    def test_multiple_members_joined_in_append_order(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="a", payload=_envelope("ITEM: a")))
        generation.append(QueueMember(member_id="b", payload=_envelope("ITEM: b")))

        body = generation.build_physical_body()

        decoded = json.loads(body)
        self.assertEqual(
            decoded["messages"][0]["content"],
            "ITEM: a\n\nITEM: b\n\nACP-QUEUE-MEMBER-COUNT: 2")

    def test_only_leaders_shared_envelope_and_path_are_used(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="a", payload=_envelope("ITEM: a")))
        # A joiner's own `shared`/`content_path` differ but must be ignored --
        # only its `member_block` should end up contributing to the train.
        joiner_envelope = _envelope("ITEM: b", content="ignored-different-sentinel")
        joiner_envelope["shared"]["model"] = "different-model-should-be-ignored"
        generation.append(QueueMember(member_id="b", payload=joiner_envelope))

        body = generation.build_physical_body()

        decoded = json.loads(body)
        self.assertEqual(decoded["model"], "claude-x")
        self.assertEqual(
            decoded["messages"][0]["content"],
            "ITEM: a\n\nITEM: b\n\nACP-QUEUE-MEMBER-COUNT: 2")

    def test_no_members_raises(self) -> None:
        generation = self._generation()
        with self.assertRaises(QueueEnvelopeError):
            generation.build_physical_body()

    def test_missing_envelope_field_raises(self) -> None:
        generation = self._generation()
        bad = _envelope("ITEM: a")
        del bad["content_path"]
        generation.append(QueueMember(member_id="a", payload=bad))
        with self.assertRaises(QueueEnvelopeError):
            generation.build_physical_body()

    def test_non_dict_payload_raises(self) -> None:
        generation = self._generation()
        generation.append(QueueMember(member_id="a", payload="not-a-dict"))
        with self.assertRaises(QueueEnvelopeError):
            generation.build_physical_body()

    def test_bad_content_path_raises(self) -> None:
        generation = self._generation()
        envelope = _envelope("ITEM: a")
        envelope["content_path"] = ["messages", 99, "content"]
        generation.append(QueueMember(member_id="a", payload=envelope))
        with self.assertRaises(QueueEnvelopeError):
            generation.build_physical_body()


if __name__ == "__main__":
    unittest.main()
