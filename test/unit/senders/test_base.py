from collections.abc import Sequence
from journalpump.senders.base import LogSender, Tagged
from typing import Any
from unittest import mock

import pytest


@pytest.mark.parametrize(
    "input_tags,expected_output_tags",
    [
        ({"foo": "bar"}, {"foo": "bar"}),
        ({"foo": "bar", "bar": None, None: "baz"}, {"foo": "bar"}),
        ({"foo|wow,": "super!cool"}, {"foo_wow": "super!cool"}),
        ({"host": "localhost:1234"}, {"host": "localhost_1234"}),
        ({"base64": "YmFzZTY0Cg=="}, {"base64": "YmFzZTY0Cg"}),
        ({"source_address": "127.0.0.1"}, {"source_address": "127.0.0.1"}),
    ],
)
def test_tagged(input_tags: dict[Any, Any], expected_output_tags: dict[str, str]) -> None:
    tagged = Tagged(None)
    assert tagged.make_tags(input_tags) == expected_output_tags


def _make_log_sender(*, max_batch_size: int) -> LogSender:
    sender = LogSender(
        name="test",
        reader=mock.Mock(),
        config={},
        field_filter=None,
        stats=mock.Mock(),
        max_send_interval=0.3,
    )
    sender.max_batch_size = max_batch_size
    sender.batch_message_overhead = 0
    return sender


def test_next_batch_length_stops_before_max_batch_size() -> None:
    sender = _make_log_sender(max_batch_size=10)
    messages = [(b"aaaa", "c1"), (b"bbbb", "c2"), (b"cccc", "c3")]
    assert sender.next_batch_length(messages) == 2


def test_next_batch_length_takes_oversized_message_alone() -> None:
    sender = _make_log_sender(max_batch_size=10)
    messages = [(b"x" * 20, "c1"), (b"y", "c2")]
    assert sender.next_batch_length(messages) == 1


def test_next_batch_length_takes_everything_that_fits() -> None:
    sender = _make_log_sender(max_batch_size=100)
    messages = [(b"a", "c1"), (b"b", "c2"), (b"c", "c3")]
    assert sender.next_batch_length(messages) == 3


class _OneByOneSender(LogSender):
    def __init__(self) -> None:
        super().__init__(
            name="test",
            reader=mock.Mock(),
            config={},
            field_filter=None,
            stats=mock.Mock(),
            max_send_interval=0.3,
        )
        self.sent: list[tuple[list[bytes], str | None]] = []

    def next_batch_length(self, messages: Sequence[tuple[bytes, str | None]]) -> int:
        return 1

    def send_messages(self, *, messages: list[bytes], cursor: str | None) -> bool:
        self.sent.append((messages, cursor))
        return True


def test_get_and_send_messages_uses_next_batch_length() -> None:
    sender = _OneByOneSender()
    for index in range(3):
        sender.msg_buffer.add_item(item=f"m{index}".encode(), cursor=f"c{index}")

    sender.get_and_send_messages()

    assert sender.sent == [([b"m0"], "c0"), ([b"m1"], "c1"), ([b"m2"], "c2")]
