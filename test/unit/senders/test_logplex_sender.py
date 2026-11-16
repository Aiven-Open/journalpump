from journalpump.senders.logplex import LogplexSender
from typing import Any
from unittest import mock

import json


def _make_sender(**config: Any) -> LogplexSender:
    return LogplexSender(
        config={"logplex_log_input_url": "https://logplex.example/logs", "logplex_token": "token", **config},
        name="test-logplex",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )


def _encode(message: str) -> bytes:
    return json.dumps({"MESSAGE": message, "_HOSTNAME": "host", "_PID": "123"}).encode("utf-8")


def _split_frames(wire: bytes) -> list[bytes]:
    frames = []
    while wire:
        count, wire = wire.split(b" ", 1)
        frames.append(wire[: int(count)])
        wire = wire[int(count) :]
    return frames


def test_format_msg_uses_octet_counted_syslog_frame() -> None:
    sender = _make_sender()
    wire = sender.format_msg(_encode("hola ñ\nnext"))
    count, payload = wire.split(b" ", 1)
    assert int(count) == len(payload)
    assert payload.startswith(b"<190>1 ")
    assert payload.endswith("hola ñ\nnext".encode())


def test_send_messages_posts_valid_frames() -> None:
    sender = _make_sender()
    sender.session = mock.Mock()
    assert sender.send_messages(messages=[_encode("one"), _encode("ñ two")], cursor="c")
    data = sender.session.post.call_args.kwargs["data"]
    frames = _split_frames(data)
    assert len(frames) == 2
    assert frames[0].startswith(b"<190>1 ") and frames[0].endswith(b"one")
    assert frames[1].endswith("ñ two".encode())
