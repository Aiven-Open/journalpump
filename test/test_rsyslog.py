# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from journalpump.rsyslog import SyslogTcpClient
from unittest import mock

import pytest


class _StubSocket:
    """Minimal socket stub that records bytes passed to sendall."""

    def __init__(self) -> None:
        self.sent = bytearray()

    def sendall(self, data: bytes) -> None:
        self.sent.extend(data)

    def close(self) -> None:
        pass


def _make_client(
    *, max_msg: int, octet_counted_framing: bool = False, escape_newlines: bool = False
) -> tuple[SyslogTcpClient, _StubSocket]:
    with mock.patch.object(SyslogTcpClient, "_connect"):
        client = SyslogTcpClient(
            server="127.0.0.1",
            port=1,
            rfc="RFC5424",
            max_msg=max_msg,
            octet_counted_framing=octet_counted_framing,
            escape_newlines=escape_newlines,
        )
    sock = _StubSocket()
    client.socket = sock  # type: ignore[assignment]
    return client, sock


def _parse_octet_counted_frames(stream: bytes) -> list[bytes]:
    """Parse an RFC 6587 octet-counted stream into individual message bodies."""
    frames: list[bytes] = []
    i = 0
    while i < len(stream):
        sp = stream.index(b" ", i)
        n = int(stream[i:sp])
        body = stream[sp + 1 : sp + 1 + n]
        assert len(body) == n, f"stream desync at offset {i}: advertised {n} bytes, got {len(body)}"
        frames.append(bytes(body))
        i = sp + 1 + n
    return frames


class TestNonTransparentFraming:
    """octet_counted_framing=False — must be byte-for-byte identical to the pre-fix behavior."""

    def test_small_body_sent_verbatim(self) -> None:
        body = b"<13>1 2024-01-01T00:00:00Z host app - - hello\n"
        max_msg = len(body) + 10
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=False)

        client.send(body)

        assert bytes(sock.sent) == body

    def test_small_body_no_extra_newline(self) -> None:
        body = b"<13>1 2024-01-01T00:00:00Z host app - - hello\n"
        max_msg = len(body) + 10
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=False)

        client.send(body)

        assert sock.sent.count(b"\n") == 1

    def test_oversize_body_truncated_and_newline_appended(self) -> None:
        body = b"A" * 200 + b"\n"
        max_msg = 50
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=False)

        client.send(body)

        expected = body[: max_msg - 1] + b"\n"
        assert bytes(sock.sent) == expected

    def test_body_exactly_max_msg_triggers_truncation(self) -> None:
        # len(body) == max_msg means the body >= max_msg branch fires
        max_msg = 50
        body = b"B" * max_msg
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=False)

        client.send(body)

        expected = body[: max_msg - 1] + b"\n"
        assert bytes(sock.sent) == expected


class TestOctetCountedFraming:
    """octet_counted_framing=True — must produce RFC 6587 section 3.4.1 frames."""

    def test_small_multiline_body_framed_correctly(self) -> None:
        body = b"<13>1 2024-01-01T00:00:00Z host app - - line1\nline2\nline3\n"
        max_msg = len(body) + 50
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body)

        wire = bytes(sock.sent)
        sp = wire.index(b" ")
        advertised_len = int(wire[:sp])
        sent_body = wire[sp + 1 :]

        assert advertised_len == len(body)
        assert sent_body == body

    def test_small_body_no_trailing_newline_outside_frame(self) -> None:
        body = b"<13>1 2024-01-01T00:00:00Z host app - - msg\n"
        max_msg = len(body) + 50
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body)

        wire = bytes(sock.sent)
        sp = wire.index(b" ")
        n = int(wire[:sp])
        # Everything after the prefix+space+body must be empty (no stray newline)
        assert len(wire) == sp + 1 + n

    def test_oversize_body_truncated_and_prefix_matches(self) -> None:
        body = b"X" * 300
        max_msg = 100
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body)

        wire = bytes(sock.sent)
        sp = wire.index(b" ")
        advertised_len = int(wire[:sp])
        sent_body = wire[sp + 1 :]

        assert advertised_len == len(sent_body)
        assert len(sent_body) <= max_msg
        assert sent_body == body[:max_msg]

    def test_back_to_back_oversize_messages_no_desync(self) -> None:
        """Two oversize messages on the same connection must parse without desync."""
        body1 = b"<13>1 2024-01-01T00:00:00Z h a - - " + b"M" * 300 + b"\n"
        body2 = b"<14>1 2024-01-01T00:00:01Z h b - - short\n"
        max_msg = 60
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body1)
        client.send(body2)

        frames = _parse_octet_counted_frames(bytes(sock.sent))
        assert len(frames) == 2
        assert frames[0] == body1[:max_msg]
        assert frames[1] == body2[:max_msg]

    def test_very_small_max_msg_produces_self_consistent_frame(self) -> None:
        """Even with max_msg=8 the frame prefix must equal the body length."""
        body = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\n"
        max_msg = 8
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body)

        frames = _parse_octet_counted_frames(bytes(sock.sent))
        assert len(frames) == 1
        assert len(frames[0]) <= max_msg

    @pytest.mark.parametrize("max_msg", [1, 4, 8, 16, 32])
    def test_various_small_max_msg_always_self_consistent(self, max_msg: int) -> None:
        body = b"Hello, multi\nline\nbody\n" * 5
        client, sock = _make_client(max_msg=max_msg, octet_counted_framing=True)

        client.send(body)

        frames = _parse_octet_counted_frames(bytes(sock.sent))
        assert len(frames) == 1
        assert len(frames[0]) <= max_msg


class TestEscapeNewlines:
    """escape_newlines=True escapes embedded CR/LF in the message before framing."""

    def _log_multiline(self, *, escape_newlines: bool) -> bytes:
        client, sock = _make_client(max_msg=4096, escape_newlines=escape_newlines)
        client.log(
            facility=1,
            severity=6,
            timestamp="2024-01-01T00:00:00.000000Z",
            hostname="host",
            program="app",
            msg="line1\nline2\r\nline3",
        )
        return bytes(sock.sent)

    def test_disabled_keeps_raw_newlines(self) -> None:
        sent = self._log_multiline(escape_newlines=False)
        assert sent == b"<14>1 2024-01-01T00:00:00.000000Z host app - - line1\nline2\r\nline3\n"

    def test_enabled_escapes_newlines(self) -> None:
        sent = self._log_multiline(escape_newlines=True)
        assert sent == b"<14>1 2024-01-01T00:00:00.000000Z host app - - line1\\nline2\\r\\nline3\n"

    def test_enabled_single_trailing_real_newline(self) -> None:
        sent = self._log_multiline(escape_newlines=True)
        assert sent.count(b"\n") == 1
