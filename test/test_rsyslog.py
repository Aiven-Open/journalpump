# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from journalpump.rsyslog import _sanitize_header_field, APP_NAME_MAX, NILVALUE, SyslogTcpClient
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
    *,
    max_msg: int,
    octet_counted_framing: bool | None = None,
    escape_newlines: bool | None = None,
    rfc: str = "RFC5424",
    log_format: str | None = None,
) -> tuple[SyslogTcpClient, _StubSocket]:
    with mock.patch.object(SyslogTcpClient, "_connect"):
        client = SyslogTcpClient(
            server="127.0.0.1",
            port=1,
            rfc=rfc,
            max_msg=max_msg,
            log_format=log_format,
            octet_counted_framing=octet_counted_framing,
            escape_newlines=escape_newlines,
        )
    sock = _StubSocket()
    client.socket = sock  # type: ignore[assignment]
    return client, sock


def _log(client: SyslogTcpClient, **overrides: str) -> None:
    fields = {
        "timestamp": "2024-01-01T00:00:00.000000Z",
        "hostname": "host",
        "program": "app",
        "msg": "hello",
        **overrides,
    }
    client.log(facility=1, severity=6, **fields)


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

    @pytest.mark.parametrize("escape_newlines", [None, True, False])
    def test_overrides_escape_newlines(self, escape_newlines: bool | None) -> None:
        client, sock = _make_client(max_msg=4096, octet_counted_framing=True, escape_newlines=escape_newlines)
        assert client.escape_newlines is False

        _log(client, msg="line1\nline2 C:\\tmp")
        frames = _parse_octet_counted_frames(bytes(sock.sent))
        assert len(frames) == 1
        body = frames[0]
        assert b"line1\nline2 C:\\tmp" in body
        assert b"\\n" not in body
        assert b"\\\\" not in body


class TestEscapeNewlines:
    """escape_newlines (on unless explicitly disabled) escapes embedded CR/LF before framing."""

    FORGED = "<13>1 2024-01-01T00:00:00Z victim sshd - - forged"
    MULTILINE = "line1\nline2\r\nline3\rline4"

    def test_disabled_keeps_raw_newlines(self) -> None:
        client, sock = _make_client(max_msg=4096, escape_newlines=False)
        _log(client, msg=self.MULTILINE)
        assert bytes(sock.sent) == b"<14>1 2024-01-01T00:00:00.000000Z host app - - line1\nline2\r\nline3\rline4\n"

    @pytest.mark.parametrize("escape_newlines", [None, True])
    def test_enabled_escapes_newlines(self, escape_newlines: bool | None) -> None:
        client, sock = _make_client(max_msg=4096, escape_newlines=escape_newlines)
        _log(client, msg=self.MULTILINE)
        assert bytes(sock.sent) == (b"<14>1 2024-01-01T00:00:00.000000Z host app - - line1\\nline2\\r\\nline3\\rline4\n")

    def test_enabled_escapes_backslashes(self) -> None:
        client, sock = _make_client(max_msg=4096)
        _log(client, msg="literal\\n real\n path C:\\tmp\\")
        assert (
            bytes(sock.sent) == b"<14>1 2024-01-01T00:00:00.000000Z host app - - literal\\\\n real\\n path C:\\\\tmp\\\\\n"
        )

    def test_disabled_keeps_raw_backslashes(self) -> None:
        client, sock = _make_client(max_msg=4096, escape_newlines=False)
        _log(client, msg="literal\\n")
        assert bytes(sock.sent) == b"<14>1 2024-01-01T00:00:00.000000Z host app - - literal\\n\n"

    def test_literal_backslash_n_cannot_forge_record_after_decoding(self) -> None:
        client, sock = _make_client(max_msg=4096)
        _log(client, msg=f"ok\\n{self.FORGED}")
        assert bytes(sock.sent).count(b"\n") == 1
        assert bytes(sock.sent).decode("unicode_escape").count("\n") == 1

    @pytest.mark.parametrize(
        "rfc,log_format,expected",
        [
            ("RFC3164", None, b"<14>Jan 01 00:00:00 h_X a_Y[1_Z]: m\\nF\n"),
            (
                "CUSTOM",
                "%timestamp:::date-rfc3339% %HOSTNAME% %app-name% %procid% %msgid% %structured-data% %msg%",
                b'2024-01-01T00:00:00.000000Z h_X a_Y 1_Z i_D x@1 a="b\\]" m\\nF\n',
            ),
        ],
    )
    def test_other_formats_escape_message_and_sanitize_header_fields(
        self, rfc: str, log_format: str | None, expected: bytes
    ) -> None:
        client, sock = _make_client(max_msg=4096, rfc=rfc, log_format=log_format)
        _log(
            client,
            hostname="h\nX",
            program="a\rY",
            pid="1\nZ",
            msgid="i\nD",
            sd='x@1 a="b\\]"',
            msg="m\nF",
        )
        assert bytes(sock.sent) == expected

    def test_sd_not_escaped(self) -> None:
        client, sock = _make_client(max_msg=4096)
        _log(client, msgid="id", sd='x@1 a="b\\]"')
        assert bytes(sock.sent) == b'<14>1 2024-01-01T00:00:00.000000Z host app - id [x@1 a="b\\]"] hello\n'


class TestSanitizeHeaderFields:
    @pytest.mark.parametrize(
        "value,max_bytes,expected",
        [
            ("a b\tc\nd\re", 255, "a_b_c_d_e"),
            ("a\x00b\u2028c", 255, "a_b_c"),
            ("\u30d7\u30ed\u30b0\u30e9\u30e0", 255, "\u30d7\u30ed\u30b0\u30e9\u30e0"),
            ("", 255, NILVALUE),
            (None, 255, NILVALUE),
            ("A" * 1000, APP_NAME_MAX, "A" * APP_NAME_MAX),
            # 2-byte characters: the half-cut one is dropped
            ("\u043f" * 100, 5, "\u043f\u043f"),
            ("\u043f", 1, NILVALUE),
        ],
        ids=["whitespace", "control", "non_ascii", "empty", "none", "over_long", "multibyte_cut", "cut_to_empty"],
    )
    def test_sanitize(self, value: str | None, max_bytes: int, expected: str) -> None:
        assert _sanitize_header_field(value, max_bytes) == expected

    # Octet-counted framing forces escape_newlines off, so it takes the same path as False
    @pytest.mark.parametrize("escape_newlines", [None, False])
    def test_header_fields_sanitized_regardless_of_escape_newlines(self, escape_newlines: bool | None) -> None:
        client, sock = _make_client(max_msg=4096, escape_newlines=escape_newlines)
        _log(client, hostname="h\nX", program="a\r" + "x" * 100, pid="1\nZ", msgid="i\x00D")
        assert bytes(sock.sent) == b"<14>1 2024-01-01T00:00:00.000000Z h_X a_" + b"x" * 46 + b" 1_Z i_D hello\n"
