from journalpump.senders.rsyslog import RsyslogSender
from typing import Any
from unittest import mock

import datetime
import json
import pytest


def _make_sender() -> RsyslogSender:
    return RsyslogSender(
        config={"rsyslog_server": "127.0.0.1", "rsyslog_port": 514},
        name="test-rsyslog",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )


def _encode(data: dict[str, Any]) -> bytes:
    return json.dumps(data).encode("utf-8")


class TestRsyslogSenderMissingTimestamp:
    def test_send_succeeds_with_timestamp(self) -> None:
        """Normal path: message with timestamp is forwarded without warnings."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        msg = _encode(
            {
                "MESSAGE": "hello",
                "timestamp": "2025-06-26T14:52:33.581000",
                "PRIORITY": "6",
            }
        )

        with mock.patch.object(sender.log, "warning") as mock_warn:
            result = sender.send_messages(messages=[msg], cursor="c1")

        assert result is True
        sender.rsyslog_client.log.assert_called_once()
        call_kwargs = sender.rsyslog_client.log.call_args.kwargs
        assert call_kwargs["timestamp"] == "2025-06-26T14:52:33.581000Z"
        mock_warn.assert_not_called()

    def test_send_succeeds_without_timestamp(self) -> None:
        """Missing timestamp does not crash; a fallback timestamp is used instead."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        msg = _encode({"MESSAGE": "hello", "PRIORITY": "6"})

        result = sender.send_messages(messages=[msg], cursor="c1")

        assert result is True
        sender.rsyslog_client.log.assert_called_once()

    def test_fallback_timestamp_is_valid_iso(self) -> None:
        """Fallback timestamp produced when field is absent is a valid ISO-8601 string."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        msg = _encode({"MESSAGE": "no-ts"})
        before = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        sender.send_messages(messages=[msg], cursor="c1")
        after = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

        call_kwargs = sender.rsyslog_client.log.call_args.kwargs
        ts_str = call_kwargs["timestamp"]
        # Strip trailing "Z" and parse
        parsed = datetime.datetime.fromisoformat(ts_str.rstrip("Z"))
        assert before <= parsed <= after

    def test_warning_logged_when_timestamp_missing(self) -> None:
        """A warning is emitted for every message that lacks a timestamp field."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        msg = _encode({"MESSAGE": "no-ts"})

        with mock.patch.object(sender.log, "warning") as mock_warn:
            sender.send_messages(messages=[msg], cursor="c1")

        mock_warn.assert_called_once()
        assert "timestamp" in mock_warn.call_args.args[0].lower()

    def test_truncated_message_without_timestamp_does_not_crash(self) -> None:
        """Messages produced by _truncate_long_message (no timestamp) are handled gracefully."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        truncated = _encode(
            {
                "error": "too large message 3101559 bytes vs maximum 1048576 bytes",
                "partial_data": '{"MESSAGE": "AUDI...',
            }
        )

        result = sender.send_messages(messages=[truncated], cursor="c1")

        assert result is True
        sender.rsyslog_client.log.assert_called_once()

    @pytest.mark.parametrize(
        "priority,expected_severity",
        [
            ("3", 3),
            ("6", 6),
        ],
    )
    def test_priority_used_as_severity(self, priority: str, expected_severity: int) -> None:
        """PRIORITY field maps to the syslog severity in the outgoing message."""
        sender = _make_sender()
        sender.rsyslog_client = mock.Mock()

        msg = _encode({"MESSAGE": "test", "timestamp": "2025-01-01T00:00:00.000000", "PRIORITY": priority})
        sender.send_messages(messages=[msg], cursor="c1")

        call_kwargs = sender.rsyslog_client.log.call_args.kwargs
        assert call_kwargs["severity"] == expected_severity
