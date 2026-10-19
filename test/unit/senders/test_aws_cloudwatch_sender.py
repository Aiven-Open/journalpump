from botocore.stub import Stubber
from collections.abc import Iterator
from journalpump.senders.aws_cloudwatch import AWSCloudWatchSender, MAX_BATCH_EVENTS
from typing import Any
from unittest import mock

import botocore.session
import json
import pytest

NOW = 1_790_000_000.0
HOUR = 3600.0
DAY = 24 * HOUR


@pytest.fixture(autouse=True)
def _no_sleep() -> Iterator[None]:
    with mock.patch("time.sleep"):
        yield


def _make_sender() -> tuple[AWSCloudWatchSender, Any]:
    logs = botocore.session.get_session().create_client("logs", region_name="us-east-1")
    sender = AWSCloudWatchSender(
        name="awscloudwatch",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
        config={"aws_cloudwatch_log_group": "group", "aws_cloudwatch_log_stream": "stream"},
        aws_cloudwatch_logs=logs,
    )
    return sender, logs


def _message(timestamp: float, text: str = "hello") -> tuple[bytes, str]:
    return json.dumps({"MESSAGE": text, "REALTIME_TIMESTAMP": timestamp}).encode("utf8"), f"cursor-{timestamp}"


class TestNextBatchLength:
    def test_cuts_before_batch_spans_24_hours(self) -> None:
        sender, _ = _make_sender()
        messages = [_message(NOW), _message(NOW + 12 * HOUR), _message(NOW + 23.9 * HOUR), _message(NOW + 24 * HOUR)]
        assert sender.next_batch_length(messages) == 3

    def test_span_counts_out_of_order_events(self) -> None:
        sender, _ = _make_sender()
        messages = [_message(NOW + 20 * HOUR), _message(NOW), _message(NOW + 24 * HOUR)]
        assert sender.next_batch_length(messages) == 2

    def test_cuts_at_max_events(self) -> None:
        sender, _ = _make_sender()
        messages = [_message(NOW) for _ in range(MAX_BATCH_EVENTS + 1)]
        assert sender.next_batch_length(messages) == MAX_BATCH_EVENTS

    def test_keeps_size_limit(self) -> None:
        sender, _ = _make_sender()
        message = _message(NOW)
        num_messages_limit = 2
        sender.max_batch_size = num_messages_limit * (len(message[0]) + sender.batch_message_overhead) + 1
        # We want to send 5 messages, but the limit is 2, so we should send 2 messages
        assert sender.next_batch_length([message] * 5) == num_messages_limit


def _put_response(**extra: Any) -> dict[str, Any]:
    return {"ResponseMetadata": {"HTTPStatusCode": 200}, "nextSequenceToken": "token", **extra}


def _put_args(*messages: tuple[bytes, str]) -> dict[str, Any]:
    return {
        "logGroupName": "group",
        "logStreamName": "stream",
        "logEvents": [
            {"timestamp": int(json.loads(body)["REALTIME_TIMESTAMP"] * 1000.0), "message": body.decode("utf8")}
            for body, _ in messages
        ],
    }


class TestSendMessages:
    def test_sends_events_in_timestamp_order(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        later = _message(NOW - HOUR, "later")
        earlier = _message(NOW - 2 * HOUR, "earlier")

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response("put_log_events", _put_response(), _put_args(earlier, later))
            assert sender.send_messages(messages=[later[0], earlier[0]], cursor="c2") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c2"  # pylint: disable=protected-access

    def test_skips_events_older_than_14_days(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        old = _message(NOW - 15 * DAY, "old")
        recent = _message(NOW - HOUR, "recent")

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response("put_log_events", _put_response(), _put_args(recent))
            assert sender.send_messages(messages=[old[0], recent[0]], cursor="c2") is True
            stubber.assert_no_pending_responses()

        sender.stats.increase.assert_any_call(  # type: ignore[attr-defined]
            "journal.aws_cloudwatch.skipped_events", inc_value=1, tags=mock.ANY
        )
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_batch_of_only_old_events_is_confirmed_without_a_request(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        old = _message(NOW - 15 * DAY)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            assert sender.send_messages(messages=[old[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access

    def test_batch_of_only_old_events_does_not_connect_the_sender(self) -> None:
        sender, logs = _make_sender()
        sender._backoff_attempt = 3  # pylint: disable=protected-access
        old = _message(NOW - 15 * DAY)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            assert sender.send_messages(messages=[old[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._connected is False  # pylint: disable=protected-access
        assert sender._backoff_attempt == 3  # pylint: disable=protected-access

    def test_reports_partially_rejected_batch(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response(
                "put_log_events",
                _put_response(rejectedLogEventsInfo={"tooNewLogEventStartIndex": 0}),
                _put_args(message),
            )
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True

        sender.stats.increase.assert_any_call(  # type: ignore[attr-defined]
            "journal.aws_cloudwatch.partially_rejected_batches", tags=mock.ANY
        )

    def test_response_without_sequence_token_is_accepted(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response("put_log_events", {"ResponseMetadata": {"HTTPStatusCode": 200}}, _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
