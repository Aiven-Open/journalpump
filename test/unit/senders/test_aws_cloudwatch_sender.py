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
        sender.max_batch_size = 2 * (len(message[0]) + sender.batch_message_overhead) + 1
        assert sender.next_batch_length([message] * 5) == 2


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

    def test_skipped_events_are_counted_once_when_a_mixed_batch_is_retried(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        old = _message(NOW - 15 * DAY, "old")
        recent = _message(NOW - HOUR, "recent")
        stats_increase: Any = sender.stats.increase

        def skipped_increments() -> list[int]:
            return [
                call.kwargs["inc_value"]
                for call in stats_increase.call_args_list
                if call.args[0] == "journal.aws_cloudwatch.skipped_events"
            ]

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            for _ in range(2):
                stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
                _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(recent))

            assert sender.send_messages(messages=[old[0], recent[0]], cursor="c2") is False
            assert skipped_increments() == []
            assert sender.send_messages(messages=[old[0], recent[0]], cursor="c2") is False
            assert skipped_increments() == []
            assert sender.send_messages(messages=[old[0], recent[0]], cursor="c2") is True
            stubber.assert_no_pending_responses()

        assert skipped_increments() == [1]

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


def _add_init_responses(stubber: Stubber) -> None:
    stubber.add_response("create_log_group", {"ResponseMetadata": {"HTTPStatusCode": 200}})
    stubber.add_response("create_log_stream", {"ResponseMetadata": {"HTTPStatusCode": 200}})
    stubber.add_response(
        "describe_log_streams",
        {"logStreams": [{"logStreamName": "stream"}]},
        {"logGroupName": "group"},
    )


def _add_invalid_parameter_error(stubber: Stubber) -> None:
    stubber.add_client_error(
        "put_log_events",
        service_error_code="InvalidParameterException",
        service_message="The batch of log events in a single PutLogEvents request cannot span more than 24 hours.",
        http_status_code=400,
    )


class TestInvalidBatch:
    def test_invalid_parameter_drops_batch(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        sender._backoff_attempt = 3  # pylint: disable=protected-access
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            _add_invalid_parameter_error(stubber)
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        sender.stats.increase.assert_any_call(  # type: ignore[attr-defined]
            "journal.aws_cloudwatch.dropped_events", inc_value=1, tags=mock.ANY
        )
        health = sender.get_state()["health"]
        assert health["status"] == "disconnected"
        assert len(health["errors"]) == 1
        assert health["errors"][0].startswith("Dropped a batch CloudWatch rejected as invalid: The batch of log events")
        assert sender._backoff_attempt == 3  # pylint: disable=protected-access

    def test_dropped_mixed_batch_reports_skipped_events(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        old = _message(NOW - 15 * DAY, "old")
        recent = _message(NOW - HOUR, "recent")

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            _add_invalid_parameter_error(stubber)
            assert sender.send_messages(messages=[old[0], recent[0]], cursor="c2") is True
            stubber.assert_no_pending_responses()

        sender.stats.increase.assert_any_call(  # type: ignore[attr-defined]
            "journal.aws_cloudwatch.skipped_events", inc_value=1, tags=mock.ANY
        )

    def test_sender_reconnects_after_dropped_batch(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        dropped = _message(NOW - 2 * HOUR)
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            _add_invalid_parameter_error(stubber)
            assert sender.send_messages(messages=[dropped[0]], cursor="c1") is True

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c2") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c2"  # pylint: disable=protected-access
        assert sender.get_state()["health"] == {"status": "connected", "elapsed": mock.ANY, "errors": []}


class TestBackoff:
    def test_backoff_grows_across_failed_sends_and_resets_after_success(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            for _ in range(2):
                stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
                _add_init_responses(stubber)
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert sender._backoff_attempt == 2  # pylint: disable=protected-access

            stubber.add_response("put_log_events", _put_response())
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            assert sender._backoff_attempt == 0  # pylint: disable=protected-access
            stubber.assert_no_pending_responses()
