from botocore.exceptions import EndpointConnectionError
from botocore.stub import Stubber
from collections.abc import Iterator
from journalpump.senders.aws_cloudwatch import (
    AWSCloudWatchSender,
    MAX_BATCH_EVENTS,
    MAX_BATCH_SPAN_MS,
    MAX_EVENT_AGE_MS,
    MAX_INIT_TRIES,
)
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


def _connection_error() -> EndpointConnectionError:
    return EndpointConnectionError(endpoint_url="https://logs.us-east-1.amazonaws.com")


def _assert_disconnected_with_error(sender: AWSCloudWatchSender, fragment: str) -> None:
    health = sender.get_state()["health"]
    assert health["status"] == "disconnected"
    assert len(health["errors"]) == 1
    assert fragment in health["errors"][0]


def _stop_after_send_attempts(sender: AWSCloudWatchSender, max_attempts: int) -> None:
    """Make the sender loop stop after max_attempts send_messages calls, so a regression fails instead of hanging."""
    real_send = sender.send_messages
    attempts = 0

    def counting_send(*, messages: list[bytes], cursor: str | None) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts >= max_attempts:
            sender.running = False
        return real_send(messages=messages, cursor=cursor)

    sender.send_messages = counting_send  # type: ignore[method-assign]


class TestReconnectFailure:
    def test_reconnect_raising_before_send_keeps_batch(self) -> None:
        sender, logs = _make_sender()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            with mock.patch.object(logs, "create_log_group", side_effect=_connection_error()):
                assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert sender._sent_cursor is None  # pylint: disable=protected-access
            assert sender._sent_count == 0  # pylint: disable=protected-access
            _assert_disconnected_with_error(sender, "EndpointConnectionError")
            assert sender._backoff_attempt == 1  # pylint: disable=protected-access
            sender.stats.unexpected_exception.assert_called()  # type: ignore[attr-defined]

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access
        assert sender._backoff_attempt == 0  # pylint: disable=protected-access

    def test_reconnect_raising_after_failed_put_keeps_batch(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
            with mock.patch.object(logs, "create_log_group", side_effect=_connection_error()):
                assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert sender._sent_cursor is None  # pylint: disable=protected-access
            assert sender._sent_count == 0  # pylint: disable=protected-access
            _assert_disconnected_with_error(sender, "EndpointConnectionError")
            # The failed put already backed off for this attempt, the failed reconnect must not back off again
            assert sender._backoff_attempt == 1  # pylint: disable=protected-access

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access
        assert sender._backoff_attempt == 0  # pylint: disable=protected-access

    def test_reconnect_client_error_exhausting_attempts_keeps_batch(self) -> None:
        sender, logs = _make_sender()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            for _ in range(MAX_INIT_TRIES):
                stubber.add_client_error("create_log_group", service_error_code="ServiceUnavailableException")
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            stubber.assert_no_pending_responses()
            assert sender._sent_cursor is None  # pylint: disable=protected-access
            assert sender._sent_count == 0  # pylint: disable=protected-access
            _assert_disconnected_with_error(sender, "ServiceUnavailableException")
            # _init_log_group backed off between its attempts, nothing is added on top
            assert sender._backoff_attempt == MAX_INIT_TRIES - 1  # pylint: disable=protected-access

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_access_denied_exhausting_attempts_keeps_batch(self) -> None:
        sender, logs = _make_sender()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            for _ in range(MAX_INIT_TRIES):
                stubber.add_client_error(
                    "create_log_group",
                    service_error_code="AccessDeniedException",
                    service_message="not authorized to create log groups",
                    http_status_code=400,
                )
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            stubber.assert_no_pending_responses()
            assert sender._sent_cursor is None  # pylint: disable=protected-access
            assert sender._sent_count == 0  # pylint: disable=protected-access
            # The customer-visible health error must carry the real AWS error, not a bare exception name
            _assert_disconnected_with_error(sender, "AccessDeniedException")
            _assert_disconnected_with_error(sender, "not authorized to create log groups")

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_non_2xx_put_response_backs_off_and_keeps_batch(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response("put_log_events", {"ResponseMetadata": {"HTTPStatusCode": 503}}, _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            stubber.assert_no_pending_responses()
            assert sender._sent_cursor is None  # pylint: disable=protected-access
            assert sender._sent_count == 0  # pylint: disable=protected-access
            _assert_disconnected_with_error(sender, "503")
            assert sender._backoff_attempt == 1  # pylint: disable=protected-access

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.assert_no_pending_responses()

        assert sender._sent_cursor == "c1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_failed_reconnect_does_not_lose_later_batches(self) -> None:
        sender, logs = _make_sender()
        first = _message(NOW - 3 * HOUR, "first")
        second = _message(NOW - 2 * HOUR, "second")
        third = _message(NOW - HOUR, "third")
        for message in (first, second, third):
            sender.msg_buffer.add_item(item=message[0], cursor=message[1])
        group_created = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        with (
            mock.patch("time.time", return_value=NOW),
            mock.patch("journalpump.senders.aws_cloudwatch.MAX_BATCH_EVENTS", 2),
            mock.patch.object(logs, "create_log_group", side_effect=[_connection_error(), group_created]),
            Stubber(logs) as stubber,
        ):
            stubber.add_response("create_log_stream", {"ResponseMetadata": {"HTTPStatusCode": 200}})
            stubber.add_response(
                "describe_log_streams",
                {"logStreams": [{"logStreamName": "stream"}]},
                {"logGroupName": "group"},
            )
            stubber.add_response("put_log_events", _put_response(), _put_args(first, second))
            stubber.add_response("put_log_events", _put_response(), {**_put_args(third), "sequenceToken": "token"})
            sender.running = True
            _stop_after_send_attempts(sender, max_attempts=10)
            sender.get_and_send_messages()
            stubber.assert_no_pending_responses()

        assert sender._sent_count == 3  # pylint: disable=protected-access
        assert sender._sent_cursor == third[1]  # pylint: disable=protected-access
        assert sender._backoff_attempt == 0  # pylint: disable=protected-access


def _sleeps(sleep: mock.Mock) -> list[float]:
    return [call.args[0] for call in sleep.call_args_list]


class TestBackoffDurations:
    """The sleep arguments follow base.py _backoff: min(1800, 0.5 * 2**attempt) / 2, plus jitter in [0, 1) of that.

    random.random is pinned to 0.5, so the sleep is 1.5 * min(1800, 0.5 * 2**attempt) / 2.
    """

    def test_failed_sends_through_the_reconnect_path_sleep_longer_each_time(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        message = _message(NOW - HOUR)

        with (
            mock.patch("time.time", return_value=NOW),
            mock.patch("random.random", return_value=0.5),
            mock.patch("time.sleep") as sleep,
            Stubber(logs) as stubber,
        ):
            for _ in range(3):
                stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
                _add_init_responses(stubber)
            for _ in range(3):
                assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            # One sleep per failed put, and the successful reconnects in between do not reset the delay
            assert _sleeps(sleep) == [0.75, 1.5, 3.0]

            # After a successful send, the next failure starts from the first delay again
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
            _add_init_responses(stubber)
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert _sleeps(sleep) == [0.75, 1.5, 3.0, 0.75]
            stubber.assert_no_pending_responses()

    def test_failing_reconnects_before_send_sleep_longer_each_time(self) -> None:
        sender, logs = _make_sender()
        message = _message(NOW - HOUR)

        with (
            mock.patch("time.time", return_value=NOW),
            mock.patch("random.random", return_value=0.5),
            mock.patch("time.sleep") as sleep,
            Stubber(logs) as stubber,
        ):
            with mock.patch.object(logs, "create_log_group", side_effect=_connection_error()):
                for _ in range(3):
                    assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert _sleeps(sleep) == [0.75, 1.5, 3.0]

            _add_init_responses(stubber)
            stubber.add_response("put_log_events", _put_response(), _put_args(message))
            assert sender.send_messages(messages=[message[0]], cursor="c1") is True
            assert _sleeps(sleep) == [0.75, 1.5, 3.0]

            sender.mark_disconnected()
            with mock.patch.object(logs, "create_log_group", side_effect=_connection_error()):
                assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert _sleeps(sleep) == [0.75, 1.5, 3.0, 0.75]

    def test_exhausted_init_attempts_keep_growing_the_delay_across_sends(self) -> None:
        sender, logs = _make_sender()
        message = _message(NOW - HOUR)

        with (
            mock.patch("time.time", return_value=NOW),
            mock.patch("random.random", return_value=0.5),
            mock.patch("time.sleep") as sleep,
            Stubber(logs) as stubber,
        ):
            for _ in range(2 * MAX_INIT_TRIES):
                stubber.add_client_error("create_log_group", service_error_code="ServiceUnavailableException")
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False
            stubber.assert_no_pending_responses()

        # Each send sleeps between its init attempts only, the failed reconnect adds nothing on top, and the
        # second send continues where the first one stopped
        assert _sleeps(sleep) == [0.75, 1.5, 3.0, 6.0]

    def test_delay_is_capped(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        sender._backoff_attempt = 50  # pylint: disable=protected-access
        message = _message(NOW - HOUR)

        with (
            mock.patch("time.time", return_value=NOW),
            mock.patch("random.random", return_value=0.5),
            mock.patch("time.sleep") as sleep,
            Stubber(logs) as stubber,
        ):
            stubber.add_client_error("put_log_events", service_error_code="ThrottlingException", http_status_code=400)
            _add_init_responses(stubber)
            assert sender.send_messages(messages=[message[0]], cursor="c1") is False

        assert _sleeps(sleep) == [1350.0]


class _PutRecorder:
    """Stands in for put_log_events: records every request and succeeds.

    It never raises, because send_messages would swallow that and the sender loop would retry forever. Instead, once
    more than max_calls requests were made it stops the sender loop, so the test fails on its assertions.
    """

    def __init__(self, sender: AWSCloudWatchSender, max_calls: int) -> None:
        self.sender = sender
        self.max_calls = max_calls
        self.requests: list[list[dict[str, Any]]] = []

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs["logEvents"])
        if len(self.requests) > self.max_calls:
            self.sender.running = False
        return _put_response()


def _drive(sender: AWSCloudWatchSender, logs: Any, messages: list[tuple[bytes, str]], max_calls: int) -> _PutRecorder:
    sender.mark_connected()
    for body, cursor in messages:
        sender.msg_buffer.add_item(item=body, cursor=cursor)
    recorder = _PutRecorder(sender, max_calls)
    sender.running = True
    with mock.patch("time.time", return_value=NOW), mock.patch.object(logs, "put_log_events", side_effect=recorder):
        sender.get_and_send_messages()
    return recorder


class TestBatchLimitsThroughSendPath:
    def test_25000_small_events_are_split_into_requests_of_at_most_10000(self) -> None:
        sender, logs = _make_sender()
        # Isolate the event count limit from the size limit
        sender.max_batch_size = 10**9
        messages = [_message(NOW - HOUR, "x") for _ in range(25_000)]

        recorder = _drive(sender, logs, messages, max_calls=3)

        assert [len(request) for request in recorder.requests] == [MAX_BATCH_EVENTS, MAX_BATCH_EVENTS, 5_000]
        assert sender._sent_count == 25_000  # pylint: disable=protected-access
        assert sender._sent_cursor == messages[-1][1]  # pylint: disable=protected-access

    def test_events_spanning_more_than_24_hours_are_split_sorted_and_sent_once(self) -> None:
        sender, logs = _make_sender()
        # 240 events, one every 30 minutes, covering 119.5 hours. A batch may not span 24 hours or more, which is 48
        # intervals of 30 minutes, so a batch holds at most 48 events (47 intervals = 23.5 h) and the 49th starts the
        # next one. 240 / 48 = exactly 5 requests of 48 events. Neighbouring events are swapped in pairs so events
        # inside a request arrive out of order; batches hold an even number of events, so no pair straddles a cut.
        ordered = [_message(NOW - 6 * DAY + index * HOUR / 2, f"event-{index}") for index in range(240)]
        messages = [ordered[index ^ 1] for index in range(240)]

        recorder = _drive(sender, logs, messages, max_calls=5)

        assert [len(request) for request in recorder.requests] == [48] * 5
        for request in recorder.requests:
            timestamps = [event["timestamp"] for event in request]
            assert timestamps == sorted(timestamps)
            assert max(timestamps) - min(timestamps) < MAX_BATCH_SPAN_MS
        for earlier, later in zip(recorder.requests, recorder.requests[1:]):
            assert earlier[-1]["timestamp"] < later[0]["timestamp"]
        sent = [event["message"] for request in recorder.requests for event in request]
        assert sorted(sent) == sorted(body.decode("utf8") for body, _ in messages)
        assert sender._sent_count == len(messages)  # pylint: disable=protected-access


class TestEventAgeBoundary:
    def test_event_at_the_cutoff_is_sent_and_older_one_is_skipped(self) -> None:
        sender, logs = _make_sender()
        sender.mark_connected()
        cutoff_ms = int(NOW * 1000.0) - MAX_EVENT_AGE_MS

        def message_at(timestamp_ms: int, text: str) -> tuple[bytes, str]:
            message = _message(timestamp_ms / 1000.0, text)
            # Guard against float rounding moving the event off the intended millisecond
            assert int(json.loads(message[0])["REALTIME_TIMESTAMP"] * 1000.0) == timestamp_ms
            return message

        too_old = message_at(cutoff_ms - 1, "too-old")
        at_cutoff = message_at(cutoff_ms, "at-cutoff")
        just_inside = message_at(cutoff_ms + 1, "just-inside")

        with mock.patch("time.time", return_value=NOW), Stubber(logs) as stubber:
            stubber.add_response("put_log_events", _put_response(), _put_args(at_cutoff, just_inside))
            assert sender.send_messages(messages=[too_old[0], at_cutoff[0], just_inside[0]], cursor="c3") is True
            stubber.assert_no_pending_responses()

        sender.stats.increase.assert_any_call(  # type: ignore[attr-defined]
            "journal.aws_cloudwatch.skipped_events", inc_value=1, tags=mock.ANY
        )
        assert sender._sent_count == 2  # pylint: disable=protected-access
