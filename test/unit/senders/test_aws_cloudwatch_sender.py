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
