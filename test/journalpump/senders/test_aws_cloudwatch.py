from botocore.stub import Stubber
from journalpump.senders import AWSCloudWatchSender
from unittest import mock

import boto3


def test_awscloudwatch_sender():
    logs = boto3.client("logs", region_name="us-east-1")

    with Stubber(logs) as stubber:
        stubber.add_client_error("create_log_group", service_error_code="ResourceAlreadyExistsException")
        stubber.add_response("create_log_stream", {"ResponseMetadata": {"HTTPStatusCode": 200}})
        stubber.add_response(
            "describe_log_streams", {"logStreams": [{
                "logStreamName": "stream",
                "uploadSequenceToken": "token"
            }]}, {"logGroupName": "group"}
        )
        sender = AWSCloudWatchSender(
            name="awscloudwatch",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config={
                "aws_cloudwatch_log_group": "group",
                "aws_cloudwatch_log_stream": "stream"
            },
            aws_cloudwatch_logs=logs
        )
        assert sender._next_sequence_token == "token"  # pylint: disable=protected-access

    with Stubber(logs) as stubber:
        stubber.add_response(
            "put_log_events", {
                "ResponseMetadata": {
                    "HTTPStatusCode": 200
                },
                "nextSequenceToken": "token1",
                "rejectedLogEventsInfo": {}
            }
        )
        sender.send_messages(messages=[b'{"REALTIME_TIMESTAMP": 1590581737.308352}'], cursor=None)
        assert sender._next_sequence_token == "token1"  # pylint: disable=protected-access
        assert sender._sent_count == 1  # pylint: disable=protected-access

    with Stubber(logs) as stubber:
        expected_args = {
            "logGroupName": "group",
            "logStreamName": "stream",
            "logEvents": [{
                "timestamp": 123456789000,
                "message": '{"MESSAGE": "Hello World!"}'
            }],
            "sequenceToken": "token1"
        }
        stubber.add_response(
            "put_log_events", {
                "ResponseMetadata": {
                    "HTTPStatusCode": 200
                },
                "nextSequenceToken": "token2",
                "rejectedLogEventsInfo": {}
            }, expected_args
        )
        with mock.patch("time.time", return_value=123456789):
            sender.send_messages(messages=[b'{"MESSAGE": "Hello World!"}'], cursor=None)
        assert sender._next_sequence_token == "token2"  # pylint: disable=protected-access
        assert sender._sent_count == 2  # pylint: disable=protected-access

    with Stubber(logs) as stubber:
        expected_args = {
            "logGroupName": "group",
            "logStreamName": "stream",
            "logEvents": [{
                "timestamp": 1590581737308,
                "message": '{"REALTIME_TIMESTAMP": 1590581737.308352}'
            }],
            "sequenceToken": "token2"
        }
        stubber.add_response(
            "put_log_events", {
                "ResponseMetadata": {
                    "HTTPStatusCode": 200
                },
                "nextSequenceToken": "token2",
                "rejectedLogEventsInfo": {}
            }, expected_args
        )
        sender.send_messages(messages=[b'{"REALTIME_TIMESTAMP": 1590581737.308352}'], cursor=None)
        assert sender._next_sequence_token == "token2"  # pylint: disable=protected-access
        assert sender._sent_count == 3  # pylint: disable=protected-access

    with Stubber(logs) as stubber:
        stubber.add_client_error("put_log_events", service_error_code="ThrottlingException")
        stubber.add_response("create_log_group", {"ResponseMetadata": {"HTTPStatusCode": 200}})
        stubber.add_response("create_log_stream", {"ResponseMetadata": {"HTTPStatusCode": 200}})
        stubber.add_response(
            "describe_log_streams", {"logStreams": [{
                "logStreamName": "stream",
                "uploadSequenceToken": "token"
            }]}, {"logGroupName": "group"}
        )
        sender.send_messages(messages=[b'{"REALTIME_TIMESTAMP": 1590581737.308352}'], cursor=None)
        assert sender._connected  # pylint: disable=protected-access
        assert sender._sent_count == 3  # pylint: disable=protected-access

    with Stubber(logs) as stubber:
        stubber.add_client_error("put_log_events", service_error_code="InternalFailure")
        stubber.add_response("create_log_group", {"ResponseMetadata": {"HTTPStatusCode": 200}})
        stubber.add_response("create_log_stream", {"ResponseMetadata": {"HTTPStatusCode": 200}})
        stubber.add_response(
            "describe_log_streams", {"logStreams": [{
                "logStreamName": "stream",
                "uploadSequenceToken": "token"
            }]}, {"logGroupName": "group"}
        )
        sender.send_messages(messages=[b'{"REALTIME_TIMESTAMP": 1590581737.308352}'], cursor=None)
        assert sender._connected  # pylint: disable=protected-access
        assert sender._sent_count == 3  # pylint: disable=protected-access
