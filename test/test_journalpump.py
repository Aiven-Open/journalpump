from .data import GCP_PRIVATE_KEY
from botocore.stub import Stubber
from collections import OrderedDict
from datetime import datetime
from journalpump.journalpump import FieldFilter, JournalObject, JournalObjectHandler, JournalPump, JournalReader, PumpReader
from journalpump.senders import (
    AWSCloudWatchSender, ElasticsearchSender, GoogleCloudLoggingSender, KafkaSender, LogplexSender, RsyslogSender
)
from journalpump.senders.aws_cloudwatch import MAX_INIT_TRIES
from journalpump.senders.base import MAX_KAFKA_MESSAGE_SIZE, MsgBuffer, SenderInitializationError
from journalpump.util import default_json_serialization
from time import sleep
from unittest import mock, TestCase

import boto3
import json
import pytest
import responses


def test_journalpump_init(tmpdir):  # pylint: disable=too-many-statements
    # Logplex sender
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {
        "field_filters": {
            "filter_a": {
                "fields": ["message"]
            }
        },
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "field_filter": "filter_a",
                        "logplex_token": "foo",
                        "logplex_log_input_url": "http://logplex.com",
                        "output_type": "logplex",
                    },
                },
            },
        },
    }

    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.field_filters) == 1
    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            assert isinstance(s, LogplexSender)
            assert s.field_filter.name == "filter_a"
            assert s.field_filter.fields == ["message"]

    # Kafka sender
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "output_type": "kafka",
                        "logplex_token": "foo",
                        "kafka_address": "localhost",
                        "kafka_topic": "foo",
                    },
                },
            },
        },
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            assert isinstance(s, KafkaSender)

    # Elasticsearch sender
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "output_type": "elasticsearch",
                        "elasticsearch_url": "https://foo.aiven.io",
                        "elasticsearch_index_prefix": "fooprefix",
                    },
                },
            },
        },
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            assert isinstance(s, ElasticsearchSender)

    # rsyslog sender
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "output_type": "rsyslog",
                        "rsyslog_server": "127.0.0.1",
                        "rsyslog_port": 514,
                    },
                },
            },
        },
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            assert isinstance(s, RsyslogSender)

    # AWS CloudWatch sender
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "output_type": "aws_cloudwatch",
                        "aws_cloudwatch_log_group": "group",
                        "aws_cloudwatch_log_stream": "stream",
                        "aws_region": "us-east-1",
                        "aws_access_key_id": "key",
                        "aws_secret_access_key": "secret"
                    }
                }
            }
        }
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    class MockCloudWatchPaginator(mock.Mock):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Paginate over three pages
            self.paginate = mock.Mock(
                return_value=[{
                    "logStreams": [{
                        "logStreamName": "page1",
                        "uploadSequenceToken": "page1"
                    }]
                }, {
                    "logStreams": [{
                        "logStreamName": "stream",
                        "uploadSequenceToken": "token"
                    }]
                }, {
                    "logStreams": [{
                        "logStreamName": "page3",
                        "uploadSequenceToken": "page3"
                    }]
                }]
            )

    class MockCloudWatch(mock.Mock):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.create_log_group = mock.Mock(return_value=None)
            self.create_log_stream = mock.Mock(return_value=None)
            self.get_paginator = MockCloudWatchPaginator()

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch("boto3.client", new=MockCloudWatch()) as mock_client, \
                mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
            assert len(r.senders) == 1
            mock_client.assert_called_once_with(
                "logs", region_name="us-east-1", aws_access_key_id="key", aws_secret_access_key="secret"
            )
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            # pylint: disable=protected-access
            s._logs.create_log_group.assert_called_once_with(logGroupName="group")
            s._logs.create_log_stream.assert_called_once_with(logGroupName="group", logStreamName="stream")
            s._logs.get_paginator.assert_called_once_with("describe_log_streams")
            s._logs.get_paginator().paginate.assert_called_once_with(logGroupName="group")
            assert s._next_sequence_token == "token"
            # pylint: enable=protected-access
            assert isinstance(s, AWSCloudWatchSender)

    # Google Cloud Logging sender
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "output_type": "google_cloud_logging",
                        "google_cloud_logging_project_id": "project-id",
                        "google_cloud_logging_log_id": "log-id",
                        "google_service_account_credentials": {
                            "type": "service_account",
                            "project_id": "project-id",
                            "private_key_id": "abcdefg",
                            "private_key": GCP_PRIVATE_KEY,
                            "client_email": "test@project-id.iam.gserviceaccount.com",
                            "client_id": "123456789",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40project-id.iam.gserviceaccount.com"  # pylint:disable=line-too-long
                        }
                    }
                }
            }
        }
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        with mock.patch.object(PumpReader, "has_persistent_files", return_value=True):
            r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            s.join()
            assert isinstance(s, GoogleCloudLoggingSender)


def test_journal_reader_tagging(tmpdir):
    config = {
        "readers": {
            "system": {
                "journal_flags": ["SYSTEM"],
                "searches": [
                    {
                        "name": "kernel.cpu.temperature",
                        "fields": {
                            "MESSAGE": r"(?P<cpu>CPU\d+): .*temperature.*",
                            "SYSLOG_IDENTIFIER": r"^(?P<from>.*)$",
                            "PRIORITY": r"^(?P<level>[0-4])$",  # emergency, alert, critical, error
                            "SYSLOG_FACILITY": r"^0$",  # kernel only
                        },
                        "tags": {
                            "section": "cputemp"
                        },
                    },
                    {
                        "name": "noresults",
                        "fields": {
                            "MESSAGE": "(?P<msg>.*)",
                            "nosuchfield": ".*",
                        },
                    },
                ],
            },
        },
    }
    journalpump_path = str(tmpdir.join("journalpump.json"))
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    pump = JournalPump(journalpump_path)
    reader = pump.readers["system"]

    # matching entry
    entry = JournalObject(
        entry={
            "MESSAGE": "CPU0: Core temperature above threshold, cpu clock throttled (total events = 1)",
            "PRIORITY": "2",
            "SYSLOG_FACILITY": "0",
            "SYSLOG_IDENTIFIER": "kernel",
        }
    )
    result = reader.perform_searches(entry)
    expected = {
        "kernel.cpu.temperature": {
            "cpu": "CPU0",
            "from": "kernel",
            "level": "2",
            "section": "cputemp",
        }
    }
    assert result == expected

    # some fields are not matching
    entry = JournalObject(
        entry={
            "MESSAGE": "CPU1: on fire",
            "PRIORITY": "1",
            "SYSLOG_FACILITY": "0",
            "SYSLOG_IDENTIFIER": "kernel",
        }
    )
    result = reader.perform_searches(entry)
    assert result == {}


class TestFieldFilter(TestCase):
    def test_whitelist(self):
        ff = FieldFilter("test", {"fields": ["_foo", "BAR"]})
        data = {"Foo": "a", "_bar": "b", "_zob": "c"}
        assert ff.filter_fields(data) == {"Foo": "a", "_bar": "b"}
        assert data == {"Foo": "a", "_bar": "b", "_zob": "c"}

    def test_blacklist(self):
        ff = FieldFilter("test", {"type": "blacklist", "fields": ["_foo"]})
        data = {"Foo": "a", "_bar": "b", "_zob": "c"}
        assert ff.filter_fields(data) == {"_bar": "b", "_zob": "c"}
        assert data == {"Foo": "a", "_bar": "b", "_zob": "c"}


class TestJournalObjectHandler(TestCase):
    def setUp(self):
        self.filter_a = FieldFilter("filter_a", {"fields": ["a"]})
        self.filter_b = FieldFilter("filter_b", {"fields": ["a", "b"]})
        self.sender_a = mock.Mock()
        self.sender_a.field_filter = self.filter_a
        self.sender_a.extra_field_values = {}
        self.sender_a.msg_buffer = MsgBuffer()
        self.sender_b = mock.Mock()
        self.sender_b.field_filter = self.filter_b
        self.sender_b.extra_field_values = {}
        self.sender_b.msg_buffer = MsgBuffer()
        self.sender_c = mock.Mock()
        self.sender_c.field_filter = None
        self.sender_c.extra_field_values = {}
        self.sender_c.msg_buffer = MsgBuffer()
        self.pump = mock.Mock()
        self.reader = mock.Mock()
        self.reader.senders = {"sender_a": self.sender_a, "sender_b": self.sender_b, "sender_c": self.sender_c}

    def test_filtered_processing(self):
        jobject = JournalObject(entry=OrderedDict(a=1, b=2, c=3, REALTIME_TIMESTAMP=1), cursor=10)
        handler = JournalObjectHandler(jobject, self.reader, self.pump)
        assert handler.process() is True
        sender_a_msgs = [(json.loads(msg.decode("utf-8")), cursor) for msg, cursor in self.sender_a.msg_buffer.messages]
        assert ({"a": 1}, 10) in sender_a_msgs
        sender_b_msgs = [(json.loads(msg.decode("utf-8")), cursor) for msg, cursor in self.sender_b.msg_buffer.messages]
        assert ({"a": 1, "b": 2}, 10) in sender_b_msgs

        largest_data = json.dumps(
            OrderedDict(a=1, b=2, c=3, REALTIME_TIMESTAMP=1, timestamp=datetime.utcfromtimestamp(1)),
            default=default_json_serialization,
        ).encode("utf-8")
        assert len(self.sender_c.msg_buffer.messages) == 1
        self.reader.inc_line_stats.assert_called_once_with(journal_bytes=len(largest_data), journal_lines=1)

    def test_too_large_data(self):
        self.pump.make_tags.return_value = "tags"
        too_large = OrderedDict(a=1, b="x" * MAX_KAFKA_MESSAGE_SIZE)
        jobject = JournalObject(entry=too_large, cursor=10)
        handler = JournalObjectHandler(jobject, self.reader, self.pump)
        assert handler.process() is True
        sender_a_msgs = [(json.loads(msg.decode("utf-8")), cursor) for msg, cursor in self.sender_a.msg_buffer.messages]
        assert ({"a": 1}, 10) in sender_a_msgs
        assert "too large message" in str(self.sender_b.msg_buffer.messages)

        self.pump.stats.increase.assert_called_once_with("journal.read_error", tags="tags")


def test_journalpump_state_file(tmpdir):
    journalpump_path = str(tmpdir.join("journalpump.json"))
    statefile_path = str(tmpdir.join("journalpump_state.json"))
    config = {
        "json_state_file_path": statefile_path,
        "readers": {
            "state_test": {
                "senders": {
                    "fake_syslog": {
                        "output_type": "rsyslog",
                        "rsyslog_server": "127.0.0.1",
                        "rsyslog_port": 514,
                    },
                },
            },
        },
    }

    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))

    pump = JournalPump(journalpump_path)
    for _, reader in pump.readers.items():
        reader.initialize_senders()
        sleep(1.1)
        reader.request_stop()
    pump.save_state()

    with open(statefile_path, "r") as fp:
        state = json.load(fp)

    assert "readers" in state
    assert "start_time" in state
    assert "state_test" in state["readers"]
    reader_state = state["readers"]["state_test"]
    assert reader_state.get("total_bytes") == 0
    assert reader_state.get("total_lines") == 0
    assert "senders" in reader_state
    assert "fake_syslog" in reader_state["senders"]
    sender_state = reader_state["senders"]["fake_syslog"]
    assert "health" in sender_state
    assert "elapsed" in sender_state["health"]
    assert sender_state["health"]["elapsed"] > 1.0
    assert "status" in sender_state["health"]
    assert sender_state["health"]["status"] == "stopped"


@responses.activate
def test_es_sender():
    url = "http://localhost:1234"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url + "/_aliases", json={})
        rsps.add(responses.POST, url + "/journalpump-2019-10-07/_bulk")
        es = ElasticsearchSender(
            name="es", reader=mock.Mock(), stats=mock.Mock(), field_filter=None, config={"elasticsearch_url": url}
        )
        assert es.send_messages(messages=[b'{"timestamp": "2019-10-07 14:00:00"}'], cursor=None)


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


def test_awscloudwatch_sender_init():
    logs = boto3.client("logs", region_name="us-east-1")

    # Test that AWSCloudWatchSender correctly raises SenderInitializationError after
    # aws_cloudwatch.MAX_INIT_TRIES attempts
    with Stubber(logs) as stubber:
        for _ in range(MAX_INIT_TRIES):
            stubber.add_client_error(
                "create_log_group",
                service_error_code="AccessDeniedException",
                http_status_code=400,
            )

        with pytest.raises(SenderInitializationError):
            AWSCloudWatchSender(
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

    # Test that AWSCloudWatchSender initializes correctly when init is retried
    # after an AccessDeniedException
    with Stubber(logs) as stubber:
        stubber.add_client_error(
            "create_log_group",
            service_error_code="AccessDeniedException",
            http_status_code=400,
        )
        stubber.add_response("create_log_group", {"ResponseMetadata": {"HTTPStatusCode": 200}})
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
        # _connected is set to True after initialization is completed
        assert sender._connected  # pylint: disable=protected-access

    # Test that AWSCloudWatchSender initializes correctly when init is retried
    # after a ServiceUnavailable error
    with Stubber(logs) as stubber:
        stubber.add_client_error(
            "create_log_group",
            service_error_code="ServiceUnavailable",
            http_status_code=503,
        )
        stubber.add_response("create_log_group", {"ResponseMetadata": {"HTTPStatusCode": 200}})
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
        # _connected is set to True after initialization is completed
        assert sender._connected  # pylint: disable=protected-access


def test_single_sender_init_fail():
    """Test JournalReader initialize_senders() behavior in the case
    where a sender fails during init and others don't.
    """
    config = {
        "senders": {
            "bar": {
                "output_type": "aws_cloudwatch",
                "aws_cloudwatch_log_group": "group",
                "aws_cloudwatch_log_stream": "stream",
                "aws_region": "us-east-1",
                "aws_access_key_id": "key",
                "aws_secret_access_key": "incorrect"
            },
            "cafe": {
                "output_type": "file",
                "file_output": "/tmp/journalpump_test.log"
            }
        }
    }

    class FailingSender:
        def __init__(self, **kwargs):
            raise SenderInitializationError

    class WorkingSender:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def request_stop(self):
            pass

    # One failing, one working sender
    JournalReader.sender_classes["aws_cloudwatch"] = FailingSender
    JournalReader.sender_classes["file"] = WorkingSender
    journal_reader = JournalReader(
        name="foo",
        config=config,
        field_filters={},
        geoip=None,
        stats=mock.Mock(),
        searches=[],
    )
    # initialize_senders() will eventually get called
    # during the operation and initializes the senders
    # We call it here directly
    journal_reader.initialize_senders()
    # Only file sender "cafe" should be in senders dict
    assert len(journal_reader.senders) == 1
    assert list(journal_reader.senders.keys()) == ["cafe"]
    assert journal_reader._initialized_senders == {"cafe"}  # pylint: disable=protected-access

    # New config creates new instance of JournalReader, so we can just create a new instance
    # Two working senders
    JournalReader.sender_classes["aws_cloudwatch"] = WorkingSender
    JournalReader.sender_classes["file"] = WorkingSender
    journal_reader = JournalReader(
        name="foo",
        config=config,
        field_filters={},
        geoip=None,
        stats=mock.Mock(),
        searches=[],
    )
    journal_reader.initialize_senders()
    # Now we should have both "bar" and "cafe"
    assert len(journal_reader.senders) == 2
    assert sorted(list(journal_reader.senders.keys())) == ["bar", "cafe"]
    assert journal_reader._initialized_senders == {"bar", "cafe"}  # pylint: disable=protected-access


@pytest.mark.parametrize(
    "has_persistent_files,has_runtime_files", ([True, True], [True, False], [False, False], [False, True])
)
def test_journalpump_init_journal_files(tmpdir, has_persistent_files, has_runtime_files):
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {
        "field_filters": {
            "filter_a": {
                "fields": ["message"]
            }
        },
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
                        "field_filter": "filter_a",
                        "logplex_token": "foo",
                        "logplex_log_input_url": "http://logplex.com",
                        "output_type": "logplex",
                    },
                },
            },
        },
    }

    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)

    assert len(a.field_filters) == 1
    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"

        with mock.patch.object(PumpReader, "has_persistent_files", return_value=has_persistent_files), \
                mock.patch.object(PumpReader, "has_runtime_files", return_value=has_runtime_files):
            r.create_journald_reader_if_missing()

        if not has_persistent_files and not has_runtime_files:
            assert not r.senders
            assert r.journald_reader is None
        else:
            assert len(r.senders) == 1
            assert r.journald_reader
            for sn, s in r.senders.items():
                assert sn == "bar"
                s.running = False
                s.join()

        r.running = False
