from collections import OrderedDict
from datetime import datetime
from journalpump.journalpump import (default_json_serialization, ElasticsearchSender, FieldFilter, MsgBuffer, JournalObject,
                                     JournalObjectHandler, JournalPump, MAX_KAFKA_MESSAGE_SIZE, KafkaSender, LogplexSender)
from unittest import mock, TestCase
import json


def test_journalpump_init(tmpdir):
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
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
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
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
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
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            assert isinstance(s, ElasticsearchSender)


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
                            "SYSLOG_FACILITY": r"^0$",          # kernel only
                        },
                        "tags": {"section": "cputemp"},
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
    entry = JournalObject(entry={
        "MESSAGE": "CPU0: Core temperature above threshold, cpu clock throttled (total events = 1)",
        "PRIORITY": "2",
        "SYSLOG_FACILITY": "0",
        "SYSLOG_IDENTIFIER": "kernel",
    })
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
    entry = JournalObject(entry={
        "MESSAGE": "CPU1: on fire",
        "PRIORITY": "1",
        "SYSLOG_FACILITY": "0",
        "SYSLOG_IDENTIFIER": "kernel",
    })
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
        self.sender_a.msg_buffer = MsgBuffer()
        self.sender_b = mock.Mock()
        self.sender_b.field_filter = self.filter_b
        self.sender_b.msg_buffer = MsgBuffer()
        self.sender_c = mock.Mock()
        self.sender_c.field_filter = None
        self.sender_c.msg_buffer = MsgBuffer()
        self.pump = mock.Mock()
        self.reader = mock.Mock()
        self.reader.senders = {"sender_a": self.sender_a, "sender_b": self.sender_b, "sender_c": self.sender_c}

    def test_filtered_processing(self):
        jobject = JournalObject(entry=OrderedDict(a=1, b=2, c=3, REALTIME_TIMESTAMP=1), cursor=10)
        handler = JournalObjectHandler(jobject, self.reader, self.pump)
        assert handler.process() is True
        assert (json.dumps({"a": 1}).encode("utf-8"), 10) in self.sender_a.msg_buffer.messages

        assert (json.dumps(OrderedDict(a=1, b=2)).encode("utf-8"), 10) in self.sender_b.msg_buffer.messages

        largest_data = json.dumps(
            OrderedDict(a=1, b=2, c=3, REALTIME_TIMESTAMP=1, timestamp=datetime.utcfromtimestamp(1)),
            default=default_json_serialization).encode("utf-8")
        assert (largest_data, 10) in self.sender_c.msg_buffer.messages
        self.reader.inc_line_stats.assert_called_once_with(journal_bytes=len(largest_data), journal_lines=1)

    def test_too_large_data(self):
        self.pump.make_tags.return_value = "tags"
        too_large = OrderedDict(a=1, b="x" * MAX_KAFKA_MESSAGE_SIZE)
        jobject = JournalObject(entry=too_large, cursor=10)
        handler = JournalObjectHandler(jobject, self.reader, self.pump)
        assert handler.process() is True
        assert (json.dumps({"a": 1}).encode("utf-8"), 10) in self.sender_a.msg_buffer.messages
        too_large_serialized = json.dumps(too_large)
        error_message = "too large message {} bytes vs maximum {} bytes".format(
            len(too_large_serialized), MAX_KAFKA_MESSAGE_SIZE)
        error_item = json.dumps({
            "error": error_message,
            "partial_data": too_large_serialized[:1024],
        }).encode("utf-8")
        assert "too large message" in str(self.sender_b.msg_buffer.messages)

        self.pump.stats.increase.assert_called_once_with("journal.read_error", tags="tags")
        self.reader.inc_line_stats.assert_called_once_with(journal_bytes=len(error_item), journal_lines=1)
