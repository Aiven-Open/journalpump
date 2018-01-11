from journalpump.journalpump import ElasticsearchSender, JournalObject, JournalPump, KafkaSender, LogplexSender
import json


def test_journalpump_init(tmpdir):
    # Logplex sender
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": {
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

    assert len(a.readers) == 1
    for rn, r in a.readers.items():
        assert rn == "foo"
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            s.running = False
            assert isinstance(s, LogplexSender)

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
