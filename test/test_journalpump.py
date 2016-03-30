from journalpump.journalpump import JournalPump, ElasticsearchSender, KafkaSender, LogplexSender
import json


def test_journalpump_init(tmpdir):
    # Logplex sender
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {
        "logplex_token": "foo",
        "logplex_log_input_url": "http://logplex.com",
        "output_type": "logplex"
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)
    a.initialize_sender()
    a.sender.running = False
    assert isinstance(a.sender, LogplexSender)

    # Kafka sender
    config = {
        "output_type": "kafka",
        "logplex_token": "foo",
        "kafka_address": "localhost",
        "kafka_topic": "foo"}
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)
    a.initialize_sender()
    a.sender.running = False
    assert isinstance(a.sender, KafkaSender)

    # Elasticsearch sender
    config = {
        "output_type": "elasticsearch",
        "elasticsearch_url": "https://foo.aiven.io",
        "elasticsearch_index_prefix": "fooprefix",
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    a = JournalPump(journalpump_path)
    a.initialize_sender()
    a.sender.running = False
    assert isinstance(a.sender, ElasticsearchSender)
