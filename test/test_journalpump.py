from kafkajournalpump.kafkajournalpump import KafkaJournalPump, KafkaSender
import json

def test_journalpump_init(tmpdir):
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {"logplex_token": "foo"}
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))

    a = KafkaJournalPump(journalpump_path)
    # Logplex sender
    a.initialize_sender()
    a.sender.running = False

    # Logplex sender
    a.sender = None
    config = {"logplex_token": "foo", "kafka_address": "localhost"}
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))

    a.initialize_sender()
    a.sender.running = False
    assert isinstance(a.sender, KafkaSender)
