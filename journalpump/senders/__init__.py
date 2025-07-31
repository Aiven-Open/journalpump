from typing import Any, Dict

import importlib

# config name <--> class mapping
output_type_to_sender_class_path: Dict[str, str] = {
    "aws_cloudwatch": "journalpump.senders.aws_cloudwatch.AWSCloudWatchSender",
    "elasticsearch": "journalpump.senders.elasticsearch_opensearch_sender.ElasticsearchSender",
    "opensearch": "journalpump.senders.elasticsearch_opensearch_sender.OpenSearchSender",
    "file": "journalpump.senders.file.FileSender",
    "google_cloud_logging": "journalpump.senders.google_cloud_logging.GoogleCloudLoggingSender",
    "kafka": "journalpump.senders.kafka.KafkaSender",
    "logplex": "journalpump.senders.logplex.LogplexSender",
    "rsyslog": "journalpump.senders.rsyslog.RsyslogSender",
    "websocket": "journalpump.senders.websocket.WebsocketSender",
}

# mapping to actual classes; primarily used by tests but used as cache here too
output_type_to_sender_class: Dict[str, Any] = {}


def get_sender_class(output_type):
    sender_class = output_type_to_sender_class.get(output_type, None)
    if sender_class is None:
        try:
            sender_class_path = output_type_to_sender_class_path[output_type]
        except KeyError as ex:
            raise ValueError(f"Unknown sender type {output_type!r}") from ex

        sender_class_module, sender_class_name = sender_class_path.rsplit(".", 1)
        sender_module = importlib.import_module(sender_class_module)
        sender_class = getattr(sender_module, sender_class_name)
        output_type_to_sender_class[output_type] = sender_class
    return sender_class
