from .base import LogSender
from kafka import errors, KafkaProducer

import logging
import socket
import time

try:
    import snappy
except ImportError:
    snappy = None

try:
    import zstandard as zstd
except ImportError:
    zstd = None

KAFKA_CONN_ERRORS = tuple(errors.RETRY_ERROR_TYPES) + (
    errors.UnknownError,
    socket.timeout,
)

logging.getLogger("kafka").setLevel(logging.CRITICAL)  # remove client-internal tracebacks from logging output


class KafkaSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.kafka_producer = None
        self.kafka_msg_key = self.config.get("kafka_msg_key")
        if self.kafka_msg_key:
            self.kafka_msg_key = self.kafka_msg_key.encode("utf8")
        self.topic = self.config.get("kafka_topic")

    def _init_kafka(self):
        self.log.info("Initializing Kafka client, address: %r", self.config["kafka_address"])
        self.mark_disconnected()
        while self.running:
            try:
                api_version = self.config.get("kafka_api_version", "0.9")
                parsed = tuple(map(int, api_version.split(".")))
                if self.kafka_producer:
                    self.kafka_producer.close()
                # make sure the python client supports it as well
                # pylint: disable=protected-access
                if zstd and "zstd" in KafkaProducer._COMPRESSORS and parsed >= (2, 1, 0):
                    compression = "zstd"
                elif snappy:
                    compression = "snappy"
                else:
                    compression = "gzip"

                self.kafka_producer = KafkaProducer(
                    api_version=api_version,
                    bootstrap_servers=self.config.get("kafka_address"),
                    compression_type=compression,
                    linger_ms=500,  # wait up 500 ms to see if we can send msgs in a group
                    reconnect_backoff_ms=1000,  # up from the default 50ms to reduce connection attempts
                    reconnect_backoff_max_ms=10000,  # up the upper bound for backoff to 10 seconds
                    security_protocol="SSL" if self.config.get("ssl") is True else "PLAINTEXT",
                    ssl_cafile=self.config.get("ca"),
                    ssl_certfile=self.config.get("certfile"),
                    ssl_keyfile=self.config.get("keyfile"),
                )
                self.log.info("Initialized Kafka Client, address: %r", self.config["kafka_address"])
                self.mark_connected()
                break
            except KAFKA_CONN_ERRORS as ex:
                self.mark_disconnected(ex)
                self.log.warning("Retriable error during Kafka initialization: %s: %s", ex.__class__.__name__, ex)
                self._backoff()
            self.kafka_producer = None
            time.sleep(5.0)

    def send_messages(self, *, messages, cursor):
        if not self.kafka_producer:
            self._init_kafka()
        try:
            for msg in messages:
                self.kafka_producer.send(topic=self.topic, value=msg, key=self.kafka_msg_key)
            self.kafka_producer.flush()
            self.mark_sent(messages=messages, cursor=cursor)
            return True
        except KAFKA_CONN_ERRORS as ex:
            self.mark_disconnected(ex)
            self.log.info("Kafka retriable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
            self._backoff()
            self._init_kafka()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to kafka")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_kafka()
        return False
