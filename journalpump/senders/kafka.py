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
                if self.kafka_producer:
                    self.kafka_producer.close()

                producer_config = {
                    "api_version": self.config.get("kafka_api_version"),
                    "bootstrap_servers": self.config.get("kafka_address"),
                    "linger_ms": 500,  # wait up 500 ms to see if we can send msgs in a group
                    "reconnect_backoff_ms": 1000,  # up from the default 50ms to reduce connection attempts
                    "reconnect_backoff_max_ms": 10000,  # up the upper bound for backoff to 10 seconds
                }

                if self.config.get("ssl"):
                    producer_config["security_protocol"] = "SSL"
                    producer_config["ssl_cafile"] = self.config.get("ca")
                    producer_config["ssl_certfile"] = self.config.get("certfile")
                    producer_config["ssl_keyfile"] = self.config.get("keyfile")
                else:
                    producer_config["security_protocol"] = "PLAINTEXT"

                # make sure the python client supports it as well
                if zstd and "zstd" in KafkaProducer._COMPRESSORS:  # pylint: disable=protected-access
                    producer_config["compression_type"] = "zstd"
                elif snappy:
                    producer_config["compression_type"] = "snappy"
                else:
                    producer_config["compression_type"] = "gzip"

                if self.config.get("socks5_proxy"):
                    # Socks5_config is supported by Aiven fork of kafka-python for the time being
                    producer_config["socks5_proxy"] = self.config.get("socks5_proxy")

                self.kafka_producer = KafkaProducer(**producer_config)

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
