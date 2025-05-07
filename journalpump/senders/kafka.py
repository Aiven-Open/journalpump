from .base import LogSender
from kafka import errors, KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

import logging
import socket

try:
    import snappy
except ImportError:
    snappy = None

try:
    import zstandard as zstd
except ImportError:
    zstd = None

logging.getLogger("kafka").setLevel(logging.CRITICAL)  # remove client-internal tracebacks from logging output


class KafkaSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.kafka_producer = None
        self.kafka_msg_key = self.config.get("kafka_msg_key")
        if self.kafka_msg_key:
            self.kafka_msg_key = self.kafka_msg_key.encode("utf8")
        self.topic = self.config.get("kafka_topic")

    def _generate_client_config(self) -> dict:
        config = {
            "api_version": self.config.get("kafka_api_version"),
            "bootstrap_servers": self.config.get("kafka_address"),
            "reconnect_backoff_ms": 1000,  # up from the default 50ms to reduce connection attempts
            "reconnect_backoff_max_ms": 10000,  # up the upper bound for backoff to 10 seconds
        }

        if self.config.get("ssl"):
            config["security_protocol"] = "SSL"
            config["ssl_cafile"] = self.config.get("ca")
            config["ssl_certfile"] = self.config.get("certfile")
            config["ssl_keyfile"] = self.config.get("keyfile")
        else:
            config["security_protocol"] = "PLAINTEXT"

        return config

    def _generate_producer_config(self) -> dict:
        producer_config = self._generate_client_config()
        producer_config["linger_ms"] = 500  # wait up 500 ms to see if we can send msgs in a group

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

        return producer_config

    def _init_kafka(self) -> None:
        self.log.info("Initializing Kafka client, address: %r for %s", self.config["kafka_address"], self.name)

        if self.kafka_producer:
            self.kafka_producer.close()
            self.kafka_producer = None

        self.mark_disconnected()

        while self.running and not self._connected:
            producer_config = self._generate_producer_config()

            try:
                kafka_producer = KafkaProducer(**producer_config)
            except (errors.KafkaError, socket.timeout, TimeoutError) as ex:
                if isinstance(ex, errors.KafkaError):
                    # Reraise exceptions that are fatal
                    if not ex.retriable:
                        raise
                self.mark_disconnected(ex)
                self.log.warning(
                    "Retriable error during Kafka initialization: %s: %s",
                    ex.__class__.__name__,
                    ex,
                )
                self._backoff()
            else:
                self.log.info(
                    "Initialized Kafka Client, address: %r for %s",
                    self.config["kafka_address"],
                    self.name,
                )
                self.kafka_producer = kafka_producer
                self.mark_connected()

        # Assume that when the topic configuration is provided we should
        # manually create it. This is useful for kafka clusters configured with
        # `auto.create.topics.enable = false`
        topic_config = self.config.get("kafka_topic_config", dict())
        num_partitions = topic_config.get("num_partitions")
        replication_factor = topic_config.get("replication_factor")
        if num_partitions is not None and replication_factor is not None:
            kafka_admin = KafkaAdminClient(**self._generate_client_config())
            try:
                kafka_admin.create_topics([NewTopic(self.topic, num_partitions, replication_factor)])
            except errors.TopicAlreadyExistsError:
                self.log.info("Kafka topic %r already exists for %s", self.topic, self.name)
            else:
                self.log.info("Create Kafka topic, address: %r for %s", self.topic, self.name)

    def send_messages(self, *, messages, cursor):
        if not self.kafka_producer:
            self._init_kafka()
        try:
            # Collect return values of send():
            # FutureRecordMetadata which will trigger when message actually sent (during flush)
            result_futures = [
                self.kafka_producer.send(topic=self.topic, value=msg, key=self.kafka_msg_key) for msg in messages
            ]
            self.kafka_producer.flush()
            for result_future in result_futures:
                # get() throws error from future, catch below
                # flush() above should have sent, getting with 1 sec timeout
                result_future.get(timeout=1)
            self.mark_sent(messages=messages, cursor=cursor)
            return True
        except (errors.KafkaError, socket.timeout, TimeoutError) as ex:
            if isinstance(ex, errors.KafkaError):
                # Reraise exceptions that are fatal
                if not ex.retriable:
                    raise
            self.mark_disconnected(ex)
            self.log.info(
                "Kafka retriable error during send: %s: %s, waiting",
                ex.__class__.__name__,
                ex,
            )
            self._backoff()
            self._init_kafka()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to kafka")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_kafka()
        return False
