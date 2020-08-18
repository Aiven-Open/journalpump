from ..statsd import StatsClient, TagsType
from .base import _LogSenderReader, JournalCursor, LogSender, LogSenderShutdown
from kafka import errors, KafkaProducer
from typing import Any, Mapping, Optional, Sequence

import logging
import socket
import sys
import time

try:
    import snappy
except ImportError:
    snappy = None

if sys.version_info >= (3, 8):
    from typing import Literal  # pylint:disable=no-name-in-module
else:
    from typing_extensions import Protocol, Literal, final, overload, TypedDict

KAFKA_RECONNECT_BACKOFF_SECS = 5.0

KAFKA_CONN_ERRORS = tuple(errors.RETRY_ERROR_TYPES) + (
    errors.UnknownError,
    socket.timeout,
)

logging.getLogger("kafka").setLevel(logging.CRITICAL)  # remove client-internal tracebacks from logging output


class KafkaSender(LogSender):
    def __init__(
        self,
        *,
        name: str,
        reader: _LogSenderReader,
        config: Mapping[str, Any],
        field_filter: Mapping[str, Any],
        stats: StatsClient,
        max_send_interval: Optional[float],
        extra_field_values: Optional[Mapping[str, str]] = None,
        tags: Optional[TagsType] = None,
        msg_buffer_max_length: int = 50000
    ):

        super().__init__(
            name=name,
            reader=reader,
            config=config,
            field_filter=field_filter,
            stats=stats,
            max_send_interval=max_send_interval
            if max_send_interval is not None else float(config.get("max_send_interval", 0.3)),
            extra_field_values=extra_field_values,
            tags=tags,
            msg_buffer_max_length=msg_buffer_max_length
        )

        self.kafka_msg_key: Optional[bytes] = None
        if "kafka_msg_key" in self.config and self.config["kafka_msg_key"] is not None:
            self.kafka_msg_key = str(self.config["kafka_msg_key"]).encode("utf8")

        self._api_version: str = str(self.config["kafka_api_version"] if "kafka_api_version" in self.config else "0.9")
        self._bootstrap_servers: Optional[str] = str(
            self.config["kafka_address"]
        ) if "kafka_address" in self.config else None
        self._compression_type: Literal["snappy", "gzip"] = "snappy" if snappy else "gzip"
        self._linger_ms: int = 500  # wait up 500 ms to see if we can send msgs in a group
        self._reconnect_backoff_ms: int = 1000  # up from the default 50ms to reduce connection attempts
        self._reconnect_backoff_max_ms: int = 10000  # up the upper bound for backoff to 10 seconds
        self._security_protocol: Literal["SSL", "PLAINTEXT"
                                         ] = "SSL" if self.config.get("ssl", False) is True else "PLAINTEXT"

        self._ssl_cafile: Optional[str] = str(self.config["ca"]) if "ca" in self.config else None
        self._ssl_certfile: Optional[str] = str(self.config["certfile"]) if "certfile" in self.config else None
        self._ssl_keyfile: Optional[str] = str(self.config["keyfile"]) if "keyfile" in self.config else None

        self.topic = self.config.get("kafka_topic")

        self._kafka_producer: Optional[KafkaProducer] = None

        self.log.info(f"Initialized Kafka Client, address: {self._bootstrap_servers}")

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        msg_idx = 0
        # Message sending can continue so long as the sender hasn't been shutdown and hasn't
        # errored. If an exit is triggered but message sending is still proceeding, we try
        # to finish it up.
        while self.running:
            try:
                # Connect/Reconnect to Kafka if not currently connected.
                if not self._kafka_producer:
                    self._kafka_producer = KafkaProducer(
                        api_version=self._api_version,
                        bootstrap_servers=self._bootstrap_servers,
                        compression_type=self._compression_type,
                        linger_ms=self._linger_ms,
                        reconnect_backoff_ms=self._reconnect_backoff_ms,
                        reconnect_backoff_max_ms=self._reconnect_backoff_max_ms,
                        security_protocol=self._security_protocol,
                        ssl_cafile=self._ssl_cafile,
                        ssl_certfile=self._ssl_certfile,
                        ssl_keyfile=self._ssl_keyfile,
                    )
                self.mark_connected()

                # This inner loop ensures we do as much sending as possible before potentially
                # aborting if the sender is shutdown (i.e. finish up this call).
                while msg_idx < len(messages):
                    self._kafka_producer.send(topic=self.topic, value=messages[msg_idx], key=self.kafka_msg_key)
                    # Increment message index.
                    msg_idx += 1
                # It is possible if we error to wind up sending messages twice. This is acceptable.
                self._kafka_producer.flush()
                self.mark_sent(messages=messages, cursor=cursor)
                return True
            except Exception as ex:
                if isinstance(ex, KAFKA_CONN_ERRORS):
                    self.log.info("Kafka retriable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
                else:
                    self.log.exception("Unexpected exception during send to kafka", exc_info=ex)
                    self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
                self.mark_disconnected(ex)
            finally:
                if self._kafka_producer is not None:
                    self._kafka_producer.close()
                    self._kafka_producer = None
                self._backoff()

        # TODO: should raise an exception, but don't want to change our orginal interface promise.
        # raise LogSenderShutdown("rsyslog sender was shutdown before all messages could be sent")
        return False
