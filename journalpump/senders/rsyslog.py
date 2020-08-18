from ..statsd import StatsClient, TagsType
from .base import _LogSenderReader, JournalCursor, LogSender, LogSenderShutdown
from journalpump.rsyslog import RSYSLOG_CONN_ERRORS, SUPPORTED_RFCS, SyslogTcpClient
from typing import Any, Dict, Mapping, Optional, Sequence

import collections
import contextlib
import json
import socket
import time

RSYSLOG_RECONNECT_BACKOFF_SECS = 5.0


class RsyslogSender(LogSender):
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
        self.sd = None

        self.default_facility: int = self.config.get("default_facility", 1)
        self.default_severity: int = self.config.get("default_severity", 6)
        self.sd = self.config.get("structured_data")

        # This is not redundant - we're ensuring we get expected types from config or
        # else erroring.
        self._server: str = str(self.config.get("rsyslog_server", ""))
        self._port: int = int(self.config.get("rsyslog_port", 514))

        _rfc = self.config.get("format", "RFC5424").upper()
        if _rfc not in SUPPORTED_RFCS:
            raise ValueError(f"Unsupported RFC, must be one of {SUPPORTED_RFCS}")
        self._rfc = _rfc

        # Type-enforced config extraction - specifically we're giving ourselves
        # a few more opportunities to fail if we get nonsense input.
        self._ca_certs: Optional[str] = None
        self._client_key: Optional[str] = None
        self._client_cert: Optional[str] = None
        self._logline: Optional[str] = None

        if "ca_certs" in self.config:
            self._ca_certs = str(self.config["ca_certs"])

        if "client_key" in self.config:
            self._client_key = str(self.config["client_key"])

        if "client_cert" in self.config:
            self._client_cert = str(self.config["client_cert"])

        if "logline" in self.config:
            self._logline = str(self.config["logline"])

        # Caches the _rsyslog_client
        self._rsyslog_client = SyslogTcpClient(
            server=self._server,
            rfc=self._rfc,
            port=self._port,
            protocol="SSL" if self.config.get("ssl") is True else "PLAINTEXT",
            cacerts=self._ca_certs,
            keyfile=self._client_key,
            certfile=self._client_cert,
            log_format=self._logline,
        )
        self.log.info(f"Initialized Rsyslog Client, server: {self._server}, port: {self._port}")

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        msg_idx = 0
        # Message sending can continue so long as the sender hasn't been shutdown and hasn't
        # errored. If an exit is triggered but message sending is still proceeding, we try
        # to finish it up.
        while self.running:
            try:
                if not self._rsyslog_client.connected:
                    self._rsyslog_client.connect()
                    self.mark_connected()

                # This inner loop ensures we do as much sending as possible before potentially
                # aborting if the sender is shutdown.
                while msg_idx < len(messages):
                    message: Dict[str, Any] = json.loads(messages[msg_idx].decode("utf8"))
                    facility: int
                    try:
                        _facility = message["SYSLOG_FACILITY"]
                        facility = int(_facility[0] if isinstance(_facility, collections.Sequence) else _facility)
                    except Exception:  # pylint: disable=broad-except
                        facility = self.default_facility

                    severity = int(message.get("PRIORITY", self.default_severity))
                    timestamp = message["timestamp"][:26] + "Z"  # Assume UTC for now
                    hostname = message.get("HOSTNAME", "<none>")
                    appname = message.get("SYSLOG_IDENTIFIER", message.get("SYSTEMD_UNIT", message.get("PROCESS_NAME")))
                    progid = message.get("PID")
                    txt = message.get("MESSAGE")

                    self._rsyslog_client.log(
                        facility=facility,
                        severity=severity,
                        timestamp=timestamp,
                        hostname=hostname,
                        program=appname,
                        pid=progid,
                        msg=txt,
                        sd=self.sd
                    )
                    # Increment message index.
                    msg_idx += 1
                # When message sending succeeds, the loop terminates here. We loop only if we are
                # trying to reconnect.
                return True
            except Exception as ex:  # pylint: disable=broad-except
                if isinstance(ex, RSYSLOG_CONN_ERRORS):
                    self.log.info("Rsyslog Client retryable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
                else:
                    self.log.exception("Unexpected exception during send to rsyslog", exc_info=ex)
                    self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
                self.mark_disconnected(ex)
            finally:
                # This could be in the except handler above. It isnt though, just in case someone
                # breaks the loop in the future. It's just fine here, so don't move it.
                self._backoff()

        # TODO: should raise an exception, but don't want to change our orginal interface promise.
        # raise LogSenderShutdown("rsyslog sender was shutdown before all messages could be sent")
        return False
