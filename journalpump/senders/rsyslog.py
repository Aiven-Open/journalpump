import collections
from typing import Mapping, Any, Optional, Sequence

from .base import LogSender, _LogSenderReader, JournalCursor, LogSenderShutdown
from journalpump.rsyslog import SyslogTcpClient, SUPPORTED_RFCS

import json
import socket
import time

from ..statsd import StatsClient, TagsType

RSYSLOG_CONN_ERRORS = (socket.timeout, ConnectionRefusedError)
RSYSLOG_RECONNECT_BACKOFF_SECS = 5.0

class RsyslogSender(LogSender):
    def __init__(self, *, name: str, reader: _LogSenderReader, config: Mapping[str, Any],
                 field_filter: Mapping[str, Any], stats: StatsClient, max_send_interval: float,
                 extra_field_values: Optional[Mapping[str, str]] = None, tags: Optional[TagsType] = None,
                 msg_buffer_max_length: int = 50000):
        super().__init__(name=name, reader=reader, config=config, field_filter=field_filter, stats=stats,
                         max_send_interval=max_send_interval, extra_field_values=extra_field_values, tags=tags,
                         msg_buffer_max_length=msg_buffer_max_length)
        self.sd = None
        self.default_facility: int = 1
        self.default_severity: int = 6

        # Caches the _rsyslog_client
        self._rsyslog_client : Optional[SyslogTcpClient] = None

    def _get_rsyslog_client(self) -> SyslogTcpClient:
        """
        _get_ryslog_client either initializes and returns a SyslogTcpClient or errors

        Returns:
            an initialized SyslogTcpClient

        """
        if self._rsyslog_client is not None:
            return self._rsyslog_client

        self.log.info("Initializing Rsyslog Client")
        while self.running:
            try:
                if self._rsyslog_client:
                    self._rsyslog_client.close()
                    self._rsyslog_client = None

                self.mark_disconnected()

                self.default_facility = self.config.get("default_facility", 1)
                self.default_severity = self.config.get("default_severity", 6)
                self.sd = self.config.get("structured_data")

                # This is not redundant - we're ensuring we get expected types from config or
                # else erroring.
                server: str = str(self.config.get("rsyslog_server",""))
                port: int = int(self.config.get("rsyslog_port", 514))

                rfc = self.config.get("format", "RFC5424").upper()
                if rfc not in SUPPORTED_RFCS:
                    raise ValueError(f"Unsupported RFC, must be one of {SUPPORTED_RFCS}")

                # Type-enforced config extraction - specifically we're giving ourselves
                # a few more opportunities to fail if we get nonsense input.
                ca_certs : Optional[str] = None
                client_key : Optional[str] = None
                client_cert : Optional[str] = None
                logline : Optional[str] = None

                if "ca_certs" in self.config:
                    ca_certs = str(self.config["ca_certs"])

                if "client_key" in self.config:
                    client_key = str(self.config["client_key"])

                if "client_cert" in self.config:
                    client_cert = str(self.config["client_cert"])

                if "logline" in self.config:
                    logline = str(self.config["logline"])

                self._rsyslog_client = SyslogTcpClient(
                    server=server,
                    rfc=rfc,
                    port=port,
                    protocol="SSL" if self.config.get("ssl") is True else "PLAINTEXT",
                    cacerts=ca_certs,
                    keyfile=client_key,
                    certfile=client_cert,
                    log_format=logline,
                )
                self.log.info(f"Initialized Rsyslog Client, server: {server}, port: {port}")
                self.mark_connected()
                return self._rsyslog_client
            except RSYSLOG_CONN_ERRORS as ex:
                self.log.warning(
                    "Retryable error during Rsyslog Client initialization: %s: %s, sleeping", ex.__class__.__name__, ex
                )
                self.mark_disconnected(ex)
                self._backoff()

            self._rsyslog_client = None
            time.sleep(RSYSLOG_RECONNECT_BACKOFF_SECS)

        # Important: we only get here if self.running becomes false. An unhandled exception just
        # gets raised.
        raise LogSenderShutdown("rsyslog client was shutdown before connection could complete")

    def send_messages(self, *, messages : Sequence[bytes], cursor: JournalCursor) -> bool:
        rsyslog_client = self._get_rsyslog_client()
        try:
            for msg in messages:
                message = json.loads(msg.decode("utf8"))
                _facility = message.get("SYSLOG_FACILITY")
                try:
                    facility = int(_facility[0] if isinstance(_facility, collections.Sequence) else _facility)
                except Exception:  # pylint: disable=broad-except
                    facility = self.default_facility

                severity = int(message.get("PRIORITY", self.default_severity))
                timestamp = message["timestamp"][:26] + "Z"  # Assume UTC for now
                hostname = message.get("HOSTNAME")
                appname = message.get("SYSLOG_IDENTIFIER", message.get("SYSTEMD_UNIT", message.get("PROCESS_NAME")))
                progid = message.get("PID")
                txt = message.get("MESSAGE")

                rsyslog_client.log(
                    facility=facility,
                    severity=severity,
                    timestamp=timestamp,
                    hostname=hostname,
                    program=appname,
                    pid=progid,
                    msg=txt,
                    sd=self.sd
                )
            self.mark_sent(messages=messages, cursor=cursor)
            return True
        except RSYSLOG_CONN_ERRORS as ex:
            self.mark_disconnected(ex)
            self.log.info("Rsyslog Client retryable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
            self._backoff()
            self._get_rsyslog_client()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to rsyslog")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._get_rsyslog_client()
        return False
