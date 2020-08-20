from .base import LogSender
from journalpump.rsyslog import SyslogTcpClient

import json
import socket
import time

RSYSLOG_CONN_ERRORS = (socket.timeout, ConnectionRefusedError)


class RsyslogSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.rsyslog_client = None
        self.sd = None
        self.default_facility = 1
        self.default_severity = 6

    def _init_rsyslog_client(self):
        self.log.info("Initializing Rsyslog Client")
        self.mark_disconnected()
        while self.running:
            try:
                if self.rsyslog_client:
                    self.rsyslog_client.close()

                self.default_facility = self.config.get("default_facility", 1)
                self.default_severity = self.config.get("default_severity", 6)
                self.sd = self.config.get("structured_data")

                server = self.config.get("rsyslog_server")
                port = self.config.get("rsyslog_port", 514)

                self.rsyslog_client = SyslogTcpClient(
                    server=server,
                    port=port,
                    rfc=self.config.get("format", "rfc5424").upper(),
                    protocol="SSL" if self.config.get("ssl") is True else "PLAINTEXT",
                    cacerts=self.config.get("ca_certs"),
                    keyfile=self.config.get("client_key"),
                    certfile=self.config.get("client_cert"),
                    log_format=self.config.get("logline"),
                )
                self.log.info("Initialized Rsyslog Client, server: %s, port: %d", server, port)
                self.mark_connected()
                break
            except RSYSLOG_CONN_ERRORS as ex:
                self.log.warning(
                    "Retryable error during Rsyslog Client initialization: %s: %s, sleeping", ex.__class__.__name__, ex
                )
                self.mark_disconnected(ex)
                self._backoff()
            self.rsyslog_client = None
            time.sleep(5.0)

    def send_messages(self, *, messages, cursor):
        if not self.rsyslog_client:
            self._init_rsyslog_client()

        if self.rsyslog_client is None:
            raise Exception("failed to initialize rsyslog client")

        try:
            for msg in messages:
                message = json.loads(msg.decode("utf8"))
                _facility = message.get("SYSLOG_FACILITY")
                try:
                    facility = int(_facility[0] if isinstance(_facility, list) else _facility)
                except Exception:  # pylint: disable=broad-except
                    facility = self.default_facility

                severity = int(message.get("PRIORITY", self.default_severity))
                timestamp = message["timestamp"][:26] + "Z"  # Assume UTC for now
                hostname = message.get("HOSTNAME")
                appname = message.get("SYSLOG_IDENTIFIER", message.get("SYSTEMD_UNIT", message.get("PROCESS_NAME")))
                progid = message.get("PID")
                txt = message.get("MESSAGE")

                self.rsyslog_client.log(
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
            self._init_rsyslog_client()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to rsyslog")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_rsyslog_client()
        return False
