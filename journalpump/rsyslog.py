# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from functools import partial

import datetime
import errno
import socket
import ssl

NILVALUE = "-"


# pylint: disable=unused-argument
def _rfc_5424_formatter(*, pri, rfc3339date, rfc3164date, hostname, app_id, proc_id, msg_id, msg, sd):
    data = f"<{pri}>1 {rfc3339date} {hostname} {app_id} {proc_id} {msg_id}"
    if sd is not None:
        data += f" [{sd}]"
    data += f" {msg}\n"
    return data.encode("utf-8", "replace")


# pylint: disable=unused-argument
def _rfc_3164_formatter(*, pri, rfc3339date, rfc3164date, hostname, app_id, proc_id, msg_id, msg, sd):
    data = f"<{pri}>{rfc3164date} {hostname} {app_id}[{proc_id}]: {msg}\n"
    return data.encode("utf-8", "replace")


def _custom_formatter(custom, **kwargs):
    return custom.format(**kwargs).encode("utf-8", "replace")


_LOGLINE_VARS = {
    "pri": "{pri}",
    "protocol-version": "1",
    "timestamp": "{rfc3164date}",
    "timestamp:::date-rfc3339": "{rfc3339date}",
    "HOSTNAME": "{hostname}",
    "app-name": "{app_id}",
    "procid": "{proc_id}",
    "msgid": "{msg_id}",
    "msg": "{msg}",
    "structured-data": "{sd}",
    "": "%",
}


def _generate_format(logline):
    """Simple tokenizer for converting rsyslog format string to python format string"""
    frmt = ""
    in_token = False
    token = ""
    for c in logline:
        if c == "%" and not in_token:
            in_token = True
        elif c == "%" and in_token:
            try:
                frmt += _LOGLINE_VARS[token]
            except KeyError:
                frmt += "-"
            token = ""
            in_token = False
        elif in_token:
            token += c
        else:
            frmt += c
            if c in {"{", "}"}:
                frmt += c

    if frmt[-1] != "\n":
        frmt += "\n"
    return frmt


class SyslogTcpClient:
    def __init__(
        self,
        *,
        server,
        port,
        rfc,
        max_msg=None,
        protocol=None,
        cacerts=None,
        keyfile=None,
        certfile=None,
        log_format=None,
        octet_counted_framing=None,
        escape_newlines=None,
    ):
        self.socket = None
        self.server = server
        self.port = port
        self.max_msg = max_msg or 2048
        self.socket_proto = socket.SOCK_STREAM
        self.ssl_context = None
        self.octet_counted_framing = False if octet_counted_framing is None else octet_counted_framing
        self.escape_newlines = False if escape_newlines is None else escape_newlines
        if rfc == "RFC5424":
            self.formatter = _rfc_5424_formatter
        elif rfc == "RFC3164":
            self.formatter = _rfc_3164_formatter
        elif rfc == "CUSTOM":
            if log_format is None:
                raise ValueError("log_format must be given when using CUSTOM format")
            self.formatter = partial(_custom_formatter, _generate_format(log_format))
        else:
            raise ValueError(f'Unknown message format "{rfc}" requested')
        if protocol is None:
            protocol = "PLAINTEXT"
        if cacerts is not None or protocol == "SSL":
            self.ssl_context = ssl.create_default_context(cafile=cacerts)
            self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            if certfile:
                self.ssl_context.load_cert_chain(certfile, keyfile)

        self._connect()

    def _connect(self):
        try:
            last_connection_error = None
            for addr_info in socket.getaddrinfo(self.server, self.port, socket.AF_UNSPEC, self.socket_proto):
                family, sock_type, sock_proto, _, sock_addr = addr_info
                try:
                    self.socket = socket.socket(family, sock_type, sock_proto)
                    if self.ssl_context is not None:
                        # Passing server_hostname allows client to pass SNI (Server Name Identification)
                        # field to server, so that the server can pick a correct server cert.
                        self.socket = self.ssl_context.wrap_socket(self.socket, server_hostname=self.server)
                    self.socket.connect(sock_addr)
                    return
                except Exception as ex:  # pylint: disable=broad-except
                    if self.socket is not None:
                        self.socket.close()
                    last_connection_error = ex
        except socket.gaierror as ex:
            raise ValueError(f"Invalid address {self.server}:{self.port}") from ex

        raise last_connection_error

    def close(self):
        if self.socket is None:
            return
        try:
            self.socket.close()
        except Exception:  # pylint: disable=broad-except
            pass
        finally:
            self.socket = None

    def send(self, message: bytes) -> None:
        for retry in [True, False]:
            try:
                if self.socket is None:
                    self._connect()
                assert self.socket is not None

                # RFC 6587 defines two framing methods for syslog over TCP.
                if self.octet_counted_framing:
                    # Octet-counted framing (RFC 6587 section 3.4.1):
                    # <MSGLEN> SP <SYSLOG-MSG>. The receiver reads exactly
                    # <MSGLEN> bytes after the space, so the advertised length
                    # must equal the body bytes actually sent. max_message_size
                    # bounds the body (matching rsyslog MaxMessageSize semantics).
                    # No trailing newline — it would be parsed as the start of
                    # the next frame's length prefix and desync the stream.
                    body = message[: self.max_msg]
                    frame = f"{len(body)} ".encode() + body
                    self.socket.sendall(frame)
                else:
                    # Non-transparent framing (RFC 6587 section 3.4.2): frames
                    # are delimited by a trailing newline. The formatter already
                    # appends '\n'; when we truncate we add it explicitly.
                    self.socket.sendall(message[: self.max_msg - 1])
                    if len(message) >= self.max_msg:
                        self.socket.sendall(b"\n")

                break
            except Exception as ex:  # pylint: disable=broad-except
                self.close()

                if not (retry and self._should_retry(ex=ex)):
                    raise

    def _should_retry(self, *, ex):
        if isinstance(ex, OSError):
            return ex.errno in (errno.EPIPE, errno.ECONNRESET, errno.ETIMEDOUT)
        # retry to send when the SSL connection was closed unexpectedly
        # with message 'EOF occurred in violation of protocol'
        if isinstance(ex, ssl.SSLEOFError):
            return True
        return False

    def log(
        self,
        *,
        facility,
        severity,
        timestamp,
        hostname,
        program,
        pid=None,
        msgid=None,
        msg=None,
        sd=None,
    ):
        if 0 <= facility <= 23 and 0 <= severity <= 7:
            pri = facility * 8 + severity
        else:
            pri = 13
        app_id = program if program else NILVALUE
        proc_id = pid if pid else NILVALUE
        msg_id = msgid if msgid else NILVALUE
        message = msg if msg else NILVALUE
        if self.escape_newlines:
            message = message.replace("\r", "\\r").replace("\n", "\\n")
        rfc3164date = datetime.datetime.strptime(timestamp[:19], "%Y-%m-%dT%H:%M:%S").strftime("%b %d %H:%M:%S")

        self.send(
            self.formatter(
                pri=pri,
                rfc3339date=timestamp,
                rfc3164date=rfc3164date,
                hostname=hostname,
                app_id=app_id,
                proc_id=proc_id,
                msg_id=msg_id,
                msg=message,
                sd=sd,
            )
        )
