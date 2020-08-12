# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from functools import partial

import datetime
import socket
import ssl

NILVALUE = "-"


# pylint: disable=unused-argument
def _rfc_5424_formatter(*, pri, rfc3339date, rfc3164date, hostname, app_id, proc_id, msg_id, msg, sd):
    data = "<{}>1 {} {} {} {} {}".format(pri, rfc3339date, hostname, app_id, proc_id, msg_id)
    if sd is not None:
        data += " [{}]".format(sd)
    data += " {}\n".format(msg)
    return data.encode("utf-8", "replace")


# pylint: disable=unused-argument
def _rfc_3164_formatter(*, pri, rfc3339date, rfc3164date, hostname, app_id, proc_id, msg_id, msg, sd):
    data = "<{}>{} {} {}[{}]: {}\n".format(pri, rfc3164date, hostname, app_id, proc_id, msg)
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
        self, *, server, port, rfc, max_msg=2048, protocol=None, cacerts=None, keyfile=None, certfile=None, log_format=None
    ):
        self.socket = None
        self.server = server
        self.port = port
        self.max_msg = max_msg
        self.socket_proto = socket.SOCK_STREAM
        self.ssl_params = None
        if rfc == "RFC5424":
            self.formatter = _rfc_5424_formatter
        elif rfc == "RFC3164":
            self.formatter = _rfc_3164_formatter
        elif rfc == "CUSTOM":
            if log_format is None:
                raise ValueError("log_format must be given when using CUSTOM format")
            self.formatter = partial(_custom_formatter, _generate_format(log_format))
        else:
            raise ValueError('Unknown message format "{}" requested'.format(rfc))
        if protocol is None:
            protocol = "PLAINTEXT"
        if cacerts is not None or protocol == "SSL":
            self.ssl_params = {
                "ssl_version": ssl.PROTOCOL_TLS,  # pylint: disable=no-member
                "cert_reqs": ssl.CERT_REQUIRED,
                "keyfile": keyfile,
                "certfile": certfile,
                "ca_certs": cacerts,
            }
        self._connect()

    def _connect(self):
        try:
            last_connection_error = None
            for addr_info in socket.getaddrinfo(self.server, self.port, socket.AF_UNSPEC, self.socket_proto):
                family, sock_type, sock_proto, _, sock_addr = addr_info
                try:
                    self.socket = socket.socket(family, sock_type, sock_proto)
                    if self.ssl_params is not None:
                        self.socket = ssl.wrap_socket(self.socket, **self.ssl_params)
                    self.socket.connect(sock_addr)
                    return
                except Exception as ex:  # pylint: disable=broad-except
                    if self.socket is not None:
                        self.socket.close()
                    last_connection_error = ex
        except socket.gaierror:
            raise ValueError("Invalid address {}:{}".format(self.server, self.port))

        raise last_connection_error

    def close(self):
        if self.socket is None:
            return
        try:
            self.socket.close()
        finally:
            self.socket = None

    def send(self, message):
        if self.socket is None:
            self._connect()
        self.socket.sendall(message[:self.max_msg - 1])
        if len(message) >= self.max_msg:
            self.socket.sendall(b"\n")

    def log(self, *, facility, severity, timestamp, hostname, program, pid=None, msgid=None, msg=None, sd=None):
        if 0 <= facility <= 23 and 0 <= severity <= 7:
            pri = facility * 8 + severity
        else:
            pri = 13
        app_id = program if program else NILVALUE
        proc_id = pid if pid else NILVALUE
        msg_id = msgid if msgid else NILVALUE
        message = msg if msg else NILVALUE
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
                sd=sd
            )
        )
