# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from functools import partial
from mypy_extensions import NamedArg
from typing import Callable, Dict, Optional

import datetime
import socket
import ssl
import sys

if sys.version_info >= (3, 8):
    from typing import Protocol, Literal, final, overload, TypedDict  # pylint:disable=no-name-in-module
else:
    from typing_extensions import Protocol, Literal, final, overload, TypedDict

SUPPORTED_RFCS = ("RFC5424","RFC3164","CUSTOM")

NILVALUE = "-"

FormatterFuncType = Callable[[
    NamedArg(int, "pri"),  # noqa:F821
    NamedArg(Optional[str], "rfc3339date"),  # noqa:F821
    NamedArg(Optional[str], "rfc3164date"),  # noqa:F821
    NamedArg(str, "hostname"),  # noqa:F821
    NamedArg(str, "app_id"),  # noqa:F821
    NamedArg(str, "proc_id"),  # noqa:F821
    NamedArg(str, "msg_id"),  # noqa:F821
    NamedArg(str, "msg"),  # noqa:F821
    NamedArg(Optional[str], "sd")  # noqa:F821
], bytes]


# pylint: disable=unused-argument
def _rfc_5424_formatter(
    *, pri: int, rfc3339date: Optional[str], rfc3164date: Optional[str], hostname: str, app_id: str, proc_id: str,
    msg_id: str, msg: str, sd: Optional[str]
) -> bytes:
    opt_data = f" [{sd}]" if sd is not None else ""
    return f"<{pri}>1 {rfc3339date} {hostname} {app_id} {proc_id} {msg_id}{opt_data} {msg}\n".encode("utf-8", "replace")


# pylint: disable=unused-argument
def _rfc_3164_formatter(
    *, pri: int, rfc3339date: Optional[str], rfc3164date: Optional[str], hostname: str, app_id: str, proc_id: str,
    msg_id: str, msg: str, sd: Optional[str]
) -> bytes:
    return f"<{pri}>{rfc3164date} {hostname} {app_id}[{proc_id}]: {msg}\n".encode("utf-8", "replace")


class CustomFormatterProtocol(Protocol):
    def format(
        self, pri: int, rfc3339date: Optional[str], rfc3164date: Optional[str], hostname: str, app_id: str, proc_id: str,
        msg_id: str, msg: str, sd: Optional[str]
    ) -> str:
        ...


def _custom_formatter(
    custom: CustomFormatterProtocol, pri: int, rfc3339date: Optional[str], rfc3164date: Optional[str], hostname: str,
    app_id: str, proc_id: str, msg_id: str, msg: str, sd: Optional[str]
) -> bytes:
    return custom.format(
        pri,
        rfc3339date,
        rfc3164date,
        hostname,
        app_id,
        proc_id,
        msg_id,
        msg,
        sd,
    ).encode("utf-8", "replace")


_LOGLINE_VARS: Dict[str, str] = {
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


def _generate_format(logline: str) -> str:
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


class _SSLParams(TypedDict):
    ssl_version: int
    cert_reqs: int
    keyfile: Optional[str]
    certfile: Optional[str]
    ca_certs: Optional[str]


class SyslogTcpClient:
    @overload
    def __init__(
        self,
        *,
        server: str,
        port: int,
        rfc: Literal["CUSTOM"],
        max_msg: int = ...,
        protocol: Optional[Literal["PLAINTEXT", "SSL"]] = ...,
        cacerts: Optional[str] = ...,
        keyfile: Optional[str] = ...,
        certfile: Optional[str] = ...,
        log_format: str
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        server: str,
        port: int,
        rfc: Literal["RFC5424"],
        max_msg: int = ...,
        protocol: Optional[Literal["PLAINTEXT", "SSL"]] = ...,
        cacerts: Optional[str] = ...,
        keyfile: Optional[str] = ...,
        certfile: Optional[str] = ...,
        log_format: Optional[str] = ...
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        server: str,
        port: int,
        rfc: Literal["RFC3164"],
        max_msg: int = ...,
        protocol: Optional[Literal["PLAINTEXT", "SSL"]] = ...,
        cacerts: Optional[str] = ...,
        keyfile: Optional[str] = ...,
        certfile: Optional[str] = ...,
        log_format: Optional[str] = ...
    ):
        ...

    @final
    def __init__(
        self,
        *,
        server: str,
        port: int,
        rfc: Literal["RFC5424", "RFC3164", "CUSTOM"],
        max_msg: int = 2048,
        protocol: Optional[Literal["PLAINTEXT", "SSL"]] = None,
        cacerts: Optional[str] = None,
        keyfile: Optional[str] = None,
        certfile: Optional[str] = None,
        log_format: Optional[str] = None
    ):
        self._socket: Optional[socket.socket] = None
        self.server: str = server
        self.port: int = port
        self.max_msg: int = max_msg
        self.socket_proto: "socket.SocketKind" = socket.SOCK_STREAM
        self.ssl_params: Optional[_SSLParams] = None
        self.formatter: FormatterFuncType
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
        # Attempt connection on initial startup
        self._connect()

    def _connect(self) -> None:
        try:
            last_connection_error = None
            for addr_info in socket.getaddrinfo(self.server, self.port, socket.AF_UNSPEC, self.socket_proto):
                family, sock_type, sock_proto, _, sock_addr = addr_info
                try:
                    self._socket = socket.socket(family, sock_type, sock_proto)
                    if self.ssl_params is not None:
                        self._socket = ssl.wrap_socket(self._socket, **self.ssl_params)
                    self._socket.connect(sock_addr)
                    return
                except Exception as ex:  # pylint: disable=broad-except
                    if self._socket is not None:
                        self._socket.close()
                    last_connection_error = ex
        except socket.gaierror:
            raise ValueError("Invalid address {}:{}".format(self.server, self.port))

        if last_connection_error is not None:
            raise last_connection_error

    def close(self) -> None:
        if self._socket is None:
            return
        try:
            self._socket.close()
        finally:
            self._socket = None

    def send(self, message: bytes) -> None:
        if self._socket is None:
            self._connect()

        if self._socket is None:
            raise RuntimeError("socket failed to connect without throwing an exception")

        self._socket.sendall(message[:self.max_msg - 1])
        if len(message) >= self.max_msg:
            self._socket.sendall(b"\n")

    def log(
        self,
        *,
        facility: int,
        severity: int,
        timestamp: str,
        hostname: str,
        program: str,
        pid: Optional[str] = None,
        msgid: Optional[str] = None,
        msg: Optional[str] = None,
        sd: Optional[str] = None
    ) -> None:
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
