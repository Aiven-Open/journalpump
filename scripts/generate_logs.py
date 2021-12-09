#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Optional, Dict

MESSAGE_DEFAULTS: Dict[str, str] = {"PRIORITY": "6", "SYSLOG_IDENTIFIER": "journald-gen-logs"}


def _encode_message(data: Dict[str, str]) -> bytes:
    """Encode to journald native protocol message.

    >>> _encode_message({"MEESAGE": "Something happened"})
    b"PRIORITY=6\nSYSLOG_IDENTIFIER=journald-gen-logs\nMESSAGE=Something happened.\n"
    """
    message = MESSAGE_DEFAULTS.copy()
    message.update(data)

    result = []
    for key, value in message.items():
        result.append(b"%s=%s" % (key.encode("utf-8"), str(value).encode("utf-8")))

    return b"\n".join(result) + b"\n"


def _message_sender(uid: int, message_socket_path: str, queue: multiprocessing.JoinableQueue):
    """Send messages to journald using native protocol.

    NB. Message send in a separate process to be able to write to the socket as non-root user.
    and get user-<uid>.journal entries instead of just syslog.journal ones
    """
    os.setuid(uid)

    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.connect(message_socket_path)

    while msg := queue.get():
        s.sendall(_encode_message(msg))
        queue.task_done()

    # ACK None
    queue.task_done()


class JournalControlProcess:
    CONTROL_SOCKET_NAME = "io.systemd.journal"
    MESSAGE_SOCKET_NAME = "socket"
    JOURNALD_BIN = "/usr/lib/systemd/systemd-journald"

    def __init__(self, *, logs_dir: pathlib.Path, uid: int) -> None:
        self._logs_dir: pathlib.Path = logs_dir
        self._runtime_dir: Optional[pathlib.Path] = None
        self._journald_process: Optional[subprocess.Popen] = None
        self._sender_process: Optional[multiprocessing.Process] = None
        self._sender_queue = multiprocessing.JoinableQueue()
        self._uid = uid

    @property
    def _control_socket_path(self) -> str:
        assert self._runtime_dir
        return str(self._runtime_dir / self.CONTROL_SOCKET_NAME)

    @property
    def _message_socket_path(self) -> str:
        assert self._runtime_dir
        return str(self._runtime_dir / self.MESSAGE_SOCKET_NAME)

    def _start_journald(self) -> subprocess.Popen:
        assert self._runtime_dir

        environment = {
            "LOGS_DIRECTORY": self._logs_dir,
            "RUNTIME_DIRECTORY": self._runtime_dir,
        }
        journald_process = subprocess.Popen([self.JOURNALD_BIN, "test"],
                                            env=environment,
                                            stdout=sys.stdout,
                                            stderr=sys.stdout)

        cur = time.monotonic()
        deadline = cur + 3

        while cur < deadline:
            files = {f.name for f in self._runtime_dir.iterdir()}
            if self.CONTROL_SOCKET_NAME in files and self.MESSAGE_SOCKET_NAME in files:
                break
            time.sleep(0.1)
            cur = time.monotonic()

        return journald_process

    def rotate(self) -> None:
        """Ask journald to rotate logs and wait for the result."""
        assert self._journald_process

        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self._control_socket_path)
        s.sendall(b'{"method": "io.systemd.Journal.Synchronize"}\0')
        s.recv(100)
        s.sendall(b'{"method": "io.systemd.Journal.Rotate"}\0')
        s.recv(100)

    def send_message(self, message: Dict[str, str]) -> None:
        """Send message to journald."""
        assert self._sender_process

        self._sender_queue.put(message)
        self._sender_queue.join()

    def _start_sender_processs(self) -> multiprocessing.Process:
        sender_process = multiprocessing.Process(
            target=_message_sender, args=(self._uid, self._message_socket_path, self._sender_queue)
        )
        sender_process.start()
        return sender_process

    def __enter__(self) -> JournalControlProcess:
        self._runtime_dir = pathlib.Path(tempfile.mkdtemp(prefix="journald_runtime_"))
        os.chown(self._runtime_dir, self._uid, -1)

        self._journald_process = self._start_journald()
        self._sender_process = self._start_sender_processs()

        return self

    def __exit__(self, *args) -> None:
        assert self._runtime_dir
        assert self._journald_process
        assert self._sender_process

        self._sender_queue.put(None)
        self._sender_process.join(timeout=3)

        self._journald_process.terminate()
        self._journald_process.wait(timeout=3)

        shutil.rmtree(self._runtime_dir)


_PARSER = argparse.ArgumentParser(
    usage="""Genrate journald log files.
This program reads messages from stdin in following format

msg Test 1
msg {"MESSAGE": "Test 1"}
rotate
msg Test 2

msg command argument be either plain message or json object
rotate command invokes journald rotation
"""
)
_PARSER.add_argument('--uid', type=int, default=1000, help='user id of log sender')


def main():
    args = _PARSER.parse_args()

    if os.geteuid() != 0:
        raise Exception("Should be run as a root user to be able to rotate")

    logs_dir = pathlib.Path(tempfile.mkdtemp(prefix="journald_logs_"))
    uid = args.uid
    os.chown(logs_dir, uid, -1)

    with JournalControlProcess(logs_dir=logs_dir, uid=uid) as journald_process:

        while entry := input():
            action, *args = entry.strip().split(" ", 1)

            if action == "rotate":
                journald_process.rotate()

            elif action == "msg":
                if len(args) != 1:
                    raise ValueError(f"Not enough args for msg {args}")

                msg = args[0].strip()

                if msg.startswith("{"):
                    msg = json.loads(msg)
                else:
                    msg = {"MESSAGE": msg}

                journald_process.send_message(msg)

    print(f"Logs avaialble in {logs_dir} directory")
    print("To see generated logs use following command:")
    print(f"journalctl -D {logs_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
