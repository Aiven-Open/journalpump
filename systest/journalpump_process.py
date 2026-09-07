# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from pathlib import Path
from typing import Self

import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


def _raise_if_journalpump_exited(journalpump: subprocess.Popen[str] | None, *, waiting_for: str) -> None:
    if journalpump is None or journalpump.poll() is None:
        return
    stderr = journalpump.stderr.read() if journalpump.stderr is not None else ""
    raise AssertionError(f"journalpump exited with {journalpump.returncode} before {waiting_for}\n{stderr}")


def _read_notifications(
    notify_socket: socket.socket,
    notifications: queue.SimpleQueue[str],
) -> None:
    while True:
        try:
            notify_datagram = notify_socket.recv(4096)
        except OSError:
            return
        if not notify_datagram:
            return
        for line in notify_datagram.decode("ascii").split("\n"):
            if line:
                notifications.put(line)


class JournalpumpProcess:
    def __enter__(self) -> Self:
        self.runtime_directory = Path(tempfile.mkdtemp(prefix="jp-runtime-"))
        self._notify_socket_name = f"jp-systest-{os.getpid()}-{os.urandom(4).hex()}"
        self._notify_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._notify_socket.bind("\0" + self._notify_socket_name)
        self._notifications: queue.SimpleQueue[str] = queue.SimpleQueue()
        self._journalpump: subprocess.Popen[str] | None = None
        self._notify_reader_thread = threading.Thread(
            target=_read_notifications,
            args=(self._notify_socket, self._notifications),
            name="journalpump-notify-reader",
            daemon=True,
        )
        self._notify_reader_thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
        self._notify_socket.close()
        self._notify_reader_thread.join(timeout=2)
        shutil.rmtree(self.runtime_directory, ignore_errors=True)

    def _wait_notification(self, notification: str, *, timeout: float) -> None:
        start_time = time.monotonic()
        elapsed_time = 0.0
        while elapsed_time < timeout:
            try:
                if self._notifications.get(timeout=timeout - elapsed_time) == notification:
                    return
            except queue.Empty:
                break
            elapsed_time = time.monotonic() - start_time
        _raise_if_journalpump_exited(self._journalpump, waiting_for=notification)
        raise TimeoutError(f"journalpump did not send {notification}")

    def start(self, config_path: Path, *, env: dict[str, str] | None = None, export_runtime_directory: bool = True) -> None:
        journalpump_env = os.environ.copy()
        if env:
            journalpump_env.update(env)
        if export_runtime_directory:
            journalpump_env["RUNTIME_DIRECTORY"] = str(self.runtime_directory)
        else:
            journalpump_env.pop("RUNTIME_DIRECTORY", None)
        journalpump_env["NOTIFY_SOCKET"] = "@" + self._notify_socket_name
        journalpump_env["PYTHONUNBUFFERED"] = "1"
        self._journalpump = subprocess.Popen(
            [sys.executable, "-m", "journalpump", str(config_path)],
            env=journalpump_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._wait_notification("READY=1", timeout=10)

    def reload(self) -> None:
        if self._journalpump is None or self._journalpump.pid is None:
            raise RuntimeError("journalpump is not running")
        os.kill(self._journalpump.pid, signal.SIGHUP)
        self._wait_notification("RELOADING=1", timeout=3)
        self._wait_notification("READY=1", timeout=3)

    def stop(self) -> int | None:
        if self._journalpump is None:
            return None
        if self._journalpump.poll() is None:
            self._journalpump.send_signal(signal.SIGTERM)
            try:
                self._journalpump.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self._journalpump.kill()
                self._journalpump.wait(timeout=5)
        return self._journalpump.returncode
