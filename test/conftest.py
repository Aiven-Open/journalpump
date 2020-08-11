from pathlib import Path, PurePath
from typing import cast, IO, Iterable, Optional

import os
import pytest
import socket
import subprocess
import sys
import threading
import time


class StandaloneJournalD:
    """TestJournalD encapsulates the data needed to communicate with a test instance"""

    def __init__(self, root_dir: Path, namespace: str):
        self._root_dir = root_dir
        self._namespace = namespace

        self._logs_dir = root_dir / "logs"
        self._run_dir = root_dir / "run"

        self._process: Optional[subprocess.Popen]
        self._idle_thread: Optional[threading.Thread]
        self._wait_thread: Optional[threading.Thread]

        self._dgram_socket: Optional[Path]
        self._socket: Optional[Path]

        self._start_journald()

    def __del__(self):
        self.shutdown()

    def _start_journald(self):
        self._run_dir.mkdir()
        self._logs_dir.mkdir()

        journal_env = os.environ.copy()
        journal_env["RUNTIME_DIRECTORY"] = self._run_dir.as_posix()
        journal_env["LOGS_DIRECTORY"] = self._logs_dir.as_posix()

        # Find the journald binary
        for journald_daemon_path in ("/usr/lib/systemd/systemd-journald", "/lib/systemd/systemd-journald"):
            if os.path.exists(journald_daemon_path):
                break

        self._process = subprocess.Popen([journald_daemon_path, self._namespace], env=journal_env)

        self._socket = self._run_dir / "stdout"
        self._dgram_socket = self._run_dir / "dev-log"

        # Wait for the socket to appear
        start = time.time()
        while True:
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(self._socket.as_posix())
                sock.close()
            except Exception as e:  # pylint: disable=broad-except
                time.sleep(0.05)
                if time.time() - start > 10:
                    raise Exception("timeout waiting for journald to start") from e
            else:
                break

        # journald shuts down if it becomes idle, so we need to keep it alive by
        # connecting to its unix socket.
        def _idle_journal(_process: subprocess.Popen, socket_path: PurePath):
            while True:
                if _process.poll() is not None:
                    return
                try:
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(socket_path.as_posix())
                    time.sleep(1)
                    sock.close()
                except Exception:  # pylint: disable=broad-except
                    pass

        self._idle_thread = threading.Thread(target=_idle_journal, args=(self._process, self._socket))
        self._idle_thread.start()

        # Dump some debugging info to stderr
        sys.stderr.write(f"journald up with pid: {self._process.pid}\n")
        sys.stderr.write(f"journald log dir: {self._logs_dir.as_posix()}\n")

    def send_log(self, s: str):
        # Write a log manually to the journald server
        if self._dgram_socket is None:
            raise Exception("journald socket not found. Has journald been started?")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if not s.endswith("\n"):
            s = f"{s}\n"
        sock.sendto(s.encode("utf8"), self._dgram_socket.as_posix())

    def logs_path(self):
        return self._logs_dir

    def get_logs(self) -> Iterable[str]:
        """Utility method which pipes the output of journalctl cat"""
        p = subprocess.Popen(
            ["journalctl", f"--directory={self._logs_dir.as_posix()}", "-o", "cat"],
            stdout=subprocess.PIPE,
        )

        try:
            while p.poll() is None:
                line = cast(IO[bytes], p.stdout).readline().decode("utf8").rstrip()
                if line != "":
                    yield line
        except Exception:  # pylint: disable=broad-except
            pass

        if p.returncode != 0:
            raise Exception(f"journalctl returned with an error: {p.returncode}")

    def shutdown(self):
        if self._process is not None:
            self._process.kill()
            self._process.wait()

        if self._idle_thread is not None:
            self._idle_thread.join()


@pytest.fixture
def journald_server(request, tmp_path):
    """Starts up a local journald unit"""
    journal_root: Path = tmp_path / "journald"
    journal_root.mkdir()

    jd = StandaloneJournalD(journal_root, f"{request.node.name}{time.time()}")

    yield jd

    jd.shutdown()
