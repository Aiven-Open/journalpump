from collections.abc import Callable, Iterator
from contextlib import contextmanager
from journalpump.journalpump import JournalPump
from journalpump.senders.base import LogSender
from pathlib import Path
from typing import Any

import json
import re
import socketserver
import threading
import time


def journalpump_initialized(journalpump: JournalPump) -> bool:
    retry = 0
    senders: list[LogSender] = []
    while retry < 3 and not senders:
        time.sleep(1)
        readers = [reader for _, reader in journalpump.readers.items()]
        senders = []
        if readers:
            for reader in readers:
                senders.extend([sender for _, sender in reader.senders.items()])
        retry += 1

    return bool(journalpump.running and senders)


def wait_for(predicate: Callable[[], bool], timeout: int) -> bool:
    cur = time.monotonic()
    deadline = cur + timeout

    while cur < deadline:
        if predicate():
            return True

        cur = time.monotonic()
        time.sleep(0.2)

    return False


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        for line in self.rfile:
            if m := re.search(rb"reload-dup (\S+)", line):
                self.server.messages.append(m.group(1).decode())  # type: ignore[attr-defined]


@contextmanager
def syslog_collector() -> Iterator[tuple[int, list[str]]]:
    """TCP syslog receiver recording delivered messages; each sender connects separately"""
    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Handler) as server:
        server.daemon_threads = True
        server.messages = []  # type: ignore[attr-defined]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            yield server.server_address[1], server.messages  # type: ignore[attr-defined]
        finally:
            server.shutdown()


def read_state(state_path: Path) -> dict[str, Any]:
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}
