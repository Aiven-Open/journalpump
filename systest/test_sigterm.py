# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump_process import JournalpumpProcess
from pathlib import Path

import json
import os

# The SIGTERM handler unregisters each reader from self.poller.
# It writes _STALE_FD into reader_by_fd and returns to the run loop.
# Each iteration later deletes those _STALE_FD keys from reader_by_fd.
# If SIGTERM arrives after the loop deletes those keys, and before the
# while self.running loop ends, _close_stale_readers unregisters the
# same fd from self.poller again.
# ping_watchdog sits in that window.
# Hold ping_watchdog for 2 seconds so stop() can send SIGTERM there.
_HOLD_WATCHDOG = """\
from systemd import daemon

import time

_notify = daemon.notify


def notify(status, *args, **kwargs):
    result = _notify(status, *args, **kwargs)
    if status == "WATCHDOG=1":
        time.sleep(2)
    return result


daemon.notify = notify
"""


def test_sigterm_after_first_iteration_exits_cleanly(tmp_path: Path) -> None:
    (tmp_path / "sitecustomize.py").write_text(_HOLD_WATCHDOG, encoding="utf-8")
    pythonpath = str(tmp_path)
    if existing_pythonpath := os.environ.get("PYTHONPATH"):
        pythonpath = pythonpath + os.pathsep + existing_pythonpath
    config_path = tmp_path / "journalpump.json"
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(
            json.dumps({"readers": {"reader": {"senders": {}}}}),
            encoding="utf-8",
        )
        journalpump_process.start(config_path, env={"PYTHONPATH": pythonpath})
        journalpump_process._wait_notification("WATCHDOG=1", timeout=5)
        returncode = journalpump_process.stop()
        stderr = ""
        if journalpump_process._journalpump is not None and journalpump_process._journalpump.stderr is not None:
            stderr = journalpump_process._journalpump.stderr.read()
        assert returncode == 0, stderr
