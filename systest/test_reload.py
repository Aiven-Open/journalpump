# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump_process import JournalpumpProcess
from pathlib import Path

import json


def test_sighup_with_changed_readers_keeps_process_alive(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(
            json.dumps({"readers": {"before_reload": {"senders": {}}}}),
            encoding="utf-8",
        )
        journalpump_process.start(config_path)
        config_path.write_text(
            json.dumps({"readers": {"after_reload": {"senders": {}}}}),
            encoding="utf-8",
        )
        journalpump_process.reload()
