# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from journalpump.journalpump import JournalPump, running_config_path
from pathlib import Path
from pytest import LogCaptureFixture

import json
import logging
import pytest


def test_running_config_path_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNTIME_DIRECTORY", raising=False)
    configured = tmp_path / "running.json"
    assert running_config_path(configured_path=str(configured)) == configured


def test_running_config_path_runtime_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_DIRECTORY", str(tmp_path))
    assert running_config_path(configured_path=None) == tmp_path / "running_config.json"


def test_running_config_path_configured_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_DIRECTORY", str(tmp_path / "runtime"))
    configured = tmp_path / "running.json"
    assert running_config_path(configured_path=str(configured)) == configured


def test_running_config_path_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNTIME_DIRECTORY", raising=False)
    assert running_config_path(configured_path=None) is None


def test_write_running_config_warns_when_path_set_without_enable(tmp_path: Path, caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    config_path = tmp_path / "journalpump.json"
    config_path.write_text(
        json.dumps(
            {
                "json_running_config_path": str(tmp_path / "running.json"),
                "readers": {"reader": {"senders": {}}},
            }
        ),
        encoding="utf-8",
    )
    JournalPump(config_path)
    assert "json_running_config_path is set but write_running_config is not true. Not writing running config." in [
        record.message for record in caplog.records
    ]
