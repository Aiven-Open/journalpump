# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump_process import JournalpumpProcess
from pathlib import Path
from typing import Any

import json
import stat


def _assert_private_running_config(path: Path, config: dict[str, Any]) -> None:
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert json.loads(path.read_text(encoding="utf-8")) == config


def test_process_writes_running_config_to_runtime_directory(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    config: dict[str, Any] = {"write_running_config": True, "readers": {"reader": {"senders": {}}}}
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        _assert_private_running_config(journalpump_process.runtime_directory / "running_config.json", config)


def test_process_writes_running_config_with_secret_filters(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    config: dict[str, Any] = {
        "write_running_config": True,
        "readers": {
            "host": {
                "senders": {},
                "secret_filters": [
                    {
                        "pattern": "AVNS_[A-Za-z0-9-_]{15,123}",
                        "replacement": "[REDACTED]",
                    },
                ],
            }
        },
    }
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        _assert_private_running_config(journalpump_process.runtime_directory / "running_config.json", config)


def test_process_does_not_write_running_config_without_opt_in(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    config: dict[str, Any] = {"readers": {"reader": {"senders": {}}}}
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        assert not (journalpump_process.runtime_directory / "running_config.json").exists()


def test_process_does_not_write_running_config_from_path_alone(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    running_config = tmp_path / "running" / "config.json"
    config: dict[str, Any] = {
        "json_running_config_path": str(running_config),
        "readers": {"reader": {"senders": {}}},
    }
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        assert not running_config.exists()
        assert not (journalpump_process.runtime_directory / "running_config.json").exists()


def test_process_does_not_write_running_config_when_enabled_without_path(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    config: dict[str, Any] = {"write_running_config": True, "readers": {"reader": {"senders": {}}}}
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path, export_runtime_directory=False)
        assert not (journalpump_process.runtime_directory / "running_config.json").exists()


def test_process_writes_running_config_to_configured_path(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    running_config = tmp_path / "running" / "config.json"
    config: dict[str, Any] = {
        "write_running_config": True,
        "json_running_config_path": str(running_config),
        "readers": {"reader": {"senders": {}}},
    }
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        _assert_private_running_config(running_config, config)
        assert stat.S_IMODE(running_config.parent.stat().st_mode) == 0o700
        assert not (journalpump_process.runtime_directory / "running_config.json").exists()


def test_process_does_not_chmod_existing_running_config_directory(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    running_dir = tmp_path / "running"
    running_dir.mkdir()
    running_dir.chmod(0o755)
    running_config = running_dir / "config.json"
    config: dict[str, Any] = {
        "write_running_config": True,
        "json_running_config_path": str(running_config),
        "readers": {"reader": {"senders": {}}},
    }
    with JournalpumpProcess() as journalpump_process:
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.start(config_path)
        _assert_private_running_config(running_config, config)
        assert stat.S_IMODE(running_dir.stat().st_mode) == 0o755
