# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump_process import JournalpumpProcess
from .util import read_state, syslog_collector, wait_for
from collections import Counter
from pathlib import Path
from systemd import journal
from typing import Any

import json
import os


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


def test_start_resumes_from_oldest_sender_cursor(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    state_path = tmp_path / "journalpump_state.json"
    identifier = f"jp-oldest-{os.getpid()}-{os.urandom(4).hex()}"
    labels = [f"m{i}" for i in range(6)]
    for label in labels:
        journal.send(f"reload-dup {label}", SYSLOG_IDENTIFIER=identifier)
    cursors: list[str] = []

    def all_ingested() -> bool:
        # journal.send() does not wait for journald, so poll with a fresh reader until every entry is visible
        reader = journal.Reader()
        reader.add_match(SYSLOG_IDENTIFIER=identifier)
        cursors[:] = [reader._get_cursor() for _ in reader]  # pylint: disable=protected-access
        reader.close()
        return len(cursors) == len(labels)

    assert wait_for(all_ingested, timeout=30), "journald ingestion"

    with (
        syslog_collector() as (slow_port, slow_messages),
        syslog_collector() as (fast_port, fast_messages),
        JournalpumpProcess() as journalpump_process,
    ):
        config: dict[str, Any] = {
            "json_state_file_path": str(state_path),
            "readers": {
                "main": {
                    "journald_filters": [f"SYSLOG_IDENTIFIER={identifier}"],
                    "senders": {
                        "a_slow": {"output_type": "rsyslog", "rsyslog_server": "127.0.0.1", "rsyslog_port": slow_port},
                        "z_fast": {"output_type": "rsyslog", "rsyslog_server": "127.0.0.1", "rsyslog_port": fast_port},
                    },
                },
            },
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        state = {
            "start_time": "2000-01-01T00:00:00",
            "readers": {
                "main": {
                    "cursor": cursors[3],
                    "senders": {"a_slow": {"sent": {"cursor": cursors[1]}}, "z_fast": {"sent": {"cursor": cursors[3]}}},
                }
            },
        }
        state_path.write_text(json.dumps(state), encoding="utf-8")

        journalpump_process.start(config_path)

        assert wait_for(lambda: set(labels[2:]) <= set(slow_messages), timeout=30), "slow sender delivery"
        assert wait_for(lambda: set(labels[4:]) <= set(fast_messages), timeout=30), "fast sender delivery"

    assert Counter(slow_messages) == Counter(labels[2:])


def test_sighup_after_restart_without_new_delivery_does_not_replay_journal(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    state_path = tmp_path / "journalpump_state.json"
    identifier = f"jp-reload-dup-{os.getpid()}-{os.urandom(4).hex()}"
    delivered = [f"delivered-{i}" for i in range(5)]

    with syslog_collector() as (port, messages), JournalpumpProcess() as journalpump_process:
        config: dict[str, Any] = {
            "json_state_file_path": str(state_path),
            "readers": {
                "main": {
                    "journald_filters": [f"SYSLOG_IDENTIFIER={identifier}"],
                    "senders": {"out": {"output_type": "rsyslog", "rsyslog_server": "127.0.0.1", "rsyslog_port": port}},
                },
            },
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        for label in delivered:
            journal.send(f"reload-dup {label}", SYSLOG_IDENTIFIER=identifier)

        journalpump_process.start(config_path)

        assert wait_for(lambda: set(delivered) <= set(messages), timeout=30), "delivery"

        # A clean shutdown saves the state after the senders finish
        assert journalpump_process.stop() == 0, "clean shutdown"

        first_start_time = read_state(state_path)["start_time"]

        # Nothing new arrives, so the restarted sender has no delivery of its own
        # when its first periodic state save runs.
        journalpump_process.start(config_path)
        assert wait_for(lambda: read_state(state_path)["start_time"] != first_start_time, timeout=30), (
            "state save after restart"
        )

        # A changed reader set makes reload recreate all readers, including "main"
        config["readers"]["extra"] = {"senders": {}}
        config_path.write_text(json.dumps(config), encoding="utf-8")
        journalpump_process.reload()
        assert wait_for(lambda: "extra" in read_state(state_path).get("readers", {}), timeout=30), (
            "state save with reloaded readers"
        )
        # The reader walks the journal in order: once the sentinel is delivered,
        # anything replayed from before it has been delivered too.
        journal.send("reload-dup sentinel", SYSLOG_IDENTIFIER=identifier)
        assert wait_for(lambda: "sentinel" in messages, timeout=30), "sentinel delivery"

    assert Counter(messages) == Counter([*delivered, "sentinel"])
