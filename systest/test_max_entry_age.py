# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump_process import JournalpumpProcess
from .util import syslog_collector, wait_for
from collections import Counter
from pathlib import Path
from systemd import journal
from typing import Any

import json
import os
import time

# The recent entries must still be younger than this when journalpump reads them, so it bounds the time journalpump
# may take to start up on a slow machine; the old entries are made older than it by sleeping, so it costs test time
MAX_ENTRY_AGE_SECS = 10


def _send_and_wait_ingested(identifier: str, labels: list[str], *, total: int) -> list[str]:
    """Send one entry per label, then return the cursors of the identifier's entries once journald has `total`"""
    for label in labels:
        journal.send(f"max-age {label}", SYSLOG_IDENTIFIER=identifier)
    cursors: list[str] = []

    def ingested() -> bool:
        # journal.send() does not wait for journald, so poll with a fresh reader until every entry is visible
        reader = journal.Reader()
        reader.add_match(SYSLOG_IDENTIFIER=identifier)
        cursors[:] = [reader._get_cursor() for _ in reader]  # pylint: disable=protected-access
        reader.close()
        return len(cursors) == total

    assert wait_for(ingested, timeout=30), (
        f"expected journald to have {total} entries for {identifier!r} within 30s, found {len(cursors)}"
    )
    return cursors


def test_resume_skips_entries_older_than_max_entry_age(tmp_path: Path) -> None:
    config_path = tmp_path / "journalpump.json"
    state_path = tmp_path / "journalpump_state.json"
    identifier = f"jp-max-age-{os.getpid()}-{os.urandom(4).hex()}"
    old = [f"old-{i}" for i in range(3)]
    recent = [f"recent-{i}" for i in range(3)]

    cursors = _send_and_wait_ingested(identifier, old, total=len(old))
    time.sleep(MAX_ENTRY_AGE_SECS + 1)
    _send_and_wait_ingested(identifier, recent, total=len(old) + len(recent))

    with syslog_collector("max-age") as (port, messages), JournalpumpProcess() as journalpump_process:
        config: dict[str, Any] = {
            "json_state_file_path": str(state_path),
            "readers": {
                "main": {
                    "max_entry_age_secs": MAX_ENTRY_AGE_SECS,
                    "journald_filters": [f"SYSLOG_IDENTIFIER={identifier}"],
                    "senders": {"out": {"output_type": "rsyslog", "rsyslog_server": "127.0.0.1", "rsyslog_port": port}},
                },
            },
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        # Resume from the first old entry, so the rest of the old entries would be sent without max_entry_age_secs
        state = {"start_time": "2000-01-01T00:00:00", "readers": {"main": {"cursor": cursors[0]}}}
        state_path.write_text(json.dumps(state), encoding="utf-8")

        journalpump_process.start(config_path)

        # The reader walks the journal in order: once the sentinel is delivered, everything before it has been too
        journal.send("max-age sentinel", SYSLOG_IDENTIFIER=identifier)
        assert wait_for(lambda: "sentinel" in messages, timeout=30), (
            f"expected the sentinel entry to be delivered within 30s, delivered so far: {messages}"
        )

    assert Counter(messages) == Counter([*recent, "sentinel"]), (
        f"expected only entries younger than {MAX_ENTRY_AGE_SECS}s ({recent}) and  sentinel to be delivered, "
        f"delivered: {messages}"
    )
