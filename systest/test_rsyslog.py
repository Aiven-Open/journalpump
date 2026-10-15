# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .util import journalpump_initialized
from journalpump.journalpump import JournalPump
from pathlib import Path
from systemd import journal
from time import sleep
from typing import Any

import json
import logging
import os
import pytest
import random
import socket
import string
import subprocess
import threading

# NOTE: make sure to use google-re >= 1.1 if this is enabled.
if os.environ.get("USE_RE2"):
    import re2 as re
else:
    import re


RSYSLOGD = "/usr/sbin/rsyslogd"

RSYSLOGD_TCP_CONF = """
$ModLoad imtcp
$InputTCPServerRun {port}
$template RemoteLogs,"{logfile}"
*.* ?RemoteLogs
& ~
"""

log = logging.getLogger(__name__)


class _TestRsyslogd:
    def __init__(self, *, workdir: Path, logfile: Path, port: int) -> None:
        if not os.path.exists(RSYSLOGD):
            raise RuntimeError(f'"{RSYSLOGD}" not available')

        self.port = port
        self.conffile = workdir / "rsyslogd.conf"
        self.process: subprocess.Popen[bytes] | None = None

        with open(self.conffile, "w", encoding="utf-8") as fp:
            print(RSYSLOGD_TCP_CONF.format(logfile=os.fspath(logfile), port=port), file=fp)

    def _wait_until_running(self) -> None:
        # Wait until the rsyslogd port is available, but if it is not up in
        # five seconds assume that it has failed to start
        attempt = 0
        s = socket.socket()
        if self.process is None:
            raise RuntimeError("rsyslogd was not started")
        while (self.process.poll() is None) and (attempt < 5):
            if s.connect_ex(("127.0.0.1", self.port)) == 0:
                s.close()
                return
            sleep(1)
            attempt += 1
        raise RuntimeError("rsyslogd failed to start correctly")

    def start(self) -> None:
        # Start rsyslogd in the foreground
        # pylint: disable=consider-using-with

        # In CI, try to disable AppArmor for rsyslogd otherwise it cannot start
        if os.environ.get("GITHUB_ACTIONS") == "true":
            try:
                subprocess.run(
                    [
                        "sudo",
                        "ln",
                        "-sf",
                        "/etc/apparmor.d/usr.sbin.rsyslogd",
                        "/etc/apparmor.d/disable/",
                    ],
                    check=False,
                    capture_output=True,
                    timeout=5,
                )
                subprocess.run(
                    [
                        "sudo",
                        "apparmor_parser",
                        "-R",
                        "/etc/apparmor.d/usr.sbin.rsyslogd",
                    ],
                    check=False,
                    capture_output=True,
                    timeout=5,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass

        self.process = subprocess.Popen([RSYSLOGD, "-f", self.conffile, "-i", "NONE", "-n", "-C"])

        self._wait_until_running()

    def stop(self) -> None:
        if self.process is not None:
            if self.process.poll() is not None:
                raise RuntimeError("rsyslogd did not start properly")
            self.process.terminate()
            self.process.wait(timeout=5)
            self.process = None


def _send_through_pump(*, config_path: Path, messages_to_send: list[dict[str, Any]]) -> str:
    """Run journalpump, send the messages to the journal and return the identifier used in them."""
    journalpump = None
    threads = []
    try:
        journalpump = JournalPump(config_path)
        pump = threading.Thread(target=journalpump.run)
        pump.start()
        threads.append(pump)

        assert journalpump_initialized(journalpump), "Failed to initialize journalpump"

        identifier = "".join(random.sample(string.ascii_uppercase + string.digits, k=8))
        for msg in messages_to_send:
            msg_text = msg["text"].format(identifier=identifier)
            kwargs = {
                key: value.format(identifier=identifier) if isinstance(value, str) else value
                for key, value in msg.items()
                if key != "text"
            }
            log.info("Sending: %s", msg_text)
            journal.send(msg_text, **kwargs)
        # Wait for everything to trickle thru
        sleep(5)
    finally:
        # Stop the journalpump and senders
        if journalpump is not None:
            journalpump.running = False
            for reader in journalpump.readers.values():
                for sender in reader.senders.values():
                    threads.append(sender)
                    sender.request_stop()

        # Wait a little while for threads to finish
        retry = 0
        while retry < 5:
            if not [thread for thread in threads if thread.is_alive()]:
                break
            sleep(1)
            retry += 1
    return identifier


def _assert_syslog_results(
    *,
    logfile: Path,
    identifier: str,
    expected_message_count: int,
    expected_info_line_ending: str | None = None,
    expected_subsequent_message: str | None = None,
    expected_separate_lines: list[str] | None = None,
    forged_marker: str | None = None,
) -> None:
    found = 0
    info_line = None
    subsequent_message_found = False
    with open(logfile, "r", encoding="utf-8") as fp:
        lines = fp.readlines()
    for txt in ["Info", "Warning", "Error", "Critical"]:
        m = re.compile(rf".*{txt} message for {identifier}.*")
        for line in lines:
            if m.match(line):
                log.info("Found: %s", line)
                found += 1
                if txt == "Info":
                    info_line = line
                break
    if expected_subsequent_message is not None:
        subsequent_pattern = re.compile(rf".*{expected_subsequent_message} for {identifier}.*")
        for line in lines:
            if subsequent_pattern.match(line):
                log.info("Found subsequent message: %s", line)
                subsequent_message_found = True
                break
        assert subsequent_message_found, (
            "Subsequent message not found — connection likely desynced after oversize octet-counted frame"
        )
    assert found == expected_message_count, "Expected messages not found in syslog"
    if expected_separate_lines is not None:
        # The entry was split by the receiver: each part must sit on its own line of the log file.
        assert info_line is not None, "Info message not found in syslog"
        assert "\\n" not in info_line, f"Newline unexpectedly escaped: {info_line!r}"
        for part_template in expected_separate_lines:
            part = part_template.format(identifier=identifier)
            assert any(part in line and "Info message" not in line for line in lines), (
                f"{part!r} not found on a separate line: {lines!r}"
            )
    if forged_marker is not None:
        marker = forged_marker.format(identifier=identifier)
        assert any(f"victim_{marker}" in line for line in lines), f"Header field was not sanitized: {lines!r}"
    if expected_info_line_ending is not None:
        assert info_line is not None, "Info message not found in syslog"
        assert info_line.endswith(expected_info_line_ending), (
            f"Info line ending mismatch: expected {expected_info_line_ending!r}, got {info_line!r}"
        )


@pytest.mark.parametrize(
    (
        "messages_to_send,sender_config,expected_message_count,expected_info_line_ending,expected_subsequent_message,"
        "expected_separate_lines"
    ),
    [
        (
            [
                {"text": "Info message for {identifier}", "PRIORITY": journal.LOG_INFO},
                {
                    "text": "Warning message for {identifier}",
                    "PRIORITY": journal.LOG_WARNING,
                },
                {"text": "Error message for {identifier}", "PRIORITY": journal.LOG_ERR},
                {
                    "text": "Critical message for {identifier}",
                    "PRIORITY": journal.LOG_CRIT,
                },
            ],
            {},  # config not specified, octet_counted_framing is False and escape_newlines is True by default
            4,
            None,
            None,
            None,
        ),
        # Default config: a multi-line entry must reach rsyslog as a single escaped record.
        (
            [
                {
                    "text": "Info message for {identifier}\nexample\nstack\ntrace",
                    "PRIORITY": journal.LOG_INFO,
                },
            ],
            {},
            1,
            "example\\nstack\\ntrace {%} -\n",
            None,
            None,
        ),
        # Opt-out: the receiver splits a multi-line entry into separate lines.
        (
            [
                {
                    "text": "Info message for {identifier}\nexample-{identifier}\nstack-{identifier}\ntrace-{identifier}",
                    "PRIORITY": journal.LOG_INFO,
                },
            ],
            {"escape_newlines": False},
            1,
            None,
            None,
            ["example-{identifier}", "stack-{identifier}", "trace-{identifier}"],
        ),
        (
            [
                {
                    "text": "Info message for {identifier}\nexample\nstack\ntrace",
                    "PRIORITY": journal.LOG_INFO,
                },
            ],
            {"octet_counted_framing": True},
            1,
            "example#012stack#012trace {%} -\n",
            None,
            None,
        ),
        # Verify that an oversize octet-counted frame is truncated without desyncing the stream.
        # A short subsequent message on the same connection must still be delivered correctly.
        (
            [
                {
                    "text": "Info message for {identifier}\n" + "X" * 300,
                    "PRIORITY": journal.LOG_INFO,
                },
                {
                    "text": "Critical message for {identifier}",
                    "PRIORITY": journal.LOG_CRIT,
                },
            ],
            {"octet_counted_framing": True, "max_message_size": 200},
            2,
            None,
            "Critical message",
            None,
        ),
    ],
)
def test_rsyslogd_tcp_sender(
    tmp_path: Path,
    messages_to_send: list[dict[str, Any]],
    sender_config: dict[str, Any],
    expected_message_count: int,
    expected_info_line_ending: str | None,
    expected_subsequent_message: str | None,
    expected_separate_lines: list[str] | None,
) -> None:
    _run_tcp_scenario(
        tmp_path=tmp_path,
        messages_to_send=messages_to_send,
        sender_config=sender_config,
        expected_message_count=expected_message_count,
        expected_info_line_ending=expected_info_line_ending,
        expected_subsequent_message=expected_subsequent_message,
        expected_separate_lines=expected_separate_lines,
    )


@pytest.mark.parametrize(
    "sender_config",
    [
        {"escape_newlines": False},
        {"octet_counted_framing": True},
    ],
    ids=["non_transparent", "octet_counted"],
)
def test_rsyslogd_forged_program_name(tmp_path: Path, sender_config: dict[str, Any]) -> None:
    """A newline in the program name must not let the receiver see a second, forged record."""
    _run_tcp_scenario(
        tmp_path=tmp_path,
        messages_to_send=[
            {
                "text": "Info message for {identifier}",
                "PRIORITY": journal.LOG_INFO,
                "SYSLOG_IDENTIFIER": "app\n<13>1 victim forged-{identifier}",
            },
        ],
        sender_config=sender_config,
        expected_message_count=1,
        forged_marker="forged-{identifier}",
    )


def _run_tcp_scenario(
    *,
    tmp_path: Path,
    messages_to_send: list[dict[str, Any]],
    sender_config: dict[str, Any],
    expected_message_count: int,
    expected_info_line_ending: str | None = None,
    expected_subsequent_message: str | None = None,
    expected_separate_lines: list[str] | None = None,
    forged_marker: str | None = None,
) -> None:
    workdir = tmp_path
    logfile = tmp_path / "test.log"
    config_path = tmp_path / "journalpump.json"
    with open(config_path, "w", encoding="utf-8") as fp:
        json.dump(
            {
                "readers": {
                    "syslog-tcp": {
                        "initial_position": "tail",
                        "senders": {
                            "rsyslog": {
                                "output_type": "rsyslog",
                                "rsyslog_server": "127.0.0.1",
                                "rsyslog_port": 5140,
                                "format": "custom",
                                "logline": "<%pri%>%timestamp% %HOSTNAME% %app-name%[%procid%]: %msg% {%%} %not-valid-tag%",
                                **dict(sender_config),
                            },
                        },
                    },
                },
            },
            fp,
        )
    rsyslogd = _TestRsyslogd(workdir=workdir, logfile=logfile, port=5140)
    try:
        rsyslogd.start()
        identifier = _send_through_pump(config_path=config_path, messages_to_send=messages_to_send)
        _assert_syslog_results(
            logfile=logfile,
            identifier=identifier,
            expected_message_count=expected_message_count,
            expected_info_line_ending=expected_info_line_ending,
            expected_subsequent_message=expected_subsequent_message,
            expected_separate_lines=expected_separate_lines,
            forged_marker=forged_marker,
        )
    finally:
        rsyslogd.stop()
