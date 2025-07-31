# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .util import journalpump_initialized
from journalpump.journalpump import JournalPump
from subprocess import Popen
from time import sleep

import json
import logging
import logging.handlers
import os
import random
import socket
import string
import threading

# NOTE: make sure to use google-re >= 1.1 if this is enabled.
if os.environ.get("USE_RE2"):
    import re2 as re
else:
    import re  # type: ignore[no-redef]


RSYSLOGD = "/usr/sbin/rsyslogd"

RSYSLOGD_TCP_CONF = """
$ModLoad imtcp
$InputTCPServerRun {port}
$template RemoteLogs,"{logfile}"
*.* ?RemoteLogs
& ~
"""


class _TestRsyslogd:
    def __init__(self, *, workdir, logfile, port):
        if not os.path.exists(RSYSLOGD):
            raise RuntimeError(f'"{RSYSLOGD}" not available')

        self.port = port
        self.conffile = f"{workdir}/rsyslogd.conf"
        self.process = None

        with open(self.conffile, "w", encoding="utf-8") as fp:
            print(RSYSLOGD_TCP_CONF.format(logfile=logfile, port=port), file=fp)

    def _wait_until_running(self):
        # Wait until the rsyslogd port is available, but if it is not up in
        # five seconds assume that it has failed to start
        attempt = 0
        s = socket.socket()
        while (self.process.poll() is None) and (attempt < 5):
            if s.connect_ex(("127.0.0.1", self.port)) == 0:
                s.close()
                return
            sleep(1)
            attempt += 1
        raise RuntimeError("rsyslogd failed to start correctly")

    def start(self):
        # Start rsyslogd in the foreground
        # pylint: disable=consider-using-with
        self.process = Popen([RSYSLOGD, "-f", self.conffile, "-i", "NONE", "-n", "-C"])
        self._wait_until_running()

    def stop(self):
        if self.process is not None:
            if self.process.poll() is not None:
                raise RuntimeError("rsyslogd did not start properly")
            self.process.terminate()
            self.process.wait(timeout=5)
            self.process = None


def _run_pump_test(*, config_path, logfile):
    journalpump = None
    threads = []
    try:
        journalpump = JournalPump(config_path)
        pump = threading.Thread(target=journalpump.run)
        pump.start()
        threads.append(pump)

        assert journalpump_initialized(journalpump), "Failed to initialize journalpump"
        identifier = "".join(random.sample(string.ascii_uppercase + string.digits, k=8))
        logger = logging.getLogger("rsyslog-tester")
        logger.info("Info message for %s", identifier)
        logger.warning("Warning message for %s", identifier)
        logger.error("Error message for %s", identifier)
        logger.critical("Critical message for %s", identifier)
        # Wait for everything to trickle thru
        sleep(5)
    finally:
        # Stop the journalpump and senders
        if journalpump is not None:
            journalpump.running = False
            for _, reader in journalpump.readers.items():
                for _, sender in reader.senders.items():
                    threads.append(sender)
                    sender.request_stop()

        # Wait a little while for threads to finish
        retry = 0
        while retry < 5:
            if not [thread for thread in threads if thread.is_alive()]:
                break
            sleep(1)
            retry += 1

    # Check the results
    found = 0
    with open(logfile, "r", encoding="utf-8") as fp:
        lines = fp.readlines()
    for txt in ["Info", "Warning", "Error", "Critical"]:
        m = re.compile(rf".*{txt} message for {identifier}.*")
        for line in lines:
            if m.match(line):
                found += 1
                break
    assert found == 4, "Expected messages not found in syslog"


def test_rsyslogd_tcp_sender(tmpdir):
    workdir = tmpdir.dirname
    logfile = f"{workdir}/test.log"
    config_path = f"{workdir}/journalpump.json"
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
        _run_pump_test(config_path=config_path, logfile=logfile)
    finally:
        rsyslogd.stop()
