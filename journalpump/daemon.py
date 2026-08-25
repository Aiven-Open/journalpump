# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""
Common Daemon functionality

"""

from pathlib import Path
from systemd import daemon, journal
from types import FrameType
from typing import Any

import json
import logging
import os
import signal
import sys

LOG_FORMAT = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
LOG_FORMAT_JOURNAL = "%(name)-20s  %(levelname)-8s  %(message)s"
LOG_FORMAT_JOURNAL_MULTI_THREAD = "%(name)-20s  %(threadName)-15s %(levelname)-8s  %(message)s"


class ServiceDaemonError(Exception):
    """ServiceDaemon error"""


class ServiceDaemon:
    def __init__(
        self,
        config_path: Path,
        require_config: bool = True,
        multi_threaded: bool = False,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.name = self.__class__.__name__.lower()
        self.log_level = log_level
        self.multi_threaded = multi_threaded
        self.journal_handler: logging.Handler | None = None
        self.configure_logging()
        self.log = logging.getLogger(self.name)
        self.config_path = config_path
        self.config_file_ctime: float | None = None
        self.config: dict[str, Any] = {}
        self.require_config = require_config
        self.reload_config()
        self.running = True

        signal.signal(signal.SIGHUP, self.sighup)
        signal.signal(signal.SIGINT, self.sigterm)
        signal.signal(signal.SIGTERM, self.sigterm)

        self.log.debug("Initialized with config_path: %r", config_path)

    def configure_logging(self) -> None:
        if os.isatty(sys.stdout.fileno()):
            # stdout logging is only enabled for user tty sessions
            logging.basicConfig(level=self.log_level, format=LOG_FORMAT)
        else:
            if self.journal_handler is None:
                self.journal_handler = journal.JournalHandler(SYSLOG_IDENTIFIER=self.name)
                logging.root.addHandler(self.journal_handler)
            self.journal_handler.setLevel(self.log_level)
            self.journal_handler.setFormatter(
                logging.Formatter(LOG_FORMAT_JOURNAL_MULTI_THREAD if self.multi_threaded else LOG_FORMAT_JOURNAL)
            )
            logging.root.setLevel(self.log_level)

    def sighup(self, signum: int, frame: FrameType | None) -> None:  # pylint: disable=unused-argument
        self.log.info("Received SIGHUP, reloading config")
        self.reload_config()

    def sigterm(self, signum: int, frame: FrameType | None) -> None:  # pylint: disable=unused-argument
        self.log.info(
            "Received SIG%s, stopping daemon...",
            "TERM" if (signum == signal.SIGTERM) else "INT",
        )
        daemon.notify("STOPPING=1")
        self.running = False

    def ping_watchdog(self) -> None:
        """Let systemd know we are still alive and well"""
        daemon.notify("WATCHDOG=1")

    def reload_config(self) -> None:
        file_ctime = None
        try:
            file_ctime = self.config_path.stat().st_ctime
        except FileNotFoundError as ex:
            if self.require_config:
                raise ServiceDaemonError(f"Cannot start without json config file at {self.config_path!r}") from ex

        if file_ctime != self.config_file_ctime:
            daemon.notify("RELOADING=1")
            self.log.info("%soading configuration", "Rel" if self.config_file_ctime else "L")
            self.config_file_ctime = file_ctime
            with self.config_path.open(encoding="utf-8") as fp:
                self.config = json.load(fp)
            self.log_level = self.config.get("log_level", logging.INFO)
            self.configure_logging()
            self.handle_new_config()
            daemon.notify("READY=1")

    def handle_new_config(self) -> None:
        """Override in subclass"""

    def run(self) -> int | None:
        raise NotImplementedError()

    def cleanup(self) -> None:
        pass

    @classmethod
    def main(cls, args: list[str]) -> int | None:
        if len(args) != 1:
            print("config file argument required")
            return 1

        exe = None
        try:
            exe = cls(config_path=Path(args[0]))
            daemon.notify("READY=1")
            return exe.run()
        except ServiceDaemonError as ex:
            logging.fatal("%s failed to start: %s", cls.__name__, ex)
            return 1
        finally:
            if exe:
                exe.cleanup()

    @classmethod
    def run_exit(cls) -> None:
        sys.exit(cls.main(sys.argv[1:]))
