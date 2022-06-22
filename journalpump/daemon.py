# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
"""
Common Daemon functionality

"""
from setproctitle import setproctitle
from systemd import daemon, journal

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
    def __init__(self, config_path, require_config=True, multi_threaded=False, log_level=logging.DEBUG):
        assert isinstance(config_path, str)
        self.name = self.__class__.__name__.lower()
        self.log_level = log_level
        self.multi_threaded = multi_threaded
        self.journal_handler = None
        self.configure_logging()
        self.log = logging.getLogger(self.name)
        self.config_path = config_path
        self.config_file_ctime = None
        self.config = None
        self.require_config = require_config
        self.reload_config()
        self.running = True

        signal.signal(signal.SIGHUP, self.sighup)
        signal.signal(signal.SIGINT, self.sigterm)
        signal.signal(signal.SIGTERM, self.sigterm)

        self.log.debug("Initialized with config_path: %r", config_path)

    def configure_logging(self):
        if os.isatty(sys.stdout.fileno()):
            # stdout logging is only enabled for user tty sessions
            logging.basicConfig(level=self.log_level, format=LOG_FORMAT)
        else:
            if not self.journal_handler:
                self.journal_handler = journal.JournalHandler(SYSLOG_IDENTIFIER=self.name)
                logging.root.addHandler(self.journal_handler)
            self.journal_handler.setLevel(self.log_level)
            self.journal_handler.setFormatter(
                logging.Formatter(LOG_FORMAT_JOURNAL_MULTI_THREAD if self.multi_threaded else LOG_FORMAT_JOURNAL)
            )
            logging.root.setLevel(self.log_level)

    def sighup(self, signum, frame):  # pylint: disable=unused-argument
        self.log.info("Received SIGHUP, reloading config")
        self.reload_config()

    def sigterm(self, signum, frame):  # pylint: disable=unused-argument
        self.log.info("Received SIG%s, stopping daemon...", "TERM" if (signum == signal.SIGTERM) else "INT")
        daemon.notify("STOPPING=1")
        self.running = False

    def ping_watchdog(self):
        """Let systemd know we are still alive and well"""
        daemon.notify("WATCHDOG=1")

    def reload_config(self):
        file_ctime = None
        try:
            file_ctime = os.path.getctime(self.config_path)
        except FileNotFoundError as ex:
            if self.require_config:
                raise ServiceDaemonError("Cannot start without json config file at {!r}".format(self.config_path)) from ex

        if file_ctime != self.config_file_ctime:
            daemon.notify("RELOADING=1")
            self.log.info("%sloading configuration", "re" if self.config_file_ctime else "")
            self.config_file_ctime = file_ctime
            with open(self.config_path) as fp:
                self.config = json.load(fp)
            self.log.info("new config: %r", self.config)
            self.log_level = self.config.get("log_level", logging.INFO)
            self.configure_logging()
            self.handle_new_config()
            daemon.notify("READY=1")

    def handle_new_config(self):
        """Override in subclass"""

    def run(self):
        raise NotImplementedError()

    def cleanup(self):
        pass

    @classmethod
    def main(cls, args):
        if len(args) != 1:
            print("config file argument required")
            return 1

        exe = None
        try:
            exe = cls(config_path=args[0])
            daemon.notify("READY=1")
            setproctitle("journalpump")
            return exe.run()
        except ServiceDaemonError as ex:
            logging.fatal("%s failed to start: %s", cls.__name__, ex)
            return 1
        finally:
            if exe:
                exe.cleanup()

    @classmethod
    def run_exit(cls):
        sys.exit(cls.main(sys.argv[1:]))
