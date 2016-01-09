"""
Common Daemon functionality

"""
from systemd import daemon, journal
import contextlib
import json as jsonlib
import logging
import os
import signal
import sys

LOG_FORMAT = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
LOG_FORMAT_JOURNAL = "%(name)-20s  %(levelname)-8s  %(message)s"
LOG_FORMAT_JOURNAL_MULTI_THREAD = "%(name)-20s  %(threadName)-15s %(levelname)-8s  %(message)s"


@contextlib.contextmanager
def AtomicCreateFile(file_path, binary=False, perm=None, uidgid=None):
    """Open a temporary file for writing, rename to final name when done"""
    tmp_file_path = file_path + "~"
    mode = "wb" if binary else "w"
    # copy permissions and ownership from existing file
    if perm is None or uidgid is None:
        try:
            st = os.stat(file_path)
        except FileNotFoundError:
            st = None
        if perm is None:
            perm = st.st_mode if st else 0o600
        if uidgid is None and st is not None:
            uidgid = (st.st_uid, st.st_gid)
    fd = os.open(tmp_file_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, perm)
    try:
        if uidgid:
            os.fchown(fd, uidgid[0], uidgid[1])
        with os.fdopen(fd, mode) as out_file:
            yield out_file

        os.rename(tmp_file_path, file_path)
    except:
        try:
            os.unlink(tmp_file_path)
        except:  # pylint: disable=bare-except
            pass

        raise


def atomic_overwrite_file(file_path, contents, json=False, ini=False, binary=None, perm=None, uidgid=None):
    """Overwrite a file with specified contents via atomic rename"""
    with AtomicCreateFile(file_path, binary=binary, perm=perm, uidgid=uidgid) as out_file:
        if json:
            jsonlib.dump(contents, out_file, indent=4, sort_keys=True)
        elif ini:
            for section, options in sorted(contents.items()):
                # NOTE: empty section is always first in sorted items and we
                # use it to place options at the "top level" before any
                # sections are declared
                if section:
                    out_file.write("[{}]\n".format(section))
                for option, value in sorted(options.items()):
                    out_file.write("{}={}\n".format(option, value))
                out_file.write("\n")
        else:
            out_file.write(contents)

    return file_path


def read_file(file_path, binary=False, json=False, default=None):
    """Read file contents, optionally decoding it from JSON"""
    mode = "rb" if binary else "r"
    try:
        with open(file_path, mode) as in_file:
            if json:
                return jsonlib.load(in_file)
            else:
                return in_file.read()
    except FileNotFoundError:
        if default is None:
            raise
        else:
            return default


class ServiceDaemonError(Exception):
    """ServiceDaemon error"""


class ServiceDaemon:
    def __init__(self, config_path, pid_file=None, require_config=True, multi_threaded=False, log_level=logging.DEBUG):
        assert isinstance(config_path, str)
        self.name = self.__class__.__name__.lower()
        self.log_level = log_level
        self.multi_threaded = multi_threaded
        self.journal_handler = None
        self.configure_logging()
        self.log = logging.getLogger(self.name)
        with open("/etc/machine-id") as fp:
            self.machine_id = fp.read().strip()
        self.lessee_id = "{}_{}".format(self.name, self.machine_id)
        self.config_path = config_path
        self.config_file_ctime = None
        self.config = None
        self.pid_file = pid_file
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
            self.journal_handler.setFormatter(logging.Formatter(
                LOG_FORMAT_JOURNAL_MULTI_THREAD if self.multi_threaded else LOG_FORMAT_JOURNAL))
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
        except FileNotFoundError:
            if self.require_config:
                raise ServiceDaemonError("Cannot start without json config file at {!r}".format(self.config_path))

        if file_ctime != self.config_file_ctime:
            daemon.notify("RELOADING=1")
            self.log.info("%sloading configuration", "re" if self.config_file_ctime else "")
            self.config_file_ctime = file_ctime
            self.config = read_file(self.config_path, json=True)
            self.log.info("new config: %r", self.config)
            self.log_level = self.config.get("log_level", logging.INFO)
            self.configure_logging()
            self.handle_new_config()
            daemon.notify("READY=1")

    def handle_new_config(self):
        """Override in subclass"""
        pass

    def run(self):
        raise NotImplementedError()

    def handle_pid_file(self):
        """kill existing daemon if pid file present and write a new one"""
        if not self.pid_file:
            return

        if os.path.exists(self.pid_file):
            pid = read_file(self.pid_file)
            try:
                os.kill(int(pid), signal.SIGKILL)
                self.log.info("killed previous %s pid %r", self.name, pid)
            except (ValueError, ProcessLookupError) as ex:
                self.log.info("invalid pid %r stored in pid file %r: %s", pid, self.pid_file, ex)

        atomic_overwrite_file(self.pid_file, str(os.getpid()))

    def cleanup(self):
        if self.pid_file:
            try:
                os.unlink(self.pid_file)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error("removing pid file at %r failed: %s: %s", self.pid_file, ex.__class__.__name__, ex)

    @classmethod
    def main(cls, args):
        if len(args) != 1:
            print("config file argument required")
            return 1

        exe = None
        try:
            exe = cls(config_path=args[0])
            daemon.notify("READY=1")
            exe.handle_pid_file()
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
