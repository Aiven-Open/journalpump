# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from . import geohash, statsd
from .daemon import ServiceDaemon
from .util import atomic_replace_file, default_json_serialization
from functools import reduce
from systemd.journal import Reader
from .senders.base import MAX_KAFKA_MESSAGE_SIZE
from .senders.base import Tagged
from .senders import AWSCloudWatchSender, RsyslogSender, LogplexSender, FileSender, KafkaSender, ElasticsearchSender
import copy
import datetime
import json
import logging
import re
import select
import systemd.journal
import time
import uuid

try:
    from geoip2.database import Reader as GeoIPReader
except ImportError:
    GeoIPReader = None


def _convert_uuid(s):
    return str(uuid.UUID(s.decode()))


def convert_mon(s):  # pylint: disable=unused-argument
    return None


def convert_realtime(t):
    return int(t) / 1000000.0  # Stock systemd transforms these into datetimes


converters = {
    "CODE_LINE": int,
    "COREDUMP_TIMESTAMP": convert_realtime,
    "MESSAGE_ID": _convert_uuid,
    "PRIORITY": int,
    "_AUDIT_ID": int,
    "_AUDIT_LOGINUID": int,
    "_AUDIT_SESSION": int,
    "_AUDIT_TYPE": int,
    "_BOOT_ID": _convert_uuid,
    "_GID": int,
    "_MACHINE_ID": _convert_uuid,
    "_PID": int,
    "_SOURCE_MONOTONIC_TIMESTAMP": convert_mon,
    "_SOURCE_REALTIME_TIMESTAMP": convert_realtime,
    "__MONOTONIC_TIMESTAMP": convert_mon,
    "__REALTIME_TIMESTAMP": convert_realtime,
}


class JournalObject:
    def __init__(self, cursor=None, entry=None):
        self.cursor = cursor
        self.entry = entry or {}


class PumpReader(Reader):
    def convert_entry(self, entry):
        """Faster journal lib _convert_entry replacement"""
        output = {}
        for key, value in entry.items():
            convert = converters.get(key)
            if convert is not None:
                try:
                    value = convert(value)
                except ValueError:
                    pass
            if isinstance(value, bytes):
                try:
                    value = bytes.decode(value)
                except Exception:  # pylint: disable=broad-except
                    pass

            output[key] = value

        return output

    def get_next(self, skip=1):
        # pylint: disable=no-member, protected-access
        """Private get_next implementation that doesn't store the cursor since we don't want it"""
        if super()._next(skip):
            entry = super()._get_all()
            if entry:
                entry["__REALTIME_TIMESTAMP"] = self._get_realtime()
                return JournalObject(cursor=self._get_cursor(), entry=self.convert_entry(entry))

        return None


class JournalReader(Tagged):
    def __init__(self, *, name, config, field_filters, geoip, stats,
                 tags=None, seek_to=None, msg_buffer_max_length=50000, searches=None, initial_position=None):
        Tagged.__init__(self, tags, reader=name)
        self.log = logging.getLogger("JournalReader:{}".format(name))
        self.name = name
        self.msg_buffer_max_length = msg_buffer_max_length
        self.initial_position = initial_position
        self.read_bytes = 0
        self.read_lines = 0
        self._last_sent_read_lines = 0
        self._last_sent_read_bytes = 0
        self._sent_bytes_diff = 0
        self._sent_lines_diff = 0
        self.last_stats_print_time = time.monotonic()
        self.geoip = geoip
        self.config = config
        self.field_filters = field_filters
        self.stats = stats
        self.cursor = seek_to
        self.registered_for_poll = False
        self.journald_reader = None
        self.last_journald_create_attempt = 0
        self.running = True
        self.senders = {}
        self._senders_initialized = False
        self.last_stats_send_time = time.monotonic()
        self.last_journal_msg_time = time.monotonic()
        self.searches = list(self._build_searches(searches))

    def create_journald_reader_if_missing(self):
        if not self.journald_reader and time.monotonic() - self.last_journald_create_attempt > 2:
            self.last_journald_create_attempt = time.monotonic()
            self.journald_reader = self.get_reader(seek_to=self.cursor)

    def update_poll_registration_status(self, poller):
        self.create_journald_reader_if_missing()
        if not self.journald_reader:
            return

        sender_over_limit = any(len(sender.msg_buffer) > self.msg_buffer_max_length for sender in self.senders.values())
        if not self.registered_for_poll and not sender_over_limit:
            self.log.info(
                "Message buffer size under threshold for all senders, starting processing journal for %r",
                self.name
            )
            self.register_for_poll(poller)
        elif self.registered_for_poll and sender_over_limit:
            self.log.info(
                "Message buffer size for at least one sender over threshold, stopping processing journal for %r",
                self.name
            )
            self.unregister_from_poll(poller)

    def register_for_poll(self, poller):
        if self.journald_reader:
            poller.register(self.journald_reader, self.journald_reader.get_events())
            self.registered_for_poll = True
            self.log.info("Registered reader %r with fd %r", self.name, self.journald_reader.fileno())

    def unregister_from_poll(self, poller):
        if self.journald_reader:
            poller.unregister(self.journald_reader)
            self.registered_for_poll = False
            self.log.info("Unregistered reader %r with fd %r", self.name, self.journald_reader.fileno())

    def get_resume_cursor(self):
        """Find the sender cursor location where a new JournalReader instance should resume reading from"""
        if not self.senders:
            self.log.info("Reader has no senders, using reader's resume location")
            return self.cursor

        for sender_name, sender in self.senders.items():
            state = sender.get_state()
            cursor = state["sent"]["cursor"]
            if cursor is None:
                self.log.info("Sender %r needs a full catchup from beginning, resuming from journal start", sender_name)
                return None

            # TODO: pick oldest sent cursor
            self.log.info("Resuming reader from sender's ('%s') position", sender_name)
            return cursor

        return None

    def request_stop(self):
        self.running = False
        for sender in self.senders.values():
            sender.request_stop()

    def initialize_senders(self):
        if self._senders_initialized:
            return

        senders = {
            "elasticsearch": ElasticsearchSender,
            "kafka": KafkaSender,
            "logplex": LogplexSender,
            "file": FileSender,
            "rsyslog": RsyslogSender,
            "aws_cloudwatch": AWSCloudWatchSender
        }
        for sender_name, sender_config in self.config.get("senders", {}).items():
            try:
                sender_class = senders[sender_config["output_type"]]
            except KeyError:
                raise Exception("Unknown sender type {!r}".format(sender_config["output_type"]))

            field_filter = None
            if sender_config.get("field_filter", None):
                field_filter = self.field_filters[sender_config["field_filter"]]

            extra_field_values = sender_config.get("extra_field_values", {})
            if not isinstance(extra_field_values, dict):
                self.log.warning("extra_field_values: %r not a dictionary object, ignoring", extra_field_values)
                extra_field_values = {}

            sender = sender_class(
                config=sender_config,
                field_filter=field_filter,
                extra_field_values=extra_field_values,
                msg_buffer_max_length=self.msg_buffer_max_length,
                name=sender_name,
                reader=self,
                stats=self.stats,
                tags=self.make_tags(),
            )
            sender.start()
            self.senders[sender_name] = sender

        self._senders_initialized = True

    def get_state(self):
        sender_state = {name: sender.get_state() for name, sender in self.senders.items()}
        search_state = {search["name"]: search.get("hits", 0) for search in self.searches}
        return {
            "cursor": self.cursor,
            "searches": search_state,
            "senders": sender_state,
            "total_lines": self.read_lines,
            "total_bytes": self.read_bytes,
        }

    def inc_line_stats(self, *, journal_lines, journal_bytes):
        self.read_bytes += journal_bytes
        self.read_lines += journal_lines

        now = time.monotonic()
        if (now - self.last_stats_send_time) < 10.0:
            # Do not send stats too often
            return

        tags = self.make_tags()
        self.stats.gauge("journal.last_read_ago", value=now - self.last_journal_msg_time, tags=tags)
        self.stats.gauge("journal.read_lines", value=self.read_lines, tags=tags)
        self.stats.gauge("journal.read_bytes", value=self.read_bytes, tags=tags)
        self.last_stats_send_time = now
        self._sent_lines_diff += self.read_lines - self._last_sent_read_lines
        self._sent_bytes_diff += self.read_bytes - self._last_sent_read_bytes
        if now - self.last_stats_print_time > 120:
            self.log.info("Processed %r journal lines (%r bytes)", self._sent_lines_diff, self._sent_bytes_diff)
            self._sent_lines_diff = 0
            self._sent_bytes_diff = 0
            self.last_stats_print_time = now

        self._last_sent_read_lines = self.read_lines
        self._last_sent_read_bytes = self.read_bytes

        for sender in self.senders.values():
            sender.refresh_stats()

    def read_next(self):
        jobject = next(self.journald_reader)
        if jobject.cursor:
            self.cursor = jobject.cursor
            self.last_journal_msg_time = time.monotonic()

        return jobject

    def get_reader(self, seek_to=None, reinit=False):
        """Return an initialized reader or None"""
        if not reinit and self.journald_reader:
            return self.journald_reader

        if self.journald_reader:
            # Close the existing reader
            self.journald_reader.close()  # pylint: disable=no-member
            self.journald_reader = None

        # convert named flags e.g. "SYSTEM" to integer values
        journal_flags = self.config.get("journal_flags")
        if isinstance(journal_flags, list):
            journal_flags = reduce(
                lambda a, b: a | b,
                [getattr(systemd.journal, flag.strip()) for flag in journal_flags],
            )

        try:
            self.journald_reader = PumpReader(
                files=self.config.get("journal_files"),
                flags=journal_flags,
                path=self.config.get("journal_path"),
            )
        except FileNotFoundError as ex:
            self.log.warning("journal for %r not available yet: %s: %s",
                             self.name, ex.__class__.__name__, ex)
            return None

        if seek_to:
            self.journald_reader.seek_cursor(seek_to)  # pylint: disable=no-member
            # Now the cursor points to the last read item, step over it so that we
            # do not read the same item twice
            self.journald_reader._next()  # pylint: disable=protected-access
        elif self.initial_position == "tail":
            self.journald_reader.seek_tail()
            self.journald_reader._next()  # pylint: disable=protected-access
        elif self.initial_position == "head":
            self.journald_reader.seek_head()
        elif isinstance(self.initial_position, int):
            # Seconds from the current boot time
            self.journald_reader.seek_monotonic(self.initial_position)

        # Only do the initial seek once when the pump is started for the first time,
        # the rest of the seeks will be to the last known cursor position
        self.initial_position = None

        for unit_to_match in self.config.get("units_to_match", []):
            self.journald_reader.add_match(_SYSTEMD_UNIT=unit_to_match)

        self.initialize_senders()

        return self.journald_reader

    def ip_to_geohash(self, tags, args):
        """ip_to_geohash(ip_tag_name,precision) -> Convert IP address to geohash"""
        if len(args) > 1:
            precision = int(args[1])
        else:
            precision = 8
        ip = tags[args[0]]
        res = self.geoip.city(ip)
        if not res:
            return ""

        loc = res.location
        return geohash.encode(loc.latitude, loc.longitude, precision)  # pylint: disable=no-member

    def _build_searches(self, searches):
        """
        Pre-generate regex objects and tag value conversion methods for searches
        """
        # Example:
        # {"name": "service_stop", "tags": {"foo": "bar"}, "search": {"MESSAGE": "Stopped target (?P<target>.+)\\."}}
        re_op = re.compile("(?P<func>[a-z_]+)\\((?P<args>[a-z0-9_,]+)\\)")
        funcs = {
            "ip_to_geohash": self.ip_to_geohash,
        }
        for search in searches:
            search.setdefault("tags", {})
            search.setdefault("fields", {})
            output = copy.deepcopy(search)
            for name, pattern in output["fields"].items():
                output["fields"][name] = re.compile(pattern)

            for tag, value in search["tags"].items():
                if "(" in value or ")" in value:
                    # Tag uses a method conversion call, e.g. "ip_to_geohash(ip_address,5)"
                    match = re_op.search(value)
                    if not match:
                        raise Exception("Invalid tag function tag value: {!r}".format(value))
                    func_name = match.groupdict()["func"]
                    try:
                        f = funcs[func_name]  # pylint: disable=unused-variable
                    except KeyError:
                        raise Exception("Unknown tag function {!r} in {!r}".format(func_name, value))

                    args = match.groupdict()["args"].split(",")  # pylint: disable=unused-variable

                    def value_func(tags, f=f, args=args):  # pylint: disable=undefined-variable
                        return f(tags, args)

                    output["tags"][tag] = value_func

            yield output

    def perform_searches(self, jobject):
        entry = jobject.entry
        results = {}
        for search in self.searches:
            all_match = True
            tags = {}
            for field, regex in search["fields"].items():
                line = entry.get(field, "")
                if not line:
                    all_match = False
                    break

                if isinstance(line, bytes):
                    try:
                        line = line.decode("utf-8")
                    except UnicodeDecodeError:
                        # best-effort decode failed
                        all_match = False
                        break

                match = regex.search(line)
                if not match:
                    all_match = False
                    break
                field_values = match.groupdict()
                for tag, value in field_values.items():
                    tags[tag] = value

            if not all_match:
                continue

            # add static tags + possible callables
            for tag, value in search.get("tags", {}).items():
                if callable(value):
                    tags[tag] = value(tags)
                else:
                    tags[tag] = value

            results[search["name"]] = tags
            if self.stats:
                self.stats.increase(search["name"], tags=self.make_tags(tags))
            search["hits"] = search.get("hits", 0) + 1
        return results


class FieldFilter:
    def __init__(self, name, config):
        self.name = name
        self.whitelist = config.get("type", "whitelist") == "whitelist"
        self.fields = [f.lstrip("_").lower() for f in config["fields"]]

    def filter_fields(self, data):
        return {name: val for name, val in data.items() if (name.lstrip("_").lower() in self.fields) is self.whitelist}


class JournalObjectHandler:
    def __init__(self, jobject, reader, pump):
        self.error_reported = False
        self.jobject = jobject
        self.json_objects = {}
        self.log = logging.getLogger(self.__class__.__name__)
        self.pump = pump
        self.reader = reader

    def process(self):
        new_entry = {}
        for key, value in self.jobject.entry.items():
            if isinstance(value, bytes):
                new_entry[key.lstrip("_")] = repr(value)  # value may be bytes in any encoding
            else:
                new_entry[key.lstrip("_")] = value

        if self.jobject.cursor is None:
            self.log.debug("No more journal entries to read")
            return False

        if self.reader.searches:
            if not self.reader.perform_searches(self.jobject):
                return True

        if not self.pump.check_match(new_entry):
            return True

        for sender in self.reader.senders.values():
            json_entry = self._get_or_generate_json(sender.field_filter, sender.extra_field_values, new_entry)
            sender.msg_buffer.add_item(item=json_entry, cursor=self.jobject.cursor)

        if self.json_objects:
            max_bytes = max(len(v) for v in self.json_objects.values())
            self.reader.inc_line_stats(journal_bytes=max_bytes, journal_lines=1)

        return True

    def _get_or_generate_json(self, field_filter, extra_field_values, data):
        ff_name = "" if field_filter is None else field_filter.name
        if ff_name in self.json_objects:
            return self.json_objects[ff_name]

        # Always set a timestamp field that gets turned into an ISO timestamp based on REALTIME_TIMESTAMP if available
        if "REALTIME_TIMESTAMP" in data:
            timestamp = datetime.datetime.utcfromtimestamp(data["REALTIME_TIMESTAMP"])
        else:
            timestamp = datetime.datetime.utcnow()
        data["timestamp"] = timestamp

        if extra_field_values:
            data.update(extra_field_values)

        if field_filter:
            data = field_filter.filter_fields(data)

        json_entry = json.dumps(data, default=default_json_serialization).encode("utf8")
        if len(json_entry) > MAX_KAFKA_MESSAGE_SIZE:
            json_entry = self._truncate_long_message(json_entry)

        self.json_objects[ff_name] = json_entry
        return json_entry

    def _truncate_long_message(self, json_entry):
        error = "too large message {} bytes vs maximum {} bytes".format(
            len(json_entry), MAX_KAFKA_MESSAGE_SIZE)
        if not self.error_reported:
            self.pump.stats.increase("journal.read_error", tags=self.pump.make_tags({
                "error": "too_long",
                "reader": self.reader.name,
            }))
            self.log.warning("%s: %s ...", error, json_entry[:1024])
            self.error_reported = True
        entry = {
            "error": error,
            "partial_data": json_entry[:1024],
        }
        return json.dumps(entry, default=default_json_serialization).encode("utf8")


class JournalPump(ServiceDaemon, Tagged):
    def __init__(self, config_path):
        Tagged.__init__(self)
        self.stats = None
        self.geoip = None
        self.poller = select.poll()
        self.readers_active_config = None
        self.readers = {}
        self.field_filters = {}
        self.previous_state = None
        self.last_state_save_time = time.monotonic()
        ServiceDaemon.__init__(self, config_path=config_path, multi_threaded=True, log_level=logging.INFO)
        self.start_time_str = datetime.datetime.utcnow().isoformat()
        self.configure_field_filters()
        self.configure_readers()

    def configure_field_filters(self):
        filters = self.config.get("field_filters", {})
        self.field_filters = {name: FieldFilter(name, config) for name, config in filters.items()}

    def configure_readers(self):
        new_config = self.config.get("readers", {})
        if self.readers_active_config == new_config:
            # No changes in readers, no reconfig required
            return

        # replace old readers with new ones
        for reader in self.readers.values():
            reader.request_stop()
            # Don't close the journald_reader here because it could currently be in use.
            # This may in some situations leak some resources but possible leak is small
            # and config reloads are typically quite infrequent.
            reader.unregister_from_poll(self.poller)

        self.readers = {}
        state = self.load_state()
        for reader_name, reader_config in new_config.items():
            reader_state = state.get("readers", {}).get(reader_name, {})
            resume_cursor = None
            for sender_name, sender in reader_state.get("senders", {}).items():
                sender_cursor = sender["sent"]["cursor"]
                if sender_cursor is None:
                    self.log.info("Sender %r for reader %r needs full sync from beginning",
                                  sender_name, reader_name)
                    resume_cursor = None
                    break

                # TODO: pick the OLDEST cursor
                resume_cursor = sender_cursor

            self.log.info("Reader %r resuming from cursor position: %r", reader_name, resume_cursor)
            initial_position = reader_config.get("initial_position")
            reader = JournalReader(
                name=reader_name,
                config=reader_config,
                field_filters=self.field_filters,
                geoip=self.geoip,
                stats=self.stats,
                msg_buffer_max_length=self.config.get("msg_buffer_max_length", 50000),
                seek_to=resume_cursor,
                initial_position=initial_position,
                tags=self.make_tags(),
                searches=reader_config.get("searches", {}),
            )
            self.readers[reader_name] = reader

        self.readers_active_config = new_config

    def handle_new_config(self):
        """Called by ServiceDaemon when config has changed"""
        stats = self.config.get("statsd") or {}
        self.stats = statsd.StatsClient(
            host=stats.get("host"),
            port=stats.get("port"),
            tags=stats.get("tags"),
        )
        self.replace_tags(self.config.get("tags", {}))
        geoip_db_path = self.config.get("geoip_database")
        if geoip_db_path:
            self.log.info("Loading GeoIP data from %r", geoip_db_path)
            if GeoIPReader is None:
                raise ValueError("geoip_database configured but geoip2 module not available")
            self.geoip = GeoIPReader(geoip_db_path)

        self.configure_field_filters()
        self.configure_readers()

    def sigterm(self, signum, frame):
        try:
            self.save_state()
        except Exception:   # pylint: disable=broad-except
            self.log.exception("Saving state at shutdown failed")

        for reader in self.readers.values():
            reader.request_stop()

        super().sigterm(signum, frame)

    def load_state(self):
        file_path = self.get_state_file_path()
        if not file_path:
            return {}

        try:
            with open(file_path, "r") as fp:
                return json.load(fp)
        except FileNotFoundError:
            return {}

    def check_match(self, entry):
        if not self.config.get("match_key"):
            return True
        if entry.get(self.config["match_key"]) == self.config["match_value"]:
            return True
        return False

    def read_single_message(self, reader):
        try:
            jobject = reader.read_next()
            if jobject is None or jobject.entry is None:
                return False

            return JournalObjectHandler(jobject, reader, self).process()
        except StopIteration:
            self.log.debug("No more journal entries to read")
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception while handling entry for %s", reader.name)
            self.stats.unexpected_exception(ex=ex, where="mainloop", tags=self.make_tags({"app": "journalpump"}))
            time.sleep(0.5)
            return False

    def read_all_available_messages(self, reader, hits):
        lines = 0
        while self.read_single_message(reader):
            lines += 1

        for search in reader.searches:
            hits[search["name"]] = search.get("hits", 0)

        return lines

    def get_state_file_path(self):
        return self.config.get("json_state_file_path")

    def save_state(self):
        state_file_path = self.get_state_file_path()
        if not state_file_path:
            return

        reader_state = {name: reader.get_state() for name, reader in self.readers.items()}
        state_to_save = {
            "readers": reader_state,
            "start_time": self.start_time_str,
        }

        if state_to_save != self.previous_state:
            with atomic_replace_file(state_file_path) as fp:
                json.dump(state_to_save, fp, indent=4, sort_keys=True)
                self.previous_state = state_to_save
                self.log.debug("Wrote state file: %r", state_to_save)

    def run(self):
        last_stats_time = 0
        while self.running:
            results = self.poller.poll(1000)
            hits = {}
            lines = 0
            for fd, _event in results:
                for reader in self.readers.values():
                    jdr = reader.journald_reader
                    if not jdr or fd != jdr.fileno():
                        continue
                    if jdr.process() == systemd.journal.APPEND:
                        lines += self.read_all_available_messages(reader, hits)
                    break
                else:
                    self.log.error("Could not find reader with fd %r", fd)

            for reader in self.readers.values():
                reader.update_poll_registration_status(self.poller)

            if hits and time.monotonic() - last_stats_time > 60.0:
                self.log.info("search hits stats: %s", hits)
                last_stats_time = time.monotonic()

            now = time.monotonic()
            if now - self.last_state_save_time > 10.0:
                self.save_state()
                self.last_state_save_time = now

            if not lines:
                for reader in self.readers.values():
                    # Refresh readers so they can send their buffered stats out
                    reader.inc_line_stats(journal_bytes=0, journal_lines=0)

                self.log.debug("No new journal lines received")

            self.ping_watchdog()


if __name__ == "__main__":
    JournalPump.run_exit()
