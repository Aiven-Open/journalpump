# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from . import geohash, statsd
from .daemon import ServiceDaemon
from .senders import get_sender_class
from .senders.base import MAX_KAFKA_MESSAGE_SIZE, Tagged
from .types import GeoIPProtocol, LOG_SEVERITY_MAPPING
from .util import atomic_replace_file, default_json_serialization
from functools import lru_cache, reduce
from systemd import journal
from typing import cast, Dict, List, NamedTuple, Optional, Tuple, Type, Union

import copy
import datetime
import fnmatch
import json
import logging
import re
import select
import time
import uuid

_5_MB = 5 * 1024 * 1024
CHUNK_SIZE = 5000


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


class SingleMessageReadResult(NamedTuple):
    has_more: bool
    bytes_read: Optional[int]


class MessagesReadResult(NamedTuple):
    exhausted: bool
    lines_read: int


class JournalObject:
    def __init__(self, cursor=None, entry=None):
        self.cursor = cursor
        self.entry = entry or {}


class PumpReader(journal.Reader):
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
    def __init__(
        self,
        *,
        name,
        config,
        field_filters,
        geoip,
        stats,
        unit_log_levels=None,
        tags=None,
        seek_to=None,
        msg_buffer_max_length=50000,
        msg_buffer_max_bytes=_5_MB,
        searches=None,
        initial_position=None,
    ):
        Tagged.__init__(self, tags, reader=name)
        self.log = logging.getLogger("JournalReader:{}".format(name))
        self.name = name
        self.msg_buffer_max_length = msg_buffer_max_length
        self.msg_buffer_max_bytes = msg_buffer_max_bytes
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
        self.unit_log_levels = unit_log_levels
        self.stats = stats
        self.cursor = seek_to
        self.journald_reader = None
        self.last_journald_create_attempt = 0
        self.running = True
        self.senders = {}
        self._initialized_senders = set()
        self._failed_senders: int = 0
        self.last_stats_send_time = time.monotonic()
        self.last_journal_msg_time = time.monotonic()
        self.persistent_gauges: Dict[str, Tuple[int, Dict[str, str]]] = dict()
        self.searches = list(self._build_searches(searches))
        self.secret_filter_matches = 0
        self.secret_filters = self._validate_and_build_secret_filters(config)
        self.secret_filter_metrics = self._configure_secret_filter_metrics(config)
        self.secret_filter_metric_last_send = time.monotonic()
        self._is_ready = True

    def invalidate(self) -> None:
        """
        Should be called when reader has stale information about files present
        e.g. some journal files were deleted and we need to release file descriptors.
        """
        if self.journald_reader:
            self.journald_reader.close()
            self.journald_reader = None

    def fileno(self) -> Optional[int]:
        return self.journald_reader and self.journald_reader.fileno()

    def create_journald_reader_if_missing(self) -> None:
        if not self.journald_reader and time.monotonic() - self.last_journald_create_attempt > 2:
            self.last_journald_create_attempt = time.monotonic()
            self.journald_reader = self.get_reader(seek_to=self.cursor)

    @property
    def is_ready(self) -> bool:
        return bool(self.journald_reader and self._is_ready)

    def get_write_limit_bytes(self) -> int:
        if self._failed_senders and not self.senders:
            return 0

        if not self.senders:
            # Some readers are only used for collecting stats about occurencies
            # of a pattern string in logs
            return _5_MB

        return self.msg_buffer_max_bytes - max(s.msg_buffer.buffer_size for s in self.senders.values())

    def get_write_limit_message_count(self) -> int:
        if self._failed_senders and not self.senders:
            return 0

        if not self.senders:
            # Some readers are only used for collecting stats about occurencies
            # of a pattern string in logs
            return CHUNK_SIZE

        return self.msg_buffer_max_length - max(len(s.msg_buffer) for s in self.senders.values())

    def update_status(self):
        self.create_journald_reader_if_missing()

        sender_over_limit = self.get_write_limit_message_count() <= 0
        if self.msg_buffer_max_bytes:
            sender_over_limit |= self.get_write_limit_bytes() <= 0

        if not self._is_ready and not sender_over_limit:
            self.log.info(
                "Message buffer size under threshold for all senders, ready to process journal for %r",
                self.name,
            )
            self._is_ready = True
        elif self._is_ready and sender_over_limit:
            self.log.info(
                "Message buffer size for at least one sender over threshold, pause processing journal for %r",
                self.name,
            )
            self._is_ready = False

    def get_resume_cursor(self):
        """Find the sender cursor location where a new JournalReader instance should resume reading from"""
        if not self.senders:
            self.log.info("Reader has no senders, using reader's resume location")
            return self.cursor

        for sender_name, sender in self.senders.items():
            state = sender.get_state()
            cursor = state["sent"]["cursor"]
            if cursor is None:
                self.log.warning(
                    "Sender %r needs a full catchup from beginning, resuming from journal start",
                    sender_name,
                )
                return None

            # TODO: pick oldest sent cursor
            self.log.info("Resuming reader from sender's ('%s') position", sender_name)
            return cursor

        return None

    def request_stop(self):
        self.running = False
        for sender in self.senders.values():
            sender.request_stop()

    def close(self):
        if self.journald_reader:
            self.journald_reader.close()

    def initialize_senders(self):
        configured_senders = self.config.get("senders", {})
        tags: Dict[str, str] = dict()
        for sender_name, sender_config in configured_senders.items():
            if sender_name in self._initialized_senders:
                continue
            sender_class = get_sender_class(sender_config["output_type"])

            field_filter = None
            if sender_config.get("field_filter", None):
                field_filter = self.field_filters[sender_config["field_filter"]]

            unit_log_levels = None
            if sender_config.get("unit_log_level", None):
                unit_log_levels = self.unit_log_levels[sender_config["unit_log_level"]]

            extra_field_values = sender_config.get("extra_field_values", {})

            if not isinstance(extra_field_values, dict):
                self.log.warning(
                    "extra_field_values: %r not a dictionary object, ignoring",
                    extra_field_values,
                )
                extra_field_values = {}
            try:
                sender = sender_class(
                    config=sender_config,
                    field_filter=field_filter,
                    unit_log_levels=unit_log_levels,
                    extra_field_values=extra_field_values,
                    msg_buffer_max_length=self.msg_buffer_max_length,
                    name=sender_name,
                    reader=self,
                    stats=self.stats,
                    tags=self.make_tags(),
                )
            except Exception:  # pylint: disable=broad-except
                # If sender init fails, log exception, don't start() the sender
                # and don't add it to self.senders dict. A metric about senders that
                # failed to start is sent at the end of this method
                self.log.exception("Sender %r failed to initialize", sender_name)
            else:
                self._initialized_senders.add(sender_name)
                sender.start()
                self.senders[sender_name] = sender

        self._failed_senders = len(configured_senders) - len(self._initialized_senders)

        # We might not re-emit this metrics for a long time, as it depends on the user
        # modifying their configuration file. This metric should stay current until
        # this occurs, or we exit.
        self.set_persistent_gauge(metric="sender.failed_to_start", value=self._failed_senders, tags=self.make_tags(tags))

    def set_persistent_gauge(self, *, metric: str, value: int, tags: Dict[str, str]):
        """Set/update a metric level (and tags) for a given metric.
        These values will be periodically re-sent, ensuring the value
        is not discarded by the statsd server.
        """
        self.persistent_gauges[metric] = (value, tags)

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

    def report_secret_filter_metrics(self):
        now = time.monotonic()
        if (now - self.last_stats_send_time) < 10.0:
            return
        tags = self.make_tags()
        self.stats.increase("journal.secret_filter_match", self.secret_filter_matches, tags=tags)
        self.secret_filter_matches = 0
        self.secret_filter_metric_last_send = now

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
            self.log.info(
                "Processed %r journal lines (%r bytes)",
                self._sent_lines_diff,
                self._sent_bytes_diff,
            )
            self._sent_lines_diff = 0
            self._sent_bytes_diff = 0
            self.last_stats_print_time = now

        self._last_sent_read_lines = self.read_lines
        self._last_sent_read_bytes = self.read_bytes

        for sender in self.senders.values():
            sender.refresh_stats()

    def read_next(self):
        if not self.journald_reader:
            return None

        jobject: JournalObject = cast(JournalObject, next(self.journald_reader))
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
                [getattr(journal, flag.strip()) for flag in journal_flags],
            )

        try:
            reader_kwargs = dict(
                files=self.config.get("journal_files"),
                flags=journal_flags,
                path=self.config.get("journal_path"),
            )
            namespace = self.config.get("journal_namespace")
            if namespace:
                reader_kwargs["namespace"] = namespace
            self.journald_reader = PumpReader(**reader_kwargs)
            if not (self.journald_reader.has_persistent_files() or self.journald_reader.has_runtime_files()):
                # If journal files are not ready (e.g. files with namespace are not yet created), reader won't fail,
                # it will silently not deliver anything. We don't want this - return None to re-create reader later
                self.log.warning("journal files for %r are not yet available", self.name)
                self.journald_reader.close()
                self.journald_reader = None
                return None
        except FileNotFoundError as ex:
            self.log.warning(
                "journal for %r not available yet: %s: %s",
                self.name,
                ex.__class__.__name__,
                ex,
            )
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
                    except KeyError as ex:
                        raise Exception("Unknown tag function {!r} in {!r}".format(func_name, value)) from ex

                    args = match.groupdict()["args"].split(",")  # pylint: disable=unused-variable

                    def value_func(tags, f=f, args=args):  # pylint: disable=undefined-variable
                        return f(tags, args)

                    output["tags"][tag] = value_func

            yield output

    def _configure_secret_filter_metrics(self, config):
        secret_filter_metrics = config.get("secret_filter_metrics")
        if secret_filter_metrics is True:
            return True
        return False

    def _validate_and_build_secret_filters(self, config):
        secret_filters = config.get("secret_filters")
        if secret_filters is None:
            return []

        if not isinstance(secret_filters, list):
            raise ValueError("invalid secret_filters configuration - must be a list")

        for secret_filter in secret_filters:
            if secret_filter.get("replacement") is None:
                raise ValueError("invalid secret_filters configuration - missing field 'replacement'")

            if secret_filter.get("pattern") is None:
                raise ValueError("invalid secret_filters configuration - missing field 'pattern'")

        # Compile / validate regex
        for idx, sfilter in enumerate(secret_filters):
            try:
                secret_filters[idx]["compiled_pattern"] = re.compile(sfilter["pattern"])
            except re.error as e:
                raise ValueError("invalid secret_filters configuration - invalid regex") from e

        return secret_filters

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


class UnitLogLevel:
    def __init__(self, name: str, config: List[Dict[str, str]]):
        self.name = name
        self.levels = config

    def filter_by_level(self, data):
        entry = data
        if isinstance(data, bytes):
            entry = json.loads(data.decode("utf-8"))
        if not entry:
            return {}
        unit = entry.get("SYSTEMD_UNIT")
        priority = entry.get("PRIORITY")
        if (not priority) or (not unit):
            # If entry does not contain unit or priority we return it as is
            return data

        if not self.levels:
            # if the config is empty we return as is
            return data

        if self._unit_match_level_glob(unit=unit, priority=priority):
            return data

        return {}

    @lru_cache(maxsize=100)
    def _unit_match_level_glob(self, *, unit: str, priority: int) -> bool:
        for level in self.levels:
            if fnmatch.fnmatch(unit, level["service_glob"]) and priority <= LOG_SEVERITY_MAPPING[level["log_level"]]:
                return True
        return False


class JournalObjectHandler:
    def __init__(self, jobject, reader, pump):
        self.error_reported = False
        self.jobject = jobject
        self.json_objects = {}
        self.log = logging.getLogger(self.__class__.__name__)
        self.pump = pump
        self.reader = reader

    def process(self) -> SingleMessageReadResult:
        new_entry = {}
        for key, value in self.jobject.entry.items():
            if isinstance(value, bytes):
                new_entry[key.lstrip("_")] = repr(value)  # value may be bytes in any encoding
            else:
                new_entry[key.lstrip("_")] = value

        if self.jobject.cursor is None:
            self.log.debug("No more journal entries to read")
            return SingleMessageReadResult(has_more=False, bytes_read=None)

        if self.reader.searches:
            if not self.reader.perform_searches(self.jobject):
                return SingleMessageReadResult(has_more=True, bytes_read=None)

        if not self.pump.check_match(new_entry):
            return SingleMessageReadResult(has_more=True, bytes_read=None)

        # Redact secrets before other filtering for effective leak detection metrics
        if self.reader.secret_filters:
            new_entry = self._apply_secret_filters(new_entry)

        for sender in self.reader.senders.values():
            json_entry = self._get_or_generate_json(
                sender.field_filter,
                sender.unit_log_levels,
                sender.extra_field_values,
                new_entry,
            )
            if json_entry:
                sender.msg_buffer.add_item(item=json_entry, cursor=self.jobject.cursor)

        max_bytes = 0
        if self.json_objects:
            max_bytes = max(len(v) for v in self.json_objects.values())
            self.reader.inc_line_stats(journal_bytes=max_bytes, journal_lines=1)

        return SingleMessageReadResult(has_more=True, bytes_read=max_bytes)

    def _get_or_generate_json(self, field_filter, unit_log_levels, extra_field_values, data):
        ff_name = "" if field_filter is None else field_filter.name
        if ff_name in self.json_objects:
            return self._filter_by_log_level(self.json_objects[ff_name], unit_log_levels)

        # Always set a timestamp field that gets turned into an ISO timestamp based on REALTIME_TIMESTAMP if available
        if "REALTIME_TIMESTAMP" in data:
            timestamp = datetime.datetime.utcfromtimestamp(data["REALTIME_TIMESTAMP"])
        else:
            timestamp = datetime.datetime.utcnow()
        data["timestamp"] = timestamp

        if extra_field_values:
            data.update(extra_field_values)

        if unit_log_levels:
            data = self._filter_by_log_level(data, unit_log_levels)

        if not data:
            # This means the data has been dropped because it did not have an acceptable log level. No reason to proceed.
            return None

        if field_filter:
            data = field_filter.filter_fields(data)

        json_entry = json.dumps(data, default=default_json_serialization).encode("utf8")
        if len(json_entry) > MAX_KAFKA_MESSAGE_SIZE:
            json_entry = self._truncate_long_message(json_entry)

        self.json_objects[ff_name] = json_entry
        return json_entry

    def _filter_by_log_level(self, data, unit_log_levels):
        return unit_log_levels.filter_by_level(data) if unit_log_levels else data

    def _truncate_long_message(self, json_entry):
        error = "too large message {} bytes vs maximum {} bytes".format(len(json_entry), MAX_KAFKA_MESSAGE_SIZE)
        if not self.error_reported:
            self.pump.stats.increase(
                "journal.read_error",
                tags=self.pump.make_tags(
                    {
                        "error": "too_long",
                        "reader": self.reader.name,
                    }
                ),
            )
            self.log.warning("%s: %s ...", error, json_entry[:1024])
            self.error_reported = True
        entry = {
            "error": error,
            "partial_data": json_entry[:1024],
        }
        return json.dumps(entry, default=default_json_serialization).encode("utf8")

    def _apply_secret_filters(self, data):
        msg = data.get("MESSAGE")
        original_msg = msg
        if msg:
            for secret_filter in self.reader.secret_filters:
                msg = secret_filter["compiled_pattern"].sub(secret_filter["replacement"], msg)
        if msg != original_msg:
            if self.reader.secret_filter_metrics:
                self.reader.secret_filter_matches += 1
            data["MESSAGE"] = msg
        return data


class JournalPump(ServiceDaemon, Tagged):
    _STALE_FD = object()

    def __init__(self, config_path):
        Tagged.__init__(self)
        self.stats = None
        self.geoip = None
        self.poller = select.poll()
        self.readers_active_config = None
        self.readers = {}
        self.field_filters = {}
        self.unit_log_levels = {}
        self.previous_state = None
        self.last_state_save_time = time.monotonic()
        ServiceDaemon.__init__(self, config_path=config_path, multi_threaded=True, log_level=logging.INFO)
        self.start_time_str = datetime.datetime.utcnow().isoformat()
        self.configure_field_filters()
        self.configure_unit_log_levels()
        self.configure_readers()
        self.stale_readers = set()
        self.reader_by_fd = {}
        self.poll_interval_ms = 1000

    def configure_field_filters(self):
        filters = self.config.get("field_filters", {})
        self.field_filters = {name: FieldFilter(name, config) for name, config in filters.items()}

    def configure_unit_log_levels(self):
        unit_log_levels = self.config.get("unit_log_levels", {})
        self.unit_log_levels = {name: UnitLogLevel(name, config) for name, config in unit_log_levels.items()}

    def configure_readers(self):
        new_config = self.config.get("readers", {})
        if self.readers_active_config == new_config:
            # No changes in readers, no reconfig required
            return

        # replace old readers with new ones
        for reader in self.readers.values():
            reader.request_stop()
            reader.unregister_from_poll(self.poller)
            self.stale_readers.add(reader)

        self.readers = {}
        state = self.load_state()

        for reader_name, reader_config in new_config.items():
            reader_state = state.get("readers", {}).get(reader_name, {})
            resume_cursor = None
            for sender_name, sender in reader_state.get("senders", {}).items():
                sender_cursor = sender["sent"]["cursor"]
                if sender_cursor is None:
                    self.log.info(
                        "Sender %r for reader %r needs full sync from beginning",
                        sender_name,
                        reader_name,
                    )
                    resume_cursor = None
                    break

                # TODO: pick the OLDEST cursor
                resume_cursor = sender_cursor

            self.log.info(
                "Reader %r resuming from cursor position: %r",
                reader_name,
                resume_cursor,
            )
            initial_position = reader_config.get("initial_position")
            reader = JournalReader(
                name=reader_name,
                config=reader_config,
                field_filters=self.field_filters,
                unit_log_levels=self.unit_log_levels,
                geoip=self.geoip,
                stats=self.stats,
                msg_buffer_max_length=self.config.get("msg_buffer_max_length", 50000),
                msg_buffer_max_bytes=self.config.get("msg_buffer_max_bytes", _5_MB),
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
            host=stats.get("host"), port=stats.get("port"), tags=stats.get("tags"), prefix=stats.get("prefix", "")
        )
        self.replace_tags(self.config.get("tags", {}))
        geoip_db_path = self.config.get("geoip_database")
        if geoip_db_path:
            self.log.info("Loading GeoIP data from %r", geoip_db_path)
            GeoIPReader: Union[Type[GeoIPProtocol], None]
            try:
                from geoip2.database import Reader as GeoIPReader  # pylint: disable=import-outside-toplevel
            except ImportError as ex:
                raise ValueError("geoip_database configured but geoip2 module not available") from ex
            self.geoip = GeoIPReader(geoip_db_path)

        self.configure_field_filters()
        self.configure_unit_log_levels()
        self.configure_readers()

    def shutdown(self):
        try:
            self.save_state()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Saving state at shutdown failed")

        for reader in self.readers.values():
            reader.request_stop()
            self.unregister_from_poll(reader)
            self.stale_readers.add(reader)

    def sigterm(self, signum, frame):
        self.shutdown()
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

    def read_single_message(self, reader) -> SingleMessageReadResult:
        try:
            jobject: Optional[JournalObject] = reader.read_next()
            if jobject is None or jobject.entry is None:
                return SingleMessageReadResult(has_more=False, bytes_read=None)

            return JournalObjectHandler(jobject, reader, self).process()
        except StopIteration:
            self.log.debug("No more journal entries to read")
            return SingleMessageReadResult(has_more=False, bytes_read=None)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception while handling entry for %s", reader.name)
            self.stats.unexpected_exception(ex=ex, where="mainloop", tags=self.make_tags({"app": "journalpump"}))
            time.sleep(0.5)
            return SingleMessageReadResult(has_more=False, bytes_read=None)

    def read_messages(self, reader, hits, chunk_size) -> MessagesReadResult:
        lines = 0

        exhausted = False

        limit_bytes = reader.get_write_limit_bytes()
        limit_count = reader.get_write_limit_message_count()

        for _ in range(chunk_size):
            if limit_bytes <= 0:
                return MessagesReadResult(exhausted=False, lines_read=lines)

            if limit_count <= 0:
                return MessagesReadResult(exhausted=False, lines_read=lines)

            has_more, bytes_read = self.read_single_message(reader)

            if bytes_read is not None:
                limit_bytes -= bytes_read
                limit_count -= 1

            if not has_more:
                exhausted = True
                break

            lines += 1

        for search in reader.searches:
            hits[search["name"]] = search.get("hits", 0)

        return MessagesReadResult(exhausted=exhausted, lines_read=lines)

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

    def _close_stale_readers(self):
        while self.stale_readers:
            reader = self.stale_readers.pop()
            self.unregister_from_poll(reader)
            # closing reader invalidates fileno, so reader should be first
            # unregistered before closing
            reader.close()

    def register_for_poll(self, reader: JournalReader) -> bool:
        fd = reader.fileno()
        # fd in this case is anonymous inotify node
        if fd is not None and fd not in self.reader_by_fd:
            self.log.info("Registered reader %r with fd %r", self.name, fd)
            self.poller.register(fd)
            self.reader_by_fd[fd] = reader
            return True

        return False

    def unregister_from_poll(self, reader: JournalReader) -> None:
        fd = reader.fileno()
        if fd is not None and fd in self.reader_by_fd:
            self.poller.unregister(fd)
            self.reader_by_fd[fd] = self._STALE_FD
            self.log.info("Unregistered reader %r with fd %r", self.name, fd)

    def refresh_gauges(self) -> None:
        if self.stats:
            for reader in self.readers.values():
                for metric, (value, tags) in reader.persistent_gauges.items():
                    self.stats.gauge(metric=metric, value=value, tags=tags)

    def run(self):  # pylint: disable=too-many-statements
        last_stats_time = 0
        poll_timeout = 0
        buffered_events = {}
        hits = {}

        while self.running:
            self._close_stale_readers()

            self.log.debug("Waiting for %dms", poll_timeout)
            results = self.poller.poll(poll_timeout)
            iteration_start_time = time.monotonic_ns()

            lines = 0
            # We keep valid events in case reader is busy with processing
            # previous batch and not ready to act on new one

            for fd, _ in results:
                reader = self.reader_by_fd.get(fd)
                if reader is self._STALE_FD:
                    continue

                if reader is None:
                    self.log.error("Could not find reader with fd %r", fd)
                    continue

                # Call to process clears state, consecutive calls won't return same value
                process_evt = reader.journald_reader.process()
                # Can be either 0 - journal.NOP, 1 - journal.APPEND or 2 - journal.INVALIDATE
                current_evt = buffered_events.get(reader, journal.NOP)
                buffered_events[reader] = max(current_evt, process_evt)

            # Remove stale reader, it should happen after we processed
            # batch of events received from poll, because we could
            # have received stale fds
            for key, value in list(self.reader_by_fd.items()):
                if value is self._STALE_FD:
                    del self.reader_by_fd[key]

            for reader, event in list(buffered_events.items()):
                if not reader.is_ready:
                    continue

                # Read messages up to set reader limit
                exhausted = False
                if event in [journal.APPEND, journal.INVALIDATE]:
                    exhausted, line_count = self.read_messages(reader, hits, chunk_size=CHUNK_SIZE)
                    lines += line_count

                if event == journal.INVALIDATE:
                    self.unregister_from_poll(reader)
                    # If file was deleted we could lose some unread messages here
                    # but if we are so behind that we reading tail file, we probably
                    # will never catch up and we want to avoid holding descriptors to
                    # deleted files
                    reader.invalidate()

                # Only delete set event when we know that all messages were drained
                if exhausted:
                    buffered_events.pop(reader)

            for reader in self.readers.values():
                reader.update_status()
                if self.register_for_poll(reader):
                    # Check if there is something to read in newly created reader
                    buffered_events[reader] = journal.APPEND

                if reader.secret_filter_metrics:
                    reader.report_secret_filter_metrics()

            if hits and time.monotonic() - last_stats_time > 60.0:
                self.log.info("search hits stats: %s", hits)
                last_stats_time = time.monotonic()
                hits = {}

            now = time.monotonic()
            if now - self.last_state_save_time > 10.0:
                self.save_state()
                self.last_state_save_time = now
                self.refresh_gauges()

            if not lines:
                for reader in self.readers.values():
                    # Refresh readers so they can send their buffered stats out
                    reader.inc_line_stats(journal_bytes=0, journal_lines=0)

                self.log.debug("No new journal lines received")

            self.ping_watchdog()
            poll_timeout = max(
                0,
                self.poll_interval_ms - (time.monotonic_ns() - iteration_start_time) // 1000,
            )

        self._close_stale_readers()


if __name__ == "__main__":
    JournalPump.run_exit()
