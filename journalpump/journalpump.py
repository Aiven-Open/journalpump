# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from . import geohash, statsd
from . daemon import ServiceDaemon
from functools import reduce
from io import BytesIO
from kafka import KafkaProducer
from requests import Session
from systemd.journal import Reader
from threading import Lock, Thread
import copy
import datetime
import json
import kafka.common
import logging
import re
import requests
import socket
import systemd.journal
import time
import uuid

try:
    import snappy
except ImportError:
    snappy = None

try:
    from geoip2.database import Reader as GeoIPReader
except ImportError:
    GeoIPReader = None


KAFKA_CONN_ERRORS = tuple(kafka.common.RETRY_ERROR_TYPES) + (
    kafka.common.UnknownError,
    socket.timeout,
)


KAFKA_COMPRESSED_MESSAGE_OVERHEAD = 30
MAX_KAFKA_MESSAGE_SIZE = 1024 ** 2  # 1 MiB


logging.getLogger("kafka").setLevel(logging.CRITICAL)  # remove client-internal tracebacks from logging output


def _convert_uuid(s):
    return str(uuid.UUID(s.decode()))


def convert_mon(s):  # pylint: disable=unused-argument
    return None


def convert_realtime(t):
    return int(t) / 1000000.0  # Stock systemd transforms these into datetimes


def default_json_serialization(obj):  # pylint: disable=inconsistent-return-statements
    if isinstance(obj, bytes):
        return obj.decode("utf8")
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()


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

        return JournalObject()


class Tagged:
    def __init__(self, tags=None, **kw):
        self._tags = (tags or {}).copy()
        self._tags.update(kw)

    def make_tags(self, tags=None):
        output = self._tags.copy()
        output.update(tags or {})
        return output

    def replace_tags(self, tags):
        self._tags = tags


class LogSender(Thread, Tagged):
    def __init__(self, *, name, reader, config, field_filter, stats, max_send_interval,
                 tags=None, msg_buffer_max_length=50000):
        Thread.__init__(self)
        Tagged.__init__(self, tags, sender=name)
        self.log = logging.getLogger("LogSender:{}".format(reader.name))
        self.name = name
        self.stats = stats
        self.config = config
        self.field_filter = field_filter
        self.msg_buffer_max_length = msg_buffer_max_length
        self.last_send_time = time.monotonic()
        self.max_send_interval = max_send_interval
        self.running = True
        self._sent_cursor = None
        self._sent_count = 0
        self._sent_bytes = 0
        self.msg_buffer = MsgBuffer()
        self.log.info("Initialized %s", self.__class__.__name__)

    def refresh_stats(self):
        tags = self.make_tags()
        self.stats.gauge("journal.last_sent_ago", value=time.monotonic() - self.last_send_time, tags=tags)
        self.stats.gauge("journal.sent_bytes", value=self._sent_bytes, tags=tags)
        self.stats.gauge("journal.sent_lines", value=self._sent_count, tags=tags)

    def get_state(self):
        return {
            "type": self.__class__.__name__,
            "buffer": {
                "cursor": self.msg_buffer.cursor,
                "total_size": self.msg_buffer.total_size,
                "entry_num": self.msg_buffer.entry_num,
                "current_queue": len(self.msg_buffer.messages),
            },
            "sent": {
                "cursor": self._sent_cursor,
                "count": self._sent_count,
                "bytes": self._sent_bytes,
            },
        }

    def mark_sent(self, *, messages, cursor):
        self._sent_count += len(messages)
        self._sent_bytes += sum(len(m) for m in messages)
        self._sent_cursor = cursor

    def request_stop(self):
        self.running = False

    def send_messages(self, *, messages, cursor):
        pass

    def maintenance_operations(self):
        # This can be overridden in the classes that inherit this
        pass

    def run(self):
        while self.running:
            self.maintenance_operations()
            if len(self.msg_buffer) > 1000 or \
               time.monotonic() - self.last_send_time > self.max_send_interval:
                self.get_and_send_messages()
            else:
                time.sleep(0.1)
        self.log.info("Stopping")

    def get_and_send_messages(self):
        start_time = time.monotonic()
        messages = None
        try:
            messages = self.msg_buffer.get_items()
            msg_count = len(messages)
            self.log.debug("Got %d items from msg_buffer", msg_count)

            while self.running and messages:
                msg_buffer_length = len(self.msg_buffer)
                if msg_buffer_length > self.msg_buffer_max_length:
                    # This makes the self.msg_buffer grow to at most msg_buffer_max_length entries
                    self.log.debug("%d entries in msg buffer, slowing down a bit by sleeping",
                                   msg_buffer_length)
                    time.sleep(1.0)

                batch_size = len(messages[0][0]) + KAFKA_COMPRESSED_MESSAGE_OVERHEAD
                index = 1
                while index < len(messages):
                    item_size = len(messages[index][0]) + KAFKA_COMPRESSED_MESSAGE_OVERHEAD
                    if batch_size + item_size >= MAX_KAFKA_MESSAGE_SIZE:
                        break
                    batch_size += item_size
                    index += 1

                messages_batch = messages[:index]
                message_bodies = [m[0] for m in messages_batch]
                if self.send_messages(messages=message_bodies, cursor=messages_batch[-1][1]):
                    messages = messages[index:]

            self.log.debug("Sending %d msgs, took %.4fs", msg_count, time.monotonic() - start_time)
            self.last_send_time = time.monotonic()
        except Exception:   # pylint: disable=broad-except
            self.log.exception("Problem sending messages: %r", messages)
            time.sleep(0.5)


class KafkaSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.kafka_producer = None
        self.topic = self.config.get("kafka_topic")

    def _init_kafka(self):
        self.log.info("Initializing Kafka client, address: %r", self.config["kafka_address"])
        while self.running:
            try:
                if self.kafka_producer:
                    self.kafka_producer.close()

                self.kafka_producer = KafkaProducer(
                    api_version=self.config.get("kafka_api_version", "0.9"),
                    bootstrap_servers=self.config.get("kafka_address"),
                    compression_type="snappy" if snappy else "gzip",
                    linger_ms=500,  # wait up 500 ms to see if we can send msgs in a group
                    reconnect_backoff_ms=1000,  # up from the default 50ms to reduce connection attempts
                    reconnect_backoff_max_ms=10000,  # up the upper bound for backoff to 10 seconds
                    security_protocol="SSL" if self.config.get("ssl") is True else "PLAINTEXT",
                    ssl_cafile=self.config.get("ca"),
                    ssl_certfile=self.config.get("certfile"),
                    ssl_keyfile=self.config.get("keyfile"),
                )
                self.log.info("Initialized Kafka Client, address: %r", self.config["kafka_address"])
                break
            except KAFKA_CONN_ERRORS as ex:
                self.log.warning("Retriable error during Kafka initialization: %s: %s, sleeping",
                                 ex.__class__.__name__, ex)
            self.kafka_producer = None
            time.sleep(5.0)

    def send_messages(self, *, messages, cursor):
        if not self.kafka_producer:
            self._init_kafka()
        try:
            for msg in messages:
                self.kafka_producer.send(topic=self.topic, value=msg)
            self.kafka_producer.flush()
            self.mark_sent(messages=messages, cursor=cursor)
            return True
        except KAFKA_CONN_ERRORS as ex:
            self.log.info("Kafka retriable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
            time.sleep(0.5)
            self._init_kafka()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception during send to kafka")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            time.sleep(5.0)
            self._init_kafka()
        return False


class FileSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.output = open(config["file_output"], "ab")

    def send_messages(self, *, messages, cursor):
        for msg in messages:
            self.output.write(msg + b"\n")

        self.mark_sent(messages=messages, cursor=cursor)
        return True


class ElasticsearchSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 10.0), **kwargs)
        self.session_url = self.config.get("elasticsearch_url")
        self.session_url_bulk = self.session_url + "/_bulk"
        self.last_index_check_time = 0
        self.request_timeout = self.config.get("elasticsearch_timeout", 10.0)
        self.index_days_max = self.config.get("elasticsearch_index_days_max", 3)
        self.index_name = self.config.get("elasticsearch_index_prefix", "journalpump")
        self.session = requests.Session()
        self.indices = set()
        self.last_es_error = None

    def _init_es_client(self):
        if self.indices:
            return True
        try:
            self.indices = set(self.session.get(self.session_url + "/_aliases").json().keys())
            self.last_es_error = None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ex:
            if ex.__class__ != self.last_es_error.__class__:
                # only log these errors once, not every 10 seconds
                self.log.warning("ES connection error: %s: %s", ex.__class__.__name__, ex)

            self.last_es_error = ex
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception connecting to ES")
            self.stats.unexpected_exception(ex, where="es_pump_init_es_client")
            return False

        self.log.info("Initialized ES connection")
        return True

    def create_index_and_mappings(self, index_name):
        try:
            self.log.info("Creating index: %r", index_name)
            res = self.session.put(self.session_url + "/{}".format(index_name), json={
                "mappings": {
                    "journal_msg": {
                        "properties": {
                            "SYSTEMD_SESSION": {"type": "text"},
                            "SESSION_ID": {"type": "text"},
                        }
                    }
                },
            })
            if res.status_code in (200, 201) or "already_exists_exception" in res.text:
                self.indices.add(index_name)
            else:
                self.log.warning("Could not create index mappings for: %r, %r %r", index_name, res.text, res.status_code)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ex:
            self.log.error("Problem creating index %r: %s: %s", index_name, ex.__class__.__name__, ex)
            self.stats.unexpected_exception(ex, where="es_pump_create_index_and_mappings")
            time.sleep(5.0)  # prevent busy loop

    @staticmethod
    def format_message_for_es(buf, header, message):
        buf.write(json.dumps(header, default=default_json_serialization).encode("utf8") + b"\n")
        # Message already in utf8 encoded bytestring form
        buf.write(message + b"\n")

    def check_indices(self):
        aliases = self.session.get(self.session_url + "/_aliases").json()
        indices = [key for key in aliases.keys() if key.startswith(self.index_name)]
        self.log.info("Checking indices, currently: %r are available, max_indices: %r", indices, self.index_days_max)
        while len(indices) > self.index_days_max:
            index_to_delete = indices.pop(0)
            self.log.info("Deleting index: %r since we only keep %d days worth of indices",
                          index_to_delete, self.index_days_max)
            try:
                self.session.delete("{}/{}".format(self.session_url, index_to_delete))
                self.indices.discard(index_to_delete)
            except Exception as ex:   # pylint: disable=broad-except
                self.log.exception("Unexpected exception deleting index %r", index_to_delete)
                self.stats.unexpected_exception(ex, where="es_pump_check_indices")

    def maintenance_operations(self):
        if time.monotonic() - self.last_index_check_time > 3600:
            self.last_index_check_time = time.monotonic()
            self.check_indices()

    def send_messages(self, *, messages, cursor):
        buf = BytesIO()
        start_time = time.monotonic()
        try:
            es_available = self._init_es_client()
            if not es_available:
                self.log.warning("Waiting for ES connection")
                time.sleep(20.0)
                return False
            for msg in messages:
                message = json.loads(msg.decode("utf8"))
                # ISO datetime's first 10 characters are equivalent to the date we need i.e. '2018-04-14'
                index_name = "{}-{}".format(self.index_name, message["timestamp"][:10])
                if index_name not in self.indices:
                    self.create_index_and_mappings(index_name)

                header = {
                    "index": {
                        "_index": index_name,
                        "_type": "journal_msg",
                    }
                }
                self.format_message_for_es(buf, header, msg)

            # If we have messages, send them along to ES
            if buf.tell():
                buf_size = buf.tell()
                buf.seek(0)
                res = self.session.post(self.session_url_bulk, data=buf, headers={
                    "content-length": str(buf_size),
                    "content-type": "application/x-ndjson",
                })
                buf.seek(0)
                buf.truncate(0)

                self.mark_sent(messages=messages, cursor=cursor)
                self.log.info("Sent %d log events to ES successfully: %r, took: %.2fs",
                              len(messages), res.status_code in {200, 201}, time.monotonic() - start_time)
        except Exception as ex:  # pylint: disable=broad-except
            short_msg = str(ex)[:200]
            self.log.exception("Problem sending logs to ES: %s: %s", ex.__class__.__name__, short_msg)
            return False
        return True


class LogplexSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 5.0), **kwargs)
        self.logplex_input_url = config["logplex_log_input_url"]
        self.request_timeout = config.get("logplex_request_timeout", 2)
        self.logplex_token = config["logplex_token"]
        self.session = Session()
        self.msg_id = "-"
        self.structured_data = "-"

    def format_msg(self, msg):
        # TODO: figure out a way to optionally get the entry without JSON
        entry = json.loads(msg.decode("utf8"))
        hostname = entry.get("_HOSTNAME", "localhost")
        pid = entry.get("_PID", "localhost")
        pkt = "<190>1 {} {} {} {} {} {}".format(
            datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00 "),
            hostname,
            self.logplex_token,
            pid,
            self.msg_id,
            self.structured_data)
        pkt += entry["MESSAGE"]
        pkt = pkt.encode("utf8")
        return '{} {}'.format(len(pkt), pkt)

    def send_messages(self, *, messages, cursor):
        auth = ('token', self.logplex_token)
        msg_data = ''.join([self.format_msg(msg) for msg in messages])
        msg_count = len(messages)
        headers = {
            "Content-Type": "application/logplex-1",
            "Logplex-Msg-Count": msg_count,
        }
        self.session.post(
            self.logplex_input_url,
            auth=auth,
            headers=headers,
            data=msg_data,
            timeout=self.request_timeout,
            verify=False
        )
        self.mark_sent(messages=messages, cursor=cursor)


class MsgBuffer:
    def __init__(self):
        self.log = logging.getLogger("MsgBuffer")
        self.messages = []
        self.lock = Lock()
        self.entry_num = 0
        self.total_size = 0
        self.last_journal_msg_time = time.monotonic()
        self.cursor = None

    def __len__(self):
        return len(self.messages)

    def get_items(self):
        messages = []
        with self.lock:  # pylint: disable=not-context-manager
            if self.messages:
                messages = self.messages
                self.messages = []
        return messages

    def add_item(self, *, item, cursor):
        with self.lock:  # pylint: disable=not-context-manager
            self.messages.append((item, cursor))
            self.last_journal_msg_time = time.monotonic()
            self.cursor = cursor

        self.entry_num += 1
        self.total_size += len(item)


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
        self.geoip = geoip
        self.config = config
        self.field_filters = field_filters
        self.stats = stats
        self.cursor = seek_to
        self.journald_reader = None
        self.running = True
        self.senders = {}
        self._senders_initialized = False
        self.last_stats_send_time = time.monotonic()
        self.last_journal_msg_time = time.monotonic()
        self.searches = list(self._build_searches(searches))

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
        }
        for sender_name, sender_config in self.config.get("senders", {}).items():
            try:
                sender_class = senders[sender_config["output_type"]]
            except KeyError:
                raise Exception("Unknown sender type {!r}".format(sender_config["output_type"]))

            field_filter = None
            if sender_config.get("field_filter", None):
                field_filter = self.field_filters[sender_config["field_filter"]]
            sender = sender_class(
                config=sender_config,
                field_filter=field_filter,
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
        lines_diff = self.read_lines - self._last_sent_read_lines
        bytes_diff = self.read_bytes - self._last_sent_read_bytes
        if lines_diff or bytes_diff:
            self.log.info("Processed %r journal lines (%r bytes)", lines_diff, bytes_diff)
        self._last_sent_read_lines = self.read_lines
        self._last_sent_read_bytes = self.read_bytes

        for sender in self.senders.values():
            sender.refresh_stats()

    def read_next(self):
        journald_reader = self.get_reader(seek_to=self.cursor)
        if journald_reader:
            jobject = next(journald_reader)
            if jobject.cursor:
                self.cursor = jobject.cursor
                self.last_journal_msg_time = time.monotonic()
        else:
            jobject = None

        inactivity_timeout = 180
        if (time.monotonic() - self.last_journal_msg_time) > inactivity_timeout and self.cursor:
            self.log.info("We haven't seen any msgs in %.1fs, reinitiate PumpReader()", inactivity_timeout)
            self.get_reader(seek_to=self.get_resume_cursor(), reinit=True)
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
            self.log.warning("journal for %r not available yet, waiting: %s: %s",
                             self.name, ex.__class__.__name__, ex)
            time.sleep(5.0)
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
                else:
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

        self.reader.perform_searches(self.jobject)

        if self.jobject.cursor is None:
            self.log.debug("No more journal entries to read")
            return False

        if not self.pump.check_match(new_entry):
            return True

        for sender in self.reader.senders.values():
            json_entry = self._get_or_generate_json(sender.field_filter, new_entry)
            sender.msg_buffer.add_item(item=json_entry, cursor=self.jobject.cursor)

        if self.json_objects:
            max_bytes = max(len(v) for v in self.json_objects.values())
            self.reader.inc_line_stats(journal_bytes=max_bytes, journal_lines=1)

        return True

    def _get_or_generate_json(self, field_filter, data):
        ff_name = "" if field_filter is None else field_filter.name
        if ff_name in self.json_objects:
            return self.json_objects[ff_name]

        if field_filter:
            data = field_filter.filter_fields(data)

        # Always set a timestamp field that gets turned into an ISO timestamp based on REALTIME_TIMESTAMP if available
        if "REALTIME_TIMESTAMP" in data:
            timestamp = datetime.datetime.utcfromtimestamp(data["REALTIME_TIMESTAMP"])
        else:
            timestamp = datetime.datetime.utcnow()
        data["timestamp"] = timestamp

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
        new_config = self.config.get("readers", [])
        if self.readers_active_config == new_config:
            # No changes in readers, no reconfig required
            return

        # replace old readers with new ones
        for reader in self.readers.values():
            reader.request_stop()

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
        elif entry.get(self.config["match_key"]) == self.config["match_value"]:
            return True
        return False

    def reader_iteration(self, reader):
        try:
            jobject = reader.read_next()
            if jobject is None:
                return False

            return JournalObjectHandler(jobject, reader, self).process()
        except StopIteration:
            self.log.debug("No more journal entries to read, sleeping")
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception during handling entry")
            self.stats.unexpected_exception(ex=ex, where="mainloop", tags=self.make_tags({"app": "journalpump"}))
            time.sleep(0.5)
            return False

    def reader_iterations(self):
        hits = {}
        lines = 0
        for reader_name, reader in self.readers.items():
            try:
                lines += int(self.reader_iteration(reader))
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception during reader %r iteration", reader_name)
                self.stats.unexpected_exception(ex=ex, where="reader_iterations",
                                                tags=self.make_tags({"app": "journalpump"}))
                continue

            for search in reader.searches:
                hits[search["name"]] = search.get("hits", 0)

        return lines, hits

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
            with open(state_file_path, "w") as fp:
                json.dump(state_to_save, fp, indent=4, sort_keys=True)
                self.previous_state = state_to_save
                self.log.debug("Wrote state file: %r", state_to_save)

    def run(self):
        last_stats_time = 0
        while self.running:
            lines, hits = self.reader_iterations()

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

                self.log.debug("No new journal lines received, sleeping")
                time.sleep(1.0)

            self.ping_watchdog()


if __name__ == "__main__":
    JournalPump.run_exit()
