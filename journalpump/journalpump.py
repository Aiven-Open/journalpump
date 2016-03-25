# Copyright 2016, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from . daemon import ServiceDaemon
from . import statsd
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaClient, SimpleProducer
from kafka.protocol import CODEC_SNAPPY, CODEC_NONE
from requests import Session
from systemd.journal import Reader
from threading import Thread, Lock
import datetime
import errno
import json
import kafka.common
import logging
import os
import socket
import systemd.journal
import time
import types
import uuid

try:
    import snappy
except ImportError:
    snappy = None


KAFKA_CONN_ERRORS = tuple(kafka.common.RETRY_ERROR_TYPES) + (
    kafka.common.UnknownError,
    socket.timeout,
)


KAFKA_COMPRESSED_MESSAGE_OVERHEAD = 30
MAX_KAFKA_MESSAGE_SIZE = 1024 ** 2


logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("kafka").setLevel(logging.CRITICAL)  # remove client-internal tracebacks from logging output


def _convert_uuid(s):
    return str(uuid.UUID(s.decode()))


def convert_mon(s):  # pylint: disable=unused-argument
    return None


def convert_realtime(t):
    return int(t) / 1000000.0  # Stock systemd transforms these into datetimes


converters = {
    "MESSAGE_ID": _convert_uuid,
    "_MACHINE_ID": _convert_uuid,
    "_BOOT_ID": _convert_uuid,
    "_SOURCE_REALTIME_TIMESTAMP": convert_realtime,
    "__REALTIME_TIMESTAMP": convert_realtime,
    "_SOURCE_MONOTONIC_TIMESTAMP": convert_mon,
    "__MONOTONIC_TIMESTAMP": convert_mon,
    "COREDUMP_TIMESTAMP": convert_realtime
}

systemd.journal.DEFAULT_CONVERTERS.update(converters)


def _convert_field(self, key, value):
    convert = self.converters.get(key, bytes.decode)
    try:
        return convert(value)
    except ValueError:
        # Leave in default bytes
        try:
            return bytes.decode(value)
        except:  # pylint: disable=bare-except
            return value


class JournalObject:
    def __init__(self, cursor=None, entry=None):
        self.cursor = cursor
        self.entry = entry or {}


def get_next(self, skip=1):
    """Own get_next implementation that doesn't store the cursor
       since we don't want it"""
    if super(Reader, self)._next(skip):  # pylint: disable=protected-access
        entry = super(Reader, self)._get_all()  # pylint: disable=protected-access
        if entry:
            entry["__REALTIME_TIMESTAMP"] = self._get_realtime()  # pylint: disable=protected-access
            return JournalObject(cursor=self._get_cursor(), entry=self._convert_entry(entry))  # pylint: disable=protected-access
    return JournalObject()


class LogSender(Thread):
    def __init__(self, config, msg_buffer, stats, max_send_interval):
        Thread.__init__(self)
        self.log = logging.getLogger("LogSender")
        self.stats = stats
        self.config = config
        self.cursor = None
        self.last_send_time = time.time()
        self.last_state_save_time = time.time()
        self.msg_buffer = msg_buffer
        self.max_send_interval = max_send_interval
        self.start_time = time.time()
        self.previous_state = None
        self.running = True
        self.log.info("Initialized LogSender")

    def send_messages(self, message_batch):
        pass

    def maintenance_operations(self):
        # This can be overridden in the classes that inherit this
        pass

    def run(self):
        while self.running:
            self.maintenance_operations()
            if len(self.msg_buffer) > 100 or \
               time.time() - self.last_send_time > self.max_send_interval:
                self.get_and_send_messages()
            else:
                time.sleep(0.1)
        self.log.info("Stopping")

    def get_and_send_messages(self):
        start_time = time.time()
        try:
            messages, cursor = self.msg_buffer.get_items()
            while self.running and messages:
                batch_size = len(messages[0]) + KAFKA_COMPRESSED_MESSAGE_OVERHEAD
                index = 1
                while index < len(messages):
                    item_size = len(messages[index]) + KAFKA_COMPRESSED_MESSAGE_OVERHEAD
                    if batch_size + item_size >= MAX_KAFKA_MESSAGE_SIZE:
                        break
                    batch_size += item_size
                    index += 1

                messages_batch = messages[:index]
                if self.send_messages(messages_batch):
                    messages = messages[index:]

            self.cursor = cursor
            self.log.debug("Sending %d msgs, cursor: %r took %.4fs",
                           len(messages), self.cursor, time.time() - start_time)

            if time.time() - self.last_state_save_time > 1.0:
                self.save_state()
            self.last_send_time = time.time()
        except:  # pylint: disable=bare-except
            self.log.exception("Problem sending messages: %r", messages)
            time.sleep(0.5)

    def save_state(self):
        state_to_save = {
            "cursor": self.cursor,
            "total_size": self.msg_buffer.total_size,
            "entry_num": self.msg_buffer.entry_num,
            "start_time": self.start_time,
            "current_queue": len(self.msg_buffer)
        }

        if state_to_save != self.previous_state:
            with open(self.config.get("json_state_file_path", "journalpump_state.json"), "w") as fp:
                json.dump(state_to_save, fp, indent=4, sort_keys=True)
                self.previous_state = state_to_save
                self.log.debug("Wrote state file: %r, %.2f entries/s processed", state_to_save,
                               self.msg_buffer.entry_num / (time.time() - self.start_time))


class KafkaSender(LogSender):
    def __init__(self, config, msg_buffer, stats):
        LogSender.__init__(self, config=config, msg_buffer=msg_buffer, stats=stats,
                           max_send_interval=config.get("max_send_interval", 0.3))
        self.config = config
        self.msg_buffer = msg_buffer
        self.stats = stats

        self.kafka = None
        self.kafka_producer = None

        if not isinstance(self.config["kafka_topic"], bytes):
            topic = self.config["kafka_topic"].encode("utf8")
        self.topic = topic

    def _init_kafka(self):
        self.log.info("Initializing Kafka client, address: %r", self.config["kafka_address"])
        while self.running:
            try:
                if self.kafka_producer:
                    self.kafka_producer.stop()
                if self.kafka:
                    self.kafka.close()

                self.kafka = KafkaClient(  # pylint: disable=unexpected-keyword-arg
                    self.config["kafka_address"],
                    ssl=self.config.get("ssl", False),
                    certfile=self.config.get("certfile"),
                    keyfile=self.config.get("keyfile"),
                    ca=self.config.get("ca")
                )
                self.kafka_producer = SimpleProducer(self.kafka, codec=CODEC_SNAPPY
                                                     if snappy else CODEC_NONE)
                self.log.info("Initialized Kafka Client, address: %r", self.config["kafka_address"])
                break
            except KAFKA_CONN_ERRORS as ex:
                self.log.warning("Retriable error during Kafka initialization: %s: %s, sleeping",
                                 ex.__class__.__name__, ex)
            self.kafka = None
            self.kafka_producer = None
            time.sleep(5.0)

    def send_messages(self, message_batch):
        if not self.kafka:
            self._init_kafka()
        try:
            self.kafka_producer.send_messages(self.topic, *message_batch)
            return True
        except KAFKA_CONN_ERRORS as ex:
            self.log.info("Kafka retriable error during send: %s: %s, waiting", ex.__class__.__name__, ex)
            time.sleep(0.5)
            self._init_kafka()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Unexpected exception during send to kafka")
            self.stats.unexpected_exception(ex=ex, where="sender", tags={"app": "journalpump"})
            time.sleep(5.0)
            self._init_kafka()


class ElasticsearchSender(LogSender):
    def __init__(self, config, msg_buffer, stats):
        LogSender.__init__(self, config=config, msg_buffer=msg_buffer, stats=stats,
                           max_send_interval=config.get("max_send_interval", 0.3))
        self.config = config
        self.msg_buffer = msg_buffer
        self.stats = stats
        self.elasticsearch_url = self.config.get("elasticsearch_url")
        self.last_index_check_time = 0
        self.request_timeout = self.config.get("elasticsearch_timeout", 10.0)
        self.index_days_max = self.config.get("elasticsearch_index_days_max", 3)
        self.index_name = self.config.get("elasticsearch_index_prefix", "journalpump")
        self.es = Elasticsearch([self.elasticsearch_url], timeout=self.request_timeout)

    def check_indices(self):
        indices = sorted(key for key in self.es.indices.get_aliases().keys() if key.startswith(self.index_name))
        if len(indices) > self.index_days_max:
            index_to_delete = indices[0]
            self.log.info("Deleting index: %r since we only keep %d days worth of indices",
                          index_to_delete, self.index_days_max)
            try:
                self.es.indices.delete(index_to_delete)
            except:   # pylint: disable=bare-except
                self.log.exception("Problem deleting index: %r", index_to_delete)

    def maintenance_operations(self):
        if time.monotonic() - self.last_index_check_time > 3600:
            self.last_index_check_time = time.monotonic()
            self.check_indices()

    def send_messages(self, message_batch):
        start_time = time.monotonic()
        try:
            actions = []
            for msg in message_batch:
                message = json.loads(msg.decode("utf8"))
                timestamp = message.get("timestamp")
                if "__REALTIME_TIMESTAMP" in message:
                    timestamp = datetime.datetime.utcfromtimestamp(message["__REALTIME_TIMESTAMP"])
                else:
                    timestamp = datetime.datetime.utcnow()

                message["timestamp"] = timestamp
                actions.append({
                    "_index": "{}-{}".format(self.index_name, datetime.datetime.date(timestamp)),
                    "_type": "journal_msg",
                    "_source": message,
                })
            if actions:
                helpers.bulk(self.es, actions)
                self.log.info("Sent %d metrics to ES, took: %.2fs",
                              len(message_batch), time.monotonic() - start_time)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.warning("Problem sending logs to ES: %s", ex.__class__.__name__)
            return False
        return True


class LogplexSender(LogSender):
    def __init__(self, config, msg_buffer, stats):
        LogSender.__init__(self, config=config, msg_buffer=msg_buffer, stats=stats,
                           max_send_interval=config.get("max_send_interval", 5.0))
        self.config = config
        self.msg_buffer = msg_buffer
        self.stats = stats
        self.logplex_input_url = self.config["logplex_log_input_url"]
        self.request_timeout = self.config.get("logplex_request_timeout", 2)
        self.logplex_token = self.config["logplex_token"]
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

    def send_messages(self, message_batch):
        auth = ('token', self.config["logplex_token"])
        msg_data = ''.join([self.format_msg(msg) for msg in message_batch])
        msg_count = len(message_batch)
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


class MsgBuffer:
    def __init__(self, cursor=None):
        self.log = logging.getLogger("MsgBuffer")
        self.msg_buffer = []
        self.lock = Lock()
        self.cursor = cursor
        self.entry_num = 0
        self.total_size = 0
        self.last_journal_msg_time = time.monotonic()
        self.log.info("Initialized MsgBuffer with cursor: %r", cursor)

    def __len__(self):
        return len(self.msg_buffer)

    def get_items(self):
        messages = []
        with self.lock:
            if self.msg_buffer:
                messages = self.msg_buffer
                self.msg_buffer = []
        return messages, self.cursor

    def set_cursor(self, cursor):
        self.cursor = cursor
        self.last_journal_msg_time = time.monotonic()

    def set_item(self, item, cursor):
        with self.lock:
            self.msg_buffer.append(item)
            self.cursor = cursor
            self.last_journal_msg_time = time.monotonic()
        self.entry_num += 1
        self.total_size += len(item)


class JournalPump(ServiceDaemon):
    def __init__(self, config_path):
        self.stats = None
        ServiceDaemon.__init__(self, config_path=config_path, multi_threaded=True, log_level=logging.INFO)
        cursor = self.load_state()
        self.msg_buffer = MsgBuffer(cursor)
        self.journald_reader = None
        self.sender = None
        self.get_reader(cursor)

    def get_reader(self, cursor):
        if self.config.get("journal_path"):
            while True:
                try:
                    self.journald_reader = Reader(path=self.config["journal_path"])
                    break
                except IOError as ex:
                    if ex.errno == errno.ENOENT:
                        self.log.warning("journal not available yet, waiting: %s: %s",
                                         ex.__class__.__name__, ex)
                        time.sleep(5.0)
                    else:
                        raise
        else:
            self.journald_reader = Reader()

        for unit_to_match in self.config.get("units_to_match", []):
            self.journald_reader.add_match(_SYSTEMD_UNIT=unit_to_match)

        if cursor:
            self.journald_reader.seek_cursor(cursor)  # pylint: disable=no-member

        self.journald_reader.get_next = types.MethodType(get_next, self.journald_reader)
        self.journald_reader._convert_field = types.MethodType(_convert_field, self.journald_reader)  # pylint: disable=protected-access

    def handle_new_config(self):
        """Called by ServiceDaemon when config has changed"""
        stats = self.config.get("statsd") or {}
        self.stats = statsd.StatsClient(
            host=stats.get("host"),
            port=stats.get("port"),
            tags=stats.get("tags"),
        )

    def sigterm(self, signum, frame):
        if self.sender:
            self.sender.running = False
        ServiceDaemon.sigterm(self, signum, frame)

    def load_state(self):
        filepath = self.config.get("json_state_file_path", "journalpump_state.json")
        if os.path.exists(filepath):
            with open(filepath, "r") as fp:
                state_file = json.load(fp)
            return state_file["cursor"]
        return None

    def check_match(self, entry):
        if not self.config.get("match_key"):
            return True
        elif entry.get(self.config["match_key"]) == self.config["match_value"]:
            return True
        return False

    def initialize_sender(self):
        if not self.sender:
            senders = {
                "elasticsearch": ElasticsearchSender,
                "kafka": KafkaSender,
                "logplex": LogplexSender,
            }
            class_name = senders.get(self.config["output_type"])
            self.sender = class_name(config=self.config, msg_buffer=self.msg_buffer, stats=self.stats)
            self.sender.start()

    def run(self):
        while self.running:
            entry = None
            try:
                self.initialize_sender()
                jobject = next(self.journald_reader)
                for key, value in jobject.entry.items():
                    if isinstance(value, bytes):
                        jobject.entry[key] = repr(value)  # value may be bytes in any encoding

                if jobject.cursor is not None:
                    if not self.check_match(jobject.entry):
                        self.msg_buffer.set_cursor(jobject.cursor)
                        continue
                    json_entry = json.dumps(jobject.entry).encode("utf8")
                    if len(json_entry) > MAX_KAFKA_MESSAGE_SIZE:
                        self.stats.increase("journal.error", tags={"error": "too_long"})
                        error = "too large message {} bytes vs maximum {} bytes".format(
                            len(json_entry), MAX_KAFKA_MESSAGE_SIZE)
                        self.log.warning("%s: %s ...", error, json_entry[:1024])
                        entry = {
                            "error": error,
                            "partial_data": json_entry[:1024],
                        }
                        json_entry = json.dumps(entry).encode("utf8")
                    self.stats.increase("journal.lines")
                    self.stats.increase("journal.bytes", inc_value=len(json_entry))
                    self.msg_buffer.set_item(json_entry, jobject.cursor)
                else:
                    self.log.debug("No more journal entries to read, sleeping")
                    if time.monotonic() - self.msg_buffer.last_journal_msg_time > 180 and self.msg_buffer.cursor:
                        self.log.info("We haven't seen any msgs in 180s, reinitiate Reader() and seek to: %r",
                                      self.msg_buffer.cursor)
                        self.get_reader(self.msg_buffer.cursor)
                        self.msg_buffer.last_journal_msg_time = time.monotonic()
                    time.sleep(0.5)
            except StopIteration:
                self.log.debug("No more journal entries to read, sleeping")
                time.sleep(0.5)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception during handling entry: %r", jobject)
                self.stats.unexpected_exception(ex=ex, where="mainloop", tags={"app": "journalpump"})
                time.sleep(0.5)

            self.ping_watchdog()


if __name__ == "__main__":
    JournalPump.run_exit()
