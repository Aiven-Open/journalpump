from threading import Lock, Thread

import logging
import random
import sys
import time

KAFKA_COMPRESSED_MESSAGE_OVERHEAD = 30
MAX_KAFKA_MESSAGE_SIZE = 1024 ** 2  # 1 MiB

MAX_ERROR_MESSAGES = 8
MAX_ERROR_MESSAGE_LEN = 128


class SenderInitializationError(Exception):
    """Error during sender initialization"""


# Map running/_connected to health
def _convert_to_health(*, running, connected):
    if running:
        return ("connected", 3) if connected else ("disconnected", 2)
    return ("stale", 2) if connected else ("stopped", 0)


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


class MsgBuffer:
    def __init__(self):
        self.log = logging.getLogger("MsgBuffer")
        self.messages = []
        self.lock = Lock()
        self.entry_num = 0
        self.total_size = 0
        self.last_journal_msg_time = time.monotonic()
        self.cursor = None
        self.buffer_size = 0

    def __len__(self):
        return len(self.messages)

    def get_items(self):
        messages = []
        with self.lock:  # pylint: disable=not-context-manager
            if self.messages:
                messages = self.messages
                self.messages = []
                self.buffer_size = 0
        return messages

    def add_item(self, *, item, cursor):
        with self.lock:  # pylint: disable=not-context-manager
            self.messages.append((item, cursor))
            self.last_journal_msg_time = time.monotonic()
            self.cursor = cursor
            self.buffer_size += sys.getsizeof(item)

        self.entry_num += 1
        self.total_size += len(item)


class LogSender(Thread, Tagged):
    def __init__(
        self,
        *,
        name,
        reader,
        config,
        field_filter,
        stats,
        max_send_interval,
        max_heartbeat_interval=None,
        unit_log_levels=None,
        extra_field_values=None,
        tags=None,
        msg_buffer_max_length=50000
    ):
        Thread.__init__(self)
        Tagged.__init__(self, tags, sender=name)
        self.log = logging.getLogger("LogSender:{}".format(reader.name))
        self.name = name
        self.stats = stats
        self.config = config
        self.extra_field_values = extra_field_values
        self.field_filter = field_filter
        self.unit_log_levels = unit_log_levels
        self.msg_buffer_max_length = msg_buffer_max_length
        self.last_maintenance_fail = 0
        self.last_send_time = time.monotonic()
        self.max_send_interval = max_send_interval
        self.max_batch_size = MAX_KAFKA_MESSAGE_SIZE
        self.last_heartbeat_time = time.monotonic()
        self.max_heartbeat_interval = max_heartbeat_interval
        self.batch_message_overhead = KAFKA_COMPRESSED_MESSAGE_OVERHEAD
        self.running = True
        self._sent_cursor = None
        self._sent_count = 0
        self._sent_bytes = 0
        self._connected = False
        self._connected_changed = time.monotonic()  # last time _connected status changed
        self._errors = []
        self.msg_buffer = MsgBuffer()
        self._backoff_attempt = 0
        self.log.info("Initialized %s", self.__class__.__name__)

    def _backoff(self, *, base=0.5, cap=1800.0):
        self._backoff_attempt += 1
        t = min(cap, base * 2 ** self._backoff_attempt) / 2
        t = random.random() * t + t
        self.log.info("Sleeping for %.0f seconds", t)
        time.sleep(t)

    def refresh_stats(self):
        tags = self.make_tags()
        self.stats.gauge("journal.last_sent_ago", value=time.monotonic() - self.last_send_time, tags=tags)
        self.stats.gauge("journal.sent_bytes", value=self._sent_bytes, tags=tags)
        self.stats.gauge("journal.sent_lines", value=self._sent_count, tags=tags)
        self.stats.gauge(
            "journal.status", value=_convert_to_health(running=self.running, connected=self._connected)[1], tags=tags
        )

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
            "health": {
                "status": _convert_to_health(running=self.running, connected=self._connected)[0],
                "elapsed": time.monotonic() - self._connected_changed,
                "errors": self._errors,
            }
        }

    def mark_connected(self):
        if not self._connected:
            self._connected_changed = time.monotonic()
        self._connected = True
        self._errors = []
        self._backoff_attempt = 0

    def mark_disconnected(self, error=None):
        if self._connected:
            self._connected_changed = time.monotonic()
        self._connected = False

        if error is None:
            return

        if isinstance(error, str):
            msg = error
        elif isinstance(error, Exception):
            msg = getattr(error, "message", repr(error))
        else:
            msg = repr(error)

        if not msg:
            return

        msg = msg[:MAX_ERROR_MESSAGE_LEN]
        if msg in self._errors:
            return
        self._errors.append(msg)

        if len(self._errors) > MAX_ERROR_MESSAGES:
            self._errors.pop(1)  # always keep the first error message

    def mark_sent(self, *, messages, cursor):
        self._sent_count += len(messages)
        self._sent_bytes += sum(len(m) for m in messages)
        self._sent_cursor = cursor
        self.mark_connected()

    def request_stop(self):
        self.running = False

    def send_messages(self, *, messages, cursor):
        # This should be overridden in the classes that inherit this
        pass

    def maintenance_operations(self):
        # This can be overridden in the classes that inherit this
        pass

    def send_heartbeat(self):
        # This can be overridden in the classes that inherit this
        pass

    def run(self):
        self.log.info("Starting")
        while self.running:
            if self.should_perform_maintenance():
                self.perform_maintenance()

            if self.should_send_hearbeat():
                self.do_send_heartbeat()

            if self.should_send_messages():
                self.get_and_send_messages()
            else:
                time.sleep(0.1)

        self.log.info("Stopping")

    def should_perform_maintenance(self):
        # Don't run maintenance operations again immediately if it just failed
        return not self.last_maintenance_fail or time.monotonic() - self.last_maintenance_fail > 60

    def should_send_messages(self):
        max_send_interval_exceeded = time.monotonic() - self.last_send_time > self.max_send_interval
        buffer_size_over_send_threshold = len(self.msg_buffer) > 1000
        return buffer_size_over_send_threshold or max_send_interval_exceeded

    def should_send_hearbeat(self):
        return self.max_heartbeat_interval and time.monotonic() - self.last_heartbeat_time > self.max_heartbeat_interval

    def perform_maintenance(self):
        self.log.info("Performing maintenance")
        try:
            self.maintenance_operations()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Maintenance operation failed: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="maintenance_operation")
            self.last_maintenance_fail = time.monotonic()

    def do_send_heartbeat(self):
        self.log.info("Sending heartbeat message")
        try:
            self.send_heartbeat()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.warning("Problem sending heartbeat message: %r", ex)
        finally:
            self.last_heartbeat_time = time.monotonic()

    def get_and_send_messages(self):
        start_time = time.monotonic()
        msg_count = None
        try:
            messages = self.msg_buffer.get_items()
            msg_count = len(messages)
            self.log.debug("Got %d items from msg_buffer", msg_count)

            while self.running and messages:
                batch_size = len(messages[0][0]) + self.batch_message_overhead
                index = 1
                while index < len(messages):
                    item_size = len(messages[index][0]) + self.batch_message_overhead
                    if batch_size + item_size >= self.max_batch_size:
                        break
                    batch_size += item_size
                    index += 1

                messages_batch = messages[:index]
                message_bodies = [m[0] for m in messages_batch]
                if self.send_messages(messages=message_bodies, cursor=messages_batch[-1][1]):
                    messages = messages[index:]

            self.log.debug("Sending %d msgs, took %.4fs", msg_count, time.monotonic() - start_time)
            self.last_send_time = time.monotonic()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Problem sending %r messages", msg_count)
            self._backoff()
