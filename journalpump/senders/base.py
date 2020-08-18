from journalpump.statsd import StatsClient, TagsType
from threading import Lock, Thread
from typing import Any, Dict, List, Mapping, NewType, Optional, Protocol, Sequence, Tuple, Iterable

import logging
import random
import time

KAFKA_COMPRESSED_MESSAGE_OVERHEAD = 30
MAX_KAFKA_MESSAGE_SIZE = 1024 ** 2  # 1 MiB

MAX_ERROR_MESSAGES = 8
MAX_ERROR_MESSAGE_LEN = 128

# Stub type for handling journal cursor values
JournalCursor = NewType("JournalCursor", str)
MessageType = Tuple[Any, JournalCursor]


# Map running/_connected to health
def _convert_to_health(*, running: bool, connected: bool) -> Tuple[str, int]:
    if running:
        return ("connected", 3) if connected else ("disconnected", 2)
    return ("stale", 2) if connected else ("stopped", 0)

class LogSenderError(Exception):
    """Base exception for LogSender specific error types"""

class LogSenderShutdown(LogSenderError):
    """Raised when an attempt to use a log sender is made after it is shutdown"""

class Tagged:
    def __init__(self, tags: Optional[TagsType] = None, **kw: str):
        self._tags: Dict[str, str] = dict(tags) if tags is not None else {}
        self._tags.update(kw)

    def make_tags(self, tags: Optional[TagsType] = None) -> Dict[str, str]:
        output = self._tags.copy()
        if tags is not None:
            output.update(tags)
        return output

    def replace_tags(self, tags: TagsType) -> None:
        self._tags = dict(tags)


class MsgBuffer:
    def __init__(self) -> None:
        self.log: logging.Logger = logging.getLogger("MsgBuffer")
        self.messages: List[MessageType] = []
        self.lock = Lock()
        self.entry_num: int = 0
        self.total_size: int = 0
        self.last_journal_msg_time: float = time.monotonic()
        self.cursor: Optional[JournalCursor] = None

    def __len__(self) -> int:
        return len(self.messages)

    def get_items(self) -> List[MessageType]:
        with self.lock:
            messages = self.messages
            self.messages = []
        return messages

    def add_item(self, *, item: Any, cursor: JournalCursor) -> None:
        with self.lock:
            self.messages.append((item, cursor))
            self.last_journal_msg_time = time.monotonic()
            self.cursor = cursor

        self.entry_num += 1
        self.total_size += len(item)


class _LogSenderReader(Protocol):
    @property
    def name(self) -> str:
        ...


class LogSender(Thread, Tagged):
    def __init__(
        self,
        *,
        name: str,
        reader: _LogSenderReader,
        config: Mapping[str, Any],
        field_filter: Mapping[str, Any],
        stats: StatsClient,
        max_send_interval: float,
        extra_field_values: Optional[Mapping[str, str]] = None,
        tags: Optional[TagsType] = None,
        msg_buffer_max_length: int = 50000
    ):
        Thread.__init__(self)
        Tagged.__init__(self, tags, sender=name)
        self.log = logging.getLogger("LogSender:{}".format(reader.name))
        self.name = name
        self.stats = stats
        self.config = config
        self.extra_field_values: Dict[str, str] = dict(extra_field_values) if extra_field_values is not None else {}
        self.field_filter = field_filter
        self.msg_buffer_max_length = msg_buffer_max_length
        self.last_maintenance_fail: float = 0
        self.last_send_time = time.monotonic()
        self.max_send_interval = max_send_interval
        self.running = True
        self._sent_cursor: Optional[JournalCursor] = None
        self._sent_count: int = 0
        self._sent_bytes: int = 0
        self._connected = False
        self._connected_changed = time.monotonic()  # last time _connected status changed
        self._errors: List[str] = []
        self.msg_buffer: MsgBuffer = MsgBuffer()
        self._backoff_attempt: int = 0
        self.log.info("Initialized %s", self.__class__.__name__)

    def _backoff(self, *, base: float = 0.5, cap: float = 1800.0) -> None:
        self._backoff_attempt += 1
        t = min(cap, base * 2 ** self._backoff_attempt) / 2
        t = random.random() * t + t
        self.log.info("Sleeping for %.0f seconds", t)
        time.sleep(t)

    def refresh_stats(self) -> None:
        tags = self.make_tags()
        self.stats.gauge("journal.last_sent_ago", value=time.monotonic() - self.last_send_time, tags=tags)
        self.stats.gauge("journal.sent_bytes", value=self._sent_bytes, tags=tags)
        self.stats.gauge("journal.sent_lines", value=self._sent_count, tags=tags)
        self.stats.gauge(
            "journal.status", value=_convert_to_health(running=self.running, connected=self._connected)[1], tags=tags
        )

    def get_state(self) -> Mapping[str, Any]:
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

    def mark_connected(self) -> None:
        if not self._connected:
            self._connected_changed = time.monotonic()
        self._connected = True
        self._errors = []
        self._backoff_attempt = 0

    def mark_disconnected(self, error: Optional[Any] = None) -> None:
        if self._connected:
            self._connected_changed = time.monotonic()
        self._connected = False

        if error is None:
            return

        if isinstance(error, str):
            msg = error
        elif isinstance(error, BaseException):
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

    def mark_sent(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> None:
        self._sent_count += len(messages)
        self._sent_bytes += sum(len(m) for m in messages)
        self._sent_cursor = cursor
        self.mark_connected()

    def request_stop(self) -> None:
        self.running = False

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        pass

    def maintenance_operations(self) -> None:
        # This can be overridden in the classes that inherit this
        pass

    def run(self) -> None:
        while self.running:
            try:
                # Don't run maintenance operations again immediately if it just failed
                if not self.last_maintenance_fail or time.monotonic() - self.last_maintenance_fail > 60:
                    self.maintenance_operations()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error("Maintenance operation failed: %r", ex)
                self.stats.unexpected_exception(ex=ex, where="maintenance_operation")
                self.last_maintenance_fail = time.monotonic()
            if len(self.msg_buffer) > 1000 or \
               time.monotonic() - self.last_send_time > self.max_send_interval:
                self.get_and_send_messages()
            else:
                time.sleep(0.1)
        self.log.info("Stopping")

    def get_and_send_messages(self) -> None:
        start_time = time.monotonic()
        msg_count = None
        try:
            messages = self.msg_buffer.get_items()
            msg_count = len(messages)
            self.log.debug("Got %d items from msg_buffer", msg_count)

            while self.running and messages:
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
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Problem sending %r messages", msg_count)
            self._backoff()
