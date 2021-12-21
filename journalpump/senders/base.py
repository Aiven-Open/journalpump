from threading import Lock, Thread

import asyncio
import logging
import random
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


class LogSender(Tagged):
    def __init__(
        self,
        *,
        name,
        reader,
        config,
        field_filter,
        stats,
        max_send_interval,
        extra_field_values=None,
        tags=None,
        msg_buffer_max_length=50000
    ):
        Tagged.__init__(self, tags, sender=name)
        self.log = logging.getLogger("LogSender:{}".format(reader.name))
        self._wait_for = 0.1
        self.name = name
        self.stats = stats
        self.config = config
        self.extra_field_values = extra_field_values
        self.field_filter = field_filter
        self.msg_buffer_max_length = msg_buffer_max_length
        self.last_maintenance_fail = 0
        self.last_send_time = time.monotonic()
        self.max_send_interval = max_send_interval
        self.max_batch_size = MAX_KAFKA_MESSAGE_SIZE
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

    def _get_backoff_secs(self, *, base=0.5, cap=1000.0):
        self._backoff_attempt += 1
        t = min(cap, base * 2 ** self._backoff_attempt) / 2
        t = random.random() * t + t
        return t

    def _backoff(self, *, base=0.5, cap=1800.0):
        raise NotImplementedError()

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
        pass

    def maintenance_operations(self):
        # This can be overridden in the classes that inherit this
        pass

    def handle_maintenance_operations(self):
        try:
            # Don't run maintenance operations again immediately if it just failed
            if self.last_maintenance_fail or time.monotonic() - self.last_maintenance_fail > 60:
                self.maintenance_operations()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Maintenance operation failed: %r", ex)
            self.stats.unexpected_exception(ex=ex, where="maintenance_operation")
            self.last_maintenance_fail = time.monotonic()

    def should_try_sending_messages(self):
        return len(self.msg_buffer) > 1000 or \
               time.monotonic() - self.last_send_time > self.max_send_interval

    def run(self):
        while self.running:
            self.handle_maintenance_operations()
            if self.should_try_sending_messages():
                self.get_and_send_messages()
            else:
                time.sleep(self._wait_for)
        self.log.info("Stopping")

    def get_message_bodies_and_cursor(self):
        messages = self.msg_buffer.get_items()
        ret = []
        while messages:
            batch_size = len(messages[0][0]) + self.batch_message_overhead
            index = 1
            while index < len(messages):
                item_size = len(messages[index][0]) + self.batch_message_overhead
                if batch_size + item_size >= self.max_batch_size:
                    break
                batch_size += item_size
                index += 1

            batch = messages[:index]
            message_bodies = [m[0] for m in batch]
            ret.append((message_bodies, batch[-1][1]))
            del messages[:index]
        return ret
    
    def get_and_send_messages(self):
        batches = self.get_message_bodies_and_cursor()
        msg_count = sum(len(batch[0]) for batch in batches)
        self.log.debug("Got %d items from msg_buffer", msg_count)
        start_time = time.monotonic()
        try:
            # pop to get free up memory as soon as the send was successful
            while batches:
                batch = batches.pop(0)
                # die retrying, backoff is part of sending mechanism
                while self.running and not self.send_messages(messages=batch[0], cursor=batch[1]):
                    pass
            self.log.debug("Sending %d msgs, took %.4fs", msg_count, time.monotonic() - start_time)
            self.last_send_time = time.monotonic()
        except Exception:  # pylint: disable=broad-except
            # there is already a broad except handler in send_messages, so why this ?
            self.log.exception("Problem sending %r messages", msg_count)
            self._backoff()


class ThreadedLogSender(Thread, LogSender):
    def __init__(self, **kw):
        Thread.__init__(self)
        LogSender.__init__(self, **kw)

    def _backoff(self, *, base=0.5, cap=1800.0):
        t = self._get_backoff_secs(base=base, cap=cap)
        self.log.info("Sleeping for %.0f seconds", t)
        time.sleep(t)


class AsyncLogSender(LogSender):
    def __init__(self, **kw):
        self.loop: asyncio.events.AbstractEventLoop = kw.pop('aio_loop')
        LogSender.__init__(self, **kw)

    async def _backoff(self, *, base=0.5, cap=1800.0):
        t = self._get_backoff_secs(base=base, cap=cap)
        self.log.info("Sleeping for %.0f seconds", t)
        await asyncio.sleep(t)

    def handle_maintenance_operations(self):
        async def noop():
            pass
        self.loop.create_task(noop())

    async def run(self):
        while self.running:
            await self.handle_maintenance_operations()
            if self.should_try_sending_messages():
                await self.get_and_send_messages()
            else:
                await asyncio.sleep(self._wait_for)
        self.log.info("Stopping")

    async def get_and_send_messages(self):
        batches = self.get_message_bodies_and_cursor(messages)
        msg_count = sum(len(batch[0]) for batch in batches)
        self.log.debug("Got %d items from msg_buffer", msg_count)
        start_time = time.monotonic()
        try:
            # pop to get free up memory as soon as the send was successful
            while batches:
                batch = batches.pop(0)
                # die retrying, backoff is part of sending mechanism
                while self.running and not await self.send_messages(messages=batch[0], cursor=batch[1]):
                    pass
            self.log.debug("Sending %d msgs, took %.4fs", msg_count, time.monotonic() - start_time)
            self.last_send_time = time.monotonic()
        except Exception:  # pylint: disable=broad-except
            # there is already a broad except handler in send_messages, so why this ?
            self.log.exception("Problem sending %r messages", msg_count)
            self._backoff()
