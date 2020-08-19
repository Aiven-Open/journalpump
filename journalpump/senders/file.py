from ..statsd import StatsClient, TagsType
from .base import _LogSenderReader, JournalCursor, LogSender
from typing import Any, Mapping, Optional, Sequence


class FileSender(LogSender):
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
        super().__init__(
            name=name,
            reader=reader,
            config=config,
            field_filter=field_filter,
            stats=stats,
            max_send_interval=max_send_interval
            if max_send_interval is not None else float(config.get("max_send_interval", 0.3)),
            extra_field_values=extra_field_values,
            tags=tags,
            msg_buffer_max_length=msg_buffer_max_length
        )
        self.output = open(config["file_output"], "ab")
        self.mark_connected()

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        for msg in messages:
            self.output.write(msg + b"\n")

        self.mark_sent(messages=messages, cursor=cursor)
        return True
