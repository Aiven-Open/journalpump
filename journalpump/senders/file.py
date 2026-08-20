from .base import LogSender
from typing import Any, BinaryIO


class FileSender(LogSender):
    def __init__(self, *, config: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.mark_disconnected()
        # The handle outlives this scope by design: every send_messages() call writes to it.
        self.output: BinaryIO = open(config["file_output"], "ab")  # noqa: SIM115  # pylint: disable=consider-using-with
        self.mark_connected()

    def send_messages(self, *, messages: list[bytes], cursor: str | None) -> bool:
        for msg in messages:
            self.output.write(msg + b"\n")

        self.mark_sent(messages=messages, cursor=cursor)
        return True
