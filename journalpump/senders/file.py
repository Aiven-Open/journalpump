from .base import LogSender


class FileSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self.mark_disconnected()
        self.output = open(config["file_output"], "ab")
        self.mark_connected()

    def send_messages(self, *, messages, cursor):
        for msg in messages:
            self.output.write(msg + b"\n")

        self.mark_sent(messages=messages, cursor=cursor)
        return True
