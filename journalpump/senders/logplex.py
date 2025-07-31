from .base import LogSender
from journalpump.util import get_requests_session

import datetime
import json


class LogplexSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 5.0), **kwargs)
        self.logplex_input_url = config["logplex_log_input_url"]
        self.request_timeout = config.get("logplex_request_timeout", 2)
        self.logplex_token = config["logplex_token"]
        self.session = get_requests_session()
        self.msg_id = "-"
        self.structured_data = "-"
        self.mark_connected()

    def format_msg(self, msg):
        # TODO: figure out a way to optionally get the entry without JSON
        entry = json.loads(msg.decode("utf8"))
        hostname = entry.get("_HOSTNAME", "localhost")
        pid = entry.get("_PID", "localhost")
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00 ")
        pkt = f"<190>1 {timestamp}{hostname} {self.logplex_token} {pid} {self.msg_id} {self.structured_data}"
        pkt += entry["MESSAGE"]
        pkt = pkt.encode("utf8")
        return f"{len(pkt)} {pkt}"

    def send_messages(self, *, messages, cursor):
        auth = ("token", self.logplex_token)
        msg_data = "".join([self.format_msg(msg) for msg in messages])
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
            verify=False,
        )
        self.mark_sent(messages=messages, cursor=cursor)
