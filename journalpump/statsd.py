"""
StatsD client

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

"""
from typing import Dict, Mapping, Optional, SupportsFloat

import socket

TagsType = Mapping[str, str]


class StatsClient:
    @staticmethod
    def _convert_tags(tags: Optional[TagsType] = None) -> Dict[str, str]:
        return dict(tags.items()) if tags is not None else {}

    def __init__(self, host: str = "127.0.0.1", port: int = 8125, tags: Optional[TagsType] = None) -> None:
        self._dest_addr = (host, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tags: Dict[str, str] = self._convert_tags(tags)

    def gauge(self, metric: str, value: SupportsFloat, tags: Optional[TagsType] = None) -> None:
        self._send(metric, b"g", value, self._convert_tags(tags))

    def increase(self, metric: str, inc_value: SupportsFloat = 1, tags: Optional[TagsType] = None) -> None:
        self._send(metric, b"c", inc_value, self._convert_tags(tags))

    def timing(self, metric: str, value: SupportsFloat, tags: Optional[TagsType] = None) -> None:
        self._send(metric, b"ms", value, self._convert_tags(tags))

    def unexpected_exception(self, ex: BaseException, where: str, tags: Optional[TagsType] = None) -> None:
        all_tags = self._convert_tags(tags)
        all_tags["exception"] = ex.__class__.__name__
        all_tags["where"] = where
        self.increase("exception", tags=all_tags)

    def _send(self, metric: str, metric_type: bytes, value: SupportsFloat, tags: TagsType) -> None:
        if self._dest_addr is None:
            return

        # format: "user.logins,service=payroll,region=us-west:1|c"
        parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
        send_tags = self._tags.copy()
        send_tags.update(tags or {})
        for tag, tag_value in send_tags.items():
            parts.insert(1, ",{}={}".format(tag, tag_value).encode("utf-8"))

        self._socket.sendto(b"".join(parts), self._dest_addr)
