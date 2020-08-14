# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from typing import Any, AnyStr, Container, IO, Iterator, Mapping, Optional, Text, Tuple, Union

import contextlib
import datetime
import os
import requests
import requests.adapters
import tempfile
import urllib3


@contextlib.contextmanager
def atomic_replace_file(file_path: AnyStr) -> Iterator[IO[Any]]:
    """Open a temporary file for writing, rename to final name when done"""
    fd, tmp_file_path = tempfile.mkstemp(
        prefix=os.path.basename(file_path),
        dir=os.path.dirname(file_path),
        suffix=b".tmp" if isinstance(file_path, bytes) else ".tmp"
    )

    try:
        with os.fdopen(fd, "w") as out_file:
            yield out_file
        os.replace(tmp_file_path, file_path)
    except Exception:  # pytest: disable=broad-except
        with contextlib.suppress(Exception):
            os.unlink(tmp_file_path)
        raise


RequestsTimeoutType = Union[float, Tuple[float, float], urllib3.Timeout]


class TimeoutAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, *args: Any, timeout: Optional[RequestsTimeoutType] = None, **kwargs: Any) -> None:
        self.timeout: Optional[RequestsTimeoutType] = timeout
        super(TimeoutAdapter, self).__init__(*args, **kwargs)

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: Union[None, float, Tuple[float, float], Tuple[float, None]] = None,
        verify: Union[bool, str] = True,
        cert: Union[None, Union[bytes, Text], Container[Union[bytes, Text]]] = None,
        proxies: Optional[Mapping[str, str]] = None
    ) -> requests.Response:

        if timeout is None:
            timeout = self.timeout

        return super().send(request, stream, timeout, verify, cert, proxies)


def get_requests_session(*, timeout: float = 60.0) -> requests.Session:
    request_session = requests.Session()
    adapter = TimeoutAdapter(timeout=timeout)
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)
    return request_session


def default_json_serialization(obj: Any) -> str:
    if isinstance(obj, bytes):
        return obj.decode("utf8")
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return repr(obj)
