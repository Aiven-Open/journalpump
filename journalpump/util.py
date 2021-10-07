# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

import contextlib
import datetime
import os
import random
import requests
import tempfile


@contextlib.contextmanager
def atomic_replace_file(file_path):
    """Open a temporary file for writing, rename to final name when done"""
    fd, tmp_file_path = tempfile.mkstemp(prefix=os.path.basename(file_path), dir=os.path.dirname(file_path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as out_file:
            yield out_file
        os.replace(tmp_file_path, file_path)
    except Exception:  # pytest: disable=broad-except
        with contextlib.suppress(Exception):
            os.unlink(tmp_file_path)
        raise


class TimeoutAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, *args, timeout=None, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, *args, **kwargs):  # pylint: disable=arguments-differ,signature-differs
        if not kwargs.get("timeout"):
            kwargs["timeout"] = self.timeout
        return super().send(*args, **kwargs)


class ExponentialBackoff:
    """
    An implementation of the exponential backoff algorithm with full jitter described at
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """

    def __init__(self, base: float, factor: float, maximum: float, jitter: bool):
        if base <= 0:
            raise ValueError("base must be positive")
        if factor <= 0:
            raise ValueError("factor must be positive")
        if maximum < base:
            raise ValueError("maximum must be greater than or equal to base")
        self._base = base
        self._factor = factor
        self._maximum = maximum
        self._jitter = jitter
        self._attempts = 0

    def next_sleep(self) -> float:
        result = self._base * (self._factor ** self._attempts)
        if result <= self._maximum:
            self._attempts += 1
        else:
            result = self._maximum

        if self._jitter:
            return random.uniform(0, result)

        return result

    def reset(self) -> None:
        self._attempts = 0


def get_requests_session(*, timeout=60):
    request_session = requests.Session()
    adapter_args = {"timeout": timeout}
    adapter = TimeoutAdapter(**adapter_args)
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)
    return request_session


def default_json_serialization(obj):  # pylint: disable=inconsistent-return-statements
    if isinstance(obj, bytes):
        return obj.decode("utf8")
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
