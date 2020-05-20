# Copyright 2019, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

import contextlib
import os
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
        super(TimeoutAdapter, self).__init__(*args, **kwargs)

    def send(self, *args, **kwargs):  # pylint: disable=arguments-differ,signature-differs
        if not kwargs.get("timeout"):
            kwargs["timeout"] = self.timeout
        return super(TimeoutAdapter, self).send(*args, **kwargs)


def get_requests_session(*, timeout=60):
    request_session = requests.Session()
    adapter_args = {"timeout": timeout}
    adapter = TimeoutAdapter(**adapter_args)
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)
    return request_session
