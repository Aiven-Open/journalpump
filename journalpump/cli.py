# Copyright 2015, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .journalpump import JournalPump

import sys


def main() -> int | None:
    return JournalPump.main(sys.argv[1:])
