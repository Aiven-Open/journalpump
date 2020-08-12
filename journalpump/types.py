"""JournalPump internal types"""
import sys

if sys.version_info >= (3, 8):
    from typing import Protocol  # pylint: disable=no-name-in-module
else:
    from typing_extensions import Protocol


class GeoIPProtocol(Protocol):
    """
    Provides the subset of functionality we depend on from GeoIP.

    GeoIP is not a required dependency, but to typecheck we want to ensure
    that we don't escalate the methods without necessity.
    """
