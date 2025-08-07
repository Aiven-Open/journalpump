"""JournalPump internal types"""

from typing import Protocol

import enum


class GeoIPProtocol(Protocol):
    """
    Provides the subset of functionality we depend on from GeoIP.

    GeoIP is not a required dependency, but to typecheck we want to ensure
    that we don't escalate the methods without necessity.
    """


class StrEnum(str, enum.Enum):
    def __str__(self):
        return str(self.value)


LOG_SEVERITY_MAPPING = {
    "EMERGENCY": 0,
    "ALERT": 1,
    "CRITICAL": 2,
    "ERROR": 3,
    "WARNING": 4,
    "NOTICE": 5,
    "INFO": 6,
    "DEBUG": 7,
}
