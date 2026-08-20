"""JournalPump internal types"""

from typing import Protocol

import enum


class GeoIPLocation(Protocol):
    @property
    def latitude(self) -> float | None: ...

    @property
    def longitude(self) -> float | None: ...


class GeoIPCity(Protocol):
    @property
    def location(self) -> GeoIPLocation: ...


class GeoIPProtocol(Protocol):
    """City-level GeoIP lookup from a database file."""

    def __init__(self, filename: str) -> None: ...

    def city(self, ip_address: str) -> GeoIPCity | None: ...


class StrEnum(str, enum.Enum):
    def __str__(self) -> str:
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
