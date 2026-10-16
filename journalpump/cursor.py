# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property


def parse_seqnum(raw: str) -> int | None:
    """Return the seqnum ("i=" field) of a raw journald cursor, or None if it is missing or invalid"""
    # Hot path (called per journal entry): scan for the "i=" field without building a cursor object or field dict
    if raw.startswith("i="):
        start = 2
    else:
        start = raw.find(";i=")
        if start < 0:
            return None
        start += 3
    end = raw.find(";", start)
    try:
        return int(raw[start:] if end < 0 else raw[start:end], 16)
    except ValueError:
        return None


@dataclass(frozen=True, slots=True)
class _Location:
    seqnum_id: int
    seqnum: int
    boot_id: int | None
    monotonic: int | None
    realtime: int | None


@dataclass(frozen=True)
class JournalCursor:
    """A journald cursor: s=<seqnum id>;i=<seqnum>;b=<boot id>;m=<monotonic>;t=<realtime>;x=...

    The raw string is parsed lazily and at most once per field set, and only when a property is accessed.
    """

    raw: str

    @cached_property
    def _fields(self) -> dict[str, str]:
        fields: dict[str, str] = {}
        for part in self.raw.split(";"):
            key, _, value = part.partition("=")
            fields.setdefault(key, value)  # first occurrence wins, same as `parse_seqnum`
        return fields

    def _hex_field(self, key: str) -> int | None:
        try:
            return int(self._fields[key], 16)
        except (KeyError, ValueError):
            return None

    @cached_property
    def location(self) -> _Location | None:
        # Cold path (resume/reload only), unlike the per-entry `parse_seqnum`, so full parsing is fine here
        seqnum_id, seqnum = self._hex_field("s"), self._hex_field("i")
        if seqnum_id is None or seqnum is None:
            return None
        return _Location(
            seqnum_id=seqnum_id,
            seqnum=seqnum,
            boot_id=self._hex_field("b"),
            monotonic=self._hex_field("m"),
            realtime=self._hex_field("t"),
        )

    @property
    def is_parseable(self) -> bool:
        return self.location is not None

    def is_same_journal_file(self, other: JournalCursor) -> bool:
        mine, theirs = self.location, other.location
        return mine is not None and theirs is not None and mine.seqnum_id == theirs.seqnum_id

    def is_older_than(self, other: JournalCursor) -> bool:
        mine, theirs = self.location, other.location
        if mine is None or theirs is None:
            return mine is None and theirs is not None
        if mine.seqnum_id == theirs.seqnum_id:
            return mine.seqnum < theirs.seqnum
        if (
            mine.boot_id is not None
            and mine.boot_id == theirs.boot_id
            and mine.monotonic is not None
            and theirs.monotonic is not None
            and mine.monotonic != theirs.monotonic
        ):
            return mine.monotonic < theirs.monotonic
        if mine.realtime is None or theirs.realtime is None:
            return False
        return mine.realtime < theirs.realtime


@dataclass(frozen=True, kw_only=True, slots=True)
class ResumeChoice:
    """Where to resume reading: `cursor` is None to start from the reader's initial position"""

    cursor: str | None
    reason: str
    warnings: tuple[str, ...] = ()


def pick_resume_cursor(
    sender_cursors: Sequence[tuple[str, str | None]],
    reader_cursor: str | None,
    *,
    reader_name: str,
    state_source: str,
) -> ResumeChoice:
    choice = ResumeChoice(cursor=reader_cursor, reason=f"reader cursor from {state_source}")
    oldest: JournalCursor | None = None
    for sender_name, sender_cursor in sender_cursors:
        if sender_cursor is None:
            return ResumeChoice(
                cursor=None,
                reason=f"full sync, sender {sender_name!r} has no confirmed delivery ({state_source})",
                warnings=(f"Sender {sender_name!r} for reader {reader_name!r} needs full sync from beginning",),
            )
        cursor = JournalCursor(sender_cursor)
        if not cursor.is_parseable:
            warning = (
                f"Sender {sender_name!r} for reader {reader_name!r} has unparseable cursor {sender_cursor!r}, "
                "needs full sync from beginning"
            )
            return ResumeChoice(
                cursor=None,
                reason=f"full sync, sender {sender_name!r} has unparseable cursor ({state_source})",
                warnings=(warning,),
            )
        if oldest is not None and not cursor.is_same_journal_file(oldest):
            # Cursors from different journal sequences have no reliable order, so do not risk skipping entries
            warning = (
                f"Sender {sender_name!r} for reader {reader_name!r} cursor {sender_cursor!r} is from a different "
                f"journal file than {oldest.raw!r}, needs full sync from beginning"
            )
            return ResumeChoice(
                cursor=None,
                reason=f"full sync, sender cursors are from different journal files ({state_source})",
                warnings=(warning,),
            )
        if oldest is None or cursor.is_older_than(oldest):
            oldest = cursor
            choice = ResumeChoice(cursor=sender_cursor, reason=f"sender {sender_name!r} sent cursor from {state_source}")
    return choice
