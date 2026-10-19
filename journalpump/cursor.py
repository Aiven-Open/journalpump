# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from journalpump.util import parse_hex


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
    value = raw[start:] if end < 0 else raw[start:end]
    return parse_hex(value)


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
        return parse_hex(self._fields.get(key, ""))

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

    def is_known_not_after(self, other: JournalCursor) -> bool:
        """True if self is known to be at or before other, False if it is newer or the order is unknown

        Only orders that journald guarantees are used: seqnums within one seqnum id, and monotonic time within one boot.
        Realtime is never used because the clock can jump, and boot ids themselves have no order.
        """
        mine, theirs = self.location, other.location
        if mine is None or theirs is None:
            return False
        if mine.seqnum_id == theirs.seqnum_id:
            return mine.seqnum <= theirs.seqnum
        # Equal monotonic times in different sequences are not the same position, so their order is unknown
        return (
            mine.boot_id is not None
            and mine.boot_id == theirs.boot_id
            and mine.monotonic is not None
            and theirs.monotonic is not None
            and mine.monotonic < theirs.monotonic
        )

    def is_known_before(self, other: JournalCursor) -> bool:
        # Comparability is symmetric, so once self <= other is known, a False reverse check means "after", not "unknown"
        return self.is_known_not_after(other) and not other.is_known_not_after(self)


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
    cursors: list[tuple[str, JournalCursor]] = []
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
        cursors.append((sender_name, cursor))
    if not cursors:
        return ResumeChoice(cursor=reader_cursor, reason=f"reader cursor from {state_source}")

    # Known orders agree with the real journal order, so if every cursor that no other cursor is known to precede is
    # at one position, that position precedes all cursors, through a chain of known orders where needed
    candidates = [
        (name, cursor) for name, cursor in cursors if not any(other.is_known_before(cursor) for _, other in cursors)
    ]
    # No candidate is known to be before another, so a candidate known not to be after the first one is at its position
    if candidates and all(cursor.is_known_not_after(candidates[0][1]) for _, cursor in candidates):
        name, cursor = candidates[0]
        return ResumeChoice(cursor=cursor.raw, reason=f"sender {name!r} sent cursor from {state_source}")

    # Never guess the order: resuming from the wrong cursor would skip entries a slower sender still needs.
    # No candidates at all means the known orders contradict each other, e.g. a corrupted state file.
    unordered = ", ".join(f"{name!r} ({cursor.raw!r})" for name, cursor in candidates or cursors)
    warning = (
        f"Sender cursors for reader {reader_name!r} cannot be ordered against each other: {unordered}, "
        "needs full sync from beginning"
    )
    return ResumeChoice(
        cursor=None,
        reason=f"full sync, sender cursors cannot be ordered ({state_source})",
        warnings=(warning,),
    )
