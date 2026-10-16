# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

from .util import make_cursor
from journalpump.cursor import JournalCursor, parse_seqnum, pick_resume_cursor, ResumeChoice

import itertools
import pytest


@pytest.mark.parametrize(
    "cursor,expected",
    [
        ("s=a1;i=1f;b=b2;m=c3;t=d4;x=e5", 0x1F),
        ("s=a1;i=1f", 0x1F),
        ("s=a1;i=1;i=2", 1),
        ("i=1f;s=a1", 0x1F),
        ("s=a1;b=b2;m=c3;t=d4", None),
        ("s=a1;i=;b=b2", None),
        ("s=a1;i=zz;b=b2", None),
        ("opaque", None),
    ],
)
def test_parse_seqnum(cursor: str, expected: int | None) -> None:
    assert parse_seqnum(cursor) == expected


def test_journal_cursor_location_first_duplicate_wins() -> None:
    location = JournalCursor("s=a1;i=1;i=2;b=b2;b=b3").location
    assert location is not None
    assert (location.seqnum, location.boot_id) == (1, 0xB2)


@pytest.mark.parametrize(
    "cursor,expected",
    [
        ("s=a1;i=1f;b=b2;m=c3;t=d4;x=e5", True),
        ("s=a1;i=1f;b=b2;m=c3;t=d4", True),
        ("s=a1;i=1f;b=b2;m=c3;t=d4;x=zz;future=field", True),
        ("", False),
        ("opaque", False),
        ("s=1;i=1;b=1;m=1", True),
        ("s=1;i=1", True),
        ("s=1;b=1;m=1;t=1", False),
        ("s=1;i=zz;b=1;m=1;t=1", False),
    ],
)
def test_journal_cursor_is_parseable(cursor: str, expected: bool) -> None:
    assert JournalCursor(cursor).is_parseable is expected


@pytest.mark.parametrize(
    "older,newer",
    [
        (make_cursor(i=9), make_cursor(i=0xA)),
        (make_cursor(i=0xA), make_cursor(i=0x10)),
        (make_cursor(i=1, m=9, t=9), make_cursor(i=2, m=1, t=1)),
        (make_cursor(s=1, m=5, t=9), make_cursor(s=2, m=6, t=1)),
        (make_cursor(s=1, b=1, t=1), make_cursor(s=2, b=2, t=2)),
        ("opaque", make_cursor()),
        ("s=1;i=1;b=1;m=1", "s=1;i=2;b=1;m=1"),
        ("s=1;i=1;b=1;t=1", "s=1;i=2;b=1;t=1"),
    ],
)
def test_journal_cursor_is_older_than(older: str, newer: str) -> None:
    result_forward = JournalCursor(older).is_older_than(JournalCursor(newer))
    result_backward = JournalCursor(newer).is_older_than(JournalCursor(older))

    assert result_forward is True
    assert result_backward is False


@pytest.mark.parametrize(
    "first,second",
    [
        (make_cursor(i=3),) * 2,
        (make_cursor(i=5, m=1, t=9), make_cursor(i=5, m=2, t=1)),
        (make_cursor(i=5, m=5, t=1), make_cursor(i=5, m=5, t=2)),
        ("opaque", "other"),
        ("opaque", "opaque"),
        ("s=1;i=1;m=1", "s=2;i=1;m=2"),
    ],
)
def test_journal_cursor_is_older_than_neither(first: str, second: str) -> None:
    assert JournalCursor(first).is_older_than(JournalCursor(second)) is False
    assert JournalCursor(second).is_older_than(JournalCursor(first)) is False


@pytest.mark.parametrize(
    "first,second,expected",
    [
        (make_cursor(s=1, i=1, b=1), make_cursor(s=1, i=2, b=2), True),
        (make_cursor(s=1), make_cursor(s=2), False),
        ("opaque", make_cursor(), False),
        ("opaque", "opaque", False),
    ],
)
def test_journal_cursor_is_same_journal_file(first: str, second: str, expected: bool) -> None:
    assert JournalCursor(first).is_same_journal_file(JournalCursor(second)) is expected
    assert JournalCursor(second).is_same_journal_file(JournalCursor(first)) is expected


def _pick(sender_cursors: list[tuple[str, str | None]]) -> ResumeChoice:
    return pick_resume_cursor(sender_cursors, "reader_cursor", reader_name="r", state_source="state file")


def test_pick_resume_cursor_without_senders_uses_reader_cursor() -> None:
    assert _pick([]) == ResumeChoice(cursor="reader_cursor", reason="reader cursor from state file")


def test_pick_resume_cursor_picks_oldest_regardless_of_order() -> None:
    old, new = make_cursor(i=1), make_cursor(i=2)

    assert _pick([("a", old), ("b", new)]).cursor == old
    assert _pick([("a", new), ("b", old)]) == ResumeChoice(cursor=old, reason="sender 'b' sent cursor from state file")


def test_pick_resume_cursor_no_confirmed_delivery() -> None:
    cursor = make_cursor()

    assert _pick([("a", cursor), ("b", None), ("c", cursor)]) == ResumeChoice(
        cursor=None,
        reason="full sync, sender 'b' has no confirmed delivery (state file)",
        warnings=("Sender 'b' for reader 'r' needs full sync from beginning",),
    )


def test_pick_resume_cursor_unparseable_cursor() -> None:
    cursor = make_cursor()

    assert _pick([("a", cursor), ("b", "opaque"), ("c", cursor)]) == ResumeChoice(
        cursor=None,
        reason="full sync, sender 'b' has unparseable cursor (state file)",
        warnings=("Sender 'b' for reader 'r' has unparseable cursor 'opaque', needs full sync from beginning",),
    )


def test_pick_resume_cursor_no_warning_when_only_boot_differs() -> None:
    first_boot, second_boot = make_cursor(b=1, i=1), make_cursor(b=2, i=2)

    choice = _pick([("a", first_boot), ("b", second_boot)])

    assert not choice.warnings


def test_pick_resume_cursor_different_journal_files_means_full_sync() -> None:
    first_file, second_file = make_cursor(s=1), make_cursor(s=2)

    choice = _pick([("a", first_file), ("b", second_file)])

    assert choice.cursor is None
    assert choice.reason == "full sync, sender cursors are from different journal files (state file)"
    assert len(choice.warnings) == 1
    assert "Sender 'b'" in choice.warnings[0]


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_pick_resume_cursor_different_journal_files_independent_of_order(order: tuple[int, ...]) -> None:
    # Realtime order contradicts seqnum order, so pairwise comparison is not transitive
    cursors = [make_cursor(s=1, i=1, b=1, t=0x30), make_cursor(s=1, i=2, b=2, t=0x10), make_cursor(s=2, i=1, b=3, t=0x20)]

    assert _pick([(f"sender{index}", cursors[index]) for index in order]).cursor is None
