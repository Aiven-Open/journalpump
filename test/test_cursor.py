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
    "first,second,forward,backward",
    [
        (make_cursor(i=9), make_cursor(i=0xA), True, False),
        (make_cursor(i=0xA), make_cursor(i=0x10), True, False),
        (make_cursor(i=0x10), make_cursor(i=0xA), False, True),
        (make_cursor(s=1, m=6), make_cursor(s=2, m=5), False, True),  # different sequences, same boot: newer first
        # same sequence: seqnum wins over monotonic time
        (make_cursor(i=1, m=9, t=9), make_cursor(i=2, m=1, t=1), True, False),
        (make_cursor(i=1, b=1, m=9), make_cursor(i=2, b=2, m=1), True, False),  # same sequence: seqnum wins over boot id
        # different sequences, same boot: monotonic time
        (make_cursor(s=1, m=5, t=9), make_cursor(s=2, m=6, t=1), True, False),
        (make_cursor(i=3), make_cursor(i=3, m=2, t=2), True, True),  # same sequence and seqnum: other fields are ignored
        # different boots, realtime is not trusted
        (make_cursor(s=1, b=1, t=1), make_cursor(s=2, b=2, t=2), False, False),
        (make_cursor(s=1, m=5), make_cursor(s=2, m=5), False, False),  # same boot, equal monotonic
        (make_cursor(s=1, b=1, m=9), make_cursor(s=2, b=2, m=1), False, False),
        ("opaque", make_cursor(), False, False),
        ("opaque", "opaque", False, False),
        ("s=1;i=1;m=1", "s=2;i=1;m=2", False, False),  # no boot id: partial cursor, only possible via a state file
    ],
)
def test_journal_cursor_order(first: str, second: str, forward: bool, backward: bool) -> None:
    first_cursor, second_cursor = JournalCursor(first), JournalCursor(second)

    assert first_cursor.is_known_not_after(second_cursor) is forward
    assert second_cursor.is_known_not_after(first_cursor) is backward
    assert first_cursor.is_known_before(second_cursor) is (forward and not backward)
    assert second_cursor.is_known_before(first_cursor) is (backward and not forward)


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
        warnings=("Sender 'b' for reader 'r' needs full sync from initial position",),
    )


def test_pick_resume_cursor_nothing_read_yet_starts_quietly() -> None:
    choice = pick_resume_cursor([("a", None), ("b", None)], None, reader_name="r", state_source="state file")

    assert choice == ResumeChoice(cursor=None, reason="no position yet, starting from initial position (state file)")


@pytest.mark.parametrize(
    "sender_cursors,reader_cursor",
    [
        ([("a", None)], "reader_cursor"),
        ([("a", make_cursor()), ("b", None)], None),
    ],
)
def test_pick_resume_cursor_full_sync_warns_when_position_known(
    sender_cursors: list[tuple[str, str | None]], reader_cursor: str | None
) -> None:
    choice = pick_resume_cursor(sender_cursors, reader_cursor, reader_name="r", state_source="state file")

    assert choice.cursor is None
    assert choice.warnings


def test_pick_resume_cursor_unparseable_cursor() -> None:
    cursor = make_cursor()

    assert _pick([("a", cursor), ("b", "opaque"), ("c", cursor)]) == ResumeChoice(
        cursor=None,
        reason="full sync, sender 'b' has unparseable cursor (state file)",
        warnings=("Sender 'b' for reader 'r' has unparseable cursor 'opaque', needs full sync from initial position",),
    )


def test_pick_resume_cursor_no_warning_when_only_boot_differs() -> None:
    first_boot, second_boot = make_cursor(b=1, i=1), make_cursor(b=2, i=2)

    choice = _pick([("a", first_boot), ("b", second_boot)])

    assert not choice.warnings


def test_pick_resume_cursor_different_sequences_unorderable_means_full_sync() -> None:
    first, second = make_cursor(s=1, b=1), make_cursor(s=2, b=2)

    choice = _pick([("a", first), ("b", second)])

    assert choice.cursor is None
    assert choice.reason == "full sync, sender cursors cannot be ordered (state file)"
    assert choice.warnings == (
        (
            f"Sender cursors for reader 'r' cannot be ordered against each other: 'a' ({first!r}), 'b' ({second!r}), "
            "needs full sync from initial position"
        ),
    )


def test_pick_resume_cursor_equal_cursors_names_first_sender() -> None:
    cursor = make_cursor(i=1)

    assert _pick([("a", cursor), ("b", make_cursor(i=1, m=2)), ("c", make_cursor(i=2))]) == ResumeChoice(
        cursor=cursor, reason="sender 'a' sent cursor from state file"
    )


def test_pick_resume_cursor_different_sequences_same_boot_picks_oldest() -> None:
    old, new = make_cursor(s=1, i=5, m=1), make_cursor(s=2, i=1, m=2)

    assert _pick([("a", new), ("b", old)]).cursor == old
    assert _pick([("a", old), ("b", new)]).cursor == old


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_pick_resume_cursor_resolves_chain_regardless_of_order(order: tuple[int, ...]) -> None:
    # c < a by monotonic time (same boot), a < b by seqnum (same sequence); b and c alone cannot be ordered
    cursors = [make_cursor(s=1, i=1, b=1, m=5), make_cursor(s=1, i=2, b=2, m=1), make_cursor(s=2, i=1, b=1, m=3)]

    choice = _pick([(f"sender{index}", cursors[index]) for index in order])

    assert choice == ResumeChoice(cursor=cursors[2], reason="sender 'sender2' sent cursor from state file")


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_pick_resume_cursor_unorderable_oldest_cursors_mean_full_sync(order: tuple[int, ...]) -> None:
    # x < y by seqnum; z cannot be ordered against either, so x and z are both possibly the oldest
    named = [("x", make_cursor(s=1, i=1, b=1)), ("y", make_cursor(s=1, i=2, b=1)), ("z", make_cursor(s=2, i=1, b=2))]

    choice = _pick([named[index] for index in order])

    assert choice.cursor is None
    assert choice.reason == "full sync, sender cursors cannot be ordered (state file)"
    assert len(choice.warnings) == 1
    assert "'x'" in choice.warnings[0] and "'z'" in choice.warnings[0]
    assert "'y'" not in choice.warnings[0]


def test_pick_resume_cursor_contradicting_orders_mean_full_sync() -> None:
    # p < q by seqnum, but q < r < p by monotonic time: only possible with a corrupted state file
    p, q, r = make_cursor(s=1, i=1, m=9), make_cursor(s=1, i=2, m=1), make_cursor(s=2, i=1, m=5)

    choice = _pick([("p", p), ("q", q), ("r", r)])

    assert choice.cursor is None
    assert len(choice.warnings) == 1
    assert all(f"'{name}'" in choice.warnings[0] for name in "pqr")
