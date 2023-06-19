from journalpump import statsd
from journalpump.journalpump import JournalReader
from unittest.mock import Mock


def test_persistent_gauge() -> None:
    stats = Mock(spec=statsd.StatsClient)
    reader = JournalReader(
        name="foo",
        config={},
        field_filters={},
        geoip=None,
        stats=stats,
        searches=[],
    )

    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_not_called()  # Don't emit the initial successful 0 value
    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_not_called()  # Don't duplicate emit
    reader.set_persistent_gauge(metric="test", value=1, tags={})
    stats.gauge.assert_not_called()  # Don't emit non-zero values, those are batched and processed separately
    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_called_once()  # Emit now that we are going from non-zero back to zero
    stats.gauge.reset_mock()
    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_not_called()  # Don't duplicate emit
