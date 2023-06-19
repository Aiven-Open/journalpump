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
    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_called_once()
    stats.reset_mock()
    reader.set_persistent_gauge(metric="test", value=1, tags={})
    reader.set_persistent_gauge(metric="test", value=2, tags={})
    stats.gauge.assert_not_called()
    reader.set_persistent_gauge(metric="test", value=0, tags={})
    stats.gauge.assert_called_once()
