from journalpump.senders import ElasticsearchSender
from unittest import mock

import responses


@responses.activate
def test_es_sender():
    url = "http://localhost:1234"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url + "/_aliases", json={})
        rsps.add(responses.POST, url + "/journalpump-2019-10-07/_bulk")
        es = ElasticsearchSender(
            name="es", reader=mock.Mock(), stats=mock.Mock(), field_filter=None, config={"elasticsearch_url": url}
        )
        assert es.send_messages(messages=[b'{"timestamp": "2019-10-07 14:00:00"}'], cursor=None)
