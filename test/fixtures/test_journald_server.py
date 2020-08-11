from test.conftest import StandaloneJournalD


def test_journald_server_is_usable(request, journald_server: StandaloneJournalD):
    assert journald_server is not None

    test_log = f"Test log from test {request.node.name}"

    journald_server.send_log(test_log)

    logs = list(journald_server.get_logs())

    assert len(logs) > 0
    assert test_log in set(logs)
