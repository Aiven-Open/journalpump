from googleapiclient.http import RequestMockBuilder as GoogleApiClientRequestMockBuilder
from httplib2 import Response as HttpLib2Response
from journalpump.senders import GoogleCloudLoggingSender
from test.journalpump.test_journalpump import _PRIVATE_KEY
from unittest import mock


def test_google_cloud_logging_sender():
    config = {
        "google_cloud_logging_project_id": "project-id",
        "google_cloud_logging_log_id": "log-id",
        "google_cloud_logging_resource_labels": {
            "location": "us-east-1",
            "node_id": "my-test-node",
        },
        "google_service_account_credentials": {
            "type": "service_account",
            "project_id": "project-id",
            "private_key_id": "abcdefg",
            "private_key": _PRIVATE_KEY,
            "client_email": "test@project-id.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40project-id.iam.gserviceaccount.com"  # pylint: disable=line-too-long
        }
    }

    expected_body = {
        "logName": "projects/project-id/logs/log-id",
        "resource": {
            "type": "generic_node",
            "labels": {
                "location": "us-east-1",
                "node_id": "my-test-node"
            },
        },
        "entries": [{
            "jsonPayload": {
                "message": "Hello"
            }
        }]
    }
    requestBuilder = GoogleApiClientRequestMockBuilder(
        {
            "logging.entries.write": (None, "{}", expected_body),
        },
        check_unexpected=True,
    )

    sender = GoogleCloudLoggingSender(
        name="googlecloudlogging",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
        config=config,
        googleapiclient_request_builder=requestBuilder
    )
    sender.send_messages(messages=[b'{"message": "Hello"}'], cursor=None)
    assert sender._sent_count == 1  # pylint: disable=protected-access

    requestBuilder = GoogleApiClientRequestMockBuilder(
        {
            "logging.entries.write": (HttpLib2Response({"status": "400"}), b"", expected_body),
        },
        check_unexpected=True,
    )

    sender = GoogleCloudLoggingSender(
        name="googlecloudlogging",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
        config=config,
        googleapiclient_request_builder=requestBuilder
    )
    sender.send_messages(messages=[b'{"message": "Hello"}'], cursor=None)
    assert sender._sent_count == 0  # pylint: disable=protected-access

    expected_body = {
        "logName": "projects/project-id/logs/log-id",
        "resource": {
            "type": "generic_node",
            "labels": {
                "location": "us-east-1",
                "node_id": "my-test-node"
            },
        },
        "entries": [{
            "timestamp": "2020-06-25T06:24:13.787255Z",
            "severity": "EMERGENCY",
            "jsonPayload": {
                "message": "Hello"
            }
        }]
    }
    requestBuilder = GoogleApiClientRequestMockBuilder(
        {
            "logging.entries.write": (None, "{}", expected_body),
        },
        check_unexpected=True,
    )

    sender = GoogleCloudLoggingSender(
        name="googlecloudlogging",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
        config=config,
        googleapiclient_request_builder=requestBuilder
    )
    sender.send_messages(
        messages=[b'{"message": "Hello", "PRIORITY": 0, "timestamp": "2020-06-25T06:24:13.787255"}'], cursor=None
    )
    assert sender._sent_count == 1  # pylint: disable=protected-access
