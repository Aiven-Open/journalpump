from .data import GCP_PRIVATE_KEY
from googleapiclient.http import RequestMockBuilder as GoogleApiClientRequestMockBuilder
from httplib2 import Response as HttpLib2Response
from journalpump.senders import GoogleCloudLoggingSender
from typing import Dict, List
from unittest import mock

import json


class TestGoogleCloudLoggingSender:

    CONFIG = {
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
            "private_key": GCP_PRIVATE_KEY,
            "client_email": "test@project-id.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40project-id.iam.gserviceaccount.com"  # pylint:disable=line-too-long
        }
    }

    def _generate_request_builder(self, entries: List[Dict[str, str]], error=None) -> GoogleApiClientRequestMockBuilder:
        """Generate typical response body with patched `entries` key"""

        expected_body = {
            "logName": "projects/project-id/logs/log-id",
            "resource": {
                "type": "generic_node",
                "labels": {
                    "location": "us-east-1",
                    "node_id": "my-test-node"
                },
            },
            "entries": entries
        }
        return GoogleApiClientRequestMockBuilder(
            {
                "logging.entries.write": (error, "{}", expected_body),
            },
            check_unexpected=True,
        )

    def test_message_string(self):
        """Check that MESSAGE as plain text was sent"""

        request_builder = self._generate_request_builder([{"jsonPayload": {"MESSAGE": "Hello"}}])

        sender = GoogleCloudLoggingSender(
            name="googlecloudlogging",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config=self.CONFIG,
            googleapiclient_request_builder=request_builder,
        )
        sender.send_messages(messages=[b'{"MESSAGE": "Hello"}'], cursor=None)
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_missing_message(self):
        """Check that missing `MESSAGE` key is fine"""

        request_builder = self._generate_request_builder([{"jsonPayload": {"not_message": "Hello"}}])

        sender = GoogleCloudLoggingSender(
            name="googlecloudlogging",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config=self.CONFIG,
            googleapiclient_request_builder=request_builder,
        )
        sender.send_messages(messages=[b'{"not_message": "Hello"}'], cursor=None)
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_message_json(self):
        """Check that MESSAGE as object was decoded and sent"""

        expected_json_message = {"test_key": "test_value"}
        request_builder = self._generate_request_builder([{"jsonPayload": {"MESSAGE": expected_json_message}}])

        sender = GoogleCloudLoggingSender(
            name="googlecloudlogging",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config=self.CONFIG,
            googleapiclient_request_builder=request_builder,
        )

        message = json.dumps({"MESSAGE": json.dumps(expected_json_message)})
        sender.send_messages(messages=[message.encode()], cursor=None)
        assert sender._sent_count == 1  # pylint: disable=protected-access

    def test_bad_request_did_not_marked_sent(self):
        """Check that message was not marked as sent if GoogleApi returns error"""
        request_builder = self._generate_request_builder(
            [{
                "jsonPayload": {
                    "MESSAGE": "Hello"
                }
            }],
            error=HttpLib2Response({"status": "400"}),
        )

        sender = GoogleCloudLoggingSender(
            name="googlecloudlogging",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config=self.CONFIG,
            googleapiclient_request_builder=request_builder,
        )
        sender.send_messages(messages=[b'{"MESSAGE": "Hello"}'], cursor=None)
        assert sender._sent_count == 0  # pylint: disable=protected-access

    def test_correct_timestamp(self):
        """Check severity mapping is converted correctly and timestamp is being sent."""

        request_builder = self._generate_request_builder([{
            "timestamp": "2020-06-25T06:24:13.787255Z",
            "severity": "EMERGENCY",
            "jsonPayload": {
                "MESSAGE": "Hello"
            }
        }])

        sender = GoogleCloudLoggingSender(
            name="googlecloudlogging",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
            config=self.CONFIG,
            googleapiclient_request_builder=request_builder,
        )
        sender.send_messages(
            messages=[b'{"MESSAGE": "Hello", "PRIORITY": 0, "timestamp": "2020-06-25T06:24:13.787255"}'], cursor=None
        )
        assert sender._sent_count == 1  # pylint: disable=protected-access
