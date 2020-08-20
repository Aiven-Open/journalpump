from .base import LogSender
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleApiClientError
from oauth2client.service_account import ServiceAccountCredentials
from typing import Any, Dict, List, Optional

import json
import logging

logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)


class GoogleCloudLoggingSender(LogSender):
    _SEVERITY_MAPPING = {  # mapping from journald priority to cloud logging severity
        7: "DEBUG",
        6: "INFO",
        5: "NOTICE",
        4: "WARNING",
        3: "ERROR",
        2: "CRITICAL",
        1: "ALERT",
        0: "EMERGENCY",
    }

    def __init__(self, *, config, googleapiclient_request_builder=None, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        credentials = None
        google_service_account = config.get("google_service_account_credentials")
        self.project_id = config["google_cloud_logging_project_id"]
        self.log_id = config["google_cloud_logging_log_id"]
        self.resource_labels: Optional[Dict[str, str]] = config.get("google_cloud_logging_resource_labels", None)
        if google_service_account:
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(google_service_account)
            self.project_id = google_service_account["project_id"]
        else:
            credentials = ServiceAccountCredentials.get_application_default()

        if googleapiclient_request_builder is not None:
            self._logs = build("logging", "v2", credentials=credentials, requestBuilder=googleapiclient_request_builder)
        else:
            self._logs = build("logging", "v2", credentials=credentials)
        self.mark_connected()

    def send_messages(self, *, messages, cursor):
        entries: List[Dict[str, Any]] = []
        for message in messages:
            msg_str = message.decode("utf8")
            msg = json.loads(msg_str)
            timestamp = msg.pop("timestamp", None)
            journald_priority = msg.pop("PRIORITY", None)
            entry = {
                "jsonPayload": msg,
            }
            if timestamp is not None:
                entry["timestamp"] = timestamp[:26] + "Z"  # assume timestamp to be UTC
            if journald_priority is not None:
                severity = GoogleCloudLoggingSender._SEVERITY_MAPPING.get(journald_priority, "DEFAULT")
                entry["severity"] = severity
            entries.append(entry)

        body = {
            "logName": "projects/%s/logs/%s" % (self.project_id, self.log_id),
            "resource": {
                "type": "generic_node",
                "labels": self.resource_labels
            } if self.resource_labels is not None else {
                "type": "generic_node"
            },
            "entries": entries
        }

        try:
            self._logs.entries().write(body=body).execute()  # pylint: disable=no-member
            self.mark_sent(messages=messages, cursor=cursor)
            return True
        except GoogleApiClientError as err:
            self.mark_disconnected(err)
            self.log.exception("Client error during send to Google Cloud Logging")
            self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self.mark_connected()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to Google Cloud Logging")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self.mark_connected()
        return False
