from ..statsd import StatsClient, TagsType
from .base import _LogSenderReader, JournalCursor, LogSender
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleApiClientError
from oauth2client.service_account import ServiceAccountCredentials
from typing import Any, Dict, List, Mapping, Optional, Sequence

import json
import logging
import sys

if sys.version_info >= (3, 8):
    from typing import Literal, TypedDict  # pylint:disable=no-name-in-module
else:
    from typing_extensions import Literal, TypedDict

logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)

_SeverityTypes = Literal["DEFAULT", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY"]


class _Entry(TypedDict, total=False):
    jsonPayload: Dict[str, Any]
    timestamp: str
    severity: _SeverityTypes


class _ResourceBody(TypedDict, total=False):
    type: Literal["generic_node"]
    labels: List[str]


class _LogBody(TypedDict):
    logName: str
    resource: _ResourceBody
    entries: List[_Entry]


class GoogleCloudLoggingSender(LogSender):
    @staticmethod
    def _get_google_severity(syslog_severity: int) -> _SeverityTypes:  # pylint: disable=too-many-return-statements
        if syslog_severity == 7:
            return "DEBUG"
        if syslog_severity == 6:
            return "INFO"
        if syslog_severity == 5:
            return "NOTICE"
        if syslog_severity == 4:
            return "WARNING"
        if syslog_severity == 3:
            return "ERROR"
        if syslog_severity == 2:
            return "CRITICAL"
        if syslog_severity == 1:
            return "ALERT"
        if syslog_severity == 0:
            return "EMERGENCY"

        return "DEFAULT"

    def __init__(
        self,
        *,
        googleapiclient_request_builder: Optional[Any] = None,
        name: str,
        reader: _LogSenderReader,
        config: Mapping[str, Any],
        field_filter: Mapping[str, Any],
        stats: StatsClient,
        max_send_interval: Optional[float],
        extra_field_values: Optional[Mapping[str, str]] = None,
        tags: Optional[TagsType] = None,
        msg_buffer_max_length: int = 50000
    ):
        super().__init__(
            name=name,
            reader=reader,
            config=config,
            field_filter=field_filter,
            stats=stats,
            max_send_interval=max_send_interval
            if max_send_interval is not None else float(config.get("max_send_interval", 0.3)),
            extra_field_values=extra_field_values,
            tags=tags,
            msg_buffer_max_length=msg_buffer_max_length
        )

        self.project_id: str = config["google_cloud_logging_project_id"]
        self.log_id: str = config["google_cloud_logging_log_id"]

        self._resource_labels: List[str] = []
        if "google_cloud_logging_resource_labels" in config:
            labels = config["google_cloud_logging_resource_labels"]
            if hasattr(labels, "__iter__") and not isinstance(labels, str):
                self._resource_labels = [str(e) for e in labels]

        if "google_service_account_credentials" in config:
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(config["google_service_account_credentials"])
            self.project_id = config["google_service_account_credentials"]["project_id"]
        else:
            credentials = ServiceAccountCredentials.get_application_default()

        self._logs = build(
            "logging", "v2", credentials=credentials, requestBuilder=googleapiclient_request_builder
        ) if googleapiclient_request_builder is not None else build("logging", "v2", credentials=credentials)
        self.mark_connected()

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        body: _LogBody = {
            "logName": "projects/%s/logs/%s" % (self.project_id, self.log_id),
            "resource": {
                "type": "generic_node",
            },
            "entries": []
        }

        if len(self._resource_labels) > 0:
            body["resource"]["labels"] = self._resource_labels[:]

        for message in messages:
            msg_str = message.decode("utf8")
            msg = json.loads(msg_str)
            timestamp = msg.pop("timestamp", None)
            journald_priority = msg.pop("PRIORITY", None)
            entry: _Entry = {
                "jsonPayload": msg,
            }
            if timestamp is not None:
                entry["timestamp"] = timestamp[:26] + "Z"  # assume timestamp to be UTC
            if journald_priority is not None:
                entry["severity"] = self._get_google_severity(journald_priority)
            body["entries"].append(entry)

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
