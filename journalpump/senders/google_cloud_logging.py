from .base import LogSender
from google.auth import default as get_application_default
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleApiClientError

import contextlib
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

    # A bit on the safe side, not exactly 256KB but this
    # is an approximation anyway
    # according to https://cloud.google.com/logging/quotas
    _LOG_ENTRY_QUOTA = 250 * 1024

    # Somewhat arbitrary maximum message size choosen, this gives a 56K
    # headroom for the other fields in the LogEntry
    _MAX_MESSAGE_SIZE = 200 * 1024

    def __init__(self, *, config, googleapiclient_request_builder=None, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        credentials = None
        google_service_account = config.get("google_service_account_credentials")
        self.project_id = config["google_cloud_logging_project_id"]
        self.log_id = config["google_cloud_logging_log_id"]
        self.resource_labels = config.get("google_cloud_logging_resource_labels", None)
        if google_service_account:
            credentials = Credentials.from_service_account_info(google_service_account)
            self.project_id = credentials.project_id
        else:
            credentials = get_application_default()

        if googleapiclient_request_builder is not None:
            self._logs = build(
                "logging",
                "v2",
                credentials=credentials,
                requestBuilder=googleapiclient_request_builder,
            )
        else:
            self._logs = build("logging", "v2", credentials=credentials)
        self.mark_connected()

    def send_messages(self, *, messages, cursor):
        body = {
            "logName": "projects/%s/logs/%s" % (self.project_id, self.log_id),
            "resource": {
                "type": "generic_node",
            },
            "entries": [],
        }

        if self.resource_labels is not None:
            body["resource"]["labels"] = self.resource_labels

        for message in messages:
            msg_str = message.decode("utf8")
            msg = json.loads(msg_str)

            # This might not measure exactly 256K but should be a good enough approximation to handle this error.
            # We try truncating the message if it isn't possible then it is skip.
            if len(message) > self._LOG_ENTRY_QUOTA:
                DEFAULT_MESSAGE = "Log entry can't be logged because its size is greater than GCP logging quota of 256K"
                if "MESSAGE" in msg:
                    msg["MESSAGE"] = f'{msg["MESSAGE"][:self._MAX_MESSAGE_SIZE]}[MESSAGE TRUNCATED]'
                    messsage_size = len(json.dumps(msg, ensure_ascii=False).encode("utf-8"))
                    if messsage_size > self._LOG_ENTRY_QUOTA:
                        msg = {"MESSAGE": DEFAULT_MESSAGE}
                else:
                    msg = {"MESSAGE": DEFAULT_MESSAGE}
            timestamp = msg.pop("timestamp", None)
            journald_priority = msg.pop("PRIORITY", None)

            if "MESSAGE" in msg:
                with contextlib.suppress(json.JSONDecodeError):
                    msg["MESSAGE"] = json.loads(msg["MESSAGE"])

            entry = {
                "jsonPayload": msg,
            }
            if timestamp is not None:
                entry["timestamp"] = timestamp[:26] + "Z"  # assume timestamp to be UTC
            if journald_priority is not None:
                severity = self._SEVERITY_MAPPING.get(journald_priority, "DEFAULT")
                entry["severity"] = severity
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
