from .base import LogSender, SenderInitializationError
from botocore import exceptions, session
from collections.abc import Sequence
from typing import Any

import json
import time

MAX_INIT_TRIES = 3

# Limits PutLogEvents enforces per request
MAX_BATCH_EVENTS = 10_000
MAX_BATCH_SPAN_MS = 24 * 60 * 60 * 1000
MAX_EVENT_AGE_MS = 14 * 24 * 60 * 60 * 1000


def _event_timestamp_ms(message: bytes) -> int:
    timestamp = json.loads(message).get("REALTIME_TIMESTAMP") or time.time()
    return int(timestamp * 1000.0)


class AWSCloudWatchSender(LogSender):
    def __init__(self, *, config: dict[str, Any], aws_cloudwatch_logs: Any = None, **kwargs: Any) -> None:
        super().__init__(
            config=config,
            max_send_interval=config.get("max_send_interval", 0.3),
            **kwargs,
        )
        self._logs = aws_cloudwatch_logs
        self.log_group = self.config.get("aws_cloudwatch_log_group")
        self.log_stream = self.config.get("aws_cloudwatch_log_stream")
        self._next_sequence_token: str | None = None

        if self._logs is None:
            if self.log_group is None or self.log_stream is None:
                raise ValueError("AWS CloudWatch log group and stream names need to be configured")
            kwargs = {}
            if self.config.get("aws_region") is not None:
                kwargs["region_name"] = self.config.get("aws_region")
            if self.config.get("aws_access_key_id") is not None:
                kwargs["aws_access_key_id"] = self.config.get("aws_access_key_id")
            if self.config.get("aws_secret_access_key") is not None:
                kwargs["aws_secret_access_key"] = self.config.get("aws_secret_access_key")
            botocore_session = session.get_session()
            self._logs = botocore_session.create_client("logs", **kwargs)

    def _init_log_group(self) -> None:  # pylint: disable=too-many-statements
        self.log.info("Initializing AWS CloudWatch")

        # Catch access denied exception(e.g. due to erroneous credentials)
        attempts_left = MAX_INIT_TRIES
        while attempts_left:
            attempts_left -= 1
            try:
                # Create the log group and stream if they don't exist yet
                # These are done in separate try-excepts, as both log group
                # or log stream may already exist
                try:
                    self._logs.create_log_group(logGroupName=self.log_group)
                except exceptions.ClientError as err:
                    # Ignore ResourceAlreadyExistsException, raise other errors
                    if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                        raise
                try:
                    self._logs.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
                except exceptions.ClientError as err:
                    # Ignore ResourceAlreadyExistsException, raise other errors
                    if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                        raise
            except exceptions.ClientError as err:
                self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
                # If we get AccessDenied, log and raise SenderInitializationError
                # if too many attempts. SenderInitializationError is handled by
                # JournalReader
                if err.response["Error"]["Code"] == "AccessDeniedException":
                    self.log.exception(
                        "Access denied exception when trying to create log group and stream in AWS Cloudwatch."
                    )
                    if not attempts_left:
                        raise SenderInitializationError(
                            f"AWS CloudWatch access denied: {err.response['Error']['Code']}: "
                            f"{err.response['Error'].get('Message', '')}"
                        ) from err
                    self._backoff()
                    continue
                # If we get some other aws exception, log it and raise if too many attempts.
                # This is not handled in any special way
                self.log.exception("AWS ClientError")
                if not attempts_left:
                    raise
                self._backoff()
            else:
                break

        try:
            paginator = self._logs.get_paginator("describe_log_streams")
            for page in paginator.paginate(logGroupName=self.log_group):
                streams = page.get("logStreams")
                if streams is not None:
                    found_stream_metadata = [stream for stream in streams if stream["logStreamName"] == self.log_stream]
                    if found_stream_metadata:
                        self._next_sequence_token = found_stream_metadata[0].get("uploadSequenceToken")
                        self.mark_connected(reset_backoff=False)
                        return
        except exceptions.ClientError as err:
            self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
            if err.response["Error"]["Code"] == "AccessDeniedException":
                self.log.exception("Access denied exception when trying to create log group and stream in AWS Cloudwatch.")
            else:
                # If we get some other aws exception, log it.
                # This is not handled in any special way
                self.log.exception("AWS ClientError")
            self._backoff()
        self.mark_disconnected()
        self.log.error("Failed to init sender. AWS CloudWatch logs could not update sequence token.")

    def next_batch_length(self, messages: Sequence[tuple[bytes, str | None]]) -> int:
        length = min(super().next_batch_length(messages), MAX_BATCH_EVENTS)
        oldest = newest = _event_timestamp_ms(messages[0][0])
        for index in range(1, length):
            timestamp = _event_timestamp_ms(messages[index][0])
            if max(newest, timestamp) - min(oldest, timestamp) >= MAX_BATCH_SPAN_MS:
                return index
            oldest = min(oldest, timestamp)
            newest = max(newest, timestamp)
        return length

    def _report_skipped(self, skipped_count: int) -> None:
        if not skipped_count:
            return
        self.log.info(
            "Skipping %d events older than %d days, CloudWatch does not accept them",
            skipped_count,
            MAX_EVENT_AGE_MS // (24 * 60 * 60 * 1000),
        )
        self.stats.increase(
            "journal.aws_cloudwatch.skipped_events",
            inc_value=skipped_count,
            tags=self.make_tags({"app": "journalpump"}),
        )

    def _reconnect(self, *, backoff_attempt_at_start: int) -> bool:
        """Run _init_log_group, returning False instead of raising so the caller keeps its batch.

        _init_log_group backs off on its own retry paths. If it raised without backing off during this send attempt
        (and the caller has not either), back off here so a failing reconnect does not spin.
        """
        try:
            self._init_log_group()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Failed to reconnect to AWS CloudWatch")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            if self._backoff_attempt == backoff_attempt_at_start:
                self._backoff()
            return False
        return True

    def send_messages(self, *, messages: list[bytes], cursor: str | None) -> bool:
        backoff_attempt_at_start = self._backoff_attempt
        oldest_accepted_ms = int(time.time() * 1000.0) - MAX_EVENT_AGE_MS
        sent_messages: list[bytes] = []
        log_events: list[dict[str, Any]] = []
        for msg in messages:
            timestamp_ms = _event_timestamp_ms(msg)
            if timestamp_ms < oldest_accepted_ms:
                continue
            sent_messages.append(msg)
            log_events.append({"timestamp": timestamp_ms, "message": msg.decode("utf8")})

        skipped_count = len(messages) - len(log_events)
        if not log_events:
            self._report_skipped(skipped_count)
            self._sent_cursor = cursor
            return True

        if not self._connected and not self._reconnect(backoff_attempt_at_start=backoff_attempt_at_start):
            return False

        log_events.sort(key=lambda event: event["timestamp"])
        kwargs: dict[str, Any] = {
            "logGroupName": self.log_group,
            "logStreamName": self.log_stream,
            "logEvents": log_events,
        }
        if self._next_sequence_token is not None:
            kwargs["sequenceToken"] = self._next_sequence_token
        try:
            response = self._logs.put_log_events(**kwargs)
        except exceptions.ClientError as err:
            err_code = err.response["Error"]["Code"]
            err_msg = err.response["Error"]["Message"]
            if err_code == "InvalidParameterException":
                self.log.error("CloudWatch rejected a batch of %d events, dropping it: %r", len(log_events), err_msg)
                self.stats.increase(
                    "journal.aws_cloudwatch.dropped_events",
                    inc_value=len(log_events),
                    tags=self.make_tags({"app": "journalpump"}),
                )
                self._report_skipped(skipped_count)
                self._sent_cursor = cursor
                self.mark_disconnected(f"Dropped a batch CloudWatch rejected as invalid: {err_msg}")
                return True
            self.mark_disconnected()
            self.log.error("Error sending events %r: %r", err_code, err_msg)
            self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._reconnect(backoff_attempt_at_start=backoff_attempt_at_start)
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to AWS CloudWatch")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._reconnect(backoff_attempt_at_start=backoff_attempt_at_start)
        else:
            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            if 200 <= status_code < 300:
                rejected = response.get("rejectedLogEventsInfo")
                if rejected:
                    self.log.warning("CloudWatch rejected some events in a batch of %d: %r", len(log_events), rejected)
                    self.stats.increase(
                        "journal.aws_cloudwatch.partially_rejected_batches",
                        tags=self.make_tags({"app": "journalpump"}),
                    )
                self._report_skipped(skipped_count)
                self.mark_sent(messages=sent_messages, cursor=cursor)
                self._next_sequence_token = response.get("nextSequenceToken")
                return True
            self.mark_disconnected(f"CloudWatch put_log_events returned HTTP status {status_code}")
            self.log.error("CloudWatch put_log_events returned HTTP status %d", status_code)
            self._backoff()
        return False
