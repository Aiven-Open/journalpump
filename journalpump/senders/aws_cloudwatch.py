from .base import ThreadedLogSender, SenderInitializationError

import boto3
import botocore
import json
import time

MAX_INIT_TRIES = 3


class AWSCloudWatchSender(ThreadedLogSender):
    def __init__(self, *, config, aws_cloudwatch_logs=None, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 0.3), **kwargs)
        self._logs = aws_cloudwatch_logs
        self.log_group = self.config.get("aws_cloudwatch_log_group")
        self.log_stream = self.config.get("aws_cloudwatch_log_stream")
        self._next_sequence_token = None
        self._init_logs()

    def _init_logs(self):
        if self._logs is None:
            if self.log_group is None or self.log_stream is None:
                raise Exception("AWS CloudWatch log group and stream names need to be configured")
            kwargs = {}
            if self.config.get("aws_region") is not None:
                kwargs["region_name"] = self.config.get("aws_region")
            if self.config.get("aws_access_key_id") is not None:
                kwargs["aws_access_key_id"] = self.config.get("aws_access_key_id")
            if self.config.get("aws_secret_access_key") is not None:
                kwargs["aws_secret_access_key"] = self.config.get("aws_secret_access_key")
            self._logs = boto3.client("logs", **kwargs)

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
                except botocore.exceptions.ClientError as err:
                    # Ignore ResourceAlreadyExistsException, raise other errors
                    if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                        raise
                try:
                    self._logs.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
                except botocore.exceptions.ClientError as err:
                    # Ignore ResourceAlreadyExistsException, raise other errors
                    if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                        raise
            except botocore.exceptions.ClientError as err:
                self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
                # If we get AccessDenied, log and raise SenderInitializationError
                # if too many attempts. SenderInitializationError is handled by
                # JournalReader
                if err.response["Error"]["Code"] == "AccessDeniedException":
                    self.log.exception(
                        "Access denied exception when trying to create log group and stream in AWS Cloudwatch."
                    )
                    if not attempts_left:
                        raise SenderInitializationError from err
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

        paginator = self._logs.get_paginator("describe_log_streams")
        for page in paginator.paginate(logGroupName=self.log_group):
            streams = page.get("logStreams")
            if streams is not None:
                found_stream_metadata = [stream for stream in streams if stream["logStreamName"] == self.log_stream]
                if found_stream_metadata:
                    self._next_sequence_token = found_stream_metadata[0].get("uploadSequenceToken")
                    self.mark_connected()
                    return
        self.mark_disconnected()
        self.log.error("Failed to init sender. AWS CloudWatch logs could not update sequence token.")

    def send_messages(self, *, messages, cursor):
        log_events = []
        for msg in messages:
            raw_message = msg.decode("utf8")
            message = json.loads(raw_message)
            timestamp = message.get("REALTIME_TIMESTAMP") or time.time()
            log_events.append({"timestamp": int(timestamp * 1000.0), "message": raw_message})
        kwargs = {"logGroupName": self.log_group, "logStreamName": self.log_stream, "logEvents": log_events}
        if self._next_sequence_token is not None:
            kwargs["sequenceToken"] = self._next_sequence_token
        try:
            response = self._logs.put_log_events(**kwargs)
        except botocore.exceptions.ClientError as err:
            err_code = err.response["Error"]["Code"]
            err_msg = err.response["Error"]["Message"]
            self.mark_disconnected()
            self.log.error("Error sending events %r: %r", err_code, err_msg)
            self.stats.unexpected_exception(ex=err, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_logs()
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to AWS CloudWatch")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
            self._backoff()
            self._init_logs()
        else:
            if 200 <= response["ResponseMetadata"]["HTTPStatusCode"] < 300:
                self.mark_sent(messages=messages, cursor=cursor)
                self._next_sequence_token = response["nextSequenceToken"]
                return True
        return False
