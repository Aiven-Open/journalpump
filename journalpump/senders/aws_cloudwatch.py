from .base import LogSender

import time
import json
import botocore
import boto3


class AWSCloudWatchSender(LogSender):
    def __init__(self, *, config, aws_cloudwatch_logs=None, **kwargs):
        super().__init__(
            config=config,
            max_send_interval=config.get("max_send_interval", 0.3),
            **kwargs
        )
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
        # Create the log group and stream if they don't exist yet
        try:
            self._logs.create_log_group(logGroupName=self.log_group)
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise
        try:
            self._logs.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise
        streams = self._logs.describe_log_streams(logGroupName=self.log_group).get("logStreams")
        if streams is not None:
            stream_metadata = [stream for stream in streams if stream['logStreamName'] == self.log_stream][0]
            self._next_sequence_token = stream_metadata.get("uploadSequenceToken")
            self.mark_connected()
        else:
            raise Exception("AWS CloudWatch logs could not update sequence token")

    def send_messages(self, *, messages, cursor):
        log_events = []
        for msg in messages:
            raw_message = msg.decode("utf8")
            message = json.loads(raw_message)
            timestamp = message.get("REALTIME_TIMESTAMP") or time.time()
            log_events.append(
                {
                    "timestamp": int(timestamp * 1000.0),
                    "message": raw_message
                }
            )
        kwargs = {
            "logGroupName": self.log_group,
            "logStreamName": self.log_stream,
            "logEvents": log_events
        }
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
