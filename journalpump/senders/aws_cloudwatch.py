from ..statsd import StatsClient, TagsType
from .base import _LogSenderReader, JournalCursor, LogSender
# Type annotations
from mypy_boto3_logs.client import Boto3ClientError, CloudWatchLogsClient, InputLogEventTypeDef
from typing import Any, Dict, List, Mapping, Optional, Sequence

import boto3
import json
import time


class AWSCloudWatchSender(LogSender):
    def __init__(
        self,
        *,
        aws_cloudwatch_logs: CloudWatchLogsClient,
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

        self._log_group: str = str(self.config["aws_cloudwatch_log_group"])
        self._log_stream: str = str(self.config["aws_cloudwatch_log_stream"])
        self._sequence_token: Optional[str] = None

        self._aws_region: Optional[str] = str(self.config["aws_region"]) if "aws_region" in self.config else None
        self._aws_access_key_id: Optional[str] = str(
            self.config["aws_access_key_id"]
        ) if "aws_access_key_id" in self.config else None
        self._aws_secret_access_key: Optional[str] = str(
            self.config["aws_secret_access_key"]
        ) if "aws_secret_access_key" in self.config else None
        self._aws_session_token: Optional[str] = str(
            self.config["aws_session_token"]
        ) if "aws_session_token" in self.config else None

        self._logs: CloudWatchLogsClient
        if aws_cloudwatch_logs is None:
            self.log.info("Initializing AWS client from parameters")
            self._logs = boto3.client(
                "logs",
                region_name=self._aws_region,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                aws_session_token=self._aws_session_token,
            )
        else:
            self.log.info("Using supplied AWS log client")
            self._logs = aws_cloudwatch_logs

    def send_messages(self, *, messages: Sequence[bytes], cursor: JournalCursor) -> bool:
        # For AWS the most common failure mode is that we don't have permission to create the
        # log group. At the very least, this is likely to only happen once - ever - if it does,
        # so normal behavior is to fail on initial log sending and retry. This is identical to
        # if we tried to initialize things in advance.

        # Message sending can continue so long as the sender hasn't been shutdown and hasn't
        # errored. If an exit is triggered but message sending is still proceeding, we try
        # to finish it up.
        while self.running:
            try:
                # This inner loop ensures we do as much sending as possible before potentially
                # aborting if the sender is shutdown.
                # TODO: handle message too large errors
                log_events: List[InputLogEventTypeDef] = []
                for msg in messages:
                    message_str = msg.decode("utf8")
                    message: Dict[str, Any] = json.loads(message_str)
                    timestamp: int = int(message["REALTIME_TIMESTAMP"]
                                         ) if "REALTIME_TIMESTAMP" in message else int(time.time())
                    log_events.append({"timestamp": timestamp, "message": message_str})

                # Technically this can paginate, but we can't do anything with rejected log entries
                # at the moment.
                self._logs.put_log_events(
                    logGroupName=self._log_group,
                    logStreamName=self._log_stream,
                    logEvents=log_events,
                )
                self.mark_sent(messages=messages, cursor=cursor)
                self.mark_connected()
                return True
            except Exception as ex:  # pylint: disable=broad-except
                self.mark_disconnected(ex)
                # Boto errors trigger recovery (i.e. do we have a log group etc.?)
                if isinstance(ex, Boto3ClientError):
                    # If just a boto client error, then we'll do our usual setup.
                    # Create the log group and stream if they don't exist yet
                    # There's a lot of nested loops here, but AFAIK this is still the clearest way
                    # to do it.
                    try:
                        self.log.debug("Attempting to create log group: %s", self._log_group)
                        self._logs.create_log_group(logGroupName=self._log_group)
                        self.log.info("Created log group: %s", self._log_group)
                        break
                    except Boto3ClientError as err:
                        if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                            self.log.exception(
                                "Error creating logGroup for delivering messages: %s", self._log_group, exc_info=err
                            )
                            # There's nothing we can obviously do to fix here, so rather then infinite
                            # loop return to the caller that we're going to fail to log and let them
                            # decide.
                            self.mark_disconnected(err)
                            return False

                    try:
                        self._logs.create_log_stream(logGroupName=self._log_group, logStreamName=self._log_stream)
                    except Boto3ClientError as err:
                        if err.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                            self.log.exception(
                                "Error creating logStream for delivering messages: %s", self._log_stream, exc_info=err
                            )
                            # There's nothing we can obviously do to fix here, so rather then infinite
                            # loop return to the caller that we're going to fail to log and let them
                            # decide.
                            self.mark_disconnected(err)
                            return False

                    try:
                        streams = self._logs.describe_log_streams(
                            logGroupName=self._log_group,
                            logStreamNamePrefix=self._log_stream,
                        )["logStreams"]
                        # Looks weird, but handles if we get nothing back.
                        for stream in [stream for stream in streams if stream["logStreamName"] == self._log_stream]:
                            self._sequence_token = stream.get("uploadSequenceToken", None)
                    except Exception as err:  # pylint: disable=broad-except
                        self.log.exception(
                            "Error describing logStreams to disover uploadSequenceToken for stream: %s",
                            self._log_stream,
                            exc_info=err
                        )
                        # There's nothing we can obviously do to fix here, so rather then infinite
                        # loop return to the caller that we're going to fail to log and let them
                        # decide.
                        self.mark_disconnected(err)
                        return False
                else:
                    # General exceptions are handled here
                    self.log.exception(
                        "Error describing logStreams to disover uploadSequenceToken for stream: %s",
                        self._log_stream,
                        exc_info=ex
                    )
                    self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))
                    # There's nothing we can obviously do to fix here, so rather then infinite
                    # loop return to the caller that we're going to fail to log and let them
                    # decide.
                    return False
                self.mark_connected()
            finally:
                # This could be in the except handler above. It isnt though, just in case someone
                # breaks the loop in the future. It's just fine here, so don't move it.
                self._backoff()

        # TODO: should raise an exception, but don't want to change our orginal interface promise.
        # raise LogSenderShutdown("rsyslog sender was shutdown before all messages could be sent")
        return False
