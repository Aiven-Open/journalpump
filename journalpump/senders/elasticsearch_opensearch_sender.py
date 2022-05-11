from abc import ABC, abstractmethod
from dataclasses import dataclass
from http import HTTPStatus
from io import BytesIO
from journalpump.senders.base import LogSender
from journalpump.util import default_json_serialization, get_requests_session
from requests import Timeout as RequestsTimeout
from requests.exceptions import ConnectionError as RequestsConnectionError
from typing import Any, Dict, Set, Union

import enum
import json
import time


@enum.unique
class SenderType(enum.Enum):
    opensearch = "opensearch"
    elasticsearch = "elasticsearch"


@dataclass(frozen=True)
class Config:
    _DEFAULT_REQUEST_TIMEOUT = 60.0

    _DEFAULT_INDEX_LIFETIME_IN_DAYS = 3

    _DEFAULT_INDEX_PREFIX = "journalpump"

    session_url: str
    request_timeout: int
    index_name: str
    index_lifetime_in_days: int
    sender_type: SenderType

    def request_url(self, path: str) -> str:
        return f"{self.session_url}/{path}"

    @staticmethod
    def create(*, sender_type: SenderType, config: Dict[str, Any]) -> "Config":
        if f"{sender_type.value}_url" not in config:
            raise ValueError(f"{sender_type.value}_url hasn't been defined")
        return Config(
            sender_type=sender_type,
            session_url=str(config[f"{sender_type.value}_url"]).rstrip("/"),
            request_timeout=config.get(f"{sender_type.value}_timeout", Config._DEFAULT_REQUEST_TIMEOUT),
            index_name=config.get(f"{sender_type.value}_index_prefix", Config._DEFAULT_INDEX_PREFIX),
            index_lifetime_in_days=config.get(f"{sender_type.value}_index_days_max", Config._DEFAULT_INDEX_LIFETIME_IN_DAYS),
        )


class _EsOsLogSenderBase(ABC, LogSender):

    _DEFAULT_MAX_SENDER_INTERVAL = 10.0

    _ONE_HOUR_LAST_INDEX_CHECK = 3600

    _SUCCESS_HTTP_STATUSES = {HTTPStatus.OK, HTTPStatus.CREATED}

    def __init__(self, *, sender_config: Config, config: Dict[str, Any], **kwargs) -> None:
        super().__init__(
            config=config,
            max_send_interval=config.get("max_send_interval", self._DEFAULT_MAX_SENDER_INTERVAL),
            **kwargs,
        )
        self._config = sender_config
        self._last_index_check_time = 0.0
        self._session = get_requests_session(timeout=self._config.request_timeout)
        # # If ca is set in config we use that, otherwise we verify using builtin CA cert list
        self._session.verify = self.config.get("ca", True)
        self._indices: Set[str] = set()
        self._last_es_error: Union[RequestsConnectionError, RequestsTimeout, None] = None

    @property
    def _indices_url(self) -> str:
        return self._config.request_url("_aliases")

    def _load_indices(self) -> bool:
        if self._indices:
            return True
        self.mark_disconnected()
        try:
            self._indices = set(self._session.get(self._indices_url).json().keys())
            self._last_es_error = None
        except (RequestsConnectionError, RequestsTimeout) as ex:
            self.mark_disconnected(ex)
            if ex.__class__ != self._last_es_error.__class__:
                # only log these errors once, not every 10 seconds
                self.log.warning("Connection error to %s: %s: %s", self._config.sender_type, ex.__class__.__name__, ex)

            self._last_es_error = ex
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception connecting to %s", self._config.sender_type)
            self.stats.unexpected_exception(ex, where="es_pump_init_es_client")
            return False

        self.log.info("Initialized Index Aware HTTP connection")
        self.mark_connected()
        return True

    @staticmethod
    def format_message_for_es(*, buf: BytesIO, header, message) -> None:
        buf.write(json.dumps(header, default=default_json_serialization).encode("utf8") + b"\n")
        # Message already in utf8 encoded bytestring form
        buf.write(message + b"\n")

    def _check_indices(self) -> None:
        aliases = self._session.get(self._indices_url).json()
        index_full_prefix = f"{self._config.index_name}-"
        indices = sorted(key for key in aliases.keys() if key.startswith(index_full_prefix))
        self.log.info(
            "Checking indices, currently: %r are available, max_indices: %r",
            indices,
            self._config.index_lifetime_in_days,
        )
        while len(indices) > self._config.index_lifetime_in_days:
            index_to_delete = indices.pop(0)
            self.log.info(
                "Deleting index: %r since we only keep %d days worth of indices",
                index_to_delete,
                self._config.index_lifetime_in_days,
            )
            try:
                self._session.delete(self._config.request_url(index_to_delete))
                self._indices.discard(index_to_delete)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception deleting index %r", index_to_delete)
                self.stats.unexpected_exception(ex, where="es_pump_check_indices")

    def maintenance_operations(self) -> None:
        if time.monotonic() - self._last_index_check_time > self._ONE_HOUR_LAST_INDEX_CHECK:
            self._last_index_check_time = time.monotonic()
            self._check_indices()

    def send_messages(self, *, messages, cursor) -> bool:
        buf = BytesIO()
        start_time = time.monotonic()
        try:
            es_available = self._load_indices()
            if not es_available:
                self.log.warning("Waiting for connection to %s", self._indices_url)
                self._backoff()
                return False
            for msg in messages:
                message = json.loads(msg.decode("utf8"))
                # ISO datetime first 10 characters are equivalent to the date we need i.e. '2018-04-14'
                idx_name = f"""{self._config.index_name}-{message["timestamp"][:10]}"""
                if idx_name not in self._indices:
                    self._create_index_and_mapping(idx_name)

                self.format_message_for_es(buf=buf, header=self._message_header(idx_name), message=msg)

            # If we have messages, send them along to OpenSearch
            if buf.tell():
                buf_size = buf.tell()
                buf.seek(0)
                # Opensearch allows using _index even when posting
                # to particular index to override the index the entry
                # so this is mostly cosmetic to avoid needing to
                # expose /_bulk. We use simply the most recent index
                # name as base as it does not really matter which one
                # we use.
                res = self._session.post(
                    self._config.request_url(f"{idx_name}/_bulk"),
                    data=buf,
                    headers={
                        "content-length": str(buf_size),
                        "content-type": "application/x-ndjson",
                    },
                    timeout=self._config.request_timeout * 2,  # 2 times bigger by default 120 seconds
                )
                buf.seek(0)
                buf.truncate(0)

                self.mark_sent(messages=messages, cursor=cursor)
                self.log.info(
                    "Sent %d log events successfully: %r, took: %.2fs",
                    len(messages),
                    res.status_code in self._SUCCESS_HTTP_STATUSES,
                    time.monotonic() - start_time,
                )
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            short_msg = str(ex)[:200]
            self.log.exception(
                "Problem sending logs to %s: %s: %s",
                self._config.sender_type,
                ex.__class__.__name__,
                short_msg,
            )
            self._backoff()
            return False

        return True

    @abstractmethod
    def _create_index_and_mapping(self, index_name: str) -> None:
        raise NotImplementedError("_create_index_and_mapping hasn't been implemented")

    @abstractmethod
    def _message_header(self, index_name: str) -> Dict[str, Any]:
        raise NotImplementedError("_message_header hasn't been implemented")


class ElasticsearchSender(_EsOsLogSenderBase):
    def __init__(self, *, config, **kwargs) -> None:
        super().__init__(
            sender_config=Config.create(sender_type=SenderType.elasticsearch, config=config),
            config=config,
            **kwargs,
        )

    def _message_header(self, index_name: str) -> Dict[str, Any]:
        return {
            "index": {
                "_index": index_name,
                "_type": "journal_msg",
            }
        }

    def _create_index_and_mapping(self, index_name: str) -> None:
        try:
            self.log.info("Creating index: %r", index_name)
            res = self._session.put(
                self._config.request_url(f"{index_name}?include_type_name=true"),
                json={
                    "mappings": {
                        "journal_msg": {
                            "properties": {
                                "SYSTEMD_SESSION": {
                                    "type": "text"
                                },
                                "SESSION_ID": {
                                    "type": "text"
                                },
                            }
                        }
                    },
                },
                timeout=self._config.request_timeout * 2,
            )
            if res.status_code in self._SUCCESS_HTTP_STATUSES or "already_exists_exception" in res.text:
                self._indices.add(index_name)
            else:
                self.mark_disconnected(f"""Cannot create index "{index_name}" ({res.status_code} {res.text})""")
                self.log.warning("Could not create index mappings for: %r, %r %r", index_name, res.text, res.status_code)
        except (RequestsConnectionError, RequestsTimeout) as ex:
            self.mark_disconnected(ex)
            self.log.error("Problem creating index %r: %s: %s", index_name, ex.__class__.__name__, ex)
            self.stats.unexpected_exception(ex, where="es_pump_create_index_and_mappings")
            self._backoff()


class OpenSearchSender(_EsOsLogSenderBase):
    def __init__(self, *, config: Dict[str, Any], **kwargs) -> None:
        super().__init__(
            sender_config=Config.create(sender_type=SenderType.opensearch, config=config),
            config=config,
            **kwargs,
        )

    def _message_header(self, index_name: str) -> Dict[str, Any]:
        return {
            "index": {
                "_index": index_name,
            }
        }

    def _create_index_and_mapping(self, index_name: str) -> None:
        try:
            self.log.info("Creating index: %r", index_name)
            res = self._session.put(
                self._config.request_url(index_name),
                json={
                    "mappings": {
                        "properties": {
                            "SYSTEMD_SESSION": {
                                "type": "text"
                            },
                            "SESSION_ID": {
                                "type": "text"
                            },
                        }
                    },
                },
            )
            if res.status_code in self._SUCCESS_HTTP_STATUSES or "already_exists_exception" in res.text:
                self._indices.add(index_name)
            else:
                self.mark_disconnected(f"""Cannot create index "{index_name}" ({res.status_code} {res.text})""")
                self.log.warning("Could not create index mappings for: %r, %r %r", index_name, res.text, res.status_code)
        except (RequestsConnectionError, RequestsTimeout) as ex:
            self.mark_disconnected(ex)
            self.log.error("Problem creating index %r: %s: %s", index_name, ex.__class__.__name__, ex)
            self.stats.unexpected_exception(ex, where="es_pump_create_index_and_mappings")
            self._backoff()
