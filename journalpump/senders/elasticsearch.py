from .base import LogSender
from io import BytesIO
from journalpump.util import default_json_serialization, get_requests_session

import json
import requests
import time


class ElasticsearchSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(config=config, max_send_interval=config.get("max_send_interval", 10.0), **kwargs)
        self.session_url = self.config.get("elasticsearch_url")
        self.last_index_check_time = 0
        self.request_timeout = self.config.get("elasticsearch_timeout", 10.0)
        self.index_days_max = self.config.get("elasticsearch_index_days_max", 3)
        self.index_name = self.config.get("elasticsearch_index_prefix", "journalpump")
        self.session = get_requests_session()
        # If ca is set in config we use that, otherwise we verify using builtin CA cert list
        self.session.verify = self.config.get("ca", True)
        self.indices = set()
        self.last_es_error = None

    def _init_es_client(self):
        if self.indices:
            return True
        self.mark_disconnected()
        try:
            self.indices = set(self.session.get(self.session_url + "/_aliases", timeout=60.0).json().keys())
            self.last_es_error = None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ex:
            self.mark_disconnected(ex)
            if ex.__class__ != self.last_es_error.__class__:
                # only log these errors once, not every 10 seconds
                self.log.warning("ES connection error: %s: %s", ex.__class__.__name__, ex)

            self.last_es_error = ex
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception connecting to ES")
            self.stats.unexpected_exception(ex, where="es_pump_init_es_client")
            return False

        self.log.info("Initialized ES connection")
        self.mark_connected()
        return True

    def create_index_and_mappings(self, index_name):
        try:
            self.log.info("Creating index: %r", index_name)
            res = self.session.put(
                self.session_url + "/{}?include_type_name=true".format(index_name),
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
                timeout=60.0,
            )
            if res.status_code in (200, 201) or "already_exists_exception" in res.text:
                self.indices.add(index_name)
            else:
                self.mark_disconnected('Cannot create index "{index_name}" ({res.status_code} {res.text})')
                self.log.warning("Could not create index mappings for: %r, %r %r", index_name, res.text, res.status_code)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as ex:
            self.mark_disconnected(ex)
            self.log.error("Problem creating index %r: %s: %s", index_name, ex.__class__.__name__, ex)
            self.stats.unexpected_exception(ex, where="es_pump_create_index_and_mappings")
            self._backoff()

    @staticmethod
    def format_message_for_es(buf, header, message):
        buf.write(json.dumps(header, default=default_json_serialization).encode("utf8") + b"\n")
        # Message already in utf8 encoded bytestring form
        buf.write(message + b"\n")

    def check_indices(self):
        aliases = self.session.get(self.session_url + "/_aliases", timeout=60.0).json()
        index_full_prefix = "{}-".format(self.index_name)
        indices = sorted(key for key in aliases.keys() if key.startswith(index_full_prefix))
        self.log.info("Checking indices, currently: %r are available, max_indices: %r", indices, self.index_days_max)
        while len(indices) > self.index_days_max:
            index_to_delete = indices.pop(0)
            self.log.info(
                "Deleting index: %r since we only keep %d days worth of indices", index_to_delete, self.index_days_max
            )
            try:
                self.session.delete("{}/{}".format(self.session_url, index_to_delete), timeout=60.0)
                self.indices.discard(index_to_delete)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception deleting index %r", index_to_delete)
                self.stats.unexpected_exception(ex, where="es_pump_check_indices")

    def maintenance_operations(self):
        if time.monotonic() - self.last_index_check_time > 3600:
            self.last_index_check_time = time.monotonic()
            self.check_indices()

    def send_messages(self, *, messages, cursor):
        buf = BytesIO()
        start_time = time.monotonic()
        try:
            es_available = self._init_es_client()
            if not es_available:
                self.log.warning("Waiting for ES connection")
                self._backoff()
                return False
            for msg in messages:
                message = json.loads(msg.decode("utf8"))
                # ISO datetime's first 10 characters are equivalent to the date we need i.e. '2018-04-14'
                index_name = "{}-{}".format(self.index_name, message["timestamp"][:10])
                if index_name not in self.indices:
                    self.create_index_and_mappings(index_name)

                header = {
                    "index": {
                        "_index": index_name,
                        "_type": "journal_msg",
                    }
                }
                self.format_message_for_es(buf, header, msg)

            # If we have messages, send them along to ES
            if buf.tell():
                buf_size = buf.tell()
                buf.seek(0)
                # Elasticsearch allows using _index even when posting
                # to particular index to override the index the entry
                # so this is mostly cosmetic to avoid needing to
                # expose /_bulk. We use simply the most recent index
                # name as base as it does not really matter which one
                # we use.
                res = self.session.post(
                    "%s/%s/_bulk" % (self.session_url, index_name),
                    data=buf,
                    headers={
                        "content-length": str(buf_size),
                        "content-type": "application/x-ndjson",
                    },
                    timeout=120.0,
                )
                buf.seek(0)
                buf.truncate(0)

                self.mark_sent(messages=messages, cursor=cursor)
                self.log.info(
                    "Sent %d log events to ES successfully: %r, took: %.2fs", len(messages), res.status_code in {200, 201},
                    time.monotonic() - start_time
                )
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            short_msg = str(ex)[:200]
            self.log.exception("Problem sending logs to ES: %s: %s", ex.__class__.__name__, short_msg)
            self._backoff()
            return False

        return True
