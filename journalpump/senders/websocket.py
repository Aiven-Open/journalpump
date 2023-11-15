from .base import LogSender, MAX_KAFKA_MESSAGE_SIZE
from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError
from aiohttp_socks.utils import Proxy
from concurrent.futures import CancelledError, TimeoutError as ConnectionTimeoutError
from journalpump import __version__
from journalpump.types import StrEnum
from journalpump.util import ExponentialBackoff
from threading import Thread
from urllib.parse import urlparse

import asyncio
import contextlib
import enum
import logging
import snappy  # pylint: disable=import-error
import socket
import ssl
import time
import websockets


@enum.unique
class JournalPumpMessageCompression(StrEnum):
    none = "none"
    snappy = "snappy"


@enum.unique
class WebsocketCompression(StrEnum):
    none = "none"
    deflate = "deflate"


class WebsocketRunner(Thread):
    def __init__(
        self,
        *,
        websocket_uri,
        socks5_proxy_url,
        ssl_enabled,
        ssl_ca,
        ssl_key,
        ssl_cert,
        websocket_compression,
        compression,
        max_batch_size,
    ):
        super().__init__(daemon=True)
        self.log = logging.getLogger(self.__class__.__name__)
        self.websocket_uri = websocket_uri
        self.socks5_proxy_url = socks5_proxy_url
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.ssl_key = ssl_key
        self.ssl_cert = ssl_cert
        self.websocket_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.websocket_loop)
        self.websocket = None
        self.connected_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.stopped_event = asyncio.Event()
        self.running = True
        # Send messages as batches, LogSender base class takes care of batch size.
        # If batching is disabled, do not adjust the max_batch_size member variable,
        # so that LogSender will still give us multiple messages in a single send_messages() call.
        self.batching_enabled = False
        if max_batch_size > 0:
            self.max_batch_size = max_batch_size
            self.batching_enabled = True
        self.batch_message_overhead = 1
        self.compression = compression
        self.websocket_compression = websocket_compression
        self.backoff = ExponentialBackoff(base=20, factor=1.8, maximum=90, jitter=True)
        # prevent websockets from logging message contents when we're otherwise in DEBUG mode
        logging.getLogger("websockets").setLevel(logging.INFO)

        self.socks5_proxy = None
        if self.socks5_proxy_url:
            self.socks5_proxy = Proxy.from_url(self.socks5_proxy_url, loop=self.websocket_loop)

    async def consumer_handler(self, websocket):
        # Dummy consumer to read and ignore messages from the websocket
        while self.running:
            _ = await websocket.recv()

    async def async_send(self, messages):
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.connected_event.wait(), 20)
        if not self.connected_event.is_set():
            return False

        if self.batching_enabled:
            # LogSender has limited the batch size already
            batch = b"\x00".join(messages)
            messages = [batch]

        for message in messages:
            if self.compression == JournalPumpMessageCompression.snappy:
                message = snappy.snappy.compress(message)

            try:
                await self.websocket.send(message)
            except Exception as ex:  # pylint:disable=broad-except
                self.log.warning("Exception while sending messages to websocket: %s", ex)
                return False

        return True

    def send(self, *, messages):
        return asyncio.run_coroutine_threadsafe(self.async_send(messages), self.websocket_loop).result()

    def run(self):
        self.log.info("WebsocketRunner starting")
        try:
            self.websocket_loop.create_task(self.comms_channel_loop())
            self.websocket_loop.run_forever()
            self.websocket_loop.close()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Waiting for stop failed; shouldn't ever happen")

        self.log.info("WebsocketRunner finished")

    def close(self):
        if self.running:
            self.log.info("Closing WebsocketRunner")
            self.running = False
            asyncio.run_coroutine_threadsafe(self.set_stop_event(), self.websocket_loop).result()

            # after an otherwise clean shutdown, there may still be a send() task waiting
            cancellations = False
            for task in asyncio.all_tasks(loop=self.websocket_loop):
                self.log.info("Cancelling remaining task: %s", str(task))
                task.cancel()
                cancellations = True

            # wait for cancellations to execute
            if cancellations:
                asyncio.run_coroutine_threadsafe(asyncio.sleep(2), self.websocket_loop).result()

            self.websocket_loop.call_soon_threadsafe(self.websocket_loop.stop)

    async def set_stop_event(self) -> None:
        self.stop_event.set()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stopped_event.wait(), 10)

    async def wait_for_stop_event(self) -> None:
        await self.stop_event.wait()

    async def websocket_connect_coro(self):
        ssl_context = None
        if self.ssl_enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_cert_chain(self.ssl_cert, keyfile=self.ssl_key)
            ssl_context.load_verify_locations(self.ssl_ca)

        headers = {"User-Agent": f"journalpump/{__version__}"}

        if self.batching_enabled:
            headers["Journalpump-Batch-Size"] = str(self.max_batch_size)
        if self.compression != JournalPumpMessageCompression.none:
            headers["Journalpump-Compression"] = self.compression

        sock = None
        url_parsed = urlparse(self.websocket_uri)
        if self.socks5_proxy:
            socks_url_parsed = urlparse(self.socks5_proxy_url)
            self.log.info(
                "Connecting via SOCKS5 proxy at %s:%d",
                socks_url_parsed.hostname,
                socks_url_parsed.port,
            )
            sock = await self.socks5_proxy.connect(dest_host=url_parsed.hostname, dest_port=url_parsed.port)
            self.log.info(
                "Connected via SOCKS5 proxy at %s:%d",
                socks_url_parsed.hostname,
                socks_url_parsed.port,
            )

        ws_compr = None if self.websocket_compression == WebsocketCompression.none else str(self.websocket_compression)
        return await websockets.connect(  # pylint:disable=no-member
            self.websocket_uri,
            ssl=ssl_context,
            compression=ws_compr,
            extra_headers=headers,
            sock=sock,
            server_hostname=url_parsed.hostname if self.ssl_enabled else None,
            close_timeout=20,
            max_size=MAX_KAFKA_MESSAGE_SIZE * 2,
        )

    async def websocket_connect(self, *, timeout=30):
        connect_task = asyncio.create_task(self.websocket_connect_coro())
        wait_for_stop_task = asyncio.create_task(self.wait_for_stop_event())

        _, pending = await asyncio.wait(
            [connect_task, wait_for_stop_task],
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout,
        )

        if connect_task.done():
            wait_for_stop_task.cancel()
            return connect_task.result()

        if wait_for_stop_task.done():
            connect_task.cancel()
            raise CancelledError("Stopping")

        for task in pending:
            task.cancel()

        raise ConnectionTimeoutError(f"Websocket connection timed out after {timeout} seconds")

    async def comms_channel_round(self):
        stop_event_task = asyncio.create_task(self.wait_for_stop_event())
        consumer_task = None
        try:
            self.log.info("Connecting to websocket at %s", self.websocket_uri)
            self.websocket = await self.websocket_connect()
            self.log.info("Connected to websocket at %s", self.websocket_uri)
            consumer_task = asyncio.create_task(self.consumer_handler(self.websocket))
            established_time = time.monotonic()
            self.connected_event.set()

            _, pending = await asyncio.wait(
                [consumer_task, stop_event_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            self.connected_event.clear()
            await self.websocket.close()
            self.websocket = None
            self.log.info(
                "Websocket connection closed after %d s: %s",
                time.monotonic() - established_time,
                self.websocket_uri,
            )

            # If we have had a long enough connection, reset backoff for a quicker
            # reconnection. Do not reset if we're thrown out quickly due to failed
            # authorization or such.
            if time.monotonic() - established_time > 60:
                self.backoff.reset()

            for task in pending:
                task.cancel()
        except ConnectionRefusedError as ex:
            self.log.warning("Websocket connection refused: %r. Retrying.", ex)
        except (ConnectionTimeoutError, asyncio.TimeoutError, CancelledError) as ex:
            self.log.warning("Websocket connection timed out: %r. Retrying.", ex)
        except socket.gaierror as ex:
            self.log.error(
                "DNS lookup for websocket endpoint or SOCKS5 proxy failed: %r. Retrying.",
                ex,
            )
        except websockets.exceptions.InvalidStatusCode as ex:
            self.log.error(
                "Websocket server rejected connection with HTTP status code: %r. Retrying.",
                ex,
            )
        except (ProxyError, ProxyConnectionError, ProxyTimeoutError) as ex:
            self.log.warning("SOCKS5 proxy connection error: %r. Retrying.", ex)
        except ssl.SSLCertVerificationError as ex:
            self.log.error("Websocket certificate verification error: %r. Retrying.", ex)
        except OSError as ex:  # Network unreachable, etc, may happen sporadically
            self.log.warning("Websocket connection error: %r. Retrying.", ex)
        except Exception as ex:  # pylint:disable=broad-except
            self.log.exception("Unhandled exception occurred on websocket connection: %r", ex)
        finally:
            self.websocket = None

        for task in [consumer_task, stop_event_task]:
            if task:
                task.cancel()

    async def sleep_before_reconnect(self):
        if self.running:
            sleep_interval = self.backoff.next_sleep()
            self.log.info("Retrying websocket connection in %.1f", sleep_interval)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), sleep_interval)

    async def comms_channel_loop(self):
        while self.running:
            await self.comms_channel_round()
            await self.sleep_before_reconnect()

        self.stopped_event.set()
        self.log.info("Websocket closed")


class WebsocketSender(LogSender):
    def __init__(self, *, config, **kwargs):
        super().__init__(
            config=config,
            max_send_interval=config.get("max_send_interval", 1.0),
            **kwargs,
        )
        self.runner = None
        self.config = config

    def _init_websocket(self) -> None:
        self.log.info("Initializing Websocket client, address: %r for %s", self.config["websocket_uri"], self.name)

        if self.runner:
            self.runner.close()
            self.runner = None

        self.mark_disconnected()

        while self.running and not self.runner:
            # retry connection
            try:
                runner = WebsocketRunner(
                    websocket_uri=self.config["websocket_uri"],
                    socks5_proxy_url=self.config.get("socks5_proxy"),
                    ssl_enabled=self.config.get("ssl"),
                    ssl_ca=self.config.get("ca"),
                    ssl_key=self.config.get("keyfile"),
                    ssl_cert=self.config.get("certfile"),
                    websocket_compression=WebsocketCompression(
                        self.config.get("websocket_compression", WebsocketCompression.none)
                    ),
                    compression=JournalPumpMessageCompression(
                        self.config.get("compression", JournalPumpMessageCompression.snappy)
                    ),
                    max_batch_size=int(self.config.get("max_batch_size", 1024**2)),
                )
                runner.start()
            except Exception as ex:  # pylint:disable=broad-except
                self.mark_disconnected(ex)
                self.log.exception(
                    "Retriable error during Websocket initialization: %s: %s",
                    ex.__class__.__name__,
                    ex,
                )
                self._backoff()
            else:
                self.log.info("Initialized Websocket client, address: %r for %s", self.config["websocket_uri"], self.name)
                self.runner = runner

    def request_stop(self):
        super().request_stop()

        if self.runner:
            self.runner.close()
            self.runner = None

        self.mark_disconnected()

    def send_messages(self, *, messages, cursor):
        if not self.runner:
            self._init_websocket()
        try:
            if self.runner.websocket is None and self._connected:
                self.mark_disconnected()

            if self.runner.send(messages=messages):
                # mark_sent marks connected, too
                self.mark_sent(messages=messages, cursor=cursor)
                return True
        except (CancelledError, asyncio.CancelledError) as ex:
            self.mark_disconnected(ex)
            self.log.info("Send to websocket failed, connection was closed for %s", self.name)
            return False
        except Exception as ex:  # pylint: disable=broad-except
            self.mark_disconnected(ex)
            self.log.exception("Unexpected exception during send to websocket")
            self.stats.unexpected_exception(ex=ex, where="sender", tags=self.make_tags({"app": "journalpump"}))

        if self.runner:
            self._backoff()

        return False
