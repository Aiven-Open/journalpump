# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from collections import deque
from journalpump.journalpump import JournalPump
from journalpump.senders.websocket import WebsocketSender

import asyncio
import json
import logging
import snappy  # pylint: disable=import-error
import threading
import time
import websockets


class WebsocketMockServer(threading.Thread):
    def __init__(
        self,
        *,
        port,
    ):
        super().__init__()
        self.daemon = True
        self.log = logging.getLogger(self.__class__.__name__)
        self.port = port
        self.in_queue = deque()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.stop_event = asyncio.Event()
        self.running = False
        self.websocket_server = None

    async def handle_incoming_websocket_message(self, *, connection):
        self.log.info("WS: Start handling incoming websocket messages")
        async for message in connection:
            self.log.info("WS: Received message: %r", message)
            self.in_queue.append(message)

    async def process_connection(self, websocket, path):
        self.log.info("WS: Client connection accepted on %s", path)
        pending = set()

        try:
            incoming_websocket_task = asyncio.create_task(self.handle_incoming_websocket_message(connection=websocket))
            _, pending = await asyncio.wait(
                [incoming_websocket_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            raised = incoming_websocket_task.exception()
            if raised:
                self.log.info("WS: Client connection closed with exception: %s", raised)
            else:
                self.log.info("WS: Client connection closed cleanly")
        except asyncio.CancelledError:
            self.log.debug("WS: Client connection accept cancelled for node")
            raise
        finally:
            if pending:
                for task in pending:
                    self.log.debug("WS: Cancelling pending task of client: %r", task)
                    task.cancel()

    async def run_websocket_server(self):
        ctx = None
        # ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        # ctx.load_cert_chain(self.websocket_tls_chain, keyfile=self.websocket_tls_chain)
        # ctx.load_verify_locations(self.ca_certs)
        # ctx.verify_mode = ssl.CERT_REQUIRED

        # websockets uses lazy_import and pylint doesn't quite get it
        async with websockets.serve(  # pylint:disable=no-member
            self.process_connection,
            "127.0.0.1",
            self.port,
            loop=self.loop,
            ssl=ctx,
            close_timeout=10,
        ) as server:
            self.websocket_server = server
            self.log.info("WS: Started serving websocket connections")
            await self.stop_event.wait()

        self.log.info("WS: Stopped serving websocket connections")

    async def stop_websocket_server(self):
        if self.websocket_server:
            self.log.info("WS: Stopping websocket server")
            self.stop_event.set()
            self.websocket_server.close()
            await self.websocket_server.wait_closed()

    def run(self):
        self.running = True
        try:
            self.loop.run_until_complete(self.run_websocket_server())
        except websockets.exceptions.InvalidMessage as ex:
            self.log.info("WS server got InvalidMessage exception: %s", ex)
        except asyncio.CancelledError:
            self.log.info("WS client task cancelled; ignoring and exiting")
        self.running = False

    def stop(self):
        asyncio.run_coroutine_threadsafe(self.stop_websocket_server(), self.loop).result()
        all_tasks = asyncio.all_tasks(loop=self.loop)
        # after a clean shutdown, there should be no tasks to cancel
        for task in all_tasks:
            task.cancel()
        self.log.info("WS: stopped")


def assert_msgs_found(ws_server, *, messages, timeout):
    # Check that all of these messages were sent to the websocket server.
    give_up_at = time.monotonic() + timeout

    msgs = []
    while time.monotonic() < give_up_at:
        while len(ws_server.in_queue) > 0:
            msgs.append(ws_server.in_queue.popleft())
        if all(msg in msgs for msg in messages):
            return
        time.sleep(0.2)

    assert all(msg in msgs for msg in messages)


def setup_pump(tmpdir, sender_config):
    journalpump_path = str(tmpdir.join("journalpump.json"))
    config = {
        "readers": {
            "foo": {
                "senders": {
                    "bar": sender_config,
                },
            },
        },
    }
    with open(journalpump_path, "w") as fp:
        fp.write(json.dumps(config))
    pump = JournalPump(journalpump_path)

    # confirm there's a correct sender set up
    sender = None
    assert len(pump.readers) == 1
    for rn, r in pump.readers.items():
        assert rn == "foo"
        r.create_journald_reader_if_missing()
        assert len(r.senders) == 1
        r.running = False
        for sn, s in r.senders.items():
            assert sn == "bar"
            # s.running = False
            assert isinstance(s, WebsocketSender)
            sender = s

    assert sender
    return pump, sender


def test_producer_nobatch(caplog, tmpdir):
    caplog.set_level(logging.INFO)
    ws_server = WebsocketMockServer(
        port=10111,
    )
    ws_server.start()

    pump, sender = setup_pump(
        tmpdir,
        {
            "output_type": "websocket",
            "websocket_uri": "ws://127.0.0.1:10111/pump-pump",
            "compression": "none",
            "max_batch_size": 0,
        },
    )

    # send some messages, and confirm they come out on the other end
    messages = [
        b'{"timestamp": "2019-10-07 14:00:00"}',
        b'{"timestamp": "2019-10-07 15:00:00"}',
    ]
    assert sender.send_messages(messages=messages, cursor=None)
    assert_msgs_found(ws_server, messages=messages, timeout=5)

    ws_server.stop()
    pump.shutdown()


def test_producer_batch(caplog, tmpdir):
    caplog.set_level(logging.INFO)
    ws_server = WebsocketMockServer(
        port=10111,
    )
    ws_server.start()

    pump, sender = setup_pump(
        tmpdir,
        {
            "output_type": "websocket",
            "websocket_uri": "ws://127.0.0.1:10111/pump-pump",
            "compression": "snappy",
            "max_batch_size": 1024,
        },
    )

    # send some messages, and confirm they come out on the other end
    messages = [
        b'{"timestamp": "2019-10-07 14:00:00"}',
        b'{"timestamp": "2019-10-07 15:00:00"}',
    ]
    expected_output = [snappy.snappy.compress(b"\x00".join(messages))]
    assert sender.send_messages(messages=messages, cursor=None)
    assert_msgs_found(ws_server, messages=expected_output, timeout=5)

    ws_server.stop()
    pump.shutdown()
