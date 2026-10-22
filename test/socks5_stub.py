# Copyright 2026, Aiven, https://aiven.io/
#
# This file is under the Apache License, Version 2.0.
# See the file `LICENSE` for details.

import contextlib
import logging
import select
import socket
import threading


class Socks5StubServer(threading.Thread):
    """Minimal SOCKS5 proxy stub (RFC 1928): CONNECT with transparent TCP relay.

    No-auth by default; when credentials are given, clients must authenticate with
    the username/password method (RFC 1929).
    """

    def __init__(self, *, username: str | None = None, password: str | None = None) -> None:
        super().__init__()
        self.daemon = True
        self.log = logging.getLogger(self.__class__.__name__)
        self.username = username
        self.password = password
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_sock.bind(("127.0.0.1", 0))
        self.listen_sock.listen(5)
        self.listen_sock.settimeout(0.5)
        self.port: int = self.listen_sock.getsockname()[1]
        self.stop_event = threading.Event()
        self.connect_targets: list[tuple[str, int]] = []
        self.auth_attempts: list[tuple[str, str]] = []
        self.bytes_relayed = 0
        self._state_lock = threading.Lock()
        self.open_sockets: set[socket.socket] = set()

    @staticmethod
    def _recv_exact(conn: socket.socket, size: int) -> bytes:
        data = b""
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                raise ConnectionError("connection closed during SOCKS5 handshake")
            data += chunk
        return data

    def _read_greeting(self, conn: socket.socket) -> None:
        ver, nmethods = self._recv_exact(conn, 2)
        if ver != 0x05:
            raise ConnectionError("unsupported SOCKS5 greeting")
        methods = self._recv_exact(conn, nmethods)
        if self.username is None:
            # only the no-authentication method is supported
            if 0x00 not in methods:
                conn.sendall(b"\x05\xff")
                raise ConnectionError("unsupported SOCKS5 greeting")
            conn.sendall(b"\x05\x00")
            return
        # credentials configured: require the username/password authentication method
        if 0x02 not in methods:
            conn.sendall(b"\x05\xff")
            raise ConnectionError("client did not offer the username/password authentication method")
        conn.sendall(b"\x05\x02")
        self._read_auth(conn)

    def _read_auth(self, conn: socket.socket) -> None:
        """Username/password authentication sub-negotiation (RFC 1929)."""
        ver, ulen = self._recv_exact(conn, 2)
        if ver != 0x01:
            raise ConnectionError(f"unsupported SOCKS5 auth version: {ver:#02x}")
        username = self._recv_exact(conn, ulen).decode("utf-8")
        (plen,) = self._recv_exact(conn, 1)
        password = self._recv_exact(conn, plen).decode("utf-8")
        self.auth_attempts.append((username, password))
        authenticated = (username, password) == (self.username, self.password)
        conn.sendall(bytes([0x01, 0x00 if authenticated else 0x01]))
        if not authenticated:
            raise ConnectionError(f"SOCKS5 authentication failed for user {username!r}")

    def _read_connect_request(self, conn: socket.socket) -> tuple[str, int]:
        ver, cmd, _rsv, atyp = self._recv_exact(conn, 4)
        if ver != 0x05 or cmd != 0x01:  # only CONNECT is supported
            raise ConnectionError("unsupported SOCKS5 request")
        if atyp == 0x01:  # IPv4
            host = socket.inet_ntoa(self._recv_exact(conn, 4))
        elif atyp == 0x03:  # domain name
            (host_len,) = self._recv_exact(conn, 1)
            host = self._recv_exact(conn, host_len).decode("idna")
        elif atyp == 0x04:  # IPv6
            host = socket.inet_ntop(socket.AF_INET6, self._recv_exact(conn, 16))
        else:
            raise ConnectionError(f"unsupported SOCKS5 address type: {atyp:#02x}")
        port = int.from_bytes(self._recv_exact(conn, 2), "big")
        return host, port

    def _handle_client(self, conn: socket.socket) -> None:
        upstream: socket.socket | None = None
        try:
            self._read_greeting(conn)
            target = self._read_connect_request(conn)
            self.log.info("SOCKS5: CONNECT request to %s", target)
            self.connect_targets.append(target)
            try:
                upstream = socket.create_connection(target, timeout=5)
            except OSError:
                # failure reply: VER=5, REP=5 (connection refused), RSV=0, bound address IPv4 0.0.0.0:0
                conn.sendall(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
                raise
            with self._state_lock:
                self.open_sockets.add(upstream)
            # success reply: VER=5, REP=0 (succeeded), RSV=0, bound address IPv4 0.0.0.0:0
            conn.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            self._relay(conn, upstream)
        except (ConnectionError, OSError) as ex:
            self.log.info("SOCKS5: connection failed: %s", ex)
        finally:
            conn.close()
            with self._state_lock:
                self.open_sockets.discard(conn)
                if upstream is not None:
                    upstream.close()
                    self.open_sockets.discard(upstream)

    def _relay(self, conn: socket.socket, upstream: socket.socket) -> None:
        peers = {conn: upstream, upstream: conn}
        relay_peers = set(peers)
        while relay_peers and not self.stop_event.is_set():
            try:
                readable, _, _ = select.select(list(relay_peers), [], [], 0.5)
            except OSError:
                break
            for src in readable:
                try:
                    data = src.recv(65536)
                except OSError:
                    data = b""
                if not data:
                    # half-close: stop reading from src and signal EOF to its peer
                    relay_peers.discard(src)
                    with contextlib.suppress(OSError):
                        peers[src].shutdown(socket.SHUT_WR)
                    continue
                try:
                    peers[src].sendall(data)
                except OSError:
                    relay_peers.discard(src)
                    continue
                with self._state_lock:
                    self.bytes_relayed += len(data)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                conn, _addr = self.listen_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            self.log.info("SOCKS5: client connection accepted")
            with self._state_lock:
                self.open_sockets.add(conn)
            threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
        self.log.info("SOCKS5: stopped")

    def stop(self) -> None:
        self.stop_event.set()
        self.listen_sock.close()
        with self._state_lock:
            sockets = list(self.open_sockets)
        for sock in sockets:
            with contextlib.suppress(OSError):
                sock.close()
        self.join(timeout=5)
