from __future__ import with_statement

import os
import socket
import traceback
import errno
from threading import Thread, RLock

from pymx.frame import Deframer
from pymx.protobuf import parse_message
from pymx.message import MultiplexerMessage

class SimpleMxServerThread(object):

    def __init__(self):
        self._lock = RLock()
        self._sock = socket.socket()
        self._sock.bind(('127.0.0.1', 0))
        self.server_address = self._sock.getsockname()
        self._sock.listen(5)
        self.message_counters = {}
        self._client = None
        self._shutdown_called = False

    @staticmethod
    def run_threaded(*args, **kwargs):
        server = SimpleMxServerThread(*args, **kwargs)
        thread = Thread(target=server.run)
        thread.setDaemon(True)
        thread.start()
        return server, thread

    def run(self):
        try:
            while True:
                client, address = self._sock.accept()
                with self._lock:
                    if self._shutdown_called:
                        client.close()
                    else:
                        self._client = client
                client_welcome = self._read_message(client)
                self._inc_counters(client_welcome)
                client.close()
                with self._lock:
                    self._client = None
        except socket.error, e:
            if e.errno in (errno.EINVAL, errno.EBADF):
                pass
            else:
                raise

    def shutdown(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()
        with self._lock:
            self._shutdown_called = True
            if self._client is not None:
                try:
                    self._client.shutdown(socket.SHUT_RDWR)
                    self._client.close()
                except Exception:
                    traceback.print_exc()

    def _inc_counters(self, message):
        self.message_counters[message.type] = self.message_counters.setdefault(
                message.type, 0) + 1

    @staticmethod
    def _read_message(sock):
        input = Deframer()
        while True:
            for contents in input.push(sock.recv(1)):
                message = parse_message(MultiplexerMessage, contents)
                return message
