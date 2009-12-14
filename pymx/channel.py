
import weakref
import socket

from asyncore import dispatcher

from .frame import Deframer
from .message import MultiplexerMessage
from .protocol import WelcomeMessage
from .protobuf import parse_message
from .bytesfifo import BytesFIFO

class Channel(dispatcher):

    write_buffer = 1024
    read_buffer = 8192
    ignore_log_types = ()

    def __init__(self, manager, address):
        map = manager.channel_map
        self._manager = weakref.ref(manager)
        dispatcher.__init__(self, map=map)
        self._outgoing_buffer = BytesFIFO(join_upto=self.write_buffer)
        self._deframer = Deframer()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(address)

    @property
    def manager(self):
        manager = self._manager()
        if manager is None:
            raise RuntimeError("The manager is gone.")
        return manager

    def writable(self):
        return self._outgoing_buffer or not self.connected

    def handle_connect(self):
        # send the welcome packet, etc.
        self.manager.handle_connect(self)

    def handle_read(self):
        for contents in self._deframer.push(self.recv(self.read_buffer)):
            message = parse_message(MultiplexerMessage, contents)
            self._receive_message(message)

    def handle_close(self):
        self.close()

    def handle_write(self):
        if not self._outgoing_buffer:
            return
        written = self.send(self._outgoing_buffer.next_chunk)
        assert written > 0
        popped = self._outgoing_buffer.get(written)
        assert len(popped) == written

    def enque_outgoing(self, bytes):
        if not bytes:
            return
        self._outgoing_buffer.append(bytes)
        self.handle_write()
