
import weakref
import socket

from asyncore import dispatcher
from google.protobuf.message import Message

from .frame import Deframer, create_frame_header
from .message import MultiplexerMessage
from .protocol import WelcomeMessage
from .protobuf import parse_message
from .bytesfifo import BytesFIFO

class Channel(dispatcher):

    write_buffer = 1024
    read_buffer = 8192
    ignore_log_types = ()

    def __init__(self, manager, address, connect_future=None, reconnect=None):
        map = manager.channel_map
        self._manager = weakref.ref(manager)
        dispatcher.__init__(self, map=map)
        self._outgoing_buffer = BytesFIFO(join_upto=self.write_buffer)
        self._deframer = Deframer()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self._address = address
        self.protocol_initialized = False
        self._reconnect = reconnect
        self._connect_future = connect_future
        self.connect(address)

    def __del__(self):
        if self._connect_future is not None:
            self._connect_future.set_error(message="No connection could be "
                    "estabilished")
            self._connect_future = None

    @property
    def manager(self):
        manager = self._manager()
        if manager is None:
            raise RuntimeError("The manager is gone.")
        return manager

    @property
    def address(self):
        return self._address

    @property
    def reconnect(self):
        return self._reconnect

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
        self.manager.handle_disconnect(self)
        self.close()

    def close(self):
        if self._connect_future is not None:
            self._connect_future.set_error(message="Connection closed")
            self._connect_future = None
        dispatcher.close(self)

    def handle_write(self):
        if not self._outgoing_buffer:
            return
        written = self.send(self._outgoing_buffer.next_chunk)
        if written:
            popped = self._outgoing_buffer.get(written)
            assert len(popped) == written, (popped, written)

    def enque_outgoing(self, bytes):
        if isinstance(bytes, Message):
            bytes = bytes.SerializeToString()
            self.enque_outgoing(create_frame_header(bytes))
        if not bytes:
            return
        self._outgoing_buffer.append(bytes)
        self.handle_write()

    def _receive_message(self, message):
        self.manager.handle_message(message, self)
        if self._connect_future is not None:
            # first message is always CONNECTION_WELCOME sent by the server
            self._connect_future.set(self)
            self._connect_future = None
