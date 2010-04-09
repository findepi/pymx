
from socket import socket as socket_

from .frame import create_frame_header, Deframer
from .protobuf import parse_message
from .message import MultiplexerMessage

class Channel(object):

    """Blocking channel implementation."""

    receive_bufsize = 4096

    def __init__(self, socket=None, address=None):
        object.__init__(self)
        if socket is None:
            if address is not None:
                socket = socket_()
                socket.connect(address)
            else:
                raise ValueError("No socket and address.")

        assert isinstance(socket, socket_)
        self._socket = socket
        self._receive_iter = iter(())
        self._deframer = Deframer()

    def close(self):
        self._socket.close()
        self._deframer = self._receive_ite = None

    def send(self, message):
        frame_contents = message.SerializeToString()
        self._socket.sendall(create_frame_header(frame_contents))
        self._socket.sendall(frame_contents)

    def receive(self):
        message = None
        while self._socket is not None:
            try:
                message_bytes = self._receive_iter.next()
            except StopIteration:
                received_bytes = self._socket.recv(self.receive_bufsize)
                if received_bytes:
                    self._receive_iter = self._deframer.push(received_bytes)
                    continue
                else:
                    self._socket = None
            else:
                message = parse_message(MultiplexerMessage, message_bytes)
            break

        return message
