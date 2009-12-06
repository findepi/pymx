
import socket

from pymx.naive_channel import Channel
from pymx.frame import create_frame_header
from pymx.protobuf import dict_message, parse_message
from pymx.message import MultiplexerMessage

from unittest import TestCase
import mox

from .testlib_encoded_messages import encoded_messages
from .testlib_chop_bytes import chop_bytes
from .testlib_encoded_messages import encoded_messages

class Socket(socket.socket):
    """Dummy ``socket`` implementation -- mox doesn't deal well with built-in
    types (mocking ``socket.socket.recv`` wasn't possible). """
    def recv(self, bufsize):
        raise NotImplementedError

def test_read_message():

    bufsize = 1024
    input_bytes = ''.join(
        bytes for case in encoded_messages for bytes in
                (create_frame_header(case['encoded']), case['encoded']))

    mock = mox.Mox()
    mock_sock = mock.CreateMock(Socket)

    collected_chunks = []
    for chunk in chop_bytes(input_bytes, 5):
        collected_chunks.append(chunk)
        mock_sock.recv(bufsize).AndReturn(chunk)
    else:
        # This does in fact test chop_bytes and nothing more.
        assert input_bytes == ''.join(collected_chunks)

        mock_sock.recv(bufsize).AndReturn('')

    mock.ReplayAll()
    channel = Channel(socket=mock_sock)
    channel.receive_bufsize = bufsize

    for case in encoded_messages:
        message = channel.receive()
        assert message is not None
        assert dict_message(message) == case['pythonized']

    assert channel.receive() is None
    assert channel.receive() is None
    assert channel.receive() is None

    mock.VerifyAll()


class TestSendMessage(TestCase):

    def setUp(self):
        self._server = socket.socket()
        self._server.bind(('localhost', 0))
        self._server.listen(1)

        self._channel = Channel(address=self._server.getsockname())
        self._server_channel = Channel(socket=self._server.accept()[0])

    def tearDown(self):
        self._server_channel.close()
        self._server.close()
        self._channel.close()

    def test_send_message(self):
        for case in encoded_messages:
            self._channel.send(parse_message(MultiplexerMessage,
                case['encoded']))

            assert dict_message(self._server_channel.receive()) == \
                    case['pythonized']
