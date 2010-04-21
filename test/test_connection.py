from __future__ import absolute_import, with_statement

import time
import asyncore
import socket
from contextlib import closing, nested

from pymx.connection import ConnectionsManager
from pymx.hacks.socket_pipe import socket_pipe
from pymx.protocol import WelcomeMessage
from pymx.message import MultiplexerMessage
from pymx.channel import Channel
from pymx.future import FutureError
from pymx.protocol_constants import MessageTypes, PeerTypes
from pymx.protobuf import make_message
from pymx.frame import create_frame

from nose.tools import raises, timed

from .testlib_mxserver import SimpleMxServerThread, JmxServerThread, \
        create_mx_server_context
from .testlib_threads import check_threads

@check_threads
def test_socket_pipe():
    reader, writer = socket_pipe()
    writer.sendall('there is nothing wrong\x00.')
    writer.close()
    reader.setblocking(True)
    assert reader.makefile('r').read() == 'there is nothing wrong\x00.'

def create_connections_manager(multiplexer_password=None):
    welcome = WelcomeMessage()
    welcome.id = 547
    welcome.type = 115
    if multiplexer_password is not None:
        welcome.multiplexer_password = multiplexer_password
    welcome_message = MultiplexerMessage()
    welcome_message.from_ = 547
    welcome_message.message = welcome.SerializeToString()
    welcome_message.type = MessageTypes.CONNECTION_WELCOME
    return ConnectionsManager(welcome_message=welcome_message,
            multiplexer_password=multiplexer_password)

@check_threads
def test_create_connections_manager():
    create_connections_manager().close()

@check_threads
def test_with_connections_manager():
    with closing(create_connections_manager()):
        pass

@check_threads
def test_many_connections_managers():
    for x in xrange(512):
        create_connections_manager().close()

@check_threads
def test_testlib_mxserver():
    for impl in (SimpleMxServerThread, JmxServerThread):
        server = impl.run_threaded()
        server.close()
        server.thread.join()

@check_threads
def test_testlib_create_mx_server_context():
    yield check_mx_server_context, {'impl': SimpleMxServerThread}
    yield check_mx_server_context, {'impl': JmxServerThread}
    yield check_mx_server_context, {}

@check_threads
def check_mx_server_context(kwargs):
    with create_mx_server_context(**kwargs) as server:
        pass

    with create_mx_server_context(**kwargs) as server:
        with closing(socket.socket()) as so:
            so.connect(server.server_address)

@check_threads
def test_channel_connect():

    class Manager(object):
        def __init__(self):
            self.channel_map = {}

        handle_connect_called = False
        def handle_connect(self, channel):
            self.handle_connect_called = True
            channel.close()

    with create_mx_server_context(impl=SimpleMxServerThread) as server:
        manager = Manager()
        Channel(manager=manager, address=server.server_address)
        asyncore.loop(map=manager.channel_map)
        assert manager.handle_connect_called

@check_threads
def test_manager_connect():
    with create_mx_server_context(impl=SimpleMxServerThread) as server:
        manager = create_connections_manager()
        manager.connect(server.server_address)
        # we can't use conect future, as we can't rely on SimpleMxServerThread
        # correctly sending welcome messages
        time.sleep(0.07)

        manager.close()
        assert sum(server.message_counters.values()) == 1, \
                server.message_counters
        assert server.message_counters[2] == 1

@check_threads
def test_reconnect():
    with closing(socket.socket()) as so:
        so.bind(('localhost', 0))
        so.listen(5)
        so.settimeout(1.8)

        with closing(create_connections_manager()) as client:
            address = so.getsockname()
            client.connect(address, reconnect=0.1)
            so_channel, _ = so.accept()
            so_channel.close()
            # wait for the client to reconnect
            so_channel, _ = so.accept()

@check_threads
def test_reconnect_first_failed():
    # find free port
    with closing(socket.socket()) as so:
        so.bind(('localhost', 0))
        address = so.getsockname()

    with closing(create_connections_manager()) as client:
        future = client.connect(address, reconnect=0.1)
        raises(FutureError)(future.wait)() # so is closed

        with closing(socket.socket()) as so:
            so.bind(address)
            so.listen(5)
            so.settimeout(0.8)
            # wait for the client to reconnect
            so_channel = so.accept()

def test_connect_no_welcome_fails():
    with nested(closing(socket.socket()), closing(create_connections_manager())
            ) as (so, client):
        so.bind(('localhost', 0))
        so.listen(1)
        address = so.getsockname()
        future = client.connect(address)
        so.accept()[0].close()
        raises(FutureError)(lambda: future.wait(timeout=1))()

def _send_welcome(so, type=PeerTypes.MULTIPLEXER, id=56987,
        multiplexer_password=None):
    welcome = make_message(WelcomeMessage, type=type, id=id,
            multiplexer_password=multiplexer_password)
    welcome_message = make_message(MultiplexerMessage,
            type=MessageTypes.CONNECTION_WELCOME,
            message=welcome.SerializeToString())
    so.sendall(create_frame(
        welcome_message.SerializeToString()))

def test_connect_welcome_ok():
    with nested(closing(socket.socket()), closing(create_connections_manager())
            ) as (so, client):
        so.bind(('localhost', 0))
        so.listen(1)
        address = so.getsockname()
        future = client.connect(address)
        with closing(so.accept()[0]) as so_channel:
            _send_welcome(so_channel)
            future.wait(timeout=0.3)

def test_channel_active_after_handshake():
    yield check_channel_active_after_handshake
    yield check_channel_active_after_handshake, ''
    yield check_channel_active_after_handshake, 'a password'

@timed(1)
@check_threads
def check_channel_active_after_handshake(multiplexer_password=None):
    with nested(closing(socket.socket()), closing(create_connections_manager( \
            multiplexer_password=multiplexer_password))) as (so, client):
        so.bind(('localhost', 0))
        so.listen(1)
        address = so.getsockname()
        msg = make_message(MultiplexerMessage, type=0)

        connect_future = client.connect(address)
        send_future = client.send_message(msg,
                connection=ConnectionsManager.ONE)
        # so has not yet replied with CONNECTION_WELCOME
        raises(FutureError)(lambda: send_future.wait(timeout=0.3))()

        with closing(so.accept()[0]) as so_channel:
            _send_welcome(so_channel,
                    multiplexer_password=multiplexer_password)
            connect_future.wait(timeout=0.3)

            # so has replies, connect_future returned, connection is active
            client.send_message(msg, connection=ConnectionsManager.ONE).wait(
                    timeout=0.3)
