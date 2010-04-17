from __future__ import absolute_import, with_statement

import time
import asyncore
import socket
from contextlib import closing

from pymx.connection import ConnectionsManager
from pymx.hacks.socket_pipe import socket_pipe
from pymx.protocol import WelcomeMessage
from pymx.message import MultiplexerMessage
from pymx.channel import Channel
from pymx.future import FutureError

from nose.tools import raises

from .testlib_mxserver import SimpleMxServerThread, JmxServerThread, \
        create_mx_server_context

def test_socket_pipe():
    reader, writer = socket_pipe()
    writer.sendall('there is nothing wrong\x00.')
    writer.close()
    reader.setblocking(True)
    assert reader.makefile('r').read() == 'there is nothing wrong\x00.'

def create_connections_manager():
    welcome = WelcomeMessage()
    welcome.id = 547
    welcome.type = 115
    welcome_message = MultiplexerMessage()
    welcome_message.from_ = 547
    welcome_message.message = welcome.SerializeToString()
    welcome_message.type = 2 # CONNECTION_WELCOME
    return ConnectionsManager(welcome_message=welcome_message)

def test_create_connections_manager():
    create_connections_manager()

def test_many_connections_managers():
    for x in xrange(512):
        create_connections_manager().close()

def test_testlib_mxserver():
    for impl in (SimpleMxServerThread, JmxServerThread):
        server = impl.run_threaded()
        server.close()
        server.thread.join()

def test_testlib_create_mx_server_context():
    yield check_mx_server_context, {'impl': SimpleMxServerThread}
    yield check_mx_server_context, {'impl': JmxServerThread}
    yield check_mx_server_context, {}

def check_mx_server_context(kwargs):
    with create_mx_server_context(**kwargs) as server:
        pass

    with create_mx_server_context(**kwargs) as server:
        with closing(socket.socket()) as so:
            so.connect(server.server_address)

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

def test_reconnect():
    """check reconnect after first successful connect"""
    with closing(socket.socket()) as so:
        so.bind(('localhost', 0))
        so.listen(5)
        so.settimeout(1.8)

        with closing(create_connections_manager()) as client:
            address = so.getsockname()
            client.connect(address, reconnect=1)
            so_channel, _ = so.accept()
            so_channel.close()
            # wait for the client to reconnect
            so_channel, _ = so.accept()

def test_reconnect_first_failed():
    """check reconnecting after first connect attempt failed"""
    # find free port
    with closing(socket.socket()) as so:
        so.bind(('localhost', 0))
        address = so.getsockname()

    with closing(create_connections_manager()) as client:
        future = client.connect(address, reconnect=0.5)
        raises(FutureError)(future.wait)()

        with closing(socket.socket()) as so:
            so.bind(address)
            so.listen(5)
            so.settimeout(0.8)
            # wait for the client to reconnect
            so_channel = so.accept()
