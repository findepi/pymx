import time
import asyncore
import socket

from pymx.connection import ConnectionsManager, _socket_pipe
from pymx.protocol import WelcomeMessage
from pymx.message import MultiplexerMessage
from pymx.channel import Channel
from pymx.client import Client

from .testlib_mxserver import SimpleMxServerThread

def test_socket_pipe():
    reader, writer = _socket_pipe()
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

def test_testlib_mxserver():
    server, server_thread = SimpleMxServerThread.run_threaded()
    server.shutdown()
    server_thread.join()

def test_testlib_mxserver_connect():
    server, server_thread = SimpleMxServerThread.run_threaded()
    so = socket.socket()
    so.connect(server.server_address)
    server.shutdown()
    server_thread.join()


def test_channel_connect():
    server, server_thread = SimpleMxServerThread.run_threaded()

    class Dict(dict):
        pass

    class Manager(object):
        handle_connect_called = False
        def handle_connect(self, channel):
            self.handle_connect_called = True
            channel.close()

    manager = Manager()
    manager.channel_map = Dict()

    Channel(manager=manager, address=server.server_address)
    asyncore.loop(map=manager.channel_map)

    assert manager.handle_connect_called
    server.shutdown()
    server_thread.join()

def test_manager_connect():
    server, server_thread = SimpleMxServerThread.run_threaded()
    manager = create_connections_manager()
    manager.connect(server.server_address)
    time.sleep(0.07) # TODO wait for connection (with timeout)
    manager.shutdown()
    assert sum(server.message_counters.values()) == 1, server.message_counters
    assert server.message_counters[2] == 1
    server.shutdown()
    server_thread.join()

def test_client_connect():
    server, server_thread = SimpleMxServerThread.run_threaded()
    client = Client(type=317)
    client.connect(server.server_address)
    time.sleep(0.07) # TODO wait for connection (with timeout)
    client.shutdown()
    server.shutdown()
    server_thread.join()
