from __future__ import absolute_import, with_statement

import time
import contextlib

from nose.tools import eq_, assert_almost_equal

from pymx.protobuf import dict_message
from pymx.client import Client

from .testlib_mxserver import SimpleMxServerThread, JmxServerThread, \
        create_mx_server_context

def test_client_shutdown():
    c = Client(type=317)
    # no close

    c = Client(type=317)
    c.close()

    c = Client(type=317)
    c.close()

    c = Client(type=317)
    c.close()
    c.close() # redundant

    with contextlib.closing(Client(type=317)):
        pass

def test_client_connect():
    yield check_client_connect, SimpleMxServerThread
    yield check_client_connect, JmxServerThread

def check_client_connect(server_impl):
    with create_mx_server_context(impl=server_impl) as server:
        client = Client(type=317)
        eq_(client.type, 317)
        client.connect(server.server_address)
        time.sleep(0.07) # TODO wait for connection (with timeout)

        msg = client.create_message(id=5)
        assert_almost_equal(msg.timestamp, time.time(), -1)
        eq_(dict(dict_message(msg), timestamp=None), {'timestamp': None, 'id':
            5, 'from': client.instance_id})

        client.close()

def test_client_connect_ping_self():
    with create_mx_server_context() as server:
        with contextlib.closing(Client(type=317)) as client:
            client.connect(server.server_address)
            time.sleep(0.07) # TODO wait for connection (with timeout)
            msg = client.create_message(to=client.instance_id, type=0)
            client.send_message(msg)
            other = client.receive(timeout=5)
            eq_(msg, other)
