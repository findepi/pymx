from __future__ import absolute_import, with_statement

import time
from contextlib import closing, nested
from functools import partial

from nose.tools import eq_, assert_almost_equal, timed, raises

from pymx.protobuf import dict_message
from pymx.client import Client, OperationTimedOut
from pymx.protocol import HEARTBIT_READ_INTERVAL
from pymx.future import wait_all, FutureError

from .testlib_threads import TestThread
from .testlib_mxserver import SimpleMxServerThread, JmxServerThread, \
        create_mx_server_context
from .testlib_client import create_test_client

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

    with closing(Client(type=317)):
        pass

    with create_test_client():
        pass

def test_client_connect():
    yield check_client_connect, SimpleMxServerThread
    yield check_client_connect, JmxServerThread

def check_client_connect(server_impl):
    with create_mx_server_context(impl=server_impl) as server:
        client = Client(type=317)
        eq_(client.type, 317)
        future = client.connect(server.server_address)
        if server_impl is SimpleMxServerThread:
            # we can't use conect future, as we can't rely on
            # SimpleMxServerThread correctly sending welcome messages
            time.sleep(0.07)
        else:
            future.wait(0.2)

        msg = client.create_message(id=5)
        assert_almost_equal(msg.timestamp, time.time(), -1)
        eq_(dict(dict_message(msg), timestamp=None), {'timestamp': None, 'id':
            5, 'from': client.instance_id})

        client.close()

def test_client_connect_ping_self():
    with create_mx_server_context() as server:
        with create_test_client()  as client:
            client.connect(server.server_address).wait(0.2)
            _check_ping(client)

def test_sending_hearbits():
    with create_mx_server_context() as server:
        with create_test_client() as client:
            client.connect(server.server_address)
            time.sleep(HEARTBIT_READ_INTERVAL + 3)
            _check_ping(client)

def _check_ping(client, to=None, event=False):
    if to is None:
        to = client
    msg = client.create_message(to=to.instance_id, type=0)
    if event:
        client.event(msg)
    else:
        client.send_message(msg)
    other = to.receive(timeout=5)
    eq_(msg, other)

def test_client_connect_event_self():
    with create_mx_server_context() as server:
        with create_test_client() as client:
            client.connect(server.server_address).wait(0.2)
            _check_ping(client, event=True)

def test_two_clients():
    with nested(create_mx_server_context(), create_test_client(),
            create_test_client()) as (server, client_a, client_b):
        wait_all(client_a.connect(server.server_address),
                client_b.connect(server.server_address), timeout=0.5)
        _check_ping(client_a, to=client_b)
        _check_ping(client_a, to=client_b, event=True)

@timed(2)
def test_synchronous_connect():
    with create_test_client() as client:
        # should not run Multiplexer server on port 1
        future = client.connect(('127.0.0.1', 1))
        raises(FutureError)(lambda: future.wait(1))()
    with create_test_client() as client:
        raises(FutureError)(
                lambda: client.connect(('127.0.0.1', 1), sync=True))()

@timed(9)
def test_deduplication():
    with nested(create_mx_server_context(), create_mx_server_context(),
            create_test_client()) as (server_a, server_b, client):
        wait_all(client.connect(server_a.server_address),
                client.connect(server_b.server_address), timeout=0.5)
        msg = client.create_message(to=client.instance_id, type=0)
        client.event(msg)
        first = client.receive(timeout=1)
        eq_(msg, first)
        try:
            second = client.receive(timeout=1)
        except OperationTimedOut:
            pass
        else:
            eq_(first, second)
            assert False, "duplicated message received"

def _echo(client):
    msg = client.receive(timeout=5)
    response = client.create_message(to=msg.from_, message=msg.message,
            type=msg.type, references=msg.id)
    client.send_message(response)

@timed(4)
def test_query():
    with nested(create_mx_server_context(), create_test_client(),
            create_test_client()) as (server, client_a, client_b):

        wait_all(client_a.connect(server.server_address),
                client_b.connect(server.server_address), timeout=0.5)

        th = TestThread(target=partial(_echo, client_b))
        th.setDaemon(True)
        th.start()

        response = client_a.query(fields={'to': client_b.instance_id},
                message='nictuniema', type=1136, timeout=1)

        eq_(response.type, 1136)
        eq_(response.message, 'nictuniema')
        eq_(response.from_, client_b.instance_id)
        eq_(response.to, client_a.instance_id)

        th.join()
