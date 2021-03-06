from __future__ import with_statement

from contextlib import closing, nested

from pymx.backend import MultiplexerBackend, PicklingMultiplexerBackend
from pymx.future import wait_all
# use backend's pickle -- we compare pickles literally
from pymx.backend import pickle
from pymx.protocol_constants import MessageTypes
from pymx.client import BackendError, OperationTimedOut, OperationFailed

from nose.tools import eq_, nottest, raises

from .testlib_client import create_test_client
from .testlib_mxserver import JmxServerThread
from .testlib_threads import TestThread, check_threads
from .test_constants import MessageTypes as TestMessageTypes, PeerTypes as \
        TestPeerTypes
from .testlib_timed import timedcontext

server = None

def setup_module():
    global server
    server = JmxServerThread.run_threaded()

def teardown_module():
    server.close()

@nottest
def create_test_backend(addresses=(), impl=MultiplexerBackend, handler=None,
        type=380):
    return closing(impl(addresses=addresses, type=type, handler=handler))

def _backend_echo(mxmsg):
    return dict((field, getattr(mxmsg, field)) for field in ('message',
        'type'))

def test_query():

    class MultiplexerBackendSubclass(MultiplexerBackend):
        def handle_message(self, mxmsg):
            self.send_message(message=mxmsg.message, type=1137)

    class PicklingMultiplexerBackendSubclass(PicklingMultiplexerBackend):
        def process_pickle(self, data):
            self.send_pickle(data)

    yield check_query, MultiplexerBackend, {'handler':
            _backend_echo}
    yield check_query, PicklingMultiplexerBackend, {'handler':
            lambda x: x}
    yield check_query, MultiplexerBackendSubclass
    yield check_query, PicklingMultiplexerBackendSubclass

@check_threads
def check_query(backend_factory, backend_kwargs={}):

    message = pickle.dumps('some data')
    backend_kwargs.setdefault('type', 380)

    with nested(create_test_client(), closing(backend_factory(
        **backend_kwargs))) as (client, backend):

        with timedcontext(4):
            client.connect(server.server_address, sync=True)

            raises(OperationFailed)(lambda: client.query(fields={'to':
                backend.instance_id, 'workflow': 'some workflow'},
                message=message, type=1136, timeout=1))()

            backend.connect(server.server_address, sync=True)

            th = TestThread(target=backend.handle_one)
            th.setDaemon(True)
            th.start()

            response = client.query(fields={'to': backend.instance_id,
                'workflow': 'some workflow'}, message=message, type=1136,
                timeout=1)

            eq_(response.message, message)
            eq_(response.from_, backend.instance_id)
            eq_(response.to, client.instance_id)
            eq_(response.workflow, 'some workflow')

            th.join()

def test_query_retransmission():
    for notify in (False, True):
        yield check_query_retransmission, '_be_nice', notify
        yield check_query_retransmission, '_no_response', notify
        yield check_query_retransmission, '_raise_exception', \
                notify
        yield check_query_retransmission, '_send_other', notify

@check_threads
def check_query_retransmission(how, notify):
    class MultiplexerBackendSubclass(MultiplexerBackend):
        __first = True
        __mxmsg = None

        def _be_nice(self):
            self.send_message(message=self.__mxmsg.message,
                    type=TestMessageTypes.TEST_RESPONSE)
            self.shutdown()

        def _no_response(self):
            pass

        def _raise_exception(self):
            raise Exception("there never will be any response")

        def _send_other(self):
            self.send_message(type=MessageTypes.PING, to=self.instance_id,
                    references=567890)

        def handle_message(self, mxmsg):
            if notify:
                self.notify_started()
            eq_(mxmsg.type, TestMessageTypes.TEST_REQUEST)
            self.__mxmsg = mxmsg
            if self.__first:
                self.__first = False
                getattr(self, how)()
            else:
                self._be_nice()

    message = pickle.dumps('some data')
    backend_factory = MultiplexerBackendSubclass
    backend_kwargs = {'type': TestPeerTypes.TEST_SERVER}

    with nested(create_test_client(), closing(backend_factory(
        **backend_kwargs))) as (client, backend):

        wait_all(client.connect(server.server_address),
                backend.connect(server.server_address), timeout=0.5)

        th = TestThread(target=backend.start)
        th.setDaemon(True)
        th.start()

        with timedcontext(2):
            # backend replies at the first time according to `how`, the
            # second time it's all right
            response = client.query(fields={'workflow': 'some workflow'},
                    message=message, type=TestMessageTypes.TEST_REQUEST,
                    timeout=0.5)

        eq_(response.message, message)
        eq_(response.type, TestMessageTypes.TEST_RESPONSE)
        eq_(response.from_, backend.instance_id)
        eq_(response.to, client.instance_id)
        eq_(response.workflow, 'some workflow')

        th.join()

    with nested(create_test_client(), closing(backend_factory(
        **backend_kwargs))) as (client, backend):

        wait_all(client.connect(server.server_address),
                backend.connect(server.server_address), timeout=0.5)

        th = TestThread(target=backend.handle_one)
        th.setDaemon(True)
        th.start()

        try:
            with timedcontext(2):
                # backend replies at the first time according to `how`,
                # then it's gone (handle_one is used in the backend thread)
                response = client.query(fields={'workflow':
                    'some workflow'}, message=message,
                    type=TestMessageTypes.TEST_REQUEST, timeout=0.5,
                    skip_resend=True)

        except BackendError:
            assert how == '_raise_exception'

        except OperationTimedOut:
            assert how in ('_no_response', '_send_other')

        else:
            assert how == '_be_nice'
            eq_(response.message, message)
            eq_(response.type, TestMessageTypes.TEST_RESPONSE)
            eq_(response.from_, backend.instance_id)
            eq_(response.to, client.instance_id)
            eq_(response.workflow, 'some workflow')

        th.join()
