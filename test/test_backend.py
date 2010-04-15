
from contextlib import closing, nested

from pymx.backend import MultiplexerBackend, PicklingMultiplexerBackend
from pymx.future import wait_all
# use backend's pickle -- we compare pickles literally
from pymx.backend import pickle
from pymx.protocol_constants import MessageTypes

from nose.tools import timed, eq_, nottest

from .testlib_client import create_test_client
from .testlib_mxserver import create_mx_server_context
from .testlib_threads import TestThread
from .test_constants import MessageTypes as TestMessageTypes, PeerTypes as \
        TestPeerTypes

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

    yield check_query, MultiplexerBackend, {'handler': _backend_echo}
    yield check_query, PicklingMultiplexerBackend, {'handler': lambda x: x}
    yield check_query, MultiplexerBackendSubclass
    yield check_query, PicklingMultiplexerBackendSubclass

@timed(4)
def check_query(backend_factory, backend_kwargs={}):

    message = pickle.dumps('some data')
    backend_kwargs.setdefault('type', 380)

    with nested(create_mx_server_context(), create_test_client(),
            closing(backend_factory(**backend_kwargs))) as (server, client,
                    backend):

        wait_all(client.connect(server.server_address),
                backend.connect(server.server_address), timeout=0.5)

        th = TestThread(target=backend.handle_one)
        th.setDaemon(True)
        th.start()

        response = client.query(fields={'to': backend.instance_id, 'workflow':
            'some workflow'}, message=message, type=1136, timeout=1)

        eq_(response.message, message)
        eq_(response.from_, backend.instance_id)
        eq_(response.to, client.instance_id)
        eq_(response.workflow, 'some workflow')

        th.join()

def test_query_retransmission():
    for notify in (False, True):
        yield check_query_retransmission, '_be_nice', notify
        yield check_query_retransmission, '_no_response', notify
        yield check_query_retransmission, '_raise_exception', notify
        yield check_query_retransmission, '_send_other', notify

@timed(4)
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
            eq_(mxmsg.from_, client.instance_id)
            self.__mxmsg = mxmsg
            if self.__first:
                self.__first = False
                getattr(self, how)()
            else:
                self._be_nice()

    message = pickle.dumps('some data')
    backend_factory = MultiplexerBackendSubclass
    backend_kwargs = {'type': TestPeerTypes.TEST_SERVER}

    with nested(create_mx_server_context(), create_test_client(),
            closing(backend_factory(**backend_kwargs))) as (server, client,
                    backend):

        wait_all(client.connect(server.server_address),
                backend.connect(server.server_address), timeout=0.5)

        th = TestThread(target=backend.loop)
        th.setDaemon(True)
        th.start()

        response = client.query(fields={'workflow': 'some workflow'},
                message=message, type=TestMessageTypes.TEST_REQUEST,
                timeout=1)

        eq_(response.message, message)
        eq_(response.type, TestMessageTypes.TEST_RESPONSE)
        eq_(response.from_, backend.instance_id)
        eq_(response.to, client.instance_id)
        eq_(response.workflow, 'some workflow')

        th.join()
