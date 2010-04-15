from contextlib import closing

from nose.tools import nottest

from pymx.client import Client

from .test_constants import PeerTypes as TestPeerTypes

@nottest
def create_test_client(type=TestPeerTypes.TEST_CLIENT):
    return closing(Client(type=type))
