import time
from random import randint
from functools import partial

from .protobuf import make_message
from .message import MultiplexerMessage
from .connection import ConnectionsManager
from .protocol import WelcomeMessage
from .protocol_constants import MessageTypes

_rand64 = partial(randint, 0, 2**64 - 1)

class Client(object):
    """``Client`` represents the set of open connections to Multiplexer
    servers. """

    def __init__(self, type, multiplexer_password=None):
        object.__init__(self)
        self._instance_id = _rand64()
        self._type = type

        welcome = make_message(WelcomeMessage, id=self.instance_id, type=type,
                multiplexer_password=multiplexer_password)

        welcome_message = make_message(MultiplexerMessage,
                type=MessageTypes.CONNECTION_WELCOME,
                message=welcome.SerializeToString(), from_=self.instance_id)

        self._manager = ConnectionsManager(welcome_message)

    @property
    def instance_id(self):
        return self._instance_id

    @property
    def type(self):
        return self._type

    def create_message(self, **kwargs):
        kwargs.setdefault('id', _rand64())
        kwargs.setdefault('from', self.instance_id)
        kwargs.setdefault('timestamp', int(time.time()))
        return make_message(MultiplexerMessage, **kwargs)

    def connect(self, *args, **kwargs):
        # make this optionally synchronous
        return self._manager.connect(*args, **kwargs)

    def send_message(self, message, connection=ConnectionsManager.ONE):
        return self._manager.send_message(message, connection=connection)

    def event(self, message):
        return self.send_message(message, connection=ConnectionsManager.ALL)

    def query(self, message, type, timeout):
        assert not isinstance(message, MultiplexerMessage)
        raise NotImplementedError

    def receive(self, timeout=None):
        return self._manager.receive(timeout=timeout)

    def close(self):
        self._manager.close()

    def __del__(self):
        self.close()

