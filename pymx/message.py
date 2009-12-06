
from .Multiplexer_pb2 import MultiplexerMessage, Compression, \
        MultiplexerMessageDescription, LoggingMethod

def _get_from(self):
    return getattr(self, 'from')

def _set_from(self, value):
    return setattr(self, 'from', value)

def _del_from(self):
    delattr(self, 'from')

MultiplexerMessage.from_ = property(_get_from, _set_from, _del_from)
