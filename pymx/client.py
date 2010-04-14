import time
from random import randint
from functools import partial
from Queue import Empty

from .protobuf import make_message
from .message import MultiplexerMessage
from .connection import ConnectionsManager
from .protocol import WelcomeMessage, BackendForPacketSearch
from .protocol_constants import MessageTypes
from .timeout import Timeout
from .decorator import parametrizable_decorator
from .exc import OperationFailed, OperationTimedOut

_rand64 = partial(randint, 0, 2**64 - 1)

class BackendError(OperationFailed):
    """Error reported by BACKEND is transformed into `BackendError` exception
    and re-raised on the client side. """
    pass

@parametrizable_decorator
def transform_message(func, message_getter=lambda x: x):
    def _transform_message(*args, **kwargs):
        response = func(*args, **kwargs)
        if response is not None:
            message = message_getter(response)
            assert isinstance(message, MultiplexerMessage)
            if message.type == MessageTypes.BACKEND_ERROR:
                raise BackendError(message.message)
        return response
    return _transform_message


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

    def connect(self, address, sync=False, timeout=5):
        """Initiate connection to Multiplexer server.

        Returns `Future`, which will be set when Multiplexer connection
        hand-shake is completed.

        :Parameters:
            - `address`: an address suitable for ``socket.connect`` call
              (``host, port`` pair)
            - `sync`: (keyword-only) if true, wait for connection
            - `timeout`: (keyword-only) timeout used, when `sync` is true
              (``None`` means no timeout)
        """
        future = self._manager.connect(address)
        if sync:
            future.wait(timeout)
        return future

    def send_message(self, message, connection=ConnectionsManager.ONE):
        return self._manager.send_message(message, connection=connection)

    def event(self, message):
        return self.send_message(message, connection=ConnectionsManager.ALL)

    @transform_message
    def query(self, message, type, timeout, fields=None, skip_resend=False):
        assert not isinstance(message, MultiplexerMessage)
        fields = dict(fields or {}, message=message, type=type)

        with self._manager.query_context_manager() as query_manager:
            # First phase - normal send & receive.
            first_request_delivery_errored = False
            query = self.create_message(**fields)
            query_manager.register_id(query.id)
            self.send_message(message=query)
            response = query_manager.receive(timeout,
                    ignore_types=(MessageTypes.REQUEST_RECEIVED,))
            if response is not None:
                if response.type != MessageTypes.DELIVERY_ERROR:
                    return response
                if skip_resend:
                    raise OperationFailed("Delivery Error respons for query "
                            "#%d" % query.id)
                else:
                    first_request_delivery_errored = True
            elif skip_resend:
                raise OperationTimedOut("No response received for query #%d",
                        query.id)

            # Second phase - searching for a working backend.
            search = self.create_message(message=
                    make_message(BackendForPacketSearch,
                        packet_type=type).SerializeToString(),
                    type=MessageTypes.BACKEND_FOR_PACKET_SEARCH)
            query_manager.register_id(search.id)
            searches_count = self.event(search)
            if not searches_count:
                raise OperationFailed("Could not broadcast backend search")
            searches_timeout = Timeout(timeout)
            while searches_timeout.remaining:
                response = query_manager.receive(searches_timeout.timeout,
                        ignore_types=(MessageTypes.REQUEST_RECEIVED,))
                if response is None:
                    raise OperationTimedOut("No response to query #%d and "
                            "backend search #%d" % (query.id, search.id))

                elif response.type == MessageTypes.DELIVERY_ERROR:
                    if response.references == query.id:
                        first_request_delivery_errored = True
                        continue
                    else:
                        # No backend for packet search responses
                        assert response.references == search.id
                        searches_count -= 1
                        if searches_count:
                            continue
                        if first_request_delivery_errored:
                            raise OperationFailed("Delivery Error responses "
                                    "for query #%d and backend search #%d" %
                                    (query.id, search.id))
                        else:
                            query_manager.unregister_id(search.id)
                            response = query_manager.receive(timeout=timeout)
                            if response is None:
                                raise OperationTimedOut("No response received "
                                        "for query #%d and backend search #%d "
                                        "errored" % (query.id, search.id))
                            assert response.references == query.id
                            if response.type == MessageTypes.DELIVERY_ERROR:
                                raise OperationFailed("Delivery Error "
                                        "responses for query #%d and backend "
                                        "search #%d" % (query.id, search.id))
                            return response

                elif response.references == query.id:
                    return response

                elif response.type == MessageTypes.PING:
                    # Found alive backend!
                    assert response.references == search.id
                    query_manager.unregister_id(search.id)
                    retransmitted = self.create_message(**fields)
                    query_manager.register_id(retransmitted.id)
                    self.send_message(retransmitted) # TODO use the same channel
                    break
                else:
                    raise OperationFailed("Unrecognized message returned from "
                            "multiplexer", response)

            # Third phase - waiting for response from proved-alive backend (or
            # from backend handling initial query).
            timer = Timeout(timeout)
            while timer.remaining:
                response = query_manager.receive(timeout,
                        ignore_types=(MessageTypes.REQUEST_RECEIVED,))

                if response is None:
                    break

                if response.type == MessageTypes.DELIVERY_ERROR:
                    if response.references == retransmitted.id:
                        raise OperationFailed("Retransmitted query #%d could "
                                "not be delivered" % retransmitted.id)
                    else:
                        assert response.references == query.id
                        first_request_delivery_errored = True
                        continue

                return response

            raise OperationTimedOut("No response received for query #%d and "
                    "retransmitted query #%d" % (query.id, retransmitted.id))

    @transform_message
    def receive(self, timeout=None):
        response = self._manager.receive(timeout=timeout)
        if response is None:
            raise OperationTimedOut
        return response

    def close(self):
        self._manager.close()

    def __del__(self):
        self.close()

