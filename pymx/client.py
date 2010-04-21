from __future__ import with_statement

import time
from random import randint
from functools import partial
from Queue import Empty
from operator import itemgetter

from .protobuf import make_message
from .message import MultiplexerMessage
from .connection import ConnectionsManager
from .protocol import WelcomeMessage, BackendForPacketSearch, RECONNECT_TIME
from .protocol_constants import MessageTypes
from .timeout import Timeout
from .decorator import parametrizable_decorator
from .exc import MultiplexerException

_rand64 = partial(randint, 0, 2**64 - 1)

class OperationFailed(MultiplexerException):
    """Raised when operation fails for any reason. """
    pass

class OperationTimedOut(OperationFailed):
    """Raised when operation times out. """
    pass

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

    ONE = ConnectionsManager.ONE
    ALL = ConnectionsManager.ALL

    def __init__(self, type, multiplexer_password=None):
        """Construct new `Client` instance.

        :Parameters:
            - `type`: peer type of new client
            - `multiplexer_password`: password send to (and optionally
              validated by) Multiplexer server
        """
        object.__init__(self)
        self._instance_id = _rand64()
        self._type = type

        welcome = make_message(WelcomeMessage, id=self.instance_id, type=type,
                multiplexer_password=multiplexer_password)

        welcome_message = make_message(MultiplexerMessage,
                type=MessageTypes.CONNECTION_WELCOME,
                message=welcome.SerializeToString(), from_=self.instance_id)

        self._manager = ConnectionsManager(welcome_message,
                multiplexer_password=multiplexer_password)

    @property
    def instance_id(self):
        """Peer ID of this client instance."""
        return self._instance_id

    @property
    def type(self):
        """Peer type of this client instance."""
        return self._type

    def create_message(self, **kwargs):
        """Construct `MultiplexerMessage` using `kwargs`.

        This is equivalent to calling
        `make_message`\ ``(MultiplexerMessage, **kwargs)``
        except that this method fills by default

            - ``id``
            - ``from``
            - ``timestamp``
        """
        kwargs.setdefault('id', _rand64())
        kwargs.setdefault('from', self.instance_id)
        kwargs.setdefault('timestamp', int(time.time()))
        return make_message(MultiplexerMessage, **kwargs)

    def connect(self, address, sync=False, timeout=5,
            reconnect=RECONNECT_TIME):
        """Initiate connection to Multiplexer server.

        Returns `Future`, which will be set when Multiplexer connection
        hand-shake is completed.

        :Parameters:
            - `address`: an address suitable for ``socket.connect`` call
              (``host, port`` pair)
            - `sync`: (keyword-only) if true, wait for connection
            - `timeout`: (keyword-only) timeout used, when `sync` is true
              (``None`` means no timeout)
            - `reconnect`: after `reconnect` seconds since losing connection to
              `address` Client should attempt to reconnect
        """
        future = self._manager.connect(address)
        if sync:
            future.wait(timeout)
        return future

    def send_message(self, message, connection=ONE):
        """Send a message.

        Returns `Future`, which will be set to a number of channels used to
        send message (this may change without warning).

        :Parameters:
            - `message`: a MultiplexerMessage object (or raw Multiplexer
              protocol frame as `str`)
            - `connection`: ``ConnectionsManager.ONE``,
              ``ConnectionsManager.ALL`` or channel instance returned by
              `receive`\ ``(with_channel=True)``
        """
        return self._manager.send_message(message, connection=connection)

    def event(self, message):
        """Broadcast a message. Equivalent to `send_message` ``(message,
        ConnectionsManager.ALL)``. """
        return self.send_message(message, connection=self.ALL)

    @transform_message
    def query(self, message, type, timeout, fields=None, skip_resend=False):
        """Perform a Multiplexer query.

        Returns `MultiplexerMessage` instance. If the ``BACKEND_ERROR`` is
        received (even after resending the request), it's converted into
        `BackendError` exception.

        :Parameters:
            - `message`: a request body (a `str`)
            - `type`: a request ``type``
            - `timeout`: timeout of each phase of the query (in seconds)
            - `fields`: optional `dict` with additional fields (example:
              ``{'workflow': '....'}``
            - `skip_resend`: if present and true, query algorithm will not send
              ``BACKEND_FOR_PACKET_SEARCH`` nor resend the request
        """
        assert not isinstance(message, MultiplexerMessage)
        fields = dict(fields or {}, message=message, type=type)
        workflow = fields.get('workflow')

        with self._manager.query_context_manager() as query_manager:
            # First phase - normal send & receive.
            first_request_delivery_errored = False
            query = self.create_message(**fields)
            query_manager.register_id(query.id)
            self.send_message(message=query)
            response = query_manager.receive(timeout,
                    ignore_types=(MessageTypes.REQUEST_RECEIVED,))
            backend_error = None
            if response is not None:
                if response.type == MessageTypes.DELIVERY_ERROR:
                    if skip_resend:
                        raise OperationFailed("Delivery Error response for "
                                "query #%d" % query.id)
                    else:
                        first_request_delivery_errored = True
                elif response.type == MessageTypes.BACKEND_ERROR:
                    backend_error = response
                else:
                    return response
            elif skip_resend:
                raise OperationTimedOut("No response received for query #%d",
                        query.id)

            # Second phase - searching for a working backend.
            search = self.create_message(message=
                    make_message(BackendForPacketSearch,
                        packet_type=type).SerializeToString(),
                    type=MessageTypes.BACKEND_FOR_PACKET_SEARCH,
                    workflow=workflow)
            query_manager.register_id(search.id)
            searches_count = self.event(search)
            if not searches_count:
                if backend_error is not None:
                    return backend_error
                raise OperationFailed("Could not broadcast backend search")
            searches_timeout = Timeout(timeout)
            while searches_timeout.remaining:
                response = query_manager.receive(
                        searches_timeout.timeout, with_channel=True,
                        ignore_types=(MessageTypes.REQUEST_RECEIVED,))
                if response is None:
                    if backend_error is not None:
                        return backend_error
                    raise OperationTimedOut("No response to query #%d and "
                            "backend search #%d" % (query.id, search.id))
                response, channel = response

                if response.type in (MessageTypes.DELIVERY_ERROR,
                        MessageTypes.BACKEND_ERROR):
                    if response.type == MessageTypes.BACKEND_ERROR and \
                            backend_error is None:
                        backend_error = response
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
                            if backend_error is not None:
                                return backend_error
                            raise OperationFailed("Delivery Error responses "
                                    "for query #%d and backend search #%d" %
                                    (query.id, search.id))
                        else:
                            query_manager.unregister_id(search.id)
                            response = query_manager.receive(timeout=timeout)
                            if response is None:
                                if backend_error is not None:
                                    return backend_error
                                raise OperationTimedOut("No response received "
                                        "for query #%d and backend search #%d "
                                        "errored" % (query.id, search.id))
                            assert response.references == query.id
                            if response.type == MessageTypes.DELIVERY_ERROR:
                                if backend_error is not None:
                                    # second response to query received...
                                    return backend_error
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
                    self.send_message(retransmitted, connection=channel)
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

                if response.type == MessageTypes.BACKEND_ERROR:
                    if backend_error is None:
                        backend_error = response

                if response.type == MessageTypes.DELIVERY_ERROR:
                    if response.references == retransmitted.id:
                        if backend_error is not None:
                            return backend_error
                        raise OperationFailed("Retransmitted query #%d could "
                                "not be delivered" % retransmitted.id)
                    else:
                        assert response.references == query.id
                        first_request_delivery_errored = True
                        continue

                return response

            if backend_error is not None:
                return backend_error
            raise OperationTimedOut("No response received for query #%d and "
                    "retransmitted query #%d" % (query.id, retransmitted.id))

    def receive(self, timeout=None, with_channel=False):
        """Receive a message from Multiplexer server. If optional parameter
        `timeout` is specified and not ``None``, receive will block for at most
        `timeout` seconds.

        Returns `MultiplexerMessage` if `with_channel` is not specified else
        ``(message, channel)`` pair. If the received message is a
        ``BACKEND_ERROR`` message, it will be converted into `BackendError`
        exception.
        """
        message, connection = self._receive(timeout=timeout)
        if with_channel:
            return message, connection
        return message

    @transform_message(message_getter=itemgetter(0))
    def _receive(self, timeout=None):
        response = self._manager.receive(timeout=timeout,
                with_channel=True)
        if response is None:
            raise OperationTimedOut
        return response

    def close(self):
        """Close this client."""
        self._manager.close()

    def __del__(self):
        self.close()

