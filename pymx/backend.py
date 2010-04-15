"""Abstract multiplexer backend functionality. """

import sys
from traceback import print_exc, format_exception
from threading import RLock

try:
    import cPickle as pickle
except ImportError:
    import pickle

from .client import Client
from .future import FutureException
from .protocol_constants import MessageTypes
from .atomic import synchronized
from .message import MultiplexerMessage


class MultiplexerBackend(object):
    """Abstract multiplexer backend functionality."""

    def __init__(self, type, addresses=(), handler=None):
        """Initialize `MultiplexerBackend`.

        If `handler` is specified, it should be a function taking
        `MultiplexerMessage` as input and returning either

            ``()``
                means that no response should be sent back,
            a string ``s``
                equivalent of returning ``{'message': s, 'type':
                MessageTypes.PING}``,
            a dict ``d``
                equivalent of calling ``backend.send_message(**d)``, see
                `send_message` for details.

        Any exceptions raised by the `handler` will be converted to
        ``BACKEND_ERROR`` messages and reported by `exception_occurred`.

        :Parameters:
            - `type`: peer type of this backend
            - `addresses`: list of addresses of Multiplexer servers
            - `handler`: optional handler that will be used if `handle_message`
              is not overriden by a subclass
        """
        object.__init__(self)
        self._client = Client(type=type)
        self.__handled_message = None
        self.__working = True
        self.__has_sent_response = None
        self._lock = RLock()
        self._handler = handler

        # connect
        connect_futures = map(self._client.connect, addresses)
        for future in connect_futures:
            try:
                future.wait(5)
            except FutureException:
                print_exc()

    @property
    def instance_id(self):
        return self._client.instance_id

    def create_message(self, *args, **kwargs):
        return self._client.create_message(*args, **kwargs)

    def connect(self, *args, **kwargs):
        return self._client.connect(*args, **kwargs)

    def start(self):
        with self._lock:
            self.__working = True
        self.loop()

    @property
    @synchronized
    def working(self):
        return self.__working

    @synchronized
    def shutdown(self):
        self.__working = False

    def loop(self):
        """Serve forever."""
        while self.working:
            self.handle_one()

    def handle_one(self, read_timeout=None):
        mxmsg, connection = self._client.receive(with_connection=True,
                timeout=read_timeout)
        self.__handle_message(mxmsg, connection)

    serve_forever = loop

    def __handle_internal_message(self, mxmsg):
        if mxmsg.type == MessageTypes.BACKEND_FOR_PACKET_SEARCH:
            self.send_message(message="", type=MessageTypes.PING)

        elif mxmsg.type == MessageTypes.PING:
            if not mxmsg.references:
                assert mxmsg.id
                self.send_message(message=mxmsg.message,
                        type=MessageTypes.PING)
            else:
                self.no_response()

        else:
            print >> sys.stderr, "Backend received unknown meta-packet " \
                    "(type=%d)" % (mxmsg.type)

    def __handle_message(self, mxmsg, connection):
        try:
            self.__handled_message = mxmsg
            self.__handled_message_source = connection

            self.__has_sent_response = False
            if mxmsg.type <= MessageTypes.MAX_MULTIPLEXER_META_PACKET:
                # internal messages
                self.__handle_internal_message(mxmsg)
                if not self.__has_sent_response:
                    print >> sys.stderr, "__handle_internal_message() " \
                            "finished without exception and without any " \
                            "response"
            else:
                # the rest
                self.handle_message(mxmsg)
                if not self.__has_sent_response:
                    print >> sys.stderr, "handle_message() finished without " \
                            "exception and without any response"

        except Exception, e:
            # report exception
            print_exc()
            if not self.__has_sent_response:
                print >> sys.stderr, "sending BACKEND_ERROR notification " \
                        "for Exception %s" % e
                self.report_error(message=str(e))
            handled = self.exception_occurred(e)
            if not handled:
                raise

        finally:
            self.__handled_message = None
            self.__handled_message_source = None

    def handle_message(self, mxmsg):
        """This method should be overriden in child classes if ``handler`` is
        not provided. """
        if self._handler is None:
            raise NotImplementedError()

        self.notify_started()
        response = self._handler(mxmsg)
        if response == ():
            self.no_response()
        elif isinstance(response, str):
            self.send_message(message=response, type=MessageTypes.PING)
        elif isinstance(response, dict):
            self.send_message(**response)
        else:
            raise ValueError("Unsupported handler return type %r" %
                    type(response))

    def notify_started(self):
        assert not self.__has_sent_response, "If you use notify_started(), " \
                "place it as a first function in your handle_message() code"
        self.send_message(message="", type=MessageTypes.REQUEST_RECEIVED)
        self.__has_sent_response = False

    def send_message(self, **kwargs):
        """Send reply message constructed using `kwargs`.

        When this function is used during incoming message handling, several
        defaults parameters are used when constructing a message:

            to
                sender of the message being handled
            references
                ``id`` of the message being handled
            working
                ``workflow`` of the message being handled
            connection
                (used to send message, not to construct it) a channel, over
                which the message being handled was received

        Underlying `Client` instance fills some additional fields, see
        `pymx.client.Client.create_message` for details.
        """
        sending_kwargs = {}
        if self.__handled_message is not None:
            self.__has_sent_response = True
            sending_kwargs['connection'] = kwargs.pop('connection',
                    self.__handled_message_source)
            kwargs.setdefault('references', self.__handled_message.id)
            kwargs.setdefault('workflow', self.__handled_message.workflow)
            kwargs.setdefault('to', self.__handled_message.from_)
        return self._client.send_message(self.create_message(**kwargs),
                **sending_kwargs)

    def send_backend_error(self, exc, trace=None):
        assert self.__handled_message is not None
        self.report_error(message=format_exception(type(exc), exc, trace))

    def no_response(self):
        self.__has_sent_response = True

    def report_error(self, message="", type=MessageTypes.BACKEND_ERROR,
            **kwargs):
        assert self.__handled_message is not None
        self.send_message(message=message, type=type, **kwargs)

    def close(self):
        """In case we ever what to finish."""
        self.shutdown()
        self._client.close()

    def exception_occurred(self, exc_info):
        """Called when `handle_message` or ``__handle_internal_message`` throws
        an exception. Returning non-true value results in exception
        propagation. """
        del exc_info
        return True # ignore


class PicklingMultiplexerBackend(MultiplexerBackend):

    """Subclass of `MultiplexerBackend` using Python pickles as message
    payload. """

    def send_pickle(self, data, type=MessageTypes.PICKLE_RESPONSE, **kwargs):
        """Method for sending data back via Multiplexer. """
        self.send_message(message=pickle.dumps(data), type=type, **kwargs)

    def process_pickle(self, data):
        """This method should be overriden in child classes if ``handler`` is
        not provided. """
        if self._handler is None:
            raise NotImplementedError

        self.notify_started()
        self.send_pickle(self._handler(data))

    def handle_message(self, mxmsg):
        try:
            data = pickle.loads(mxmsg.message)
        except (pickle.UnpicklingError, EOFError):
            print >> sys.stderr, "Failed to pickle.load(%r) in #%d" % \
                    (mxmsg.message, mxmsg.id)
            raise
        return self.process_pickle(data)
