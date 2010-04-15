
from __future__ import absolute_import, with_statement

import os
from random import choice
from threading import RLock, Thread, currentThread
from functools import wraps, partial
from collections import deque
from Queue import Queue, Empty
from functools import partial
import asyncore
from google.protobuf.message import Message
from .channel import Channel
from .message import MultiplexerMessage
from .frame import create_frame_header
# TODO require heartbits
from .protocol import HEARTBIT_WRITE_INTERVAL, HEARTBIT_READ_INTERVAL
from .protocol_constants import MessageTypes
from .protobuf import make_message
from .timer import Timer
from .atomic import Atomic, synchronized
from .timeout import Timeout
from .future import Future
from .limitedset import LimitedSet

try:
    file_dispatcher = asyncore.file_dispatcher
    assert file_dispatcher
except AttributeError:
    # e.g. we are on windows
    file_dispatcher = None
    from .hacks.socket_pipe import socket_pipe


def _listify(seq):
    """Returns ``seq`` or ``list(seq)`` whichever supports ``__len__`` magic
    method. """
    if callable(getattr(seq, '__len__', None)):
        return seq
    return list(seq)

def _schedule_in_io_thread(method):
    @wraps(method)
    def schedule_in_io_thread_wrapper(self, *args, **kwargs):
        future = Future()
        if currentThread() is self._io_thread:
            return method(self, future, *args, **kwargs)
        else:
            self._enque_io_task(method, self, future, *args, **kwargs)
        return future
    return schedule_in_io_thread_wrapper

def _in_io_thread_only(method):
    @wraps(method)
    def in_io_thread_wrapper(self, *args, **kwargs):
        assert currentThread() is self._io_thread
        return method(self, *args, **kwargs)
    return in_io_thread_wrapper

class ConnectionsManager(object):

    __creation_counter = Atomic(0)

    ONE = 1
    ALL = 2

    _lock = None
    """A lock for shared data structures accessed by 2+ threads."""

    _channel_map = None
    """A dictionary of dispatchers used by ``asyncore``. Accessed only by IO
    thread (and in __init__). """

    _tasks = None
    """A list of tasks scheduled for IO thread."""

    _task_notifier_pipe = None
    """A file-like object that can be used to wake up IO thread (or None if
    this functionality is not available). Never accessed from IO thread. """

    _io_thread = None
    """An IO thread object. Never accessed from IO thread. """

    _is_closing = False
    """Specifies close order has already been issued. """

    _incoming_messages = None
    """Queue of incoming messages not yet consumed by the client."""

    _query_responses = None
    """Dictionary of queues with respones to multiplexer queries."""

    _recent_messages_pool = None
    """Deduplication leaking set."""

    def __init__(self, welcome_message):
        object.__init__(self)
        self._lock = RLock()
        self._channel_map = {}
        self._tasks = []
        self._incoming_messages = Queue()
        self._query_responses = {}
        self._recent_messages_pool = LimitedSet()

        assert isinstance(welcome_message, MultiplexerMessage)
        welcome_message = welcome_message.SerializeToString()
        self._welcome_frame = create_frame_header(welcome_message) + \
                welcome_message

        self._task_notifier_pipe = None
        self._create_task_notifier()

        self._timer = Timer()

        self._io_thread = None
        self._start_io_thread()

    def _create_task_notifier(self):
        """Create ``self._task_notifier_pipe`` and register appropriate
        read-end in the ``self._channel_map``. """

        if file_dispatcher is not None:
            # create pipe-based task notifier
            read_end, write_end = os.pipe()
            try:
                writer = os.fdopen(write_end, 'w', 0)
                _TaskNotifier(fd=read_end, map=self._channel_map)
            except Exception:
                try:
                    os.close(write_end)
                except Exception:
                    pass
                raise
            finally:
                os.close(read_end)
            self._task_notifier_pipe = writer

        else:
            # create socket-based task notifier
            read_end, write_end = socket_pipe()
            _TaskNotifier(sock=read_end, map=self._channel_map)
            self._task_notifier_pipe = write_end.makefile('w', 0)

    def _start_io_thread(self):
        self._io_thread = Thread(target=self._io_main,
                name='ConnectionsManager-IO-Thread-#' +
                str(ConnectionsManager.__creation_counter.inc()))
        self._io_thread.setDaemon(True)
        self._io_thread.start()

    def _io_main(self):
        while self._channel_map:
            asyncore.loop(count=1, map=self.channel_map)
            with self._lock:
                tasks, self._tasks[:] = self._tasks[:], ()
            for task in tasks:
                task()

    @property
    def channel_map(self):
        return self._channel_map

    @property
    def _all_channels(self):
        return (ch for ch in self.channel_map.itervalues()
                if isinstance(ch, Channel))

    def _enque_io_task(self, *args, **kwargs):
        task = partial(*args, **kwargs)
        assert currentThread() is not self._io_thread
        with self._lock:
            self._tasks.append(task)
        self._task_notifier_pipe.write('t')

    def close(self):
        with self._lock:
            if self._is_closing:
                return
            self._is_closing = True
        self._shutdown()
        self._io_thread.join()

    @_schedule_in_io_thread
    def _shutdown(self, future):
        with future:
            asyncore.close_all(map=self.channel_map)
            future.set(True)

    @_schedule_in_io_thread
    def connect(self, future, address):
        """Initiates asynchronous connection."""
        with future:
            assert currentThread() is self._io_thread, \
                    "this code must be called by IO thread only"
            Channel(address=address, manager=self, connect_future=future)

    @_in_io_thread_only
    def handle_connect(self, channel):
        assert channel.connected
        channel.enque_outgoing(self._welcome_frame)
        self._send_heartbit(channel)

    def _send_heartbit(self, channel):
        if channel.connected:
            channel.enque_outgoing(make_message(MultiplexerMessage,
                type=MessageTypes.HEARTBIT))
            self._timer.schedule(HEARTBIT_WRITE_INTERVAL,
                    partial(self._send_heartbit, channel))

    @_schedule_in_io_thread
    def send_message(self, future, message, connection):
        with future:
            if isinstance(message, Message):
                message = message.SerializeToString()
                message = create_frame_header(message) + message
            channels = self._get_channels(connection)
            i = -1
            for i, channel in enumerate(channels):
                assert isinstance(channel, Channel), self.channel_map
                channel.enque_outgoing(message)
            future.set(i + 1) # TODO we don't know when it's flushed

    def _get_channels(self, connection):
        if connection is ConnectionsManager.ALL:
            return self._all_channels
        if connection is ConnectionsManager.ONE:
            channels = _listify(self._all_channels)
            if not channels:
                raise RuntimeError(
                        "send_message(via ONE) called when no active channels")
            return (choice(channels),)
        if isinstance(connection, Channel):
            return (connection,)
        raise ValueError("Could not select channel for connection", connection)

    def __handle_connection_welcome(self, message, channel):
        # TODO could validate it's the first CONNECTION_WELCOME on this
        # channel
        pass

    def __handle_heartbit(self, message, channel):
        pass

    @synchronized
    def __get_queue_for_message(self, message):
        return self._query_responses.get(message.references,
                self._incoming_messages)

    @synchronized
    def set_queue_for_message(self, references, queue):
        if queue is None:
            old = self._query_responses.pop(references, None)
        else:
            old = self._query_responses.get(references)
            self._query_responses[references] = queue
        return old

    @staticmethod # this function is called with explicit 'self'
    def __default_message_handler(self, message, channel):
        self.__get_queue_for_message(message).put({'message': message,
            'channel': channel})

    __message_handlers = {
            MessageTypes.CONNECTION_WELCOME: __handle_connection_welcome,
            MessageTypes.HEARTBIT: __handle_heartbit,
        }

    @_in_io_thread_only
    def handle_message(self, message, channel):
        if not self._recent_messages_pool.add(message.id):
            return
        handler = self.__message_handlers.get(message.type,
                self.__default_message_handler)
        handler(self, message, channel)

    def query_context_manager(self, message_id=None):
        return _QueryContextManager(self, message_id=message_id)

    def receive(self, *args, **kwargs):
        return receive(self._incoming_messages, *args, **kwargs)


#def receive(queue, timeout, ignore_types=(), filter=None):
def receive(queue, timeout, ignore_types=(), with_channel=False,
        message_acceptor=lambda received: True):
    timer = Timeout(timeout)
    #filter = (filter or {}).items()
    while timer.remaining:
        try:
            incoming = queue.get(timeout=timer.timeout)
            received, channel = incoming['message'], incoming['channel']
        except Empty:
            break

        # check if it's not an ignored type
        if received.type in ignore_types:
            continue

        ## check if it's not excluded by the filter
        #try:
            #(1 for field, value in filter
                    #if getattr(received, field) != value).next()
            ## ... there is a non-matching (field, value) pair
            #continue
        #except StopIteration:
            #pass

        if not message_acceptor(received):
            continue

        # received message passed all tests
        if with_channel:
            return received, channel
        return received

    return None


class _QueryContextManager(object):
    def __init__(self, manager, message_id=None, queue=None):
        self._manager = manager
        self._queue = queue or Queue()
        self._devnull = None
        self._message_ids = set()
        self._active_ids = set()
        if message_id is not None:
            self._is_entered = True # bluff
            self.register_id(message_id)
        self._is_entered = False

    def register_id(self, message_id):
        assert self._is_entered
        assert message_id not in self._message_ids
        self._message_ids.add(message_id)
        self._active_ids.add(message_id)
        self._manager.set_queue_for_message(message_id, self._queue)

    def unregister_id(self, message_id):
        assert message_id in self._message_ids
        if message_id in self._active_ids:
            q = self._manager.set_queue_for_message(message_id, self.devnull)
            self._active_ids.remove(message_id)

    @property
    def queue(self):
        return self._queue

    @property
    def devnull(self):
        if self._devnull is None:
            self._devnull = Queue()
        return self._devnull

    def receive(self, *args, **kwargs):
        assert 'message_acceptor' not in kwargs
        kwargs['message_acceptor'] = self._accept_message
        return receive(self.queue, *args, **kwargs)

    def _accept_message(self, received):
        if received.references not in self._active_ids:
            # Message delivered to `self.queue` based on message ID which is
            # already unregistered - such message is effectively dropped.
            return False
        return True

    def __enter__(self):
        self._is_entered = True
        return self

    def __exit__(self, *exc_info):
        assert self._is_entered
        self._is_entered = False
        exc_info = None
        for message_id in self._message_ids:
            q = self._manager.set_queue_for_message(message_id, None)
        self._message_ids = []


class _TaskNotifier(file_dispatcher or asyncore.dispatcher):

    ignore_log_types = ()

    def handle_read(self):
        self.recv(512)

    def writable(self):
        return False


