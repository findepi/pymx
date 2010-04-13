
from __future__ import absolute_import, with_statement

import os
from random import choice
from threading import RLock, Thread, currentThread
from functools import wraps, partial
from collections import deque
from Queue import Queue, Empty
from functools import partial

import asyncore
try:
    file_dispatcher = asyncore.file_dispatcher
    assert file_dispatcher
except AttributeError:
    # e.g. we are on windows
    file_dispatcher = None
    from .hacks.socket_pipe import socket_pipe

from .channel import Channel
from .message import MultiplexerMessage
from .frame import create_frame_header
# TODO require heartbits
from .protocol import HEARTBIT_WRITE_INTERVAL, HEARTBIT_READ_INTERVAL
from .protocol_constants import MessageTypes
from .protobuf import make_message
from .timer import Timer
from .atomic import Atomic

def _listify(seq):
    """Returns ``seq`` or ``list(seq)`` whichever supports ``__len__`` magic
    method. """
    if callable(getattr(seq, '__len__', None)):
        return seq
    return list(seq)

def _schedule_in_io_thread(method):
    @wraps(method)
    def schedule_in_io_thread_wrapper(self, *args, **kwargs):
        if currentThread() is self._io_thread:
            return method(self, *args, **kwargs)
        else:
            self._enque_io_task(method, self, *args, **kwargs)
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

    def __init__(self, welcome_message):
        object.__init__(self)
        self._lock = RLock()
        self._channel_map = {}
        self._tasks = []
        self._incoming_messages = Queue()

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
                name='C-Manager-IO-Thread-#' +
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
    def _shutdown(self):
        asyncore.close_all(map=self.channel_map)

    @_schedule_in_io_thread
    def connect(self, address):
        """Initiates asynchronous connection."""
        assert currentThread() is self._io_thread, \
                "this code must be called by IO thread only"
        Channel(address=address, manager=self)

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
    def send_message(self, message, connection):
        channels = self._get_channels(connection)
        for channel in channels:
            assert isinstance(channel, Channel), self.channel_map
            channel.enque_outgoing(message)

    def _get_channels(self, connection):
        if connection is ConnectionsManager.ALL:
            return self._all_channels
        if connection is ConnectionsManager.ONE:
            channels = _listify(self._all_channels)
            if not channels:
                raise RuntimeError(
                        "send_message(via ONE) called when no active channels")
            return (choice(channels),)
        raise ValueError("Could not select channel for connection", connection)


    def __handle_connection_welcome(self, message):
        # TODO could validate it's the first CONNECTION_WELCOME on this
        # channel
        pass

    def __handle_heartbit(self, message):
        pass

    @staticmethod # this function is called with explicit 'self'
    def __default_message_handler(self, message):
        self._incoming_messages.put(message)

    __message_handlers = {
            MessageTypes.CONNECTION_WELCOME: __handle_connection_welcome,
            MessageTypes.HEARTBIT: __handle_heartbit,
        }

    def handle_message(self, message):
        handler = self.__message_handlers.get(message.type,
                self.__default_message_handler)
        handler(self, message)

    def receive(self, timeout):
        try:
            return self._incoming_messages.get(timeout=timeout)
        except Empty:
            return None


class _TaskNotifier(file_dispatcher or asyncore.dispatcher):

    ignore_log_types = ()

    def handle_read(self):
        self.recv(512)

    def writable(self):
        return False


