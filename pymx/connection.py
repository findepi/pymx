
from __future__ import with_statement

import os
import sys
import socket
import random
from threading import RLock, Thread, currentThread
from functools import wraps, partial

import asyncore
try:
    file_dispatcher = asyncore.file_dispatcher
except AttributeError:
    file_dispatcher = None

from .channel import Channel
from .message import MultiplexerMessage
from .frame import create_frame_header

#def synchronized(method):
    #@wraps(method)
    #def synchronized_wrapper(self, *args, **kwargs):
        #with self._lock:
            #return method(self, *args, **kwargs)
    #return synchronized_wrapper

class _Dict(dict):
    pass

def _schedule_in_io_thread(method):
    @wraps(method)
    def schedule_in_io_thread_wrapper(self, *args, **kwargs):
        if currentThread() is self._io_thread:
            return method(*args, **kwargs)
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

    def __init__(self, welcome_message):
        object.__init__(self)
        self._lock = RLock()
        self._channel_map = _Dict()
        self._tasks = []

        assert isinstance(welcome_message, MultiplexerMessage)
        welcome_message = welcome_message.SerializeToString()
        self._welcome_frame = create_frame_header(welcome_message) + \
                welcome_message

        self._task_notifier_pipe = None
        self._create_task_notifier()

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
            read_end, write_end = _socket_pipe()
            _TaskNotifier(sock=read_end, map=self._channel_map)
            self._task_notifier_pipe = write_end.makefile('w', 0)

    def _start_io_thread(self):
        self._io_thread = Thread(target=self._io_main)
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

    def _enque_io_task(self, *args, **kwargs):
        task = partial(*args, **kwargs)
        assert currentThread() is not self._io_thread
        with self._lock:
            self._tasks.append(task)
        self._task_notifier_pipe.write('t')

    @_schedule_in_io_thread
    def connect(self, address):
        """Initiates asynchronous connection."""
        assert currentThread() is self._io_thread, \
                "this code must be called by IO thread only"
        Channel(address=address, manager=self)

    def shutdown(self):
        self._shutdown()
        self._io_thread.join()

    @_schedule_in_io_thread
    def _shutdown(self):
        asyncore.close_all(map=self.channel_map)

    @_in_io_thread_only
    def handle_connect(self, channel):
        channel.enque_outgoing(self._welcome_frame)

class _TaskNotifier(file_dispatcher if file_dispatcher is not None else
        asyncore.dispatcher):

    ignore_log_types = ()

    def handle_read(self):
        self.recv(512)

    def writable(self):
        return False


def _socket_pipe():
    """Like ``os.pipe()`` but creates sockets instead of connected file
    descriptors. To be used on impaired platforms that do not support
    ``select`` over pipes (e.g. Windows). """

    # Create read0end acceptor.
    read_acceptor = socket.socket()
    read_acceptor.bind(('127.0.0.1', 0))
    read_acceptor.listen(10)
    read_acceptor.setblocking(False)

    # Create writer and connect it
    writer = socket.socket()
    writer.connect(read_acceptor.getsockname())

    # Wait for connection from the right socket
    for _ in xrange(10):
        reader, writer_address = read_acceptor.accept()
        if writer_address != writer.getsockname():
            sys.stderr.write(__name__ + "._socket_pipe: Waring: port " +
                    "scanning detected.\n")
            reader.close()
            continue
        break
    else:
        raise RuntimeError("_socket_pipe: did not receive writer connection.")

    read_acceptor.close()

    # Verify, that the connected socket is really the right one.
    test_message = str(random.random())
    writer.sendall(test_message)
    while test_message:
        test_chunk = reader.recv(len(test_message))
        if not test_chunk or not test_message.startswith(test_chunk):
            raise RuntimeError("_socket_pipe: invalid test data received.")
        test_message = test_message[len(test_chunk):]

    return reader, writer
