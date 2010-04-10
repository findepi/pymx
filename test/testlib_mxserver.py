from __future__ import with_statement

import os.path
import socket
import traceback
import errno
import signal
import time
import random
from threading import Thread, RLock
import subprocess
import contextlib

from pkg_resources import resource_filename
from distutils.spawn import find_executable

from pymx.frame import Deframer
from pymx.protobuf import parse_message
from pymx.message import MultiplexerMessage


class _ThreadEnabledServerMixin(object):
    @classmethod
    def run_threaded(cls, *args, **kwargs):
        server = cls(*args, **kwargs)
        thread = Thread(target=server._run)
        server.thread = thread
        thread.setDaemon(True)
        thread.start()
        return server

    def _run(self):
        raise NotImplementedError("Subclass responsibility")

    def _shutdown(self):
        raise NotImplementedError("Subclass responsibility")

    def close(self):
        self._shutdown()
        self.thread.join()


class SimpleMxServerThread(_ThreadEnabledServerMixin, object):

    def __init__(self):
        object.__init__(self)
        _ThreadEnabledServerMixin.__init__(self)

        self._lock = RLock()
        self._sock = socket.socket()
        self._sock.bind(('127.0.0.1', 0))
        self.server_address = self._sock.getsockname()
        self._sock.listen(5)
        self.message_counters = {}
        self._client = None
        self._shutdown_called = False

    def _run(self):
        try:
            while True:
                with self._lock:
                    sock = self._sock
                if sock is None:
                    break
                client, address = sock.accept()
                with self._lock:
                    if self._shutdown_called:
                        client.close()
                        break
                    else:
                        self._client = client
                client_welcome = self._read_message(client)
                self._inc_counters(client_welcome)
                client.close()
                with self._lock:
                    self._client = None
        except socket.error, e:
            if e.errno in (errno.EINVAL, errno.EBADF):
                pass
            else:
                raise

    def _shutdown(self):
        with self._lock:
            self._shutdown_called = True
        # wakeup listening thread by issuing a connect
        s = socket.socket()
        s.settimeout(1)
        try:
            s.connect(self.server_address)
        except socket.error:
            traceback.print_exc()

        with self._lock:
            self._sock.close()
            self._sock = None

        with self._lock:
            if self._client is not None:
                try:
                    self._client.shutdown(socket.SHUT_RDWR)
                    self._client.close()
                except Exception:
                    traceback.print_exc()

    def _inc_counters(self, message):
        self.message_counters[message.type] = self.message_counters.setdefault(
                message.type, 0) + 1

    @staticmethod
    def _read_message(sock):
        input = Deframer()
        while True:
            for contents in input.push(sock.recv(1)):
                message = parse_message(MultiplexerMessage, contents)
                return message

class _SubprocessPseudoThread(object):
    def __init__(self, subproc):
        object.__init__(self)
        self.subproc = subproc

    def join(self):
        self.subproc.wait()


class JmxServerThread(_ThreadEnabledServerMixin, object):

    def __init__(self):
        object.__init__(self)
        _ThreadEnabledServerMixin.__init__(self)

        for _ in xrange(100):
            self.server_address = ('127.0.0.1',
                    random.randint(1024 + 1, 2**16 - 1))
            if not self._check_connectible():
                break
        else:
            raise RuntimeError("Can't find a free port to setup JMX server")

        jmx_jar = resource_filename(__name__, 'jmx-0.9-withdeps.jar')
        if not os.path.exists(jmx_jar):
            raise RuntimeError("jmx jar could not be found")

        jmx_config = resource_filename(__name__, 'jmx.test.configuration')
        if not os.path.exists(jmx_config):
            raise RuntimeError("jmx configuration could not be found")

        java = find_executable('java')
        if java is None:
            raise RuntimeError(
                    "java is not available, please adjust your PATH")
        jmx_server_command = [java,
                '-Djava.util.logging.config.file=' + jmx_config,
                '-jar', jmx_jar, 'server',
                '-host', self.server_address[0],
                '-port', str(self.server_address[1])]
        self.subproc = subprocess.Popen(jmx_server_command)

    @classmethod
    def run_threaded(cls, *args, **kwargs):
        server = cls(*args, **kwargs)
        server._run()
        server.thread = _SubprocessPseudoThread(server.subproc)
        return server

    def _run(self):
        self._wait_connectible()
        if self.subproc.poll() is not None:
            raise RuntimeError("JMX server is dead")

    def _wait_connectible(self):
        # wait for JMX startup up to 15 seconds
        step = 0.05
        limit = time.time() + 15
        while time.time() <= limit:
            if self._check_connectible():
                return
            time.sleep(min(step, limit - time.time()))
        if self._check_connectible():
            return
        raise RuntimeError("JMX server is not connectible")

    def _check_connectible(self, timeout=1):
        sock = socket.socket()
        sock.settimeout(timeout)
        sock.setblocking(True)
        try:
            sock.connect(self.server_address)
        except socket.error:
            return False
        sock.close()
        return True

    def _shutdown(self):
        self.subproc.terminate() # FIXME requires python 2.6+

def create_mx_server_context(impl=JmxServerThread):
    return contextlib.closing(impl.run_threaded())
