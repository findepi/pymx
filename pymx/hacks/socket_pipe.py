from __future__ import absolute_import, with_statement

import sys
import socket
import random


def socket_pipe():
    """Like ``os.pipe()`` but creates sockets instead of connected file
    descriptors. To be used on impaired platforms that do not support
    ``select`` over pipes (e.g. Windows). """

    # Create read0end acceptor.
    read_acceptor = socket.socket()
    read_acceptor.bind(('localhost', 0))
    read_acceptor.listen(10)
    read_acceptor.setblocking(False)

    # Create writer and connect it
    writer = socket.socket()
    writer.setblocking(True)
    writer.connect(read_acceptor.getsockname())

    # Wait for connection from the right socket
    for _ in xrange(10):
        reader, writer_address = read_acceptor.accept()
        reader.setblocking(True)
        if writer_address != writer.getsockname():
            sys.stderr.write(__name__ + ".socket_pipe: Waring: port "
                    "scanning detected.\n")
            reader.close()
            continue
        break
    else:
        raise RuntimeError("socket_pipe: did not receive writer connection.")

    read_acceptor.close()

    # Verify, that the connected socket is really the right one.
    test_message = str(random.random())
    writer.sendall(test_message)
    while test_message:
        test_chunk = reader.recv(len(test_message))
        if not test_chunk or not test_message.startswith(test_chunk):
            raise RuntimeError("socket_pipe: invalid test data received.")
        test_message = test_message[len(test_chunk):]

    return reader, writer

