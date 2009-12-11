import sys
from collections import deque

class BytesFIFO(object):

    def __init__(self, content=''):
        """Initializes new `BytesFIFO` instance."""
        self._chunks = deque()
        self._total_length = 0
        self.put(content)

    def put(self, chunk):
        """Appends ``chunk`` to this FIFO contents."""
        if not chunk:
            return
        self._chunks.append(chunk)
        self._total_length += len(chunk)

    @property
    def available_bytes(self):
        """Returns the number of available bytes in this FIFO."""
        return self._total_length

    def get(self, max_bytes):
        """Returns at mosts ``max_bytes`` from FIFO. Returns at least one byte
        of the FIFO is not empty. """
        if not self._chunks:
            return ''
        next_chunk = self._chunks[0]
        if len(next_chunk) <= max_bytes:
            popped = self._chunks.popleft()
            self._total_length -= len(popped)
            assert popped is next_chunk
            return next_chunk
        else:
            self._chunks[0] = next_chunk[max_bytes:]
            self._total_length -= max_bytes
            return next_chunk[:max_bytes]

    def get_all(self, bytes=None):
        """Returns an iteratable over bytes chunks that some up to at most
        ``bytes`` bytes. Returns as much data as is available. """
        if bytes is None:
            chunks, self._chunks, self._total_length = self._chunks, deque(), 0
            return chunks

        def _get_all():
            remaining = bytes
            while remaining > 0:
                chunk = self.get(remaining)
                yield chunk
                remaining -= len(chunk)
                assert remaining >= 0

        return _get_all()

