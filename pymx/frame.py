
import struct
import zlib
from collections import deque

class InvalidFrameError(Exception):
    pass

class FrameTooShortError(InvalidFrameError):
    pass

class FrameTooLongError(InvalidFrameError):
    pass

class FrameCorruptedError(InvalidFrameError):
    pass

_frame_header_format = "Ii"
_frame_header_length = struct.calcsize(_frame_header_format)

def create_frame_header(contents):
    """Create a message frame header for given contents."""
    header = struct.pack(_frame_header_format, len(contents),
            zlib.crc32(contents))
    return header

def unpack_frame_header(header):
    return struct.unpack(_frame_header_format, header)

def check_contents_crc(contents, expected_crc):
    if zlib.crc32(contents) != expected_crc:
        raise FrameCorruptedError((contents, expected_crc))

def check_frame(frame):
    """Validate frame correctness. Raises appropriate exception when the frame
    is invalid. """
    if len(frame) < _frame_header_length:
        raise FrameTooShortError("Frame %r is too short." % (frame,))

    length, crc = unpack_frame_header(frame[:_frame_header_length])
    if len(frame) < _frame_header_length + length:
        raise FrameTooShortError
    elif len(frame) > _frame_header_length + length:
        raise FrameTooLongError

    contents = _frame_contents(frame)
    assert len(contents) == length
    check_contents_crc(contents, crc)

def _frame_contents(frame):
    return frame[_frame_header_length:]

def unpack_frame_contents(frame):
    check_frame(frame)
    return _frame_contents(frame)


class Deframer(object):

    """An efficient stream defragmenter. Push the byte stream data chunks
    through a ``Deframer`` instance to extract and validate individual frames.
    """

    PRE_HEADER = 1
    PRE_CONTENTS = 2

    def __init__(self):
        self._chunks = deque()
        self._total_length = 0
        self._state = self.PRE_HEADER

        self._length = self._crc = None

    def push(self, chunk):
        """Returns iterator over zero of more unpacked frame contents available
        in the `Deframer` memory, after receiving additional ``chunk``. """
        if not chunk:
            return iter(())

        self._chunks.append(chunk)
        self._total_length += len(chunk)

        return self._get_available_contents()

    def _get_available_contents(self):
        while True:
            contents = self._get_next_frame_contents()
            if contents is not None:
                yield contents
            else:
                return

    def _get_next_frame_contents(self):
        if self._state == self.PRE_HEADER and self.available_bytes >= \
                _frame_header_length:
            self._length, self._crc = \
                    unpack_frame_header(self._get_header())
            self._state = self.PRE_CONTENTS

        if self._state == self.PRE_CONTENTS and self.available_bytes >= \
                self._length:

            contents = self._get_contents()
            self._state = self.PRE_HEADER
            check_contents_crc(contents, self._crc)
            return contents

    @property
    def available_bytes(self):
        return self._total_length

    def _get_header(self):
        assert self.available_bytes >= _frame_header_length
        return ''.join(self._get_data(_frame_header_length))

    def _get_contents(self):
        assert self.available_bytes >= self._length
        return ''.join(self._get_data(self._length))

    def _get_data(self, data_length):
        """Extract ``data_length`` bytes from chunks lists. Returns iterator
        over extracted chunks. """
        assert self.available_bytes == self._total_length >= data_length >= 0
        while data_length:
            next_chunk = self._chunks[0]
            if len(next_chunk) <= data_length:
                popped = self._chunks.popleft()
                self._total_length -= len(popped)
                assert popped is next_chunk
                yield next_chunk
                data_length -= len(next_chunk)
            else:
                self._chunks[0] = next_chunk[data_length:]
                self._total_length -= data_length
                yield next_chunk[:data_length]
                return
