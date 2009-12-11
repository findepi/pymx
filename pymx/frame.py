
import struct
import zlib
from collections import deque

from .bytesfifo import BytesFIFO

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
        self._bytes_fifo = BytesFIFO()
        self._state = self.PRE_HEADER
        self._length = self._crc = None

    def push(self, chunk):
        """Returns iterator over zero of more unpacked frame contents available
        in the `Deframer` memory, after receiving additional ``chunk``. """
        self._bytes_fifo.put(chunk)
        return self._get_available_contents()

    def _get_available_contents(self):
        while True:
            contents = self._get_next_frame_contents()
            if contents is not None:
                yield contents
            else:
                return

    def _get_next_frame_contents(self):
        if self._state == self.PRE_HEADER and \
                self._bytes_fifo.available_bytes >=  _frame_header_length:
            self._length, self._crc = \
                    unpack_frame_header(self._get_header())
            self._state = self.PRE_CONTENTS

        if self._state == self.PRE_CONTENTS and \
                self._bytes_fifo.available_bytes >= self._length:

            contents = self._get_contents()
            self._state = self.PRE_HEADER
            check_contents_crc(contents, self._crc)
            return contents

    def _get_header(self):
        assert self._bytes_fifo.available_bytes >= _frame_header_length
        return ''.join(self._get_data(_frame_header_length))

    def _get_contents(self):
        assert self._bytes_fifo.available_bytes >= self._length
        return ''.join(self._get_data(self._length))

    def _get_data(self, data_length):
        return self._bytes_fifo.get_all(data_length)
