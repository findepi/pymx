
from nose.tools import assert_equal, assert_raises

from pymx.frame import Deframer, create_frame_header, unpack_frame_contents, \
        FrameTooShortError,FrameTooLongError, rameCorruptedError

from .testlib_chop_bytes import chop_bytes
from .testlib_random import get_random

random = get_random()

frame_contents = [
        # frame contents are repeated for testing Deframer
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'r', 's', 't', 'u', 'w', 'x', 'y', 'z',
        '', '', '', '', '', '', '', '', '',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        '', 'a', '', 'some string', '<message>', '\x00', '\n', '\xff',
        ''.join(chr(random.randint(0, 255)) for _ in xrange(1024)),
        ''.join(chr(random.randint(0, 255)) for _ in xrange(1024)) * 64,
    ]

def test_framing():
    for contents in frame_contents:
        assert _unpack_contents(contents) == contents
        assert_raises(FrameTooShortError, _unpack_contents, contents,
                change_length=True)
        assert_raises(FrameTooLongError, _unpack_contents, contents,
                extend_contents=True)
        assert_raises(FrameCorruptedError, _unpack_contents, contents,
                change_crc=True)

def _unpack_contents(contents, change_length=False, change_crc=False,
        extend_contents=False):
    header = create_frame_header(contents)
    length, crc = header[:4], header[4:]
    assert len(length) == len(crc)
    if change_length:
        length = chr(ord(length[0]) + 1) + length[1:]
    if change_crc:
        crc = chr(ord(crc[0]) + 1) + crc[1:]
    if extend_contents:
        contents += '\x01'
    return unpack_frame_contents(length + crc + contents)

def test_deframer():
    yield check_deframer, 0, 2
    yield check_deframer, 0, 9
    yield check_deframer, 0, 127
    yield check_deframer, 7, 7
    yield check_deframer, 8, 8
    yield check_deframer, 9, 9
    yield check_deframer, 16, 80
    yield check_deframer, 80, 80
    yield check_deframer, 512, 512

def check_deframer(min_chunk_length, max_chunk_length):
    def _genrate_chunks():
        input_bytes = ''.join(
            bytes for contents in frame_contents for bytes in
                (create_frame_header(contents), contents))
        return chop_bytes(input_bytes, min_length=min_chunk_length,
                max_length=max_chunk_length)

    def _generate_contents(deframer):
        for chunk in _genrate_chunks():
            for contents in deframer.push(chunk):
                yield contents

    deframed_contents = list(_generate_contents(Deframer()))
    assert_equal(deframed_contents, frame_contents)
