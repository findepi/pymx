
from pymx.message import MultiplexerMessage
from pymx.protobuf import parse_message, dict_message

from .testlib_encoded_messages import encoded_messages

def test_serialize():
    # The order of serialized fields is not guaranteed, so we can check
    # serialization using one field only.
    msg = MultiplexerMessage()
    msg.type = 3 # This should be the only required field.
    assert msg.SerializeToString() == ' \x03'

def test_deserialize():
    for case in encoded_messages:
        msg = parse_message(MultiplexerMessage, case['encoded'])
        assert dict((field.name, value) for field, value in msg.ListFields()) \
                == case['pythonized'] == dict_message(msg, all_fields=False)

def test_from():
    # Check that ``from_`` and ``from`` can be used interchangeably.
    msg = MultiplexerMessage()
    msg.from_ = 10
    assert getattr(msg, 'from') == 10
    setattr(msg, 'from', 20)
    assert msg.from_ == 20

    # Check that the value assigned via ``from_`` is properly serialized.
    msg = MultiplexerMessage()
    msg.from_ = 15
    assert msg.SerializePartialToString() == '\x10\x0f'

    # Check hat the deserialized ``from`` can be read via ``from_``.
    assert parse_message(MultiplexerMessage, '\x10\x0f', partial=True).from_ == 15
