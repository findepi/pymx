
from google.protobuf.message import DecodeError

from nose.tools import eq_, raises

from pymx.message import MultiplexerMessage
from pymx.protobuf import parse_message, dict_message, make_message, \
        message_getattr

from .testlib_encoded_messages import encoded_messages
from .TestMessages_pb2 import VariousFields, VariousFieldsList

def test_make_message():
    def _make_message_kwargs_as_dict(type, *args, **kwargs):
        if args:
            return make_message(type, *args, **kwargs)
        else:
            return make_message(type, kwargs)

    yield check_make_message, make_message
    yield check_make_message, _make_message_kwargs_as_dict

def check_make_message(make_message):
    r = {'req_uint32': 1, 'rep_uint32': [3,4]}
    msg = make_message(VariousFields, **r)
    eq_(msg.req_uint32, 1)
    eq_(msg.opt_uint32, 0)
    assert list(msg.rep_uint32) == [3,4]
    eq_(dict_message(msg), r)
    assert dict_message(msg, all_fields=True) == dict(r, opt_uint32=0)

    msg2 = make_message(VariousFieldsList, field=r)
    eq_(msg2.field, msg)
    eq_(dict_message(msg2, recursive=True), {'field': r})

    msg3 = make_message(VariousFieldsList,
            field=make_message(VariousFields, **r))
    eq_(msg3.field, msg)
    eq_(dict_message(msg3, recursive=True), {'field': r})

    raises(ValueError)(lambda: make_message(VariousFieldsList, field=[]))()
    raises(ValueError)(lambda: make_message(VariousFieldsList, field=1))()

    r = range(10)
    msgs = make_message(VariousFieldsList, fields=[
            {'req_uint32': 1},
            {'opt_uint32': 2, 'rep_uint32': []},
            {'rep_uint32': r},
        ])
    eq_(msgs.fields[0].req_uint32, 1)
    eq_(msgs.fields[0].opt_uint32, 0)
    eq_(list(msgs.fields[0].rep_uint32), [])

    eq_(msgs.fields[1].req_uint32, 0)
    eq_(msgs.fields[1].opt_uint32, 2)
    eq_(list(msgs.fields[1].rep_uint32), [])

    eq_(msgs.fields[2].req_uint32, 0)
    eq_(list(msgs.fields[2].rep_uint32), r)

def test_message_getattr():
    msg = VariousFields()
    assert message_getattr(msg, 'opt_uint32') is None
    assert message_getattr(msg, 'req_uint32') is None
    raises(AttributeError, ValueError)(
            lambda: message_getattr(msg, 'rep_uint32'))()

    msg.req_uint32 = msg.opt_uint32 = 1
    eq_(message_getattr(msg, 'req_uint32'), 1)
    eq_(message_getattr(msg, 'opt_uint32'), 1)

    x = ()
    eq_(message_getattr(x, 'count'), ().count)
    assert message_getattr(x, 'nothingspecial', Ellipsis) is Ellipsis

@raises(AttributeError)
def test_del_from_raises():
    del MultiplexerMessage().from_

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

    raises(DecodeError)(lambda: parse_message(VariousFields, ''))()

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
