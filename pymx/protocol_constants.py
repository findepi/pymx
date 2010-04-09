
class PeerTypes:
    MULTIPLEXER = 1
    ALL_TYPES = 2
    MAX_MULTIPLEXER_SPECIAL_PEER_TYPE = 99

class MessageTypes:
    PING = 1
    CONNECTION_WELCOME = 2
    BACKEND_FOR_PACKET_SEARCH = 3
    HEARTBIT = 4
    DELIVERY_ERROR = 5
    MAX_MULTIPLEXER_META_PACKET = 99
    REQUEST_RECEIVED = 113
    BACKEND_ERROR = 114