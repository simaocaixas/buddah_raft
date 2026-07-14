_MESSAGE_TYPES: dict[str, type] = {}
# "append_entry": AppendEntries, "request_vote": RequestVote ...

class MessageHandler(object):

    def setup(self, state):
        self._state = state

    def handle(self, msg):
        with self.lock:
            if msg.type == _MESSAGE_TYPES.get('append_entry'):
                self._state.do_append_entry(msg)
            elif msg.type == _MESSAGE_TYPES.get('append_entry_response'):
                self._state.do_append_entry_response(msg)
            elif msg.type == _MESSAGE_TYPES.get('request_vote'):
                self._state.on_vote_request(msg)
            elif msg.type == _MESSAGE_TYPES.get('request_vote_response'):
                self._state.on_vote_response(msg)

class Message():
    type: str = None

    def __init__(self, sender):
        self._sender = sender

    def __init_subclass__(cls, **kwargs) -> None:
            super().__init_subclass__(**kwargs)
            if cls.type is not None:
                _MESSAGE_TYPES[cls.type] = cls

    def to_dict(self) -> dict:
        pass

    @staticmethod
    def from_dict(raw: dict) -> 'Message':
        cls = _MESSAGE_TYPES[raw["type"]]
        return cls(**raw["data"])
