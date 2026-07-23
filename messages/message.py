_MESSAGE_TYPES: dict[str, type] = {}
# "append_entry": AppendEntries, "request_vote": RequestVote ...

class MessageHandler(object):

    def handle(self, msg, state):

        if msg.type == 'append_entry':
            state.on_append_entry(msg)

        elif msg.type == 'append_entry_response':
            state.on_append_entry_response(msg)

        elif msg.type == 'request_vote':
            state.on_request_vote(msg)

        elif msg.type == 'request_vote_response':
            state.on_request_vote_response(msg)

class Message():
    type: str = None

    def __init__(self, sender, reciever=None):
        self._sender = sender
        self._reciever = reciever

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
