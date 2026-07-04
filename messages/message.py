_MESSAGE_TYPES: dict[str, type] = {}
# "append_entry": AppendEntries, "request_vote": RequestVote ...

class Message():
    type: str = None

    def __init__(self, sender, reciver):
        self._sender = sender
        self._reciver = reciver

    def __init_subclass__(cls, **kwargs) -> None:
            super().__init_subclass__(**kwargs)
            if cls.type is not None:
                _MESSAGE_TYPES[cls.type] = cls

    def to_dict(self) -> dict:
        pass

    def handle(self, server, msg_queue) -> dict:
        pass

    @staticmethod
    def from_dict(raw: dict) -> 'Message':
        cls = _MESSAGE_TYPES[raw["type"]]
        return cls(**raw["data"])