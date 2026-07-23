from messages.message import Message

class AppendEntriesResponse(Message):
    type = "append_entry_response"

    def __init__(self, sender, reciever, current_term, success = True) -> None:
        Message.__init__(self, sender, reciever)
        self._term = current_term
        self._success = success

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'current_term': self._term,
            'success': self._success,
        }