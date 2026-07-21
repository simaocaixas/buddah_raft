from messages.message import Message

class AppendEntriesResponse(Message):
    type = "append_entry_response"

    def __init__(self, sender, reciever, msg, current_term, success = True) -> None:
        Message.__init__(self, sender, reciever)
        self._msg = msg
        self._term = current_term
        self._success = success

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'current_term': self._term,
            'msg': self._msg,
            'success': self._success,
        }