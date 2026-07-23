from messages.message import Message

class RequestVoteResponse(Message):
    type = "request_vote_response"

    def __init__(self, sender: int, reciever: int, term: int, success = True) -> None:
        Message.__init__(self, sender, reciever)
        self._term = term
        self._success = success

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'term': self._term,
            'success': self._success,
        }