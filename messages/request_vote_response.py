from messages.message import Message

class RequestVoteResponse(Message):
    type = "request_vote_response"

    def __init__(self, sender: int, term: int, msg, sucess = True) -> None:
        Message.__init__(self, sender)
        self._sender = sender
        self._term = term
        self._msg = msg
        self._sucess = sucess
