from messages.message import Message

class RequestVoteResponse(Message):
    type = "request_vote_response"

    def __init__(self, sender: int, reciver: int, term: int, msg, success = True) -> None:
        Message.__init__(self, sender, reciver)
        self._term = term
        self._msg = msg
        self._success = success
