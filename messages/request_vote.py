from messages.message import Message

class RequestVote(Message):
    type = "request_vote"

    def __init__(self, sender, reciever: int, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> None:
        Message.__init__(self, sender, reciever)
        self._term = term
        self._candidate_id = candidate_id
        self._last_log_index = last_log_index
        self._last_log_term = last_log_term

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'term': self._term,
            'candidate_id': self._candidate_id,
            'last_log_index': self._last_log_index,
            'last_log_term': self._last_log_term
        }