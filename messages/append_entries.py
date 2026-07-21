from messages.message import Message

class AppendEntries(Message):
    type = "append_entry"

    def __init__(self, sender :int, reciever, term: int, leader_id: int, prev_log_idx: int, prev_log_term: int, entries: list[int], leader_commit: int) -> None:
        Message.__init__(self, sender, reciever)
        self._term = term
        self._leader_id = leader_id
        self._prev_log_idx = prev_log_idx
        self._prev_log_term = prev_log_term
        self._entries = entries
        self._leader_commit = leader_commit

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'term': self._term,
            'leader_id': self._leader_id,
            'prev_log_idx': self._prev_log_idx,
            'prev_log_term': self._prev_log_term,
            'entries': [e.to_dict() for e in self._entries],
            'leader_commit': self._leader_commit,
        }