from messages.message import Message

class AppendEntries(Message):
    type = "append_entry"

    def __init__(self, sender :int, reciver, term: int, leader_id: int, prev_log_idx: int, prev_log_term: int, entries: list[int], leader_commit: int) -> None:
        Message.__init__(self, sender, reciver)
        self._term = term
        self._leader_id = leader_id
        self._prev_log_idx = prev_log_idx
        self._prev_log_term = prev_log_term
        self._entries = entries
        self._leader_commit = leader_commit
