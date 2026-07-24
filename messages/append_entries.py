from messages.message import Message
from messages.log_entry import LogEntry

class AppendEntries(Message):
    type = "append_entry"

    def __init__(self, sender :int, reciever, term: int, leader_id: int, prev_log_idx: int, prev_log_term: int, log_entry: LogEntry | dict, leader_commit: int) -> None:
        Message.__init__(self, sender, reciever)
        self._term = term
        self._leader_id = leader_id
        self._prev_log_idx = prev_log_idx
        self._prev_log_term = prev_log_term
        self._log_entry = LogEntry.from_dict(log_entry)
        self._leader_commit = leader_commit

    def to_dict(self):
        return {
            'sender': self._sender,
            'reciever': self._reciever,
            'term': self._term,
            'leader_id': self._leader_id,
            'prev_log_idx': self._prev_log_idx,
            'prev_log_term': self._prev_log_term,
            'log_entry': self._log_entry.to_dict(),
            'leader_commit': self._leader_commit,
        }