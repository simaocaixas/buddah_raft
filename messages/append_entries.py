from messages.message import Message

class AppendEntries(Message):
    type = "append_entry"

    def __init__(self, term: int, leader_id: int, prev_log_idx: int, prev_log_term: int, entries: list[int], leader_commit: int) -> None:
        self._term = term
        self._leader_id = leader_id
        self._prev_log_idx = prev_log_idx
        self._prev_log_term = prev_log_term
        self._entries = entries
        self._leader_commit = leader_commit

    def handle(self, server, msg_queue) -> None:
        if self._term < self._server.term:
            msg_queue.put((self.server.term, False))
        elif self._prev_log_idx >= len(self._server.log) or self._server.log[self._prev_log_idx].term != self._prev_log_term:
            msg_queue.put((self.server.term, False))
        else:
            insert_idx = self._prev_log_idx + 1

            if insert_idx < len(self._server.log) and self._server.log[insert_idx].term != self._term:
                # conflicting entry delete it and everything after it
                self._server.log = self._server.log[:insert_idx]

            self._server.log.extend(self._entries)

            msg_queue.put((self.server.term, True))

            if self._leader_commit > self._server._commit_indx:
                self._server._commit_indx = min(self._leader_commit, len(self._server.log) - 1)
