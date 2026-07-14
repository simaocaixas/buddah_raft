class Follower():
    
    def set_server(self, server):
        self._server = server

    def do_append_entry(self, message):
        sender = message._id
        term = message._term
        leader_commit = message._leader_commit
        prev_log_idx = message._prev_log_idx
        prev_log_term = message._prev_log_term
        entries = message._entries

        if term < self._server.term:
            self._server._msg_queue.put((self._server.term, False, sender))
            return None

        if leader_commit > self._server._commit_indx:
            self._server._commit_indx = min(leader_commit, len(self._server._log) - 1)

        elif prev_log_idx >= len(self._server._log) or self._server._log[prev_log_idx]._term != prev_log_term:
            self._server._msg_queue.put((self._server.term, False, self._sender))
            return None
        else:
            insert_idx = self._prev_log_idx + 1

            if insert_idx < len(self._server._log) and self._server._log[insert_idx].term != term:
                # conflicting entry delete it and everything after it
                self._server._log = self._server._log[:insert_idx]

            self._server._log.extend(entries)
            self._server._msg_queue.put((self._server._term, True, sender))
            self._server._presistantData._last_log_idx = prev_log_idx
            self._server._presistantData._last_log_term = prev_log_term
            return None
