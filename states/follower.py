from messages.append_entries_response import AppendEntriesResponse
from states.state import State

class Follower(State):
    name = "follower"

    def on_append_entry(self, message):
        sender = message._sender
        term = message._term
        leader_commit = message._leader_commit
        prev_log_idx = message._prev_log_idx
        prev_log_term = message._prev_log_term
        entries = message._entries

        if term < self._server._current_term:
            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None

        self._deadline = self._next_timeout()

        if leader_commit > self._server._commit_idx:
            self._server._commit_idx = min(leader_commit, len(self._server._log) - 1)

        elif len(self._server._log) < prev_log_idx or (len(self._server._log) > 0 and self._server._log[prev_log_idx]._term != prev_log_term)        :
            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None
        else:
            insert_idx = prev_log_idx + 1

            if insert_idx < len(self._server._log) and self._server._log[insert_idx]._term != term:
                # conflicting entry delete it and everything after it
                self._server._log = self._server._log[:insert_idx]

            self._server._log.extend(entries)

            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, True)
            self._server.enqueue(response)


            self._server._last_log_idx = prev_log_idx
            self._server._last_log_term = prev_log_term
            return None

    def on_election_timeout(self):
        candidate = State.create("candidate")
        self._server._state = candidate
        candidate.set_server(self._server)
