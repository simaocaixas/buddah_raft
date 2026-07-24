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

        # Term Update
        if term > self._server._current_term:
            self._server._current_term = term
            self._server._voted_for = None

        # Stale leader check, reject right away
        if term < self._server._current_term:
            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None

        # This is a valid leader so now we can safefly reset the deadline
        self._deadline = self._next_timeout()

        if len(self._server._log) <= prev_log_idx:
            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None

        # Conflict Detetion in a earlier entry
        if prev_log_idx >= 0 and self._server._log[prev_log_idx]._term != prev_log_term:

            # Here we already know that all the entries after _server._log[prev_log_idx] are to be discarded
            self._server._log = self._server._log[:prev_log_idx]
            self._server._last_log_idx = prev_log_idx - 1
            self._server._last_log_term = self._server._log[self._server._last_log_idx]._term if self._server._log else -1

            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None
        else:
            # Heartbeat Detection
            if (entries):
                self._server._log = self._server._log[:prev_log_idx + 1]

                # Append any new entries not already in the log
                for entry in entries:
                    self._server._log.append(entry)

                self._server._last_log_idx = len(self._server._log) - 1
                self._server._last_log_term = entries[-1]._term

            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, True)
            self._server.enqueue(response)

            index_of_last_new_entry = prev_log_idx + len(entries)
            if leader_commit > self._server._commit_idx:
                self._server._commit_idx = min(leader_commit, index_of_last_new_entry)

        return None

    def on_election_timeout(self):
        candidate = State.create("candidate")
        self._server._state = candidate
        candidate.set_server(self._server)
