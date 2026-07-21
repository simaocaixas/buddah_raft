from messages.append_entries_response import AppendEntriesResponse
from messages.request_vote_response import RequestVoteResponse
from states.state import State

class Follower(State):

    def on_append_entry(self, message):
        sender = message._sender
        term = message._term
        leader_commit = message._leader_commit
        prev_log_idx = message._prev_log_idx
        prev_log_term = message._prev_log_term
        entries = message._entries

        if term < self._server._persistent_data._current_term:
            response = AppendEntriesResponse(self._server._id, sender, message, self._server._persistent_data._current_term, False)
            self._server._msg_queue.put(response)
            return None

        if leader_commit > self._server._commit_idx:
            self._server._commit_idx = min(leader_commit, len(self._server._persistent_data._log) - 1)

        elif prev_log_idx >= len(self._server._persistent_data._log) or self._server._persistent_data._log[prev_log_idx]._term != prev_log_term:
            response = AppendEntriesResponse(self._server._id, sender, message, self._server._persistent_data._current_term, False)
            self._server._msg_queue.put(response)
            return None
        else:
            insert_idx = prev_log_idx + 1

            if insert_idx < len(self._server._persistent_data._log) and self._server._persistent_data._log[insert_idx]._term != term:
                # conflicting entry delete it and everything after it
                self._server._persistent_data._log = self._server._persistent_data._log[:insert_idx]

            self._server._persistent_data._log.extend(entries)

            response = AppendEntriesResponse(self._server._id, sender, message, self._server._persistent_data._current_term, True)
            self._server._msg_queue.put(response)


            self._server._persistent_data._last_log_idx = prev_log_idx
            self._server._persistent_data._last_log_term = prev_log_term
            return None


    def on_vote_request(self, message):
        sender = message._sender
        candidate_term = message._term
        candidate_id = message._candidate_id
        candidate_last_log_idx = message._last_log_index
        candidate_last_log_term = message._last_log_term

        if candidate_term < self._server._persistent_data._current_term:
            response = RequestVoteResponse(self._server._id,
                                           sender,
                                           self._server._persistent_data._current_term,
                                           message,
                                           False
                                           )

        elif (self._server._persistent_data._voted_for is None or self._server._persistent_data._voted_for == candidate_id) \
                and candidate_last_log_idx >= self._server._persistent_data._last_log_idx \
                and candidate_last_log_term >= self._server._persistent_data._last_log_term:
            self._server._persistent_data._voted_for = candidate_id
            response = RequestVoteResponse(self._server._id,
                            sender,
                            self._server._persistent_data._current_term,
                            message,
                            True
                            )
        else:
            response = RequestVoteResponse(self._server._id,
                            sender,
                            self._server._persistent_data._current_term,
                            message,
                            False
                            )
        self._server._msg_queue.put(response)

        return None