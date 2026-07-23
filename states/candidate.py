from collections import defaultdict
from messages import AppendEntriesResponse, RequestVote
from states.state import State


class Candidate(State):
    name = "candidate"

    def set_server(self, server):
        super().set_server(server)
        self._voters = defaultdict(int)
        self._start_election()

    def on_request_vote_response(self, message):
        voter = message._sender
        success = message._success
        term = message._term

        if term > self._server._persistent_data._current_term:
            self._server._persistent_data._current_term = term
            self._server._persistent_data._voted_for = None
            follower = State.create("follower")
            follower.set_server(self._server)
            self._server._state = follower
            return None

        # A response from a stale term of ours no longer applies.
        if term < self._server._persistent_data._current_term:
            return None

        self._voters[voter] = success
        if list(self._voters.values()).count(True) > (len(self._server._neighbors) / 2):
            leader = State.create("leader")
            leader.set_server(self._server)
            self._server._state = leader

            return None

    def on_append_entry(self, message):
        leader_term = message._term
        sender = message._sender

        # This append entry from a legit leader?
        if leader_term >= self._server._persistent_data._current_term:
            follower = State.create("follower")
            follower.set_server(self._server)
            # Update state and process a normal append entry
            self._server._state = follower
            self._server._state.on_append_entry(message)

            return None

        elif leader_term < self._server._persistent_data._current_term:
            response = AppendEntriesResponse(self._server._id, sender, self._server._persistent_data._current_term, False)
            self._server._msg_queue.put(response)
            return None

    def on_election_timeout(self):
        self._start_election()

    def _start_election(self):
        self._server._persistent_data._current_term += 1
        self._voters[self._server._id] = True
        self._deadline = self._next_timeout()

        request = RequestVote(self._server._id,
                              None,
                        self._server._persistent_data._current_term,
                        self._server._id,
                        self._server._persistent_data._last_log_idx,
                        self._server._persistent_data._last_log_term,
                        )

        self._server._msg_queue.put(request)
