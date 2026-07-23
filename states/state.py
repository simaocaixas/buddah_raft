import random
import time
from messages import RequestVoteResponse

_STATE_TYPES: dict[str, type] = {}
# "follower": Follower, "candidate": Candidate, "leader": Leader ...

class State():
    name: str = None

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        if cls.name is not None:
            _STATE_TYPES[cls.name] = cls

    @staticmethod
    def create(name: str) -> 'State':
        return _STATE_TYPES[name]()

    def set_server(self, server):
        self._server = server
        self._deadline = self._next_timeout()

    def on_command(self, commands):
        """Called when a client sends a command to be replicated."""

    def on_append_entry(self, message):
        """Called when an AppendEntries RPC is received from a leader."""

    def on_append_entry_response(self, message):
        """Called when a follower replies to our AppendEntries RPC."""

    def on_request_vote(self, message):
        candidate_term = message._term

        if candidate_term > self._server._persistent_data._current_term:
            self._server._persistent_data._current_term = candidate_term
            self._server._persistent_data._voted_for = None
            follower = State.create("follower")
            follower.set_server(self._server)
            self._server._state = follower
            return self._server._state.on_request_vote(message)

        sender = message._sender
        candidate_id = message._candidate_id
        candidate_last_log_idx = message._last_log_index
        candidate_last_log_term = message._last_log_term

        if candidate_term < self._server._persistent_data._current_term:
            response = RequestVoteResponse(self._server._id,
                                           sender,
                                           self._server._persistent_data._current_term,
                                           False
                                           )

        elif (self._server._persistent_data._voted_for is None or self._server._persistent_data._voted_for == candidate_id) \
                and candidate_last_log_idx >= self._server._persistent_data._last_log_idx \
                and candidate_last_log_term >= self._server._persistent_data._last_log_term:
            self._server._persistent_data._voted_for = candidate_id
            self._deadline = self._next_timeout()
            response = RequestVoteResponse(self._server._id,
                            sender,
                            self._server._persistent_data._current_term,
                            True
                            )
        else:
            response = RequestVoteResponse(self._server._id,
                            sender,
                            self._server._persistent_data._current_term,
                            False
                            )
        self._server._msg_queue.put(response)

        return None

    def on_request_vote_response(self, message):
        """Called when a peer replies to our RequestVote RPC."""

    def send_heart_beat(self):
        """Called on the heartbeat tick to keep followers from timing out."""

    def on_election_timeout(self):
        """Called when this server's election deadline elapses."""

    def _next_timeout(self):
        self._current_time = time.time()
        return self._current_time + random.uniform(3, 6)

