from collections import defaultdict
from messages import AppendEntries
from states.state import State

class Leader(State):
    name = "leader"

    def __init__(self):
        self._next_indexes = defaultdict(int)
        self._match_index = defaultdict(int)

    def set_server(self, server):
        super().set_server(server)

        neighbors = self._server._neighbors

        for n in neighbors:
            self._next_indexes[n] = self._server._persistent_data._last_log_idx + 1
            self._match_index[n] = 0

        self._server.heartbeat_tick()

    def on_command(self, commands):

        entry = AppendEntries(self._server._id,
                              None,
                                self._server._persistent_data._current_term,
                                self._server._id,
                                self._server._persistent_data._last_log_idx,
                                self._server._persistent_data._last_log_term,
                                commands,
                                self._server._commit_idx
                                )

        # Upon receiving client command the leader appends it to its Log
        self._server._persistent_data._log.append(entry)

        self._server._msg_queue.put(entry)

        return None

    def on_append_entry_response(self, message):

        neighbor_term = message._term
        sender = message._sender
        success = message._success

        # Was the append entry successfully appended to followers log?
        if (not success):

            # Was it because my term is out of date (stale leader)?
            if neighbor_term > self._server._persistent_data._current_term:

                self._server._persistent_data._current_term = neighbor_term
                self._server._persistent_data._voted_for = None

                follower = State.create("follower")
                follower.set_server(self._server)
                self._server._state = follower

                return None

            # It needed an earlier entry
            self._next_indexes[sender] = max(0, self._next_indexes[sender] - 1)

            previous_entry_index = max(0, self._next_indexes[sender] - 1)
            previous_entry_term = self._server._persistent_data._log[previous_entry_index]._term
            current = self._server._persistent_data._log[self._next_indexes[sender]]

            entry = AppendEntries(self._server._id,
                                  sender,
                                        self._server._persistent_data._current_term,
                                        self._server._id,
                                        previous_entry_index,
                                        previous_entry_term,
                                        [current],
                                        self._server._commit_idx
                                        )

            self._server._msg_queue.put(entry)

        else:
            self._next_indexes[sender] = min(self._next_indexes[sender] + 1, self._server._persistent_data._last_log_idx + 1)

        return None

    def send_heart_beat(self) -> None:
        new_entry = AppendEntries(self._server._id,
                                  None,
                                  self._server._persistent_data._current_term,
                                  self._server._id,
                                  self._server._persistent_data._last_log_idx,
                                  self._server._persistent_data._last_log_term,
                                  [],
                                  self._server._commit_idx
                                  )

        self._server._msg_queue.put(new_entry)