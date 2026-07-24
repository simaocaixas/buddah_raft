from collections import defaultdict
from messages import AppendEntries, AppendEntriesResponse, LogEntry
from states.state import State
import threading

class Leader(State):
    name = "leader"

    def __init__(self):
        self._next_indexes = defaultdict(int)
        self._match_index = defaultdict(int)

    def set_server(self, server):
        super().set_server(server)

        neighbors = self._server._neighbors

        for n in neighbors:
            self._next_indexes[n] = self._server._last_log_idx + 1
            self._match_index[n] = -1

        self._heartbeat_tick()

    def on_command(self, commands):

        # A log entry are commands associated with a logic timestamp (term)
        log_entry = LogEntry(self._server._current_term, commands)

        msg = AppendEntries(self._server._id,
                              None,
                                self._server._current_term,
                                self._server._id,
                                self._server._last_log_idx,
                                self._server._last_log_term,
                                log_entry,
                                self._server._commit_idx
                                )

        # Upon receiving client command the leader appends it to its Log
        self._server._log.append(log_entry)

        self._server._last_log_idx = len(self._server._log) - 1
        self._server._last_log_term = self._server._current_term

        # The leader has all of its own entries replicated
        self._match_index[self._server._id] = self._server._last_log_idx
        self._server.enqueue(msg)

        return None

    # This is here to avoid a split brain problem
    def on_append_entry(self, message):
        sender = message._sender
        term = message._term

        # There is another leader and i am the stale one
        # I should convert to follower and answer
        if term > self._server._current_term:

            self._stop_heartbeat()

            follower = State.create("follower")
            follower.set_server(self._server)
            # Update state and process a normal append entry
            self._server._state = follower
            self._server._state.on_append_entry(message)

            return None

        # There is another leader and he is the stale one
        # I should convert him to follower sending my with current term
        if term < self._server._current_term:
            response = AppendEntriesResponse(self._server._id, sender, self._server._current_term, False)
            self._server.enqueue(response)
            return None

        # My term == his term should be impossible
        return None


    def on_append_entry_response(self, message):

        neighbor_term = message._term
        sender = message._sender
        success = message._success

        # Was the append entry successfully appended to followers log?
        if (not success):

            # Was it because my term is out of date (stale leader)?
            if neighbor_term > self._server._current_term:

                self._server._current_term = neighbor_term
                self._server._voted_for = None

                self._stop_heartbeat()

                follower = State.create("follower")
                follower.set_server(self._server)
                self._server._state = follower

                return None

            # It needed an earlier entry
            self._next_indexes[sender] = max(0, self._next_indexes[sender] - 1)

            previous_entry_index = max(0, self._next_indexes[sender] - 1)
            previous_entry_term = self._server._log[previous_entry_index]._term
            log_entry = self._server._log[self._next_indexes[sender]]

            entry = AppendEntries(self._server._id,
                                  sender,
                                        self._server._current_term,
                                        self._server._id,
                                        previous_entry_index,
                                        previous_entry_term,
                                        log_entry,
                                        self._server._commit_idx
                                        )

            self._server.enqueue(entry)

        else:
            # The AppendEntry was successfull, we can advance both match_index and next_indexes
            self._match_index[sender] = max(self._next_indexes[sender], self._match_index[sender])
            self._next_indexes[sender] = min(self._next_indexes[sender] + 1, self._server._last_log_idx + 1)

            # Can we garantee that a majority quorum commited the entry?
            majority = len(self._server._neighbors) // 2 + 1
            indexes = sorted(self._match_index.values(), reverse=True)
            
            # N is the minimum index that every node log has already commited
            N = indexes[majority - 1]

            if N > self._server._commit_idx and N >= 0 and self._server._log[N]._term == self._server._current_term:
                self._server._commit_idx = N

        return None


    # Since it only needs a majority sized quorum to be elected the
    # leader can still keep reciving votes and vote responses, this is that safeguard
    def on_request_vote(self, message):
        term = message._term
        if term > self._server._current_term:
            self._stop_heartbeat()
        return super().on_request_vote(message)

    def on_request_vote_response(self, message):
        term = message._term
        if term > self._server._current_term:
            self._server._current_term = term
            self._server._voted_for = None
            self._stop_heartbeat()
            follower = State.create("follower")
            follower.set_server(self._server)
            self._server._state = follower
            return None

    def _heartbeat_tick(self):
        self._send_heart_beat()
        timer = threading.Timer(1, self._heartbeat_tick)
        timer.daemon = True
        timer.start()
        self._timer = timer

    def _stop_heartbeat(self):
        self._timer.cancel()

    def _send_heart_beat(self) -> None:
        empty_log_entry = LogEntry(self._server._current_term, [])

        heartbeat = AppendEntries(self._server._id,
                                  None,
                                  self._server._current_term,
                                  self._server._id,
                                  self._server._last_log_idx,
                                  self._server._last_log_term,
                                  empty_log_entry,
                                  self._server._commit_idx
                                  )

        self._server.enqueue(heartbeat)