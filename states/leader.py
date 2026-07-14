from collections import defaultdict
from messages.append_entries import AppendEntries

class Leader():
    
    def __init__(self):
        self._next_indexes = defaultdict(int)
        self._match_index = defaultdict(int)

    def set_server(self, server):
        self._server = server
        
        neighboors = self._server._neighboor_ports

        for n in neighboors:
            self._nextIndexes[n.id_] = self._server._last_log_idx + 1
            self._match_index[n.id_] = 0


    def do_append_entry_response(self, message):
        
        data = message.get('data')
        
        if (not data.sucess):
            self._nextIndexes[message.sender] -= 1
        
            previous_entry_index = max(0, self._nextIndexes[message.sender] - 1)
            previous_entry_term = self._server._log[previous_entry_index]._term
            current = self._server._log[self._nextIndexes[message.sender]]

            new_entry = AppendEntries(self._server._id,
                                        self._server._presistent_data._current_term,
                                        self._server._id,
                                        previous_entry_index,
                                        previous_entry_term,
                                        [current],
                                        self._server._commit_idx
                                        )

            self._server._log.append(new_entry)
            self._msg_queue.append(new_entry)

        else:
            self._next_indexes[message.sender] = max(self._next_indexes[message.sender] + 1, self._server._last_log_idx)
        
        return None


    def send_heart_beat(self) -> None:
        new_entry = AppendEntries(self._server._presistent_data._current_term,
                                  self._server._id,
                                  self._server._presistent_data._last_log_idx,
                                  self._server._presistent_data._last_log_term,
                                  [],
                                  self._server._commit_idx
                                  )
        self._msg_queue.append(new_entry)
