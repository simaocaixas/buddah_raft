from collections import defaultdict
from messages.request_vote_response import RequestVoteResponse
from messages.request_vote import RequestVote
from states.leader import Leader
from states.follower import Follower

class Candidate():
    
    def set_server(self, server):
        self._server = server
        self._voters = defaultdict(int)

    def on_vote_request(self, message):
        data = message.get('data')
        
        candidate_term = data.get('term')
        candidate = data.get('candidate_id')
        candidate_last_log_idx = data.get('last_log_idx')
        candidate_last_log_term = data.get('last_log_term')

        if candidate_term < self._server._persistant_data._current_term:
            response = RequestVoteResponse(self._server._id,
                                           self._server._persistant_data._current_term,
                                           message,
                                           False
                                           )
        elif self._server._persistent_data._voted_for == None or self._server._persistent_data._voted_for == candidate:
            if candidate_last_log_idx >= self._server._persistent_data._last_log_idx and candidate_last_log_term >= self._server._persistent_data._last_log_term:
                response = RequestVoteResponse(self._server._id,
                                self._server._persistant_data._current_term,
                                message,
                                True
                                )
                self._server._persistent_data._voted_for = candidate
        self._server._msg_queue.put(response)

        return None

    def on_vote_response(self, message):
        data = message.get('data')
        voter = data.get('sender')
        success = data.get('success')

        self._voters[voter] = success
        if self._voters.values().count(True) > (len(self._server._neighboors / 2)):
            leader = Leader()
            leader.set_server(self._server)

            self._server._state = leader

            return None

    def do_append_entry(self, message):
        data = message.get('data')
        leader_term = data.get('term')
        sender = message.get('sender')

        # This append entry from a legit leader?
        if leader_term >= self._server._persistant_data._current_term:
            follower = Follower()
            follower.set_server(self._server)

            # Update state and process a normal append entry
            self._server._state = follower
            self._server._state.do_append_entry(message)
        
            # TODO Reset election timeout
            return None
        elif leader_term < self._server._persistant_data._current_term:
            self._server._msg_queue.put((self._server.term, False, sender))
            return None
    
    def start_election(self):
        self._server._persistent_data.current_term += 1
        self._voters[self._server._id] = True
        # TODO reset election timer
        request = RequestVote(self._server._id,
                        self._server._persistant_data._current_term,
                        self._server._persistant_data._last_log_idx,
                        self._server._persistant_data._last_log_term,
                        )

        self._server._msg_queue.put(request)
