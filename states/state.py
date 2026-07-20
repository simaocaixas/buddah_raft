class State():
    
    def set_server(self, server):
        self._server = server

    def on_command(self, commands):
        pass

    def on_append_entry(self, message):
        pass

    def on_append_entry_response(self, message):
        pass

    def on_request_vote(self, message):
        pass

    def on_request_vote_response(self, message):
        pass

    def send_heart_beat(self):
        pass