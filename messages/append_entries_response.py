from messages.message import Message
from states.follower import Follower

class AppendEntriesReponse(Message):
    type = "append_entry_response"

    def __init__(self, sender, msg, current_term, sucess = True) -> None:
        Message.__init__(self, sender)
        self._msg = msg
        self._current_tearm = current_term
        self._sucess = sucess

