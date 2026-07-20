from messages.message import Message
from states.follower import Follower

class AppendEntriesResponse(Message):
    type = "append_entry_response"

    def __init__(self, sender, msg, current_term, success = True) -> None:
        Message.__init__(self, sender)
        self._msg = msg
        self._term = current_term
        self._success = success

