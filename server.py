import zmq
import time

from enum import Enum
from .command import Command

class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

# Updated on stable storage before responding to RPCs
class PresistentData():

    def __init__(self, current_term: int, voted_for: int, log: list[Command]) -> None:
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log

class VolitileData():

    def __init__(self, commited_index: int, last_applied: int) -> None:
        self._commited_index = commited_index
        self._last_applied = last_applied

class Server():

    def __init__(self, id: int, presistent_data: PresistentData, volitile_data:VolitileData, role: Role) -> None:
        self._id = id
        self._presistent_data = presistent_data
        self._volitile_data = volitile_data
        self._role = role

def main():
    print("Node Starting...")

    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    while True:

        message = socket.recv()
        print(f"Received request: {message}")

        time.sleep(1)

        socket.send(b"World")

if __name__ == "__main__":
    main()
