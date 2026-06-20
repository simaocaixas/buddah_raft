import zmq
import time
import sys
import threading

from .states import State
from .messages import AppendEntries

# Updated on stable storage before responding to RPCs
class PresistentData():

    def __init__(self, current_term: int, voted_for: int, log: list[AppendEntries]) -> None:
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log

class VolitileData():

    def __init__(self, commited_index: int, last_applied: int) -> None:
        self._commited_index = commited_index
        self._last_applied = last_applied

class Server():

    def __init__(self, id: int, presistent_data: PresistentData, volitile_data: VolitileData, state: State) -> None:
        self._id = id
        self._presistent_data = presistent_data
        self._volitile_data = volitile_data
        self._state = state

    def _on_reciving_command(self) -> None:
        pass

    def _send_message(self) -> None:
        pass

    def _send_heart_beat(self) -> None:
        pass

class SubThread(threading.Thread):
    
    def __init__(self, neighboor_ports: int):
        self._neighboor_ports = neighboor_ports
    
    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)

        for i in range(len(self._neighboor_ports)):
            nport = self._neighboor_ports[i]
            socket.connect("tcp://localhost{nport}")

        while True:
            # recive messages from other peers
            # handle messages depending on wich state am i
            pass

class PubThread(threading.Thread):
    pass

def main():

    neighboor_count = sys.argv - 2
    port = sys.argv[1]

    neighboor_ports = []
    for i in range(neighboor_count):
        n_port = int(sys.argv[i + 2])
        neighboor_ports.append(n_port)


    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://localhost:{port}")


    sub_thread = SubThread(neighboor_ports)
    sub_thread.daemon = True
    sub_thread.start()

if __name__ == "__main__":
    main()
