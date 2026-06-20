import zmq
import sys
import threading
import time
import logging
from queue import Queue


from states.state import State
from messages.append_entries import AppendEntries

logger = logging.getLogger(__name__)
msg_queue = Queue(maxsize=100)

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

    def _send_append_entry(self) -> None:
        msg_queue.put('append this entry')

    def _send_heart_beat(self) -> None:
        msg_queue.put('heartbeat')

class SubThread(threading.Thread):

    def __init__(self, neighboor_ports: int, zmq_context):
        super().__init__()
        self._neighboor_ports = neighboor_ports
        self._zmq_context = zmq_context

    def run(self):
        socket = self._zmq_context.socket(zmq.SUB)

        for i in range(len(self._neighboor_ports)):
            nport = self._neighboor_ports[i]
            socket.connect(f"tcp://localhost:{nport}")
            socket.setsockopt_string(zmq.SUBSCRIBE, "")
        while True:
            # recive messages from pubs
            # handle messages depending on wich state am i
            string = socket.recv_string()
            logger.debug(f"Recived:{string}")

class PubThread(threading.Thread):

    def __init__(self, port: int, zmq_context):
        super().__init__()
        self._port = port
        self._zmq_context = zmq_context

    def run(self):
        socket = self._zmq_context.socket(zmq.PUB)
        socket.bind(f"tcp://localhost:{self._port}")

        while True:
            # send messages to subs
            socket.send_string("ping")
            time.sleep(1)
            logger.debug("Sent: ping")

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format=" %(levelname)s - %(asctime)s - %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p'
    )

    logger.info(f"Staring Server Recived Argv: {sys.argv}")

    neighboor_count = len(sys.argv) - 2
    port = int(sys.argv[1])

    neighboor_ports = []
    for i in range(neighboor_count):
        n_port = int(sys.argv[i + 2])
        neighboor_ports.append(n_port)

    zmq_context = zmq.Context()


    pub_thread = PubThread(port, zmq_context)
    pub_thread.daemon = True
    pub_thread.start()

    sub_thread = SubThread(neighboor_ports, zmq_context)
    sub_thread.daemon = True
    sub_thread.start()

    sub_thread.join()
    pub_thread.join()


if __name__ == "__main__":
    main()
