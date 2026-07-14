import zmq
import sys
import threading
import time
import logging
from queue import Queue


from states import State, Leader, Follower
from messages.append_entries import AppendEntries
from messages.message import Message
from messages.message import MessageHandler

logger = logging.getLogger(__name__)

# Updated on stable storage before responding to RPCs
class PresistentData():

    def __init__(self, current_term: int, voted_for: int, log: list[AppendEntries]) -> None:
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._last_log_idx = 0;
        self._last_log_term = None;

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
        self._commit_indx = 0
        self._msg_queue = Queue(maxsize=100)
        self.start_heartbeat()

    def _on_reciving_command(self, commands) -> None:
        if isinstance(self._state, Leader):

            new_entry = AppendEntries(self._id,
                                      self._presistent_data._current_term,
                                      self._id,
                                      self._presistent_data._last_log_idx,
                                      self._presistent_data._last_log_term,
                                      commands,
                                      self._commit_idx
                                      )

            # Upon reciving client command the leader appends it to its Log
            self._log.append(new_entry)
            logger.debug(f"Recived Client Commands: {commands}")

            self._msg_queue.append(new_entry)

        elif isinstance(self._state, Follower):
            # Redirect to leader
            pass

    def _send_append_entry(self) -> None:
        self._msg_queue.put('append this entry')

    def _schedule(self):
        self._send_heart_beat()
        t = threading.Timer(0.1, self._schedule)
        t.daemon = True
        t.start()

    def start_heartbeat(self):
        t = threading.Timer(0.1, self._schedule)
        t.daemon = True
        t.start()

class SubThread(threading.Thread):

    def __init__(self, neighboor_ports: int, zmq_context, server):
        super().__init__()
        self._neighboor_ports = neighboor_ports
        self._zmq_context = zmq_context
        self._server = server

    def run(self):
        socket = self._zmq_context.socket(zmq.SUB)

        for i in range(len(self._neighboor_ports)):
            # TO-DO: a msg handler shared among threads
            msg_handler = MessageHandler()
            nport = self._neighboor_ports[i]
            socket.connect(f"tcp://localhost:{nport}")
            socket.setsockopt_string(zmq.SUBSCRIBE, "")
        while True:
            raw = socket.recv_json()
            logger.debug(f"Received: {raw}")
            msg = Message.from_dict(raw)
            msg_handler.setup(self._server._state)
            msg_handler.handle(msg)

class PubThread(threading.Thread):

    def __init__(self, port: int, zmq_context, server):
        super().__init__()
        self._port = port
        self._zmq_context = zmq_context
        self._server = server

    def run(self):
        socket = self._zmq_context.socket(zmq.PUB)
        socket.bind(f"tcp://localhost:{self._port}")

        while True:
            # send messages to subs
            msg = self._msg_queue.get()

            if isinstance(msg, AppendEntries):
                socket.send_json({"data": msg.to_dict()})
                logger.debug("Sent: append_entry")

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

    server = Server(port, PresistentData(0, None, []), VolitileData(0, 0), Follower())

    pub_thread = PubThread(port, zmq_context, server)
    pub_thread.daemon = True
    pub_thread.start()

    sub_thread = SubThread(neighboor_ports, zmq_context, server)
    sub_thread.daemon = True
    sub_thread.start()

    sub_thread.join()
    pub_thread.join()

if __name__ == "__main__":
    main()
