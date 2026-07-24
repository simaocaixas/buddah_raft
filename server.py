import zmq
import sys
import logging
import json
import yaml
import time
import queue
from queue import Queue

from states import State, Follower
from messages import AppendEntries, Message, MessageHandler

logger = logging.getLogger(__name__)

class Server():

    def __init__(self, id: int,
                 current_term: int,
                 voted_for: int,
                 log: list[AppendEntries],
                 committed_index: int,
                 last_applied: int,
                 state: State,
                 neighbors: dict[int, tuple[str, int]]) -> None:

        self._id = id

        # Persistent Data
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._last_log_idx = -1;
        self._last_log_term = -1;

        # Volatile Data
        self._committed_index = committed_index
        self._last_applied = last_applied


        self._commit_idx = -1
        self._neighbors = neighbors
        self._msg_queue = Queue(maxsize=1000)
        self._state = state
        self._state.set_server(self)

    def enqueue(self, msg) -> None:
        try:
            self._msg_queue.put_nowait(msg)
        except queue.Full:
            logger.warning(f"Message queue is full, dropping message: {msg.to_dict()}")

    def run_router(self, port, zmq_context):
        logger.debug(f"Starting on tcp://localhost:{port}")

        socket = zmq_context.socket(zmq.ROUTER)
        socket.setsockopt_string(zmq.IDENTITY, str(self._id))
        socket.setsockopt(zmq.RECONNECT_IVL, 100)
        socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2000)
        socket.bind(f"tcp://localhost:{port}")

        # Establish a connection with other nodes when possible
        # Keeps retrying every 100ms with a max back off up to 2s
        for neighboor_id, (neighboor_host, neighboor_port) in self._neighbors.items():

            if neighboor_id == self._id:
                continue

            socket.connect(f"tcp://{neighboor_host}:{neighboor_port}")

        msg_handler = MessageHandler()

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        # Checks incoming messages, timeout-deadline and does a msg_queue flush
        while True:
            # Waits up to 100ms for a message, returns immediately if one arrives;
            events = dict(poller.poll(100))

            # I decided to prioritize the message flush over the handling of incoming messages
            # Only one incoming message is handled per loop cycle
            if socket in events:
                sender, _, raw = socket.recv_multipart()
                in_msg = Message.from_dict(json.loads(raw))

                logger.debug(f"{self._state.name.upper()} - Reciving a message: {in_msg.to_dict()}")

                msg_handler.handle(in_msg, self._state)

            if time.time() >= self._state._deadline:
                self._state.on_election_timeout()

            while not self._msg_queue.empty():
                out_msg = self._msg_queue.get()
                payload = json.dumps({"type": out_msg.type, "data": out_msg.to_dict()}).encode()

                logger.debug(f"{self._state.name.upper()} - Sending a message: {out_msg.to_dict()}")

                if out_msg._reciever is None:
                    # No reciever set means Broadcast
                    for nid in self._neighbors:
                        if nid == self._id:
                            continue

                        socket.send_multipart([str(nid).encode(), b"", payload])
                else:
                    # Reciever set means Unicast
                    socket.send_multipart([str(out_msg._reciever).encode(), b"", payload])

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format=" %(levelname)s - %(asctime)s - %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p'
    )

    logger.info(f"Starting Server Received Argv: {sys.argv}")

    neighboors = dict()
    id = int(sys.argv[1])

    zmq_context = zmq.Context()

    with open('cluster.yaml', 'r') as f:
        cluster = yaml.safe_load(f)

    nodes = cluster['nodes']
    
    for n in nodes:
        neighboor_id = n['id']
        neighboors[neighboor_id] = (n['host'], n['port'])
    server = Server(id, 0, None, [], 0, 0, Follower(), neighboors)
    server.run_router(neighboors[id][1], zmq_context)

if __name__ == "__main__":
    main()
