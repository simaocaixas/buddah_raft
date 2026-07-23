import zmq
import sys
import threading
import logging
import json
import yaml
import time
from queue import Queue

from states import State, Follower
from messages import AppendEntries, Message, MessageHandler


logger = logging.getLogger(__name__)

# Updated on stable storage before responding to RPCs
class PersistentData():

    def __init__(self, current_term: int, voted_for: int, log: list[AppendEntries]) -> None:
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._last_log_idx = 0;
        self._last_log_term = 0;

class VolatileData():

    def __init__(self, committed_index: int, last_applied: int) -> None:
        self._committed_index = committed_index
        self._last_applied = last_applied

class Server():

    def __init__(self, id: int, persistent_data: PersistentData, volatile_data: VolatileData, state: State, neighbors: dict[int, tuple[str, int]]) -> None:
        self._id = id
        self._persistent_data = persistent_data
        self._volatile_data = volatile_data
        self._commit_idx = 0
        self._msg_queue = Queue(maxsize=100)
        self._neighbors = neighbors
        self._state = state
        self._state.set_server(self)

    # I belive this never stops
    def heartbeat_tick(self):
        self._state.send_heart_beat()
        t = threading.Timer(1, self.heartbeat_tick)
        t.daemon = True
        t.start()

    class RouterThread(threading.Thread):

        def __init__(self, port: int, zmq_context, server):
            super().__init__()
            self._port = port
            self._zmq_context = zmq_context
            self._server = server

            socket = self._zmq_context.socket(zmq.ROUTER)
            socket.setsockopt_string(zmq.IDENTITY, str(self._server._id))
            socket.bind(f"tcp://localhost:{self._port}")

            self._socket = socket

            for neighboor_id, (neighboor_host, neighboor_port) in self._server._neighbors.items():

                if neighboor_id == self._server._id:
                    continue

                socket.connect(f"tcp://{neighboor_host}:{neighboor_port}")

            return None

        def run(self):
            logger.debug(f"Started on tcp://localhost:{self._port}")

            msg_handler = MessageHandler()

            poller = zmq.Poller()
            poller.register(self._socket, zmq.POLLIN)

            while True:
                events = dict(poller.poll(100))

                # Anything from other peer
                if self._socket in events:
                    sender, _, raw = self._socket.recv_multipart()
                    msg_handler.setup(self._server._state)
                    in_msg = Message.from_dict(json.loads(raw))
                    
                    logger.debug(f"{self._server._state.name.upper()} - Reciving a message: {in_msg.to_dict()}")
                    
                    msg_handler.handle(in_msg)

                if time.time() >= self._server._state._deadline:
                    self._server._state.on_election_timeout()

                # Send everything in the queue
                while not self._server._msg_queue.empty():
                    out_msg = self._server._msg_queue.get()
                    payload = json.dumps({"type": out_msg.type, "data": out_msg.to_dict()}).encode()
                
                    logger.debug(f"{self._server._state.name.upper()} - Sending a message: {out_msg.to_dict()}")

                    if out_msg._reciever is None:
                        for nid in self._server._neighbors:
                            if nid == self._server._id:
                                continue
                            self._socket.send_multipart([str(nid).encode(), b"", payload])
                    else:
                        # If reciever exists, out_msg is a response, we need to reply just to this node
                        self._socket.send_multipart([str(out_msg._reciever).encode(), b"", payload])

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
    server = Server(id, PersistentData(0, None, []), VolatileData(0, 0), Follower(), neighboors)

    router_thread = Server.RouterThread(neighboors[id][1], zmq_context, server)
    router_thread.daemon = True
    router_thread.start()
    router_thread.join()

if __name__ == "__main__":
    main()
