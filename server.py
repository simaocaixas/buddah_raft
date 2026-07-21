import zmq
import sys
import threading
import time
import logging
import asyncio
import json
from queue import Queue


from states import State, Leader, Follower
from messages.append_entries import AppendEntries
from messages.message import Message
from messages.message import MessageHandler

logger = logging.getLogger(__name__)

# Updated on stable storage before responding to RPCs
class PersistentData():

    def __init__(self, current_term: int, voted_for: int, log: list[AppendEntries]) -> None:
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._last_log_idx = 0;
        self._last_log_term = None;

class VolatileData():

    def __init__(self, committed_index: int, last_applied: int) -> None:
        self._committed_index = committed_index
        self._last_applied = last_applied

class Server():

    def __init__(self, id: int, persistent_data: PersistentData, volatile_data: VolatileData, state: State, neighbor_ports: list[int]) -> None:
        self._id = id
        self._persistent_data = persistent_data
        self._volatile_data = volatile_data
        self._state = state
        self._commit_idx = 0
        self._msg_queue = Queue(maxsize=100)
        self._neighbor_ports = neighbor_ports

    def heartbeat_tick(self):
        self._state.send_heart_beat()
        t = threading.Timer(0.1, self.heartbeat_tick)
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


    async def run(self):
        logger.debug(f"Started on tcp://localhost:{self._port}")

        msg_handler = MessageHandler()
        msg_handler.setup(self._server._state)

        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)

        while True:
            events = dict(poller.poll(100))

            if self._socket in events:
                # handle event in the socket queue 
                pass

            while not self._server._msg_queue.empty():
                out_msg = self._server._msg_queue.get_nowait()
                payload = json.dumps({"type": out_msg.type, "data": out_msg.to_dict()}).encode()

                if out_msg._reciver is None:
                    # broadcast
                    pass
                else:
                    self._socket.send_multipart([str(out_msg._reciver).encode(), b"", payload])
def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format=" %(levelname)s - %(asctime)s - %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p'
    )

    logger.info(f"Starting Server Received Argv: {sys.argv}")

    neighbor_count = len(sys.argv) - 2
    port = int(sys.argv[1])

    neighbor_ports = []
    for i in range(neighbor_count):
        n_port = int(sys.argv[i + 2])
        neighbor_ports.append(n_port)

    zmq_context = zmq.Context()

    server = Server(port, PersistentData(0, None, []), VolatileData(0, 0), Follower(), neighbor_ports)

    router_thread = RouterThread(port, zmq_context, server)
    router_thread.daemon = True
    router_thread.start()

    router_thread.join()

if __name__ == "__main__":
    main()
