import zmq
import time 


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
