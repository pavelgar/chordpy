import socket
import logging
import pickle
import selectors
import random
import string
import types
from argparse import ArgumentParser


sel = selectors.DefaultSelector()


def random_key():
    """
    Generates and returns a random key.
    """
    return random.randint(0, 1000000000)


def random_string(length):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def init(client: tuple, host: tuple, messages: list):
    logging.info(f"Initializing {len(messages)} message(s) to {host}.")

    for key, value in messages.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(host)
        events = selectors.EVENT_WRITE | selectors.EVENT_READ
        # Payload consists of pickled tuple of ("command", message)
        # Message is a tuple of (key, value) pair
        payload = pickle.dumps(("add", (key, value)))
        data = types.SimpleNamespace(id=key, addr=host, msg=payload, sent=False)
        sel.register(sock, events, data=data)


def process(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)
        if recv_data:
            data.msg += recv_data
        else:
            sel.unregister(sock)
            sock.close()
            status, (key, value) = pickle.loads(data.msg)
            logging.info(f"Received status {status} for {key} [{data.addr}].")

    if mask & selectors.EVENT_WRITE:
        if not data.sent:
            logging.info(f"Sending message #{data.id}.")
            sock.sendall(data.msg)
            data.sent = True


def get_args():
    parser = ArgumentParser(
        prog="chordpy_client",
        description="Client for communicating with a Chordpy ring.",
    )
    parser.add_argument("addr", help="IP address of the ring.")
    parser.add_argument("port", help="Port used by Chordpy", type=int)
    parser.add_argument(
        "gen", help="Number of messages to generate (for large-scale testing)", type=int
    )
    # parser.add_argument("msg", help="A message to send")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    messages = dict((random_key(), random_string(10)) for _ in range(args.gen))
    my_ip = socket.gethostbyname(socket.gethostname())
    client = (my_ip, 8080)
    server = (args.addr, args.port)
    init(client, server, messages)

    try:
        while True:
            events = sel.select(timeout=1)
            for key, mask in events:
                process(key, mask)
    except KeyboardInterrupt:
        logging.info("Stopping the client...")
    finally:
        sel.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    main()
