import time
import types
import socket
import selectors
import logging
from ast import literal_eval
from key import Key


class Node:
    def __init__(self, id: int, port: int) -> None:
        self.id = id
        self.predecessor = None
        self.successor = None

        # Determine own IPv4 address
        hostname = socket.gethostname()
        self.ip = socket.gethostbyname(hostname)
        logging.info(f"{hostname}:{self.id} [{self.ip}:{port}]")
        self.port = port

        # Init a non-blocking listening socket
        my_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        my_sock.bind(("", port))
        my_sock.listen()
        my_sock.setblocking(False)

        # Register the first socket
        slctrs = selectors.DefaultSelector()
        slctrs.register(my_sock, selectors.EVENT_READ, data=None)
        self.slctrs = slctrs

    def join(self, dest, retries=3):
        """
        Joins a ring at the given destination _dest_.

        _dest_ is a tuple of (address, port) to connect to.

        _retries_ is the number of connection attempts to make.
        """
        remote, port = dest
        remote_ip = socket.gethostbyname(remote)
        logging.info(f"Connecting to ring at {dest}")
        remote_id = self.id
        if remote_ip != self.ip:
            payload = f"key:{self.id}".encode("utf-8")
            retry_count = 0
            data = None
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                while retry_count < retries:
                    try:
                        sock.connect((remote_ip, port))
                        # Ask for remote's id
                        sock.sendall(payload)
                        data = sock.recv(1024).decode("utf-8")
                        break
                    except socket.error as e:
                        logging.warn(
                            "Connecting to ring failed, reason:", exc_info=True
                        )
                        time.sleep(1)
                        retry_count += 1
                        logging.info(f"Retrying to join... (count: {retry_count})")

            if data:
                remote_id = int(data)
            else:
                raise Exception(f"{remote} didn't send me its id! Stopping...")

        # Save my successor
        self.successor = (remote_id, remote_ip)
        logging.info(f"Succesfully joined the ring at {dest}.")

    def run(self, stabilize_every=3):
        """
        Starts the main event loop of the node.
        _stabilize_every_ denotes the period in seconds after which this node tries to stabilize the ring.
        """
        logging.info("Starting main event loop...")
        time_counter = 0  # Keep track of elapsed time. Used for syncing.
        try:
            while True:  # Main event loop
                # Wait for new event(s)
                start_time = time.time()
                events = self.slctrs.select(timeout=stabilize_every)
                for key, mask in events:
                    if key.data is None:
                        # Accept new connection
                        self.accept(key.fileobj)
                    else:
                        # Respond to existing connection
                        self.process(key, mask)

                time_counter += time.time() - start_time
                if time_counter >= stabilize_every:
                    time_counter = 0
                    logging.info("Running stabilization...")
                    self.create_request(self.successor[1], "request", str(self.id))
        finally:
            self.slctrs.close()

    def accept(self, sock):
        """
        Accepts a new connection, creates a new namespace for it, and stores it.
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        logging.info(f"New connection from {addr}")
        data = types.SimpleNamespace(addr=addr, cmd="", msg=b"", read=False)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE  # Possible events
        self.slctrs.register(conn, events, data=data)

    def process(self, key, mask):
        """
        Reads received bytes and stores them in the connection namespace (max of 1024 bytes at a time). After all the bytes are read, responds accordingly to the request and closes the connection.
        """
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:  # Handle events that need reading
            recv_data = sock.recv(1024)
            if hasattr(data, "sent") and data.sent:  # Handle response
                if recv_data:
                    data.res += recv_data

                if len(recv_data) < 1024:
                    logging.info(f"Closing connection to: {data.addr}")
                    self.slctrs.unregister(sock)
                    sock.close()
                    cmd = data.req.split(":")[0]
                    msg = data.res.decode("utf-8")
                    self.handle_response(cmd, msg)

            elif hasattr(data, "read") and not data.read:
                if recv_data:
                    # Extract the "command" of this request
                    if not data.cmd:
                        data.cmd, msg = recv_data.decode("utf-8").split(":", 1)
                        data.msg = msg.encode("utf-8")
                    # If the cmd is already set, simply store the rest of the message
                    else:
                        data.msg += recv_data

                    if len(recv_data) < 1024:  # Check if this was the last "chunk"
                        data.read = True
                else:
                    logging.info(f"Closing connection from: {data.addr}")
                    self.slctrs.unregister(sock)
                    sock.close()

        if mask & selectors.EVENT_WRITE:  # Handle events that need writing
            if hasattr(data, "sent") and not data.sent:  # Handle sending
                logging.info(f"Sending request '{data.req}' to [{data.addr}]")
                sock.send(data.req.encode("utf-8"))
                data.sent = True  # Marks this message as "sent"

            elif hasattr(data, "read") and data.read:  # Handle responding
                msg = data.msg.decode("utf-8")
                res = self.handle_request(data)
                if res is not None:
                    logging.info(
                        f"Responding to request '{data.cmd}' with '{res}'. [{data.addr}]"
                    )
                    sock.send(res.encode("utf-8"))
                data.read = False  # Marks this message as "processed"

    def handle_request(self, data: types.SimpleNamespace) -> str:
        """
        Top-level function to determine how to handle each type of request that this node receives.

        Returns a (string) message that is a response to the received message.
        """
        if data.cmd == "key":  # Respond to a query of my key
            return str(self.id)
        if data.cmd == "notify":  # Handle a predecessor proposal
            peer_id = int(data.msg.decode("utf-8"))
            peer_ip = data.addr[0]
            if self.predecessor is None or Key.between(
                peer_id, self.predecessor[0], self.id
            ):
                self.predecessor = (peer_id, peer_ip)
                logging.info(f"Got a new peer: [{self.predecessor}]")
            return None

        if data.cmd == "request":  # Respond to a query of my predecessor
            return str(self.predecessor)

    def handle_response(self, cmd: str, msg: str) -> None:
        """
        Top-level function to determine how to handle each type of response that this node receives.
        """
        if cmd == "request":
            # Run stabilization after receiving my successor's predecessor
            self.stabilize(msg)

    def create_request(self, ip: str, cmd: str, msg: str) -> None:
        """
        Registers a new request to be sent.

        _ip_ an ip address of a peer to connect to.

        _cmd_ a command to which the peer can respond to.

        _msg_ the payload of the command.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        to = (ip, self.port)
        sock.connect_ex(to)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(addr=to, req=f"{cmd}:{msg}", res=b"", sent=False)
        self.slctrs.register(sock, events, data=data)

    def stabilize(self, data: str) -> None:
        """
        Try to stabilize the ring on my part by notifying my successor, or successor's predecessor, of my existence. Creates a new request, if necessary.

        _data_ is my current successor's predecessor.
        """
        pred = literal_eval(data)  # Should either be None or a tuple of size two
        logging.info(f"My successor's ({self.successor[0]}) predecessor is {pred}.")

        if pred is None:
            # My successor has no predecessor
            # Notify of my existence:
            self.create_request(self.successor[1], "notify", str(self.id))

        elif isinstance(pred, tuple) and len(pred) == 2:
            # My successor has a predecessor...
            pred_id = int(pred[0])
            if pred_id == self.successor[0]:
                # ...but it's pointing to itself.
                # Notify of my existence:
                self.create_request(self.successor[1], "notify", str(self.id))
            else:
                # ...and it's pointing to some other node.
                # > Determine where I am in respect to my current
                # > successor and my successor's predecessor...
                if Key.between(pred_id, self.id, self.successor[0]):
                    # ...I am before both of them.
                    self.successor = pred
                    self.create_request(self.successor[1], "request", str(self.id))
                else:
                    # ...I am between them.
                    # Notify of my existence:
                    self.create_request(self.successor[1], "notify", str(self.id))
        else:
            logging.error(f"Received malformed data: {pred}, when trying to stabilize.")
