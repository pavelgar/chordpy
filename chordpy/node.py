import time
import types
import socket
import pickle
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
            payload = pickle.dumps(("key", self.id))
            retry_count = 0
            data = None
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                while retry_count < retries:
                    try:
                        sock.connect((remote_ip, port))
                        # Ask for remote's id
                        sock.sendall(payload)
                        data = sock.recv(1024)
                        break
                    except socket.error as e:
                        logging.warn(f"Connecting to ring failed, reason: {e}")
                        time.sleep(1)
                        retry_count += 1
                        logging.info(f"Retrying to join... (count: {retry_count})")

            # Let this fail as the ring will otherwise be broken with an incorrect id
            remote_id = pickle.loads(data)

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
                    logging.debug(f"Running stabilization from ({self.ip})...")
                    self.create_request(
                        self.successor[1], "request", self.id, res_wait=True
                    )
        finally:
            self.slctrs.close()

    def accept(self, sock) -> None:
        """
        Accepts a new connection, creates a new namespace for it, and stores it.
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        logging.debug(f"New connection from {addr}")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE  # Possible events
        data = types.SimpleNamespace(addr=addr, msg=b"", read=False)
        self.slctrs.register(conn, events, data=data)

    def create_request(self, ip: str, cmd: str, data, res_wait=False) -> None:
        """
        Registers a new request to be sent.

        _ip_ an ip address of a peer to connect to.

        _cmd_ a command to which the peer can respond to.

        _data_ the payload of the command.

        _res_wait_ denotes whether the request should wait for a response or close the connection immediately after sending.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        to = (ip, self.port)
        sock.connect_ex(to)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(
            addr=to,
            req=(cmd, data),
            res=b"",
            sent=False,
            res_wait=res_wait,
            timeout=6,
            start_time=0,
        )
        self.slctrs.register(sock, events, data=data)

    def process(self, key, mask):
        """
        Reads received bytes and stores them in the connection namespace (max of 1024 bytes at a time). After all the bytes are read, responds accordingly to the request and closes the connection.
        """
        sock = key.fileobj
        data = key.data
        if (  # Handle request timeouts
            hasattr(data, "start_time")
            and data.start_time > 0
            and (time.time() - data.start_time) > data.timeout
        ):
            logging.info(
                f"Processing of my request {data.req} to [{data.addr}] took too long. Discarding..."
            )
            self.slctrs.unregister(sock)
            sock.close()
            return

        if mask & selectors.EVENT_READ:  # Handle events that need reading
            recv_data = sock.recv(1024)
            if hasattr(data, "sent") and data.sent:  # Handle response
                if recv_data:
                    data.res += recv_data
                if len(recv_data) < 1024:  # This was the last "chunk" to read
                    logging.debug(f"Closing connection to: {data.addr}.")
                    self.slctrs.unregister(sock)
                    sock.close()
                    cmd = data.req[0]
                    try:
                        res = pickle.loads(data.res)
                    except pickle.UnpicklingError as e:
                        logging.error(
                            f"Received unpickleable data from [{data.addr}] (no action taken):",
                            exc_info=True,
                        )
                    else:
                        self.handle_response(cmd, res, peer=data.addr)

            elif hasattr(data, "read") and not data.read:  # Handle request
                if recv_data:
                    data.msg += recv_data
                    if len(recv_data) < 1024:  # This was the last "chunk" to read
                        data.read = True
                else:  # Safeguard against the case of len(msg) == 1024
                    data.read = True

        if mask & selectors.EVENT_WRITE:  # Handle events that need writing
            if hasattr(data, "sent") and not data.sent:  # Handle sending
                logging.debug(f"Sending request '{data.req}' to [{data.addr}]")
                sock.sendall(pickle.dumps(data.req))

                if data.res_wait:  # Should we wait for a response
                    data.sent = True  # Marks this message as "sent"
                    data.start_time = time.time()  # Start timeout
                else:
                    logging.debug(
                        f"Closing connection to: {data.addr}. (no response expected)"
                    )
                    self.slctrs.unregister(sock)
                    sock.close()

            elif hasattr(data, "read") and data.read:  # Handle responding
                try:
                    req = pickle.loads(data.msg)
                except pickle.UnpicklingError as e:
                    logging.error(
                        f"Received unpickleable data from [{data.addr}] (no action taken):",
                        exc_info=True,
                    )
                else:
                    if isinstance(req, (tuple, list)) and len(req) == 2:
                        res = self.handle_request(*req, peer=data.addr)
                        if res is not False:  # Allow return value of None and empty str
                            logging.debug(f"Responding to [{data.addr}] with '{res}'.")
                            sock.sendall(pickle.dumps(res))
                    else:
                        logging.warn(f"Received malformatted RPC: {req}")

                logging.debug(f"Closing connection from: {data.addr}.")
                self.slctrs.unregister(sock)
                sock.close()

    def handle_request(self, cmd: str, data, peer: tuple):
        """
        Top-level function to determine how to handle each type of request that this node receives.

        Returns a string or object that is the response to the received message.
        """
        logging.debug(f"Got a command [{cmd}] with data '{data}'")
        if cmd == "key":  # Respond to a query of my key
            return self.id
        elif cmd == "notify":  # Handle a predecessor proposal
            peer_id = data
            peer_ip = peer[0]
            if self.predecessor is None or Key.between(
                peer_id, self.predecessor[0], self.id
            ):
                self.predecessor = (peer_id, peer_ip)
                logging.debug(f"Got a new peer: [{self.predecessor}]")
        elif cmd == "request":  # Respond to a query of my predecessor
            return self.predecessor
        elif cmd == "add":  # Addition of a new key-value
            self.add(peer, data)

        return False  # No response needs to be sent

    def handle_response(self, cmd: str, data, peer: tuple) -> None:
        """
        Top-level function to determine how to handle each type of response that this node receives.
        """
        if cmd == "request":
            # Run stabilization after receiving my successor's predecessor
            self.stabilize(data)

    def stabilize(self, pred) -> None:
        """
        Try to stabilize the ring on my part by notifying my successor, or successor's predecessor, of my existence. Creates a new request, if necessary.

        _data_ is my current successor's predecessor.
        """
        if pred is None:
            # My successor has no predecessor
            # Notify of my existence:
            self.create_request(self.successor[1], "notify", self.id)

        elif isinstance(pred, (tuple, list)) and len(pred) == 2:
            # My successor has a predecessor...
            pred_id = pred[0]
            if pred_id == self.successor[0]:
                # ...but it's pointing to itself.
                # Notify of my existence:
                self.create_request(self.successor[1], "notify", self.id)
            else:
                # ...and it's pointing to some other node.
                # > Determine where I am in respect to my current
                # > successor and my successor's predecessor...
                if Key.between(pred_id, self.id, self.successor[0]):
                    # ...I am before both of them.
                    self.successor = pred
                    self.create_request(
                        self.successor[1], "request", self.id, res_wait=True
                    )
                else:
                    # ...I am between them.
                    # Notify of my existence:
                    self.create_request(self.successor[1], "notify", self.id)

    def add(self, client: tuple, data: tuple) -> None:
        """
        Processes the insertion of key-value pair request.

        _client_ is the source of address of the requester.

        _data_ is the tuple containing the key-value pair (in that order).
        """
        if not (isinstance(data, tuple) and len(data) == 2):
            logging.warn(f"Malformatted tuple of key-value pair: {data}.")
            return

        key, value = data
