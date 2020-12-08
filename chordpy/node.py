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
        self.successor_down = False
        # My successor's successor
        self.next = None

        # Determine own IPv4 address
        hostname = socket.gethostname()
        self.ip = socket.gethostbyname(hostname)
        logging.info(f"{hostname}:{self.id} [{self.ip}:{port}]")
        self.port = port

        # Init the storage of this node
        self.storage = dict()
        # Init the replica of predecessor's storage
        self.replica = dict()

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

    def run(self, stabilize_every=5):
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
                    logging.debug(
                        f"Running stabilization from ({self.ip}) [s: {self.successor}, n: {self.next}]..."
                    )
                    if not self.successor_down:
                        # I assume, that it's down and wait for it to prove otherwise
                        self.successor_down = True
                        to = (self.successor[1], self.port)
                        self.create_request(
                            to, "request", self.id, res_wait=True, timeout=2
                        )
                    elif self.successor_down and self.next:
                        to = (self.next[1], self.port)
                        self.create_request(
                            to, "request", self.id, res_wait=True, timeout=2
                        )
                    else:
                        logging.error("Unable to stabilize the ring, shutting down...")
                        break
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

    def create_request(
        self, to: tuple, cmd: str, data, res_wait=False, get_res=False, timeout=0
    ):
        """
        Registers a new request to be sent.

        _to_ a tuple of (addr, port) of a peer to connect to.

        _cmd_ a command to which the peer can respond to.

        _data_ the payload of the command.

        _res_wait_ denotes whether the request should wait for a response or close the connection immediately after sending.

        _get_res_ denotes whether this method should return the value of this response.

        _timeout_ sets a period (seconds) after which the event should be discarded if there was no response. (0 = no timeout)
        """
        if get_res:  # TODO: Make this also non-blocking
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(to)
                payload = pickle.dumps((cmd, data))
                sock.sendall(payload)
                res = b""
                while True:
                    recv_data = sock.recv(1024)
                    if not recv_data:
                        break
                    res += recv_data

            return pickle.loads(res)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        if timeout:
            sock.settimeout(timeout)
        sock.connect_ex(to)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(
            addr=to,
            req=(cmd, data),
            res=b"",
            sent=False,
            res_wait=res_wait,
            timeout=timeout,
            start_time=0,
        )
        self.slctrs.register(sock, events, data=data)

    def process(self, key, mask):
        """
        Processes incoming and outgoing events according to the respective handlers.
        If the handlers return a value (other than False) it will be used to answer the request with.
        Events can set various control parameters, like a timeout time.
        """
        sock = key.fileobj
        data = key.data

        if (  # Handle request timeouts
            hasattr(data, "start_time")
            and data.start_time > 0
            and (time.time() - data.start_time) > data.timeout
        ):
            # logging.info(f"Timeout of request {data.req} to [{data.addr}].")
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
                    data.start_time = time.time()  # Start timeout timer
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
                        f"Could not unpickle data from [{data.addr}] (no action):",
                        exc_info=True,
                    )
                except EOFError as e:
                    logging.warn(f"{e} from [{data.addr}] (no action)")
                else:
                    if isinstance(req, (tuple, list)) and len(req) == 2:
                        res = self.handle_request(*req, peer=data.addr)
                        if res is not False:
                            # Allows return value of None and empty str
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
            rest = self.notify(data, peer)
            if rest:
                logging.info(f"Handing over to {peer} data: {rest}")
            return rest
        elif cmd == "request":  # Respond to a query of my predecessor + successor
            return (self.predecessor, self.successor)
        elif cmd == "add":  # Addition of a new key-value
            return self.add(data)
        elif cmd == "lookup":  # Lookup of a value for a key
            return self.lookup(data)
        elif cmd == "replicate":
            self.replicate(*data)

        return False  # No response needs to be sent

    def handle_response(self, cmd: str, data, peer: tuple) -> None:
        """
        Top-level function to determine how to handle each type of response that this node receives.
        """
        if cmd == "request":
            # Run stabilization after receiving my successor's predecessor and successor
            self.successor_down = False  # Successor is not down as it responded
            self.stabilize(peer, *data)
        elif cmd == "notify":  # Finish the succesful notification with a merge
            if data:
                self.merge(*data)

    def stabilize(self, peer: tuple, pred, succ) -> None:
        """
        Try to stabilize the ring on my part by notifying my successor, or successor's predecessor, of my existence. Creates a new request, if necessary.

        _data_ is a tuple of my current successor's predecessor and successor.
        """
        if pred is None:
            # My peer has no predecessor
            # Notify of my existence:
            if self.next and peer[0] == self.next[0]:
                self.successor = peer
                self.next = succ
            self.create_request(peer, "notify", self.id, res_wait=True)

        elif isinstance(pred, (tuple, list)) and len(pred) == 2:
            # My peer has a predecessor...
            pred_id = pred[0]
            if pred_id == self.successor[0]:
                # ...but it's pointing to itself.
                # Notify of my existence:
                if self.next and peer[0] == self.next[0]:
                    self.successor = peer
                    self.next = succ
                self.create_request(peer, "notify", self.id, res_wait=True)
            else:
                # ...and it's pointing to some other node.
                # > Determine where I am in respect to my current
                # > successor (peer) and its predecessor...
                if Key.between(pred_id, self.id, self.successor[0]):
                    # ...I am before both of them.
                    self.next = self.successor
                    self.successor = pred
                    self.create_request(peer, "request", self.id, res_wait=True)
                else:
                    # ...I am between them.
                    # Adopt my peer's successor
                    self.next = succ
                    if self.next and peer[0] == self.next[0]:
                        self.successor = peer
                    # Notify of my existence:
                    self.create_request(peer, "notify", self.id, res_wait=True)

    def notify(self, id: int, peer: tuple):
        if self.predecessor is None or Key.between(id, self.predecessor[0], self.id):
            self.predecessor = (id, peer[0])
            logging.debug(f"Got a new predecessor: [{self.predecessor}]")
            return self.handover(id)
        return None

    def add(self, data: tuple) -> None:
        """
        Processes the insertion of key-value pair request.

        _data_ is a tuple containing the key-value pair to store.
        """
        if not (isinstance(data, tuple) and len(data) == 2):
            logging.warn(f"Malformatted data of key-value pair: {data}.")
            return "bad request"

        key, value = data
        predecessor = self.predecessor[0] if self.predecessor else self.id
        if Key.between(key, predecessor, self.id):
            k = str(key)
            self.storage[k] = value
            logging.info(
                f"{key}:{value} stored successfully.\nClient and predecessor will be notified."
            )
            self.create_request((self.successor[1], self.port), "replicate", data)
            return key
        else:
            to = (self.successor[1], self.port)
            return self.create_request(to, "add", data, get_res=True)

    def lookup(self, data: int) -> None:
        """
        Processes the lookup of key-value pair request.

        _data_ is an int key to find.
        """
        if not isinstance(data, int):
            logging.warn(f"Received key is not an integer: {data}.")
            return "bad request"

        client, key = data
        if Key.between(key, self.predecessor[0], self.id):
            return self.storage.get(str(key), None)
        else:
            to = (self.successor[1], self.port)
            return self.create_request(to, "lookup", data, get_res=True)

    def merge(self, coll: dict, repl: dict) -> None:
        """
        Processes the merging of received data into this node's storage.

        _coll_ and _repl_ are the dicts containing the key-value pairs that need to be merged.
        """
        if not (isinstance(coll[0], dict) and isinstance(repl[1], dict)):
            logging.warn(
                f"Malformatted dict of key-value pairs to be merged: {coll} and {repl}."
            )
            return

        self.storage = {**coll, **self.storage}
        self.replica = {**repl, **self.replica}

    def handover(self, peer_id) -> dict:
        storage = dict()
        replica = dict()

        for k in list(self.storage):
            key = int(k)
            if Key.between(key, peer_id, self.id):
                storage[k] = self.storage.pop(k)
        for k in list(self.replica):
            key = int(k)
            if Key.between(key, self.predecessor[0], peer_id):
                replica[k] = self.replica.pop(k)
        return (storage, replica)

    def replicate(self, key: int, value):
        k = str(key)
        self.replica[k] = value
        logging.info(f"Replicated ({key}, {value}).")
