import time
import types
import socket
import selectors


class Node:
    def __init__(self, id: int, port: int) -> None:
        self.id = id

        # Determine own IPv4 address
        hostname = socket.gethostname()
        self.ip = socket.gethostbyname(hostname)
        print(f"Hostname: {hostname} [{self.ip}]")

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
        remote, port = dest
        remote_ip = socket.gethostbyname(remote)
        print(f"Connecting to ring at {dest}")
        remote_id = self.id
        if remote_ip != self.ip:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            payload = f"key:{self.id}".encode("utf-8")
            retry_count = 0
            data = None
            while retry_count < retries:
                try:
                    sock.connect((remote_ip, port))
                    # Ask for remote's id
                    sock.sendall(payload)
                    data = sock.recv(1024).decode("utf-8")
                    break
                except socket.error as e:
                    print("Connection failed, reason:", e)
                    time.sleep(1)
                    retry_count += 1
                    print(f"Retrying to join... (count: {retry_count})")

            sock.close()
            if data:
                remote_id = int(data)
            else:
                raise Exception("Host didn't send it's key!")

            # Save my successor
        self.successor = (remote_id, remote_ip)
        print(f"Succesfully joined the ring at {dest}.")

    def run(self):
        """
        Starts the main event loop of the node.
        """
        print("Starting event loop")
        try:
            while True:  # Main event loop
                # Wait for new event(s)
                events = self.slctrs.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        # Accept new connection
                        self.accept(key.fileobj)
                    else:
                        # Respond to existing connection
                        self.respond(key, mask)
        except Exception as e:
            print("\nInterrupted, exiting...", e)
        finally:
            self.slctrs.close()

    def accept(self, sock):
        """
        Accepts a new connection, creates a new namespace for it, and stores it.
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        print("New connection from", addr)
        data = types.SimpleNamespace(addr=addr, msg=b"", cmd="", done=False)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE  # Possible events
        self.slctrs.register(conn, events, data=data)

    def respond(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                # Extract the "command" of this message.
                # It's always the first few bytes of a message before a colon character
                if not data.cmd:
                    data.cmd, msg = recv_data.decode("utf-8").split(":", 1)
                    data.msg = msg.encode("utf-8")
                # If the cmd is already set, simply store the rest of the message
                else:
                    data.msg += recv_data

                if len(recv_data) < 1024:  # Check if this was the last "chunk"
                    data.done = True

            else:  # No more data to read
                print("Closing connection to", data.addr, "(end of data)")
                self.slctrs.unregister(sock)
                sock.close()

        if (mask & selectors.EVENT_WRITE) and data.done:  # Check also for done reading
            msg = data.msg.decode("utf-8")
            res = self.handle_message(data.cmd, msg)
            sock.send(res.encode("utf-8"))
            data.done = False  # Marks this message as "processed"

    def handle_message(self, cmd: str, message: str) -> str:
        """
        Top-level function to determine how to handle each type of message that this node receives.

        Return value is a message that is a response to the received message.
        """
        if cmd == "key":
            # Send my key to the requester
            return str(self.id)
        elif cmd == "notify":
            #
            pass
        elif cmd == "request":
            pass
        elif cmd == "status":
            pass

    def stabilize(self, pred, id, succ):
        key, id = succ
        if pred is None:
            # Notify succ of our existence
            pass
        elif pred == self.id:
            pass  # Do nothing
        # elif pred
