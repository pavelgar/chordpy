import time
import types
import socket
import selectors
from selectors import EVENT_READ, EVENT_WRITE


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

        if remote_ip == self.ip:
            self.successor = (self.ip, self.id)
        else:
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
            if not data:
                raise Exception("Could not connect to destination!")
            # Save the remote's id
            self.successor = (remote_ip, int(data))  # Tuple of (address, id)

    def run(self):
        """
        Starts the main event loop of the node.
        """
        print("Starting event loop")
        try:
            while True:  # Main event loop
                events = self.slctrs.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        # Accept new connection
                        self.accept(key.fileobj)
                    else:
                        # Respond to existing connection
                        self.respond(key, mask)
        except KeyboardInterrupt:
            print("\nInterrupted, exiting...")
        finally:
            self.slctrs.close()

    def accept(self, sock):
        """
        Accepts a new connection, creates a new namespace for it, and stores it.
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        print("New connection from", addr)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = EVENT_READ | EVENT_WRITE
        self.slctrs.register(conn, events, data=data)

    def respond(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                data.outb += recv_data
            else:
                print("closing connection to", data.addr)
                self.slctrs.unregister(sock)
                sock.close()

        if mask & EVENT_WRITE:
            if data.outb:
                print("Responding with", repr(data.outb), "to", data.addr)
                sent = sock.send(data.outb)
                data.outb = data.outb[sent:]

    def handle_message(self, key, data):
        """
        Top-level function to determine how to handle each type of message that this node receives.

        Return value is a message that is a response to the received message.
        """
        if key == "key":
            # Send my key to the requester
            return self.id
        elif key == "notify":
            #
            pass
        elif key == "request":
            pass
        elif key == "status":
            pass

    def stabilize(self, pred, id, succ):
        key, id = succ
        if pred is None:
            # Notify succ of our existence
            pass
        elif pred == self.id:
            pass  # Do nothing
        # elif pred
