from argparse import ArgumentParser
from key import Key
from node import Node


def get_args():
    parser = ArgumentParser(
        prog="chordpy",
        description="Starts a node that will join a ring specified by the arguments.",
    )
    parser.add_argument("addr", help="IP address of the master node")
    parser.add_argument(
        "port", help="Port used by this node AND the master node", type=int
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_args()
    REMOTE, PORT = args.addr, args.port

    my_id = Key.generate()
    me = Node(my_id, PORT)
    me.join((REMOTE, PORT))
    me.run()  # Runs the event loop forever
