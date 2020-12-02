from random import randint


class Key:
    """
    Object for working with node keys.
    """

    @staticmethod
    def generate() -> int:
        """
        Generates and returns an unique id for this node.
        """
        return randint(0, 1000000000)

    @staticmethod
    def between(key: int, fr: int, to: int) -> bool:
        """
        Checks if a key is between _from_ and _to_ or equal to _to_ on the ring.

        If _from_ and _to_ are equal, returns True.
        """
        if fr == to:
            return True
        if fr > to:
            return key > fr or key <= to
        return key > fr and key <= to
