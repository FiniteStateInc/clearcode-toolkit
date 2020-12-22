import itertools
from typing import Generator, List, TypeVar

T = TypeVar('T')


def interleave(generators: List[Generator[T, None, None]]) -> Generator[T, None, None]:
    """
    Interleaves the entities from any number of generators by taking the next item from each in a round-robin fashion.
    :param generators: The generators to consume
    :return: A generator of interleaved results
    """

    generators=generators[:]

    def drain():
        try:
            for i in itertools.cycle(range(len(generators))):
                yield next(generators[i])
        except StopIteration as e:
            generators.pop(i)

    while generators:
        yield from drain()
