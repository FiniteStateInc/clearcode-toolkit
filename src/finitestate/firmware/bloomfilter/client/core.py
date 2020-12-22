from abc import ABC, abstractmethod
from typing import Callable, List, TypeVar

T = TypeVar('T')


class BloomFilterClient(ABC):
    @abstractmethod
    def exists(self, key: str, objects: List[T], value_func: Callable[[T], str] = str) -> List[T]:
        """
        Checks a list of objects for existence in a bloom filter, returning a list of all objects that do exist. If the
        key does not exist, the list of objects is returned unchanged.

        :param key: The bloom filter key
        :param objects: The objects to check
        :param value_func: A function that will return the value to check for each object - defaults to str

        :return: A list of values that exist in the bloom filter
        """
        raise NotImplemented()
