from itertools import chain, compress
from typing import Callable, List

from more_itertools import chunked
from redisbloom.client import Client

from .core import BloomFilterClient, T

DEFAULT_MAX_COMMAND_PARAMS = 1000


class RedisBloomFilterClient(BloomFilterClient):
    """
    A RedisBloom-based bloom filter client.
    """

    def __init__(self, *, redis_client: Client = None, redis_host: str = None, redis_port: int = None, max_command_params: int = DEFAULT_MAX_COMMAND_PARAMS):
        if redis_client is not None:
            self.redis_client = redis_client
        else:
            self.redis_client = Client(host=redis_host, port=redis_port)
        self.__max_command_params = max_command_params

    def exists(self, key: str, objects: List[T], value_func: Callable[[T], str] = str) -> List[T]:
        # If the bloom filter key doesn't exist, all values should be returned.

        if not key or not objects or not self.redis_client.exists(key):
            return objects

        if not isinstance(objects, list):
            raise ValueError("The objects parameter must be a list")

        # Split the provided object list according to the maximum number of parameters allowed per Redis command.

        if self.__max_command_params:
            commands = [chunk for chunk in chunked(objects, self.__max_command_params)]
        else:
            commands = [objects]

        # Create a pipeline to send all the commands to Redis at once.

        pipeline = self.redis_client.pipeline(transaction=False)

        for command in commands:
            pipeline.bfMExists(key, *map(value_func, command))

        # Execute and get results for all the pipelined commands.

        results = pipeline.execute()

        # Build the final results.

        return [value for value in chain(*[compress(*pairs) for pairs in zip(commands, results)])]
