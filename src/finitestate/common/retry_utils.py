import time
from functools import partial, wraps
from typing import List, Union, Callable, Optional
from pydoc import locate

__all__ = [
    'retry',
]


def exponential_backoff(retry_count: int):
    """
    Sleeps for 2 * the retry count number of seconds
    :param retry_count: The retry count
    """
    time.sleep(2 ** retry_count)


def retry(
        max_retries: int = 10,
        on: Union[type, List[type], str, List[str], Callable[[Exception], bool], List[Callable[[Exception], bool]]] = None,
        backoff: Optional[Callable[[int], None]] = None
):
    """
    A decorator for implementing retries with customizable (defaults to exponential) back-off.

    :param f: The function to wrap
    :param max_retries: The maximum number of attempts that should be made to call the wrapped function.
    :param on: A type name, type, or callable, or list thereof, indicating which Exception types should result in a retry.  If not specified, any Exception will be retried.
    :param backoff: A function that applies the correct back-off methodology, defaults to exponential if not specified
    :return: The return from the wrapped function.
    """

    if not backoff:
        backoff = exponential_backoff

    # Locate string-based conditions upfront so that incorrectly defined retry conditions immediately fail.

    if on is not None:
        if not isinstance(on, list):
            on = [on]

        for i in range(len(on)):
            if isinstance(on[i], str):
                located = locate(on[i])
                if located is not None:
                    on[i] = located
                else:
                    raise ValueError(f'Unable to locate {on[i]}')

    def is_retryable(e: Exception) -> bool:
        nonlocal on
        if on is None:
            return True

        for condition in on:
            if isinstance(condition, type):
                if isinstance(e, condition):
                    return True
            elif hasattr(condition, "__call__"):  # Use an else block because Exception implements __call__
                try:
                    if condition(e):
                        return True
                except e:
                    pass
        return False

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if retry_count < max_retries and is_retryable(e):
                        retry_count += 1
                        backoff(retry_count)
                    else:
                        raise e

        return wrapped

    return decorator
