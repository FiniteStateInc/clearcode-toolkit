"""
    timer.py

    Contains helper functions used to time and profile blocks of code.
"""

import logging
import time

logger = logging.getLogger('timer')


class CodeTimer:
    """ Helper class that measures the time it takes to run a block of code.
        Basic usage:
            with CodeTimer('Loop 1000 times'):
                for i in range (1000):
                    pass

            Output:
              '[*] Loop 1000 times - 0.002345 s
    """

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()

    def report(self):
        self.took = (time.time() - self.start)
        logger.debug('{} - {:.3f} s'.format(self.name, self.took))

    def __exit__(self, exc_type, exc_value, traceback):
        self.report()
