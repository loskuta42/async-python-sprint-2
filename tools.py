import functools
from queue import Queue

from threading import Thread
from typing import Iterable
import sys


class CyclicIterator:
    def __init__(self, container: Iterable):
        self.container = container
        self.cont_iter = iter(container)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.cont_iter)
        except StopIteration:
            self.cont_iter = iter(self.container)
            return next(self.cont_iter)


class KThread(Thread):
    """A subclass of threading.Thread, with a kill() method."""

    def __init__(self, *args, **keywords):
        Thread.__init__(self, *args, **keywords)
        self.killed = False

    def start(self):
        """Start the thread."""
        self.__run_backup = self.run
        self.run = self.__run
        Thread.start(self)

    def __run(self):
        """Hacked run function, which installs the trace."""
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True


class JobException(BaseException):
    pass


class KillException(BaseException):
    pass


def result_decorator(queue: Queue):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                queue.put(result)
                return result
            except BaseException as ex:
                queue.put(JobException(ex))

        return wrapper

    return actual_decorator


def coroutine(method):
    def wrapper(*args, **kwargs):
        gen = method(*args, **kwargs)
        gen.send(None)
        return gen

    return wrapper
