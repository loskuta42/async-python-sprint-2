import datetime as dt
import logging.config
from datetime import datetime
from queue import Queue
from threading import Timer
from typing import Callable, Union

import yaml

from enums import JobStatus
from tools import JobException, KThread, result_decorator


with open('logging_config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

ATTRIBUTE_OF_JOB_TO_FILE = [
    'target',
    'name',
    'max_working_time',
    'tries',
    'args',
    'kwargs',
    'start_at',
    'result_queue',
    'dependencies',
    '_status'
]


def name_gen():
    n = 1
    while True:
        yield f'Job-{n}'
        n += 1


NAME_GEN = name_gen()


class Job:
    """Job class for schedule."""
    def __init__(
            self,
            target: Callable,
            start_at: Union[str, datetime] = '',
            max_working_time: int = 0,
            args: tuple = tuple(),
            kwargs: dict = {},
            dependencies: list = [],
            tries: int = 0,
    ):
        """

        :param target: function for schedule
        :param start_at: start time str(hh:mm) or datetime
        :param max_working_time: max time for work in seconds
        :param args: arguments for target function
        :param kwargs: key arguments for target function
        :param dependencies: list of functions-dependencies for target
        :param tries: number of capable restarts for functions
        """
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.thread = None
        self.tries = tries
        self.start_at = self._count_start_datetime(start_at) if isinstance(start_at, str) else start_at
        self.max_working_time = max_working_time
        self.result_queue = Queue()
        self.do_func = result_decorator(self.result_queue)(self.do_func)
        self.name = next(NAME_GEN)
        self._status = JobStatus.CREATED
        self.dependencies = dependencies
        self._timer = None

    def __repr__(self) -> str:
        return f'Job:{self.name}, status:{self._status}'

    def set_name(self, name: str) -> None:
        self.name = name

    def set_result(self, result: str) -> None:
        self.result_queue.put(result)

    def set_dependencies(self, dependencies: list) -> None:
        self.dependencies = dependencies

    @staticmethod
    def _count_start_datetime(start_at: str) -> datetime:
        if start_at:
            start = datetime.strptime(start_at, '%H:%M')
            now = datetime.now()
            hour, minute = start.hour, start.minute
            start_time = now.replace(hour=hour, minute=minute)
            if start_time < now:
                return start_time + dt.timedelta(days=1)
            return start_time
        return datetime.now()

    def do_func(self, *args, **kwargs):
        return self.target(*args, **kwargs)

    def _set_final_status(self) -> None:
        result = self.result_queue.get()
        if isinstance(result, JobException):
            self.status = JobStatus.RAISED_EXCEPTION
            logger.error(
                f'exception "{result}" in {self.name}'
            )
            if self._timer:
                self._timer.cancel()
        else:
            self.result_queue.put(result)
            self.status = JobStatus.DONE
            if self._timer:
                self._timer.cancel()

    def terminate_job(self) -> None:
        if self.result_queue.empty() and self.thread.is_alive():
            self.thread.kill()
            self.status = JobStatus.TERMINATED
            logger.info(
                f'Status of job has changed : {self.name}, {self._status}'
            )
        else:
            self._set_final_status()

    def pause(self) -> None:
        if not self.result_queue.empty() and not self.thread.is_alive():
            self._set_final_status()
        logger.info(
            f'Stop running of {self.name}'
        )
        self.thread.kill()

    @property
    def status(self) -> JobStatus:
        if self._status not in [
            JobStatus.TERMINATED,
            JobStatus.DONE,
            JobStatus.RAISED_EXCEPTION
        ]:
            if not self.thread:
                return JobStatus.WAITING
            elif not self.thread.is_alive():
                self._set_final_status()
        return self._status

    @status.setter
    def status(self, status: str) -> None:
        self._status = status

    def run_thread(self) -> None:
        thread_object = KThread(
            target=self.do_func,
            args=self.args,
            kwargs=self.kwargs
        )
        if self.max_working_time:
            self._timer = Timer(self.max_working_time, self.terminate_job)
            self._timer.start()
        thread_object.start()
        self.thread = thread_object
        self.status = JobStatus.WORKING
        logger.info(
            f'Status of job has changed : {self.name}, {self._status}'
        )

    def restart(self) -> None:
        self.tries -= 1
        logger.info(
            f'Restart of {self.name}'
        )
        if not self.result_queue.empty():
            self.result_queue.get()
        self.thread = None
        self.run_thread()
