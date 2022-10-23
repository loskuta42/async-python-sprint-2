from enum import Enum


class JobStatus(Enum):
    """Represents job status."""

    CREATED = 'created'
    DONE = 'done'
    TERMINATED = 'terminated'
    RAISED_EXCEPTION = 'raised_exception'
    WAITING = 'waiting'
    WORKING = 'working'
    FAILED = 'failed'


class SchedulerStatus(Enum):
    """Represents scheduler status."""
    CREATED = 'created'
    RUNNING = 'running'
    FINISH = 'finish'
    EXIT = 'exit'
    RESTART = 'restart'
    STOP = 'stop'

