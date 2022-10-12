from datetime import datetime
from typing import Callable
from threading import Timer, Condition


class Job:
    def __init__(
            self,
            target: Callable,
            *args,
            start_at: str = "",
            max_working_time: int = -1,
            tries: int = 0,
            dependencies: list = []
    ):
        self.start_at = datetime.strptime(start_at, '%H:%M').time()
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.target = target
        self.args = args

    def run(self):
        if self.start_at:
            t = Timer(self.run, )

    def pause(self):
        pass

    def stop(self):
        pass
