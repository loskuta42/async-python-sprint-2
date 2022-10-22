import logging
import pickle
import time
from collections.abc import Generator
from datetime import datetime
from multiprocessing import Process
from threading import Event, Thread
from typing import Optional

from tools import CyclicIterator, coroutine
from job import Job

logger = logging.getLogger()

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    datefmt='%H:%M:%S',
    format='%(asctime)s: %(name)s - %(levelname)s - %(message)s'
)

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

JOB_KWARGS_NAMES_FOR_RECREATE = [
    'target',
    'max_working_time',
    'tries',
    'args',
    'kwargs',
    'start_at',
    'dependencies'
]


class Scheduler(Thread):
    """
    Scheduler of tasks.
    """

    def __init__(
            self,
            pool_size: int = 10,
            file_name_for_save: str = 'states.pickle',
            restart_mode: bool = False,
            states_file_path_for_restart_mode: Optional[str] = None
    ) -> None:
        """
        :param pool_size: number of concurrent jobs.
        :param file_name_for_save: file name for save states after finish or stop
        :param restart_mode: start_scheduler from states of file
        """
        super().__init__()
        self.pool_size = pool_size
        self.pool: list = [None] * self.pool_size
        self.waiting_jobs: list = []
        self.finished_jobs: list = []
        self.cyclic_iterator = CyclicIterator(range(self.pool_size))
        self.stop_event: Event = Event()
        self.run_job_in_pool = self._run_job_in_pool()
        self.get_job_from_wait_list = self._get_job()
        self.status: str = 'created'
        self.restart_event: Event = Event()
        self.exit_event: Event = Event()
        self.file_name_save_for_save: str = file_name_for_save
        self.restart_mode = restart_mode
        self._file_for_restart_mode = states_file_path_for_restart_mode

    @coroutine
    def _run_job_in_pool(self) -> Generator:
        while True:
            ind, job = (yield)
            self.pool[ind] = job
            job.run_thread()

    def _get_job(self) -> Generator:
        temp_jobs = []
        while True:
            while self.waiting_jobs:
                job = self.waiting_jobs.pop()
                if job.start_at <= datetime.now():
                    if job.dependencies:
                        deps_status = [dep.status for dep in job.dependencies]
                        if deps_status.count('done') == len(job.dependencies):
                            temp_jobs.reverse()
                            self.waiting_jobs.extend(temp_jobs)
                            temp_jobs.clear()
                            yield job
                        elif 'failed' in deps_status:
                            job.status = 'failed'
                            self.finished_jobs.append(job)
                        else:
                            temp_jobs.append(job)
                    else:
                        temp_jobs.reverse()
                        self.waiting_jobs.extend(temp_jobs)
                        temp_jobs.clear()
                        yield job
                else:
                    temp_jobs.append(job)
            if temp_jobs:
                temp_jobs.reverse()
                self.waiting_jobs.extend(temp_jobs)
                temp_jobs.clear()
                yield None

    def schedule(self, job: Job) -> None:
        """Schedule the job."""
        logger.info(
            'scheduled: %s ' % job.name
        )
        self.waiting_jobs.append(job)
        job.status = 'waiting'

    def run(self) -> None:
        if self.restart_mode:
            self.stop_event = Event()
            logger.info('Scheduler restart')
            try:
                self._prepare_states_for_run(self._file_for_restart_mode)
            except Exception:
                logger.exception('Problems in restart_mode: ')
                return
        while True:
            self.waiting_jobs.sort(key=lambda x: x.start_at, reverse=True)
            self.status = 'running'
            for ind in self.cyclic_iterator:
                time.sleep(1)
                logger.info(
                    'waiting: %s pool: %s' % (self.waiting_jobs, self.pool)
                )
                logger.info(
                    'scheduler status: %s' % self.status
                )
                if not self.waiting_jobs and self.pool.count(None) == self.pool_size:
                    self.status = 'finish'
                    break
                elif self.stop_event.is_set():
                    self._save_states_to_file(self.file_name_save_for_save)
                    break
                elif self.pool[ind] is None:
                    if self.waiting_jobs and (job := next(self.get_job_from_wait_list)):
                        self.run_job_in_pool.send((ind, job))

                    else:
                        continue
                else:
                    job = self.pool[ind]
                    job_status = job.status
                    if job_status == 'done':
                        result = job.result_queue.get()
                        job.result_queue.put(result)
                        self.pool[ind] = None
                        self.finished_jobs.append(job)

                    elif job_status in ['terminated', 'raised_exception']:
                        if job.tries:
                            job.restart()
                        else:
                            self.pool[ind] = None
                            job.status = 'failed'
                            self.finished_jobs.append(job)
            logger.info(
                'Scheduler finish working finished jobs: %s' % self.finished_jobs
            )
            if self.exit_event.is_set():
                self.status = 'exit'
                break
            elif self.restart_event.is_set():
                self.status = 'restart'
                time.sleep(1)
                self.restart_event.clear()
                continue

    def restart(self) -> None:
        logger.info('Scheduler restart')
        self.stop_event.clear()
        self.restart_event.set()
        self._prepare_states_for_run(self.file_name_save_for_save)

    def exit(self) -> None:
        logger.info('Scheduler exit')
        self.stop_event.set()
        self.exit_event.set()

    def stop(self) -> None:
        self.status = 'stop'
        self.stop_event.set()
        time.sleep(1)

    @staticmethod
    def _prepare_job_kwargs(job_state: dict) -> dict:
        return {
            attr_name: attr_value
            for attr_name, attr_value in job_state.items()
            if attr_name in JOB_KWARGS_NAMES_FOR_RECREATE
        }

    def _get_categories_to_jobs(self, saved_states_file: str = 'states.pickle') -> dict:
        with open(saved_states_file, 'rb') as file:
            states = pickle.load(file)
        jobs = []

        category_to_jobs = {
            'pool': [],
            'waiting': [],
            'finished': []
        }
        for job_state in states:
            job_kwargs = self._prepare_job_kwargs(job_state)
            job_obj = Job(**job_kwargs)
            job_obj.set_name(job_state['name'])
            if job_state['result'] is None or job_state['result']:
                job_obj.set_result(job_state['result'])
            job_obj.status = job_state['_status']
            category_to_jobs[job_state['scheduler_lst']].append(job_obj)
            jobs.append(job_obj)

        for category_of_jobs in category_to_jobs.values():
            for job in category_of_jobs:
                if job.dependencies:
                    dep_jobs = [
                        job_dep
                        for job_dep in jobs
                        if job_dep.name in job.dependencies
                    ]
                    job.set_dependencies(dep_jobs)
        return category_to_jobs

    def _prepare_states_for_run(self, saved_states_file: str = 'states.pickle') -> None:
        category_to_jobs = self._get_categories_to_jobs(saved_states_file=saved_states_file)
        jobs_for_pool = category_to_jobs['pool']
        if len(self.pool) < len(jobs_for_pool):
            raise Exception(
                'Wrong pool size at restart, '
            )
        for ind, job in enumerate(jobs_for_pool):
            self.pool[ind] = job
            if job.status not in ['done', 'failed', 'terminated', 'raised_exception']:
                job.run_thread()
        self.waiting_jobs = category_to_jobs['waiting']
        self.finished_jobs = category_to_jobs['finished']

    @coroutine
    def _get_job_state(self):
        result = {}
        while True:
            job = yield result
            job_attrs = job.__dict__
            result = {}
            for atr_name, atr_value in job_attrs.items():
                if atr_name not in ATTRIBUTE_OF_JOB_TO_FILE:
                    continue
                if atr_name == 'dependencies':
                    result[atr_name] = [dep.name for dep in atr_value]
                elif atr_name == 'result_queue':
                    if atr_value.empty():
                        result['result'] = None
                    else:
                        result['result'] = atr_value.get()
                else:
                    result[atr_name] = atr_value

    def _save_states_to_file(self, save_to_filename: str = 'states.pickle') -> None:
        to_file = [
        ]
        get_job_state_coro = self._get_job_state()
        for pool_job in self.pool:
            if pool_job is None:
                continue
            else:
                pool_job.pause()
                job_state = get_job_state_coro.send(pool_job)
                job_state['scheduler_lst'] = 'pool'
                to_file.append(job_state)
        self.pool = [None]*self.pool_size

        for waiting_job in self.waiting_jobs:
            job_state = get_job_state_coro.send(waiting_job)
            job_state['scheduler_lst'] = 'waiting'
            to_file.append(job_state)
        self.waiting_jobs.clear()

        for finished_job in self.finished_jobs:
            job_state = get_job_state_coro.send(finished_job)
            job_state['scheduler_lst'] = 'finished'
            to_file.append(job_state)
        self.finished_jobs.clear()

        with open(save_to_filename, 'wb') as file:
            pickle.dump(to_file, file, protocol=pickle.HIGHEST_PROTOCOL)
