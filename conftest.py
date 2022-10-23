import datetime as dt
import json
import os
import time
from datetime import datetime
from queue import Queue
from typing import Iterable
from urllib.request import urlopen

import pytest

from job import Job, name_gen
from scheduler import Scheduler


LIST_TO_FILE = ['All work and no play makes Jack a dull boy.'] * 10


def func_create_dir(dir_name: str):
    if os.path.exists(dir_name):
        raise Exception('%s exists' % dir_name)
    os.mkdir(dir_name)


def func_rename_dir(old_name: str, new_name: str):
    if not os.path.exists(old_name):
        raise Exception('%s not exists' % old_name)
    os.rename(old_name, new_name)


def func_remove_dir(dir_name: str):
    if not os.path.exists(dir_name):
        raise Exception('%s not exists' % dir_name)
    os.rmdir(dir_name)


def func_create_file(file_name: str):
    if os.path.exists(file_name):
        raise Exception('%s exists' % file_name)
    with open(file_name, 'w'): pass


def func_write_to_file(file_name: str, data: Iterable):
    if not os.path.exists(file_name):
        raise Exception('%s not exists' % file_name)
    with open(file_name, 'w') as file:
        for item in data:
            print(item, file=file)


def func_read_from_file(file_name: str):
    if not os.path.exists(file_name):
        raise Exception('%s not exists' % file_name)
    with open(file_name) as file:
        print([line for line in file])


def func_rename_file(file_name: str, new_file_name: str):
    print('---')
    if os.path.exists(file_name):
        print('---')
        os.rename(file_name, new_file_name)
    else:
        print('---')
        raise Exception('%s not exists' % file_name)


def func_remove_file(file_name: str):
    if not os.path.exists(file_name):
        raise Exception('%s not exists' % file_name)
    os.remove(file_name)


def func_get_urls(urls: list, queue: Queue):
    for url in urls:
        with urlopen(url) as req:
            resp = req.read().decode('utf-8')
            if req.status != 200:
                raise Exception(
                    'Error during execute request. {}: {}'.format(
                        resp.status, resp.reason
                    )
                )
            queue.put(resp)
    queue.put(None)


def func_analyzing_of_req(queue: Queue):
    result = []
    while True:

        resp = queue.get()
        if resp is None:
            break
        resp = json.loads(resp)
        name = resp.get('name')
        gender = resp.get('home')
        result.append((name, gender))

    return result


def func_source_get_urls(urls: list):
    result = []
    for url in urls:
        with urlopen(url) as req:
            resp = req.read().decode('utf-8')
            if req.status != 200:
                raise Exception(
                    'Error during execute request. {}: {}'.format(
                        resp.status, resp.reason
                    )
                )
            result.append(resp)
    return result


def func_pipe_analyze_data(result_queue_source: Queue):
    result = []
    data = result_queue_source.get()
    for resp in data:
        resp = json.loads(resp)
        name = resp.get('name')
        gender = resp.get('gender')
        result.append((name, gender))
    return result


def func_target_write_to_file(file_name, result_pipe_queue: Queue):
    data = result_pipe_queue.get()
    with open(file_name, 'w') as file:
        for item in data:
            print(item, file=file)
    return


def func_long_work(working_time):
    time.sleep(working_time)
    return 'Done'


def func_with_exception(working_time):
    time.sleep(working_time)
    raise Exception('something goes wrong')


@pytest.fixture
def name_generator():
    return name_gen()


@pytest.fixture
def one_sec_job():
    return Job(func_long_work, args=(1,))


@pytest.fixture
def two_sec_job():
    return Job(func_long_work, args=(2,))


@pytest.fixture()
def job_with_exception():
    return Job(func_with_exception, args=(0.1,))


@pytest.fixture
def time_out_job():
    return Job(func_long_work, args=(2,), max_working_time=1)


@pytest.fixture
def job_with_fix_start():
    return Job(func_long_work, args=(1,), start_at=(datetime.now() + dt.timedelta(seconds=5)))


@pytest.fixture
def job_with_fix_late_start():
    return Job(func_long_work, args=(1,), start_at=(datetime.now() + dt.timedelta(hours=5)))


@pytest.fixture
def job_with_two_tries():
    return Job(func_long_work, args=(1,), tries=2)


@pytest.fixture
def job_create_dir():
    return Job(func_create_dir, args=('dir_test',))


@pytest.fixture
def job_rename_dir():
    return Job(func_rename_dir, args=('dir_test', 'dir_test_renamed'))


@pytest.fixture
def job_create_file():
    return Job(func_create_file, args=('file_test.txt',), )


@pytest.fixture
def job_write_to_file():
    return Job(func_write_to_file, args=('file_test.txt', LIST_TO_FILE,))


@pytest.fixture
def job_read_from_file():
    return Job(func_read_from_file, args=('file_test.txt',))


@pytest.fixture
def job_rename_file():
    return Job(func_rename_file, kwargs={
        'file_name': 'file_test.txt',
        'new_file_name': 'file_new_name.txt'
    })


@pytest.fixture
def job_remove_file():
    return Job(func_remove_file, args=('file_new_name.txt',))


@pytest.fixture
def job_remove_dir():
    return Job(func_remove_dir, args=('dir_test_renamed',))


queue_for_test_net = Queue()


@pytest.fixture
def job_get_by_url_for_net():
    return Job(func_get_urls, args=([
                                        'https://swapi.dev/api/people/1/',
                                        'https://swapi.dev/api/people/2/',
                                        'https://swapi.dev/api/people/3/'
                                    ], queue_for_test_net))


@pytest.fixture
def job_analyzing_of_req_for_net():
    return Job(func_analyzing_of_req, args=(queue_for_test_net,))


@pytest.fixture
def job_source_get_urls():
    return Job(func_source_get_urls, args=([
                                               'https://swapi.dev/api/people/1/',
                                               'https://swapi.dev/api/people/2/',
                                               'https://swapi.dev/api/people/3/'
                                           ],))


@pytest.fixture
def job_pipe_analyze_data():
    return Job(
        func_pipe_analyze_data,
        args=()
    )


@pytest.fixture
def job_target_write_to_file():
    return Job(
        func_target_write_to_file,
        args=()
    )


@pytest.fixture
def default_scheduler_for_3_jobs():
    return Scheduler(pool_size=3, file_name_for_save='save_test.pickle')


@pytest.fixture
def default_scheduler_for_2_jobs():
    return Scheduler(pool_size=2, file_name_for_save='save_test.pickle')


@pytest.fixture
def scheduler_for_3_jobs_restart_mode():
    return Scheduler(
        pool_size=5,
        states_file_path_for_restart_mode='save_test.pickle',
        restart_mode=True
    )


@pytest.fixture
def default_scheduler_for_5_jobs():
    return Scheduler(pool_size=5, file_name_for_save='test.pickle')


@pytest.fixture
def job_attrs_for_recreate():
    return {
        'target': print,
        'max_working_time': 5,
        'tries': 5,
        'args': (2, 3),
        'kwargs': {'sep': '\t'},
        'start_at': datetime.now(),
        'dependencies': ['get_url']
    }


@pytest.fixture
def scheduler_with_jobs_in_all_lists(
        default_scheduler_for_5_jobs,
        one_sec_job,
        job_with_exception,
        job_with_fix_start,
        job_with_fix_late_start,
        job_with_two_tries
):
    job_with_exception.set_dependencies([job_with_two_tries])
    default_scheduler_for_5_jobs.run_job_in_pool.send((0, one_sec_job))
    default_scheduler_for_5_jobs.run_job_in_pool.send((1, job_with_fix_start))
    default_scheduler_for_5_jobs.waiting_jobs = [
        job_with_fix_late_start,
    ]
    default_scheduler_for_5_jobs.finished_jobs = [
        job_with_two_tries
    ]
    return default_scheduler_for_5_jobs
