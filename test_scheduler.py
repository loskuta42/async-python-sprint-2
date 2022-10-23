import os
import pickle
import time

from conftest import func_with_exception
from enums import JobStatus


def test_run_job_in_pool(default_scheduler_for_5_jobs, one_sec_job):
    default_scheduler_for_5_jobs.run_job_in_pool.send((0, one_sec_job))
    assert default_scheduler_for_5_jobs.pool[0] is one_sec_job
    assert one_sec_job.status == JobStatus.WORKING


def test_get_job_from_wait_list_generator(
        default_scheduler_for_5_jobs,
        one_sec_job,
        job_with_exception,
        job_with_fix_start,
        job_with_fix_late_start,
        job_with_two_tries
):
    default_scheduler_for_5_jobs.waiting_jobs.extend(
        [job_with_fix_late_start,
         job_with_fix_start,
         one_sec_job]
    )
    job_with_exception.set_dependencies([job_with_two_tries])
    default_scheduler_for_5_jobs.waiting_jobs.append(job_with_exception)
    next_job_for_start = next(default_scheduler_for_5_jobs.get_job_from_wait_list)
    assert one_sec_job == next_job_for_start
    assert default_scheduler_for_5_jobs.waiting_jobs == [job_with_fix_late_start,
                                                         job_with_fix_start,
                                                         job_with_exception
                                                         ]


def test_schedule_method(default_scheduler_for_5_jobs, one_sec_job):
    default_scheduler_for_5_jobs.schedule(one_sec_job)
    assert default_scheduler_for_5_jobs.waiting_jobs[0] == one_sec_job


def test_prepare_job_kwargs_method(
        default_scheduler_for_5_jobs,
        job_attrs_for_recreate,
):
    extra_attrs = {
        'status': JobStatus.DONE,
        'pool_size': 4
    }
    test_attrs = job_attrs_for_recreate.copy()
    job_attrs_for_recreate.copy().update(extra_attrs)
    result = default_scheduler_for_5_jobs._prepare_job_kwargs(test_attrs)
    assert result == job_attrs_for_recreate


def test_get_job_state(
        default_scheduler_for_5_jobs,
        job_with_exception,
        job_with_two_tries
):
    job_with_exception.set_dependencies([job_with_two_tries])
    expected = {
        'target': func_with_exception,
        'name': job_with_exception.name,
        'max_working_time': job_with_exception.max_working_time,
        'tries': job_with_exception.tries,
        'args': job_with_exception.args,
        'kwargs': job_with_exception.kwargs,
        'start_at': job_with_exception.start_at,
        'result': None,
        'dependencies': [job_with_exception.dependencies[0].name],
        '_status': job_with_exception._status
    }
    coro = default_scheduler_for_5_jobs._get_job_state()
    result = coro.send(job_with_exception)
    assert result == expected


def test_save_states_to_file_method(
        scheduler_with_jobs_in_all_lists
):
    one_sec_job = scheduler_with_jobs_in_all_lists.pool[0]
    job_with_fix_start = scheduler_with_jobs_in_all_lists.pool[1]
    job_with_fix_late_start = scheduler_with_jobs_in_all_lists.waiting_jobs[0]
    job_with_two_tries = scheduler_with_jobs_in_all_lists.finished_jobs[0]

    scheduler_with_jobs_in_all_lists.start()
    scheduler_with_jobs_in_all_lists.stop_event.set()
    scheduler_with_jobs_in_all_lists._save_states_to_file(
        save_to_filename='save_test.pickle'
    )
    coro_get_job = scheduler_with_jobs_in_all_lists._get_job_state()
    one_sec_job_state = coro_get_job.send(
        one_sec_job
    )
    one_sec_job_state['scheduler_lst'] = 'pool'
    job_with_fix_start_state = coro_get_job.send(
        job_with_fix_start
    )
    job_with_fix_start_state['scheduler_lst'] = 'pool'
    job_with_fix_late_start_state = coro_get_job.send(
        job_with_fix_late_start
    )
    job_with_fix_late_start_state['scheduler_lst'] = 'waiting'
    job_with_two_tries_state = coro_get_job.send(
        job_with_two_tries
    )
    job_with_two_tries_state['scheduler_lst'] = 'finished'
    with open('save_test.pickle', 'rb') as file:
        states = pickle.load(file)
    assert one_sec_job_state in states
    assert job_with_fix_start_state in states
    assert job_with_fix_late_start_state in states
    assert job_with_two_tries_state in states
    assert os.path.exists('save_test.pickle')
    scheduler_with_jobs_in_all_lists.exit_event.set()
    os.remove('save_test.pickle')


def test_get_categories_to_jobs_method(
        scheduler_with_jobs_in_all_lists
):
    scheduler_with_jobs_in_all_lists.start()
    scheduler_with_jobs_in_all_lists.stop_event.set()
    category_to_job = {
        'pool': [],
        'waiting': [],
        'finished': []
    }
    for pool_job in scheduler_with_jobs_in_all_lists.pool:
        if pool_job is None:
            continue
        else:
            category_to_job['pool'].append(pool_job)

    for waiting_job in scheduler_with_jobs_in_all_lists.waiting_jobs:
        category_to_job['waiting'].append(waiting_job)

    for finished_job in scheduler_with_jobs_in_all_lists.finished_jobs:
        category_to_job['finished'].append(finished_job)
    scheduler_with_jobs_in_all_lists._save_states_to_file(
        save_to_filename='save_test.pickle'
    )
    scheduler_with_jobs_in_all_lists.exit()
    result = scheduler_with_jobs_in_all_lists._get_categories_to_jobs(
        'save_test.pickle'
    )
    result_pool = result['pool']
    category_to_job_pool = result['pool']
    for job1, job2 in zip(result_pool, category_to_job_pool):
        assert job1.__dict__ == job2.__dict__
    result_waiting = result['waiting']
    category_to_job_waiting = result['waiting']
    for job1, job2 in zip(result_waiting, category_to_job_waiting):
        assert job1.__dict__ == job2.__dict__
    result_finished = result['finished']
    category_to_job_finished = result['finished']
    for job1, job2 in zip(result_finished, category_to_job_finished):
        assert job1.__dict__ == job2.__dict__
    scheduler_with_jobs_in_all_lists.exit_event.set()
    os.remove('save_test.pickle')


def test_stop(
        default_scheduler_for_3_jobs,
        one_sec_job,
        two_sec_job
):
    default_scheduler_for_3_jobs.schedule(
        one_sec_job
    )
    default_scheduler_for_3_jobs.schedule(
        two_sec_job
    )
    default_scheduler_for_3_jobs.start()
    default_scheduler_for_3_jobs.stop()
    assert len(default_scheduler_for_3_jobs.waiting_jobs) == 2
    default_scheduler_for_3_jobs.exit_event.set()
    time.sleep(1)
    os.remove('save_test.pickle')


def test_restart(
        default_scheduler_for_2_jobs,
        one_sec_job,
        two_sec_job
):
    default_scheduler_for_2_jobs.schedule(
        one_sec_job
    )
    default_scheduler_for_2_jobs.schedule(
        two_sec_job
    )
    default_scheduler_for_2_jobs.start()
    default_scheduler_for_2_jobs.stop()
    time.sleep(2)
    default_scheduler_for_2_jobs.restart()
    assert len(default_scheduler_for_2_jobs.finished_jobs) != 2
    time.sleep(6)
    assert len(default_scheduler_for_2_jobs.finished_jobs) == 2
    default_scheduler_for_2_jobs.exit_event.set()


def test_restart_mode(
        scheduler_for_3_jobs_restart_mode,
        one_sec_job,
        two_sec_job
):
    scheduler_for_3_jobs_restart_mode.schedule(
        one_sec_job
    )
    scheduler_for_3_jobs_restart_mode.schedule(
        two_sec_job
    )
    scheduler_for_3_jobs_restart_mode.start()
    time.sleep(10)
    assert len(scheduler_for_3_jobs_restart_mode.finished_jobs) == 2
    scheduler_for_3_jobs_restart_mode.exit_event.set()
    os.remove('save_test.pickle')


def test_exit(
        default_scheduler_for_2_jobs,
        one_sec_job,
        two_sec_job
):
    default_scheduler_for_2_jobs.schedule(
        one_sec_job
    )
    default_scheduler_for_2_jobs.schedule(
        two_sec_job
    )
    default_scheduler_for_2_jobs.start()
    time.sleep(1)
    default_scheduler_for_2_jobs.exit()
    time.sleep(2)
    assert len(default_scheduler_for_2_jobs.pool) == 2
    assert not default_scheduler_for_2_jobs.is_alive()
    os.remove('save_test.pickle')


