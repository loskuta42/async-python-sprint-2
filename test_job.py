import datetime as dt
import time
from datetime import datetime

from enums import JobStatus


def test_name_gen(name_generator):
    job_name_one = next(name_generator)
    job_name_two = next(name_generator)
    assert job_name_one == 'Job-1'
    assert job_name_two == 'Job-2'


def test_set_job_name(one_sec_job):
    one_sec_job.set_name('redrum')
    assert one_sec_job.name == 'redrum'


def test_set_job_result(one_sec_job):
    one_sec_job.set_result('redrum')
    assert one_sec_job.result_queue.get() == 'redrum'


def test_set_job_dependencies(one_sec_job, job_with_exception, job_with_two_tries):
    one_sec_job.set_dependencies([job_with_exception, job_with_two_tries])
    assert one_sec_job.dependencies == [job_with_exception, job_with_two_tries]


def test_count_datetime_from_str(one_sec_job):
    job_start_time = datetime.strptime(one_sec_job._count_start_datetime('13:22').strftime("%Y-%m-%d %H:%M"),
                                       "%Y-%m-%d %H:%M")
    now = datetime.now()
    temp_time = now.replace(hour=13, minute=22)
    start_time = temp_time if temp_time > now else (temp_time + dt.timedelta(days=1))
    start_time = datetime.strptime(start_time.strftime("%Y-%m-%d %H:%M"), "%Y-%m-%d %H:%M")
    assert start_time == job_start_time


def test_run_job(one_sec_job):
    one_sec_job.run_thread()
    assert one_sec_job.status == JobStatus.WORKING


def test_finish_job(one_sec_job):
    one_sec_job.run_thread()
    time.sleep(1.2)
    assert one_sec_job.status == JobStatus.DONE


def test_exception_in_job(job_with_exception):
    job_with_exception.run_thread()
    time.sleep(0.3)
    assert job_with_exception.status == JobStatus.RAISED_EXCEPTION


def test_can_not_set_status(one_sec_job):
    one_sec_job.status = JobStatus.FAILED
    assert one_sec_job.status == JobStatus.WAITING


def test_get_status(one_sec_job):
    assert one_sec_job.status == JobStatus.WAITING


def test_restart_job(job_with_two_tries):
    job_with_two_tries.run_thread()
    thread_one = job_with_two_tries.thread
    tries_one = job_with_two_tries.tries
    job_with_two_tries.restart()
    thread_two = job_with_two_tries.thread
    tries_two = job_with_two_tries.tries
    assert thread_one is not thread_two
    assert tries_one != tries_two


def test_terminate_job(one_sec_job):
    one_sec_job.run_thread()
    one_sec_job.terminate_job()
    assert one_sec_job.status == JobStatus.TERMINATED


def test_job_for_timeout(time_out_job):
    time_out_job.run_thread()
    time.sleep(1.2)
    assert time_out_job.status == JobStatus.TERMINATED
