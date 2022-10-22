import os
import time


def test_scheduler_with_system_actions(
        default_scheduler_for_5_jobs,
        job_create_dir,
        job_rename_dir,
        job_remove_dir,
        job_create_file,
        job_write_to_file,
        job_read_from_file,
        job_rename_file,
        job_remove_file

):
    job_remove_dir.set_dependencies([job_rename_dir])
    job_rename_dir.set_dependencies([job_create_dir])

    default_scheduler_for_5_jobs.schedule(job_create_dir)
    default_scheduler_for_5_jobs.schedule(job_rename_dir)
    default_scheduler_for_5_jobs.schedule(job_remove_dir)

    job_remove_file.set_dependencies([job_rename_file])
    job_rename_file.set_dependencies([job_read_from_file])
    job_read_from_file.set_dependencies([job_write_to_file])
    job_write_to_file.set_dependencies([job_create_file])

    default_scheduler_for_5_jobs.schedule(job_create_file)
    default_scheduler_for_5_jobs.schedule(job_rename_file)
    default_scheduler_for_5_jobs.schedule(job_write_to_file)
    default_scheduler_for_5_jobs.schedule(job_read_from_file)

    default_scheduler_for_5_jobs.schedule(job_remove_file)

    default_scheduler_for_5_jobs.start()
    time.sleep(25)
    assert len(default_scheduler_for_5_jobs.finished_jobs) == 8
    default_scheduler_for_5_jobs.exit()


def test_work_with_net(
        default_scheduler_for_2_jobs,
        job_get_by_url_for_net,
        job_analyzing_of_req_for_net
):
    default_scheduler_for_2_jobs.schedule(job_get_by_url_for_net)
    default_scheduler_for_2_jobs.schedule(job_analyzing_of_req_for_net)
    default_scheduler_for_2_jobs.start()
    time.sleep(10)
    assert len(job_analyzing_of_req_for_net.result_queue.get()) == 3
    assert len(default_scheduler_for_2_jobs.finished_jobs) == 2
    default_scheduler_for_2_jobs.exit()


def test_pipeline(
        default_scheduler_for_3_jobs,
        job_source_get_urls,
        job_pipe_analyze_data,
        job_target_write_to_file
):
    job_pipe_analyze_data.set_dependencies([job_source_get_urls])
    job_pipe_analyze_data.args = (job_source_get_urls.result_queue,)
    job_target_write_to_file.set_dependencies([job_pipe_analyze_data])
    job_target_write_to_file.args = ('file_test_pipeline.txt', job_pipe_analyze_data.result_queue)
    default_scheduler_for_3_jobs.schedule(job_source_get_urls)
    default_scheduler_for_3_jobs.schedule(job_pipe_analyze_data)
    default_scheduler_for_3_jobs.schedule(job_target_write_to_file)
    default_scheduler_for_3_jobs.start()
    time.sleep(6)
    assert os.path.exists('file_test_pipeline.txt')
    with open('file_test_pipeline.txt') as f:
        result = [eval(line) for line in f]

    assert len(result) == 3
    assert result[0][0] == 'Luke Skywalker'
    assert result[0][1] == 'male'
    default_scheduler_for_3_jobs.exit_event.set()

