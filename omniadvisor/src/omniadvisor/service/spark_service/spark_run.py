import multiprocessing
import time

from requests.exceptions import HTTPError

from common.constant import OA_CONF
from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.service.spark_service.spark_command_reconstruct import spark_command_reconstruct
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import save_trace_data


def spark_run(load, conf):
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = spark_command_reconstruct(load, conf)

    # 判断当前的conf是否和load.default_config相同 不相同则在submit_cmd前增加超时时间
    if conf != load.default_config:
        # 获取当前default_config的平均测试性能
        baseline_results = get_tuning_result(load, load.default_config)
        timeout_sec = OA_CONF.timeout_ratio * baseline_results.runtime
        submit_cmd = f"timeout {timeout_sec} " + submit_cmd

    # 根据执行命令创建测试记录
    exam_record = ExamRecordRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    try:
        ExamRecordRepository.update_start_time(exam_record)
        exitcode, spark_output = spark_executor.submit_spark_task(submit_cmd)
        ExamRecordRepository.update_end_time(exam_record)
    except TimeoutError:
        # 任务提交超时等
        exam_record.delete()
        global_logger.error('Spark command submission timed out.')
        raise
    except OSError:
        # 权限不足等
        exam_record.delete()
        global_logger.error('Spark command submission permission denied.')
        raise
    except Exception:
        exam_record.delete()
        global_logger.error('During Spark command submission, known error occurred.')
        raise

    # 不存在application_id的情况下不提取time_taken 直接返回
    if exitcode != 0:
        try:
            return ExamRecordRepository.update_exam_result(
                exam_record=exam_record,
                status=OA_CONF.ExecStatus.fail,
                runtime=OA_CONF.exec_fail_return_runtime,
                trace=OA_CONF.exec_fail_return_trace
            ), ""
        except RuntimeError:
            exam_record.delete()
            raise

    status = OA_CONF.ExecStatus.success
    # 提取application_id与 time_taken(runtime)

    try:
        spark_submit_cmd, application_id, runtime = spark_executor.parser_spark_output(spark_output)
    except Exception:
        exam_record.delete()
        global_logger.error('During parsing spark output, known error occurred.')
        raise

    try:
        exam_record = ExamRecordRepository.update_exam_result(
            exam_record=exam_record,
            status=status,
            runtime=runtime
        )
    except RuntimeError:
        exam_record.delete()
        raise

    # 根据ApplicantID在子进程中获取Trace
    p = multiprocessing.Process(target=_update_trace_from_history_server, args=(exam_record, application_id))
    p.daemon = False
    p.start()

    return exam_record, spark_output


# TODO  这个也要端到端验证一下 用了子进程会不会对ExamRecordRepository有同时读写的问题？
def _update_trace_from_history_server(exam_record: ExamRecord, application_id: str):
    """
    创建一个子进程对history_server进行轮询 超时时间为10s
    :param application_id: applicaction_id
    :return:
    """
    trace_dict = {}
    history_server_url = OA_CONF.spark_history_rest_url
    spark_fetcher = SparkFetcher(history_server_url)
    start_time = time.time()
    while time.time() - start_time < OA_CONF.spark_fetch_trace_timeout:
        try:
            trace_sql = spark_fetcher.get_spark_sql_by_app(application_id)
            trace_stages = spark_fetcher.get_spark_stages_by_app(application_id)
            trace_executor = spark_fetcher.get_spark_executor_by_app(application_id)
        except HTTPError as httpe:
            time.sleep(1)
            global_logger.warning(f"HistoryServer访问错误:{httpe}")
            continue

        trace_dict['sql'] = save_trace_data(data=trace_sql, data_dir=OA_CONF.data_dir)
        trace_dict['stages'] = save_trace_data(data=trace_stages, data_dir=OA_CONF.data_dir)
        trace_dict['executor'] = save_trace_data(data=trace_executor, data_dir=OA_CONF.data_dir)
        break

    ExamRecordRepository.update_exam_result(exam_record, trace=trace_dict)
