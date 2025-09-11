import multiprocessing
import time
from typing import Tuple

from requests.exceptions import HTTPError

from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import save_trace_data


_RETURN_CODE_MAP = {
    0: "Success",
    1: "Spark SQL failed",
    2: "CLI argument error",
    124: "timeout",
    143: "Terminated by SIGTERM",
    255: "Spark subprocess(eg. Excutor,external command) terminated abnormally."
}


def spark_run(load: Load, config: dict, wait_for_trace: bool = True,
              exec_in_isolation: bool = False) -> Tuple[ExamRecord, str]:
    """
    输入负载与配置，执行Spark任务 并从Spark命令的返回值中获取
    生成一条记录本次执行信息的exam_record
    同时从执行结果中获取Application

    :param load: 负载
    :param config: 参数配置
    :param wait_for_trace: 是否阻塞等待获取trace
    :param exec_in_isolation: 是否在隔离队列中执行
    :return:测试记录实例, Spark任务执行的返回结果
    """
    # 是否要在隔离中执行，若在隔离中执行，则将队列改为隔离队列
    if exec_in_isolation and OA_CONF.backend_retest_queue:
        exec_attr = load.exec_attr
        exec_attr['queue'] = OA_CONF.backend_retest_queue
    else:
        exec_attr = load.exec_attr
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = SparkCMDParser.reconstruct_cmd(exec_attr=exec_attr, conf_params=config)

    # 判断当前执行配置是否为租户基线配置，若不相同则需设置超时时间
    if config != load.default_config:
        timeout = _calc_timeout_from_load(load=load)
    else:
        timeout = 0

    # 执行spark任务，并获得执行结果
    global_logger.debug('Spark exec cmd is: %s, timeout is: %d', submit_cmd, timeout)
    exec_result = SparkExecutor.submit_spark_task(cmd_fields=submit_cmd, timeout=timeout)
    if exec_result.exitcode == 0:
        global_logger.info('Spark Load %d execute success', load.id)
        exam_record_status = OA_CONF.ExecStatus.success
        exam_record_runtime = exec_result.duration
        exam_record_application_id = exec_result.application_id
    else:
        global_logger.warning('Spark Load %d execute failed, return code: %d, error code describe: %s', load.id,
                              exec_result.exitcode, _RETURN_CODE_MAP.get(exec_result.exitcode, 'Abnormal exit code'))
        exam_record_status = OA_CONF.ExecStatus.fail
        exam_record_runtime = OA_CONF.exec_fail_return_runtime
        exam_record_application_id = OA_CONF.exec_fail_return_application_id

    # 构建测试记录
    exam_record = ExamRecordRepository.create(
        load=load,
        config=config,
        status=exam_record_status,
        runtime=exam_record_runtime,
        start_time=exec_result.start_time,
        end_time=exec_result.end_time,
        application_id=exam_record_application_id
    )

    if exec_result.exitcode == 0:
        # 根据ApplicantID获取Trace
        if wait_for_trace:
            # 阻塞获取trace
            global_logger.info('Going to fetch Spark execute trace, the process is blocking.')
            status = _update_trace_and_runtime_from_history_server(
                exam_record=exam_record,
                application_id=exec_result.application_id
            )
            if not status:
                raise RuntimeError('Unknown exception happen when fetching spark trace, please check history server.')
        else:
            # 不阻塞获取trace，通过子进程进行获取
            global_logger.info('Going to fetch Spark execute trace, the process is non-blocking.')
            p = multiprocessing.Process(
                target=_update_trace_and_runtime_from_history_server,
                args=(exam_record, exec_result.application_id),
                name="omniadvisor_fetch_trace_and_runtime"
            )
            p.daemon = False
            p.start()

    return exam_record, exec_result.output


def _calc_timeout_from_load(load: Load) -> float:
    """
    根据负载的基线执行时间计算该负载在复测时的超时时间

    :param load: 负载
    :return: 超时时间
    """
    # 超时时间为基线执行用时的倍数
    baseline_result = get_tuning_result(load=load, config=load.default_config)
    return OA_CONF.spark_exec_timeout_ratio * baseline_result.runtime


def _update_trace_and_runtime_from_history_server(exam_record: ExamRecord, application_id: str) -> bool:
    """
    根据application_id对history_server进行轮询, 查询该任务的trace信息和runtime信息，并刷新ExamRecord中相关字段的信息

    :param application_id: Spark任务application_id
    :return:
    """
    trace_dict = {}
    history_server_url = OA_CONF.spark_history_rest_url
    history_server_username = OA_CONF.spark_history_username
    history_server_password = OA_CONF.spark_history_password
    spark_fetcher = SparkFetcher(history_server_url, history_server_username, history_server_password)
    start_time = time.time()
    while time.time() - start_time < OA_CONF.spark_fetch_trace_timeout:
        try:
            trace_sql = spark_fetcher.get_spark_sql_by_app(application_id)
            trace_stages = spark_fetcher.get_spark_stages_by_app(application_id)
            trace_executor = spark_fetcher.get_spark_executor_by_app(application_id)
            runtime = spark_fetcher.get_spark_runtime_by_app(application_id)
        except HTTPError as httpe:
            time.sleep(OA_CONF.spark_fetch_trace_interval)
            global_logger.debug(f"Cannot access history server: %s", httpe)
            continue
        except Exception as e:
            global_logger.error('Unknown exception happen, error info: %s', e)
            return False
        trace_dict['sql'] = save_trace_data(data=trace_sql, data_dir=OA_CONF.data_dir)
        trace_dict['stages'] = save_trace_data(data=trace_stages, data_dir=OA_CONF.data_dir)
        trace_dict['executor'] = save_trace_data(data=trace_executor, data_dir=OA_CONF.data_dir)
        global_logger.info(f"Fetch Spark execute trace and runtime success.")
        break
    else:
        global_logger.error(f'Failed to get App %s trace and runtime from %s', application_id, history_server_url)
        return False

    ExamRecordRepository.update_runtime(exam_record, runtime=runtime)
    ExamRecordRepository.update_trace(exam_record, trace=trace_dict)
    return True
