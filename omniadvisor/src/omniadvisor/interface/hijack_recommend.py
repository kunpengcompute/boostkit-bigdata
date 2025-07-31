import hashlib
import json
import multiprocessing
import re
import sys

from common.constant import OA_CONF
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import float_format

_NAME_STR = 'name'
_USER_CONFIG_STR = 'user_config'
_FILE_CONTENT_STR = 'file_content'


def _get_exec_config_from_load(load: Load):
    """
    根据负载属性，按优先级获取即将执行的参数配置

    :param load: 负载实例
    :return: 待执行配置
    """
    # 如果负载不需要推荐优化配置，直接使用默认配置
    if not load.tuning_needed:
        global_logger.info("Load doesn't need tuning, running with default config.")
        return load.default_config

    # 如果负载需要推荐优化配置 根据优先级选择配置
    if load.test_config:
        # 第一使用待测试配置
        global_logger.info("Test config found, running with test config.")
        return load.test_config
    elif load.best_config:
        # 第二使用最优配置
        global_logger.info("Best config found, running with best config.")
        return load.best_config
    else:
        # 最后使用租户默认配置
        global_logger.info("Only default config found, running with default config.")
        return load.default_config


def _process_load_config(load: Load, config: dict):
    """
    1.检查当前负载中test_config是否完成复测流程
    2.根据复测情况判断是否需要清空load中的test_config
    3.根据测试性能判断是否需要刷新load中保存的best_config
    :param load: 本次测试用负载
    :param config: 本次测试用配置
    :return:
    """
    # 获取测试配置的调优结果
    tuning_result = get_tuning_result(load=load, config=config)

    if config == load.best_config:
        if tuning_result.status == OA_CONF.TuningResultStatus.fail:
            # 最优配置回滚
            tuning_result_history = get_tuning_result_history(load=load)
            tuning_result_history.refresh_best_config()
            global_logger.info('Best config of load failed too much, roll back to next best config.')
        else:
            pass

    elif config == load.test_config:
        # 测试配置执行失败
        if tuning_result.status == OA_CONF.ExecStatus.fail:
            # 清空test_config
            LoadRepository.update_test_config(load=load, test_config=OA_CONF.empty_config)
        # 测试配置执行成功
        elif tuning_result.status == OA_CONF.ExecStatus.success:
            # 清空test_config
            LoadRepository.update_test_config(load=load, test_config=OA_CONF.empty_config)
            # 更新最优配置
            tuning_result_history = get_tuning_result_history(load=load)
            tuning_result_history.refresh_best_config()
            global_logger.info(
                'Current tuning result: %s, best tuning result ever: %s, best tuning speedup: %s %%',
                float_format(tuning_result.runtime),
                float_format(tuning_result_history.best_tuning_result.runtime),
                float_format(tuning_result_history.boost_percentage)
            )
        else:
            pass

    else:
        pass


def _get_load_name_from_exec_attr(exec_attr: dict):
    """
    获取任务名，无则空
    """
    return exec_attr.get('name', '')


def _calculate_hash_value(exec_attr: dict, user_config: dict):
    """
    根据 执行参数和用户配置计算负载唯一标识，hash值
    """
    class_info = exec_attr.get('class', '')
    if 'SparkSQLCLIDriver' in class_info:
        # spark-sql，则提取 -e 和 -f，替换
        if 'e' in exec_attr:
            file_content = _remove_time(exec_attr.get('e', ''))
        else:
            file_content = exec_attr.get('f', '')
    else:
        # spark-submit，则提取 file 及之后的内容，包含 file的入参
        unknown_content = exec_attr.get('unknown', [])
        unknown_content = [item.strip() for item in unknown_content]
        if len(unknown_content) > 0:
            file_content = unknown_content[0]
        else:
            file_content = ''

    name = _get_load_name_from_exec_attr(exec_attr)
    data = {
        _NAME_STR: _remove_time(name),
        _USER_CONFIG_STR: user_config,
        _FILE_CONTENT_STR: file_content
    }
    json_str = json.dumps(data, sort_keys=True)
    hash_value = hashlib.sha256(json_str.encode()).hexdigest()
    return hash_value


def _remove_time(content: str):
    """
    移除时间信息，可精确到小时
    """
    for pattern in OA_CONF.date_patterns:
        content = re.sub(pattern, '{date}', content)
    return content.strip()


def _create_or_update_load(exec_attr: dict, default_config: dict):
    """
    无则创建，有则更新 name 和 exec_attr 字段。如果日期信息等一直保持不变，后期复测时，会导致测试出现异常
    """
    name = _get_load_name_from_exec_attr(exec_attr)
    hash_value = _calculate_hash_value(exec_attr, default_config)
    loads = LoadRepository.query_by_hash_value(hash_value)
    if loads:
        global_logger.info("Load found in database, update and ready to get config to execute.")
        return LoadRepository.update_name_and_exec_attr(loads.pop(), name, exec_attr)
    else:
        # 负载没有创建过
        load = LoadRepository.create(name, exec_attr, default_config, hash_value)
        global_logger.info("Load not found in database, created new one and execute.")
        TuningRecordRepository.create(load=load, config=default_config, method=OA_CONF.TuningMethod.user)
        return load


def hijack_recommend(argv: list):
    """
    任务劫持，使能参数下发执行任务
    :param argv: Spark执行命令字段
    """
    # 获取用户传入的Spark命令 并解析命令
    global_logger.debug("Hijack input params: %s", argv)
    exec_attr, user_config = SparkCMDParser.parse_cmd(argv=argv)

    # 查询或创建相应负载
    load = _create_or_update_load(exec_attr=exec_attr, default_config=user_config)
    # 获取待执行参数配置
    exec_config = _get_exec_config_from_load(load=load)

    # 根据配置和负载执行Spark任务
    global_logger.info("Going to execute Spark load ……")
    exam_record, output = spark_run(load=load, config=exec_config, wait_for_trace=False)
    # 执行结果分析
    if exam_record.status == OA_CONF.ExecStatus.success:
        # 打印结果输出
        global_logger.info("Spark execute success, going to print Spark output.")
        print(output, end="", flush=True)
    # 若执行失败 则判断是否需要拉起安全机制
    else:
        if exec_config != user_config:
            global_logger.warning("Spark execute failed, ready to activate security protection mechanism.")
            safe_exam_record, safe_output = spark_run(load=load, config=user_config, wait_for_trace=False)
            global_logger.info("Spark execute in security protection mechanism, going to print Spark output.")
            # 打印安全机制下任务的输出
            print(safe_output, end="", flush=True)
        else:
            global_logger.warning("Spark execute failed in user config, going to print Spark output.")
            print(output, end="", flush=True)

    if exec_config != load.default_config:
        # 创建子进程处理负载配置，不阻塞
        p = multiprocessing.Process(target=_process_load_config, args=(load, exec_config))
        p.daemon = False
        p.start()


def main():
    """
    任务劫持主函数
    """
    hijack_recommend(argv=sys.argv[1:])
