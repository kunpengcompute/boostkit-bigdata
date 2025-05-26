import sys

from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.utils.logger import global_logger
from omniadvisor.service.spark_service.spark_run import spark_run

from common.constant import OA_CONF


def _query_or_create_load(name: str, exec_attr: dict, default_config: dict):
    """
    根据名称和默认配置，查询或创建负载

    :param name: 负载名称
    :param exec_attr: 执行属性
    :param default_config: 默认参数配置
    :return: 负载实例
    """
    # 从负载数据库中查询是否有相关联的Load
    loads = LoadRepository.query_by_name_and_default_config(name=name, default_config=default_config)
    # 如果查询不到负载信息，则创建新的负载
    if not loads:
        global_logger.info("Not exit available load, create new one.")
        load = LoadRepository.create(name=name, exec_attr=exec_attr, default_config=default_config)
        TuningRecordRepository.create(load=load, config=default_config, method=OA_CONF.TuningMethod.user)
    else:
        load = loads.pop()

    return load


def _get_exec_config_from_load(load: Load):
    """
    根据负载属性，按优先级获取即将执行的参数配置

    :param load: 负载实例
    :return: 待执行配置
    """
    # 如果负载不需要推荐优化配置，直接使用默认配置
    if not load.tuning_needed:
        return load.default_config

    # 如果负载需要推荐优化配置 根据优先级选择配置
    if load.test_config:
        # 第一使用待测试配置
        return load.test_config
    elif load.best_config:
        # 第二使用最优配置
        return load.best_config
    else:
        # 最后使用租户默认配置
        return load.default_config


def _process_load_config(load: Load):
    """
    1.检查当前负载中test_config是否完成复测流程
    2.根据复测情况判断是否需要清空load中的test_config
    3.根据测试性能判断是否需要刷新load中保存的best_config
    :param load: 本次测试用负载
    :return:
    """
    tuning_result = get_tuning_result(load, load.test_config)
    if tuning_result.status == OA_CONF.ExecStatus.fail:
        LoadRepository.update_test_config(load, {})
    elif tuning_result.status == OA_CONF.ExecStatus.success:
        # 获取当前best_config的平均测试性能
        best_config_results = get_tuning_result(load, load.best_config)
        # 若调优性能优于最佳性能 刷新当前的best_config
        if tuning_result.runtime < best_config_results.runtime:
            boost_percentage = round(
                (best_config_results.runtime - tuning_result.runtime) / best_config_results.runtime, 2)
            global_logger.info(f"tuning_runtime={tuning_result.runtime} is "
                               f"quicker than best_runtime={best_config_results.runtime}, "
                               f"boost percentage = {boost_percentage}"
                               f"found a better spark config")
            LoadRepository.update_best_config(load, load.test_config)
        # 当前test_config达到调优轮次后 置空test_config
        LoadRepository.update_test_config(load, {})


def hijack_recommend(argv: list):
    """
    任务劫持，使能参数下发执行任务

    :param argv: Spark执行命令字段
    """
    # 获取用户传入的Spark命令 并解析命令
    exec_attr, user_config = SparkCMDParser.parse_cmd(argv=argv)
    # 提取任务名字
    if 'name' not in exec_attr.keys():
        raise ValueError(f'Task name not in Spark submit cmd!')
    name = exec_attr['name']

    # 查询或创建相应负载
    load = _query_or_create_load(name=name, exec_attr=exec_attr, default_config=user_config)
    # 获取待执行参数配置
    exec_config = _get_exec_config_from_load(load=load)

    # 根据配置和负载执行Spark任务
    exam_record, output = spark_run(load, exec_config)
    # 执行结果分析 若执行失败则调度用户默认配置重新拉起任务
    if exam_record.status != OA_CONF.ExecStatus.success and exec_config != user_config:
        safe_exam_record, safe_output = spark_run(load, user_config)
        # 打印安全机制下任务的输出
        print(safe_output)
    else:
        # 打印结果输出
        print(output)

    if exec_config == load.test_config:
        _process_load_config(load=load)


def main():
    """
    任务劫持主函数
    """
    hijack_recommend(argv=sys.argv[1:])
