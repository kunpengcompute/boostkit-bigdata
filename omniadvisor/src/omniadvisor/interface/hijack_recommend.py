import sys

from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.spark_service.spark_parameter_parser import SparkParameterParser
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
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
        # TODO 如果用户在这里创建了一个格式异常的config, 会引起spark执行失败 这个应该如何处理
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
        tuning_records = TuningRecordRepository.query_by_load_and_config(load, load.test_config)
        # 如果不存在调优记录 则针对当前的(负载，配置)组合创建新的调优记录
        if not tuning_records:
            # TODO 这个地方的method信息我获取不到 这个信息应该是tuning组件传过来的
            tuning_records = TuningRecordRepository.create(load, load.test_config, method="Iterative")
        else:
            tuning_records = tuning_records.pop()

        # 当前(负载，配置)组合调优轮次不足，优先推荐该配置的组合进行前台复测
        if tuning_records.rounds < OA_CONF.tuning_retest_times:
            # 对rounds进行 + 1
            TuningRecordRepository.rounds_increment_by_one(tuning_records)
            # TODO 在这里对round进行+1可能会导致一个问题 就是这条推荐失败之后 还是会有一条调优记录 这个怎么处理
            return load.test_config
        # 当前(负载，配置)组合的调优轮次已经达到了OA_CONF.tuning_retest_times
        else:
            # 比较test_config 和 best_config的性能测试 决定是否刷新load的best_config
            test_config_bp = ExamRecordRepository.query_best_performance_by_load_and_config(load, load.test_config)
            best_config_bp = ExamRecordRepository.query_best_performance_by_load_and_config(load, load.best_config)
            if test_config_bp.runtime < best_config_bp.runtime:
                LoadRepository.update_best_config(load,load.test_config)
            # 清空当前load的test_config
            LoadRepository.update_test_config(load, {})
    elif load.best_config:
        # 第二使用最优配置
        return load.best_config
    else:
        # 最后使用租户默认配置
        return load.default_config


def hijack_recommend(spark_sql_cmd: str):
    """
    任务劫持，使能参数下发执行任务

    :param spark_sql_cmd: Spark执行命令
    """
    # 获取用户传入的Spark命令 并解析命令
    spark_parameter_parser = SparkParameterParser(spark_sql_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()

    # 提取名字 执行参数 租户默认配置
    name = parsed_parameter.name
    exec_attr = parsed_parameter.exec_attr
    user_config = parsed_parameter.conf_params

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


def main():
    """
    任务劫持主函数
    """
    # 仅支持输入一条SQL命令，例如："spark-sql --database test -f 1.sql"
    if len(sys.argv) != 2:
        raise ValueError(f"Excepted 1 arguments but receive {len(sys.argv) - 1}")

    hijack_recommend(spark_sql_cmd=sys.argv[1])
