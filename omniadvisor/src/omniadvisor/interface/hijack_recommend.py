import hashlib
import json
import multiprocessing
import re
import sys
from typing import Any

from common.constant import OA_CONF
from common.exceptions import UnknownEncodingError
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import float_format, safe_read_file, file_exists

_PARAM_NAME_IN_EXEC_ATTR = 'name'
_PARAM_F_IN_EXEC_ATTR = 'f'
_PARAM_E_IN_EXEC_ATTR = 'e'
_PARAM_CLASS_IN_EXEC_ATTR = 'class'
_USER_CONFIG_STR = 'user_config'
_FILE_CONTENT_STR = 'file_content'
_EMPTY_STR = ''
_UNKNOWN_STR = 'unknown'


def _get_exec_config_from_load(load: Load) -> dict[str, str]:
    """
    根据负载属性，按优先级规则获取即将执行的参数配置

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


def _process_load_config(load: Load, config: dict) -> None:
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


def _get_load_name_from_exec_attr(exec_attr: dict) -> str:
    """
    获取负载的name参数，无则返回空字符串

    :param exec_attr: Spark任务的执行配置
    :return: 负载的name参数
    """
    return exec_attr.get('name', '')


def _calculate_hash_value(exec_attr: dict, config: dict) -> str:
    """
    根据 执行参数和用户配置计算负载唯一标识，hash值

    :param exec_attr: 负载的执行参数
    :param config: 负载的配置
    :return: 计算得出的哈希值
    """
    if _is_spark_sql_cli(exec_attr):
        # spark-sql，则提取 -e 和 -f，替换
        if _PARAM_E_IN_EXEC_ATTR in exec_attr:
            file_content = _remove_time(exec_attr.get(_PARAM_E_IN_EXEC_ATTR, _EMPTY_STR))
        else:
            file_content = exec_attr.get(_PARAM_F_IN_EXEC_ATTR, _EMPTY_STR)
    else:
        # spark-submit，则提取 file 及之后的内容，包含 file的入参
        unknown_content = exec_attr.get(_UNKNOWN_STR, [])
        unknown_content = [item.strip() for item in unknown_content]
        if len(unknown_content) > 0:
            file_content = unknown_content[0]
        else:
            file_content = _EMPTY_STR

    name = exec_attr.get(_PARAM_NAME_IN_EXEC_ATTR, _EMPTY_STR)
    data = {
        _PARAM_NAME_IN_EXEC_ATTR: _remove_time(name),
        _USER_CONFIG_STR: config,
        _FILE_CONTENT_STR: file_content
    }
    json_str = json.dumps(data, sort_keys=True)
    hash_value = hashlib.sha256(json_str.encode()).hexdigest()
    return hash_value


def _remove_time(content: str) -> str:
    """
    移除时间信息，可精确到小时

    :param content: 待移除的文本内容
    :return:
    """
    for pattern in OA_CONF.date_patterns:
        content = re.sub(pattern, OA_CONF.date_replacement_value, content)
    return content.strip()


def _is_spark_sql_cli(exec_attr: dict) -> bool:
    """
    判断是否为 spark-sql类型

    :param exec_attr: 执行参数
    :return:
    """
    class_name = exec_attr.get(_PARAM_CLASS_IN_EXEC_ATTR, _EMPTY_STR)
    return OA_CONF.spark_sql_cli_driver == class_name


def _has_modify_keywords(exec_attr: dict) -> bool:
    """
    检查是否含有修改数据库的操作

    :param exec_attr: 执行参数
    :return:
    """
    if _is_spark_sql_cli(exec_attr):
        # spark-sql，则提取 -e 和 -f，替换
        if _PARAM_E_IN_EXEC_ATTR in exec_attr:
            file_content = exec_attr.get(_PARAM_E_IN_EXEC_ATTR, _EMPTY_STR)
        else:
            # 这里也有可能是相对路径
            file_path = exec_attr.get(_PARAM_F_IN_EXEC_ATTR, _EMPTY_STR)
            exists = file_exists(file_path)
            if not exists:
                # 这里初步判断文件用相对路径的比较少，所以打warning
                global_logger.warning(
                    'File %s is not exists, for safety’s sake, it contains modify operation by default', file_path
                )
                return True
            try:
                file_content = safe_read_file(file_path)
            except (UnknownEncodingError, Exception) as e:
                # 初步判断，目前已有的编码，能覆盖，所以打warning
                global_logger.warning(
                    'An unexpected situation: %s occurred when reading file %s , for safety’s sake, it contains '
                    'modify operation by default', e, file_path
                )
                return True

        return _any_modify_keywords_in_sql(file_content)
    else:
        return True


def _create_or_update_load(exec_attr: dict, config: dict) -> Load:
    """
    根据exec_attr和config查询是否存在对应的负载，无则创建新负载并返回，有则更新 name 和 exec_attr 字段后返回该负载。
    (如果日期信息等一直保持不变，后期复测时，会导致测试出现异常)

    :param exec_attr: 负载执行参数
    :param config: 负载配置
    :return: 更新后或者新创建的负载
    """
    name = exec_attr.get(_PARAM_NAME_IN_EXEC_ATTR, _EMPTY_STR)
    hash_value = _calculate_hash_value(exec_attr, config)
    loads = LoadRepository.query_by_hash_value(hash_value)
    if loads:
        global_logger.info("Load found in database, update and ready to get config to execute.")
        return LoadRepository.update_name_and_exec_attr(loads.pop(), name, exec_attr)
    else:
        # 负载没有创建过
        backend_retest_forbidden = _has_modify_keywords(exec_attr)
        load = LoadRepository.create(name, exec_attr, config, hash_value, backend_retest_forbidden)
        global_logger.info("Load not found in database, created new one and execute.")
        TuningRecordRepository.create(load=load, config=config, method=OA_CONF.TuningMethod.user)
        return load


def _any_modify_keywords_in_sql(sql: str) -> bool:
    """
    判断在当前sql中是否存在任何的修改动作

    :param sql: sql内容
    :return:
    """
    # 正则转义关键词（虽然目前不需要，但防御性更强）
    escaped_keywords = [re.escape(k) for k in OA_CONF.modify_keywords]

    # 后缀是空格、换行、制表符、SQL常见符号 或字符串结束
    pattern = re.compile(r'(?:' + '|'.join(escaped_keywords) + r')(?=\s|[(),;]|$)', re.IGNORECASE)

    return bool(pattern.search(sql))


def hijack_recommend(argv: list) -> None:
    """
    对用户的任务进行劫持，使能参数并下发执行任务

    :param argv: Spark任务的执行命令
    :return:
    """
    # 非SUBMIT动作(指kill任务/查询状态/查询版本)的提交直接回退到原生spark-submit脚本执行 不被特性所劫持
    SparkCMDParser.validate_submit_arguments(argv)

    # 获取用户传入的Spark命令 并解析命令
    global_logger.debug("Hijack input params: %s", argv)
    exec_attr, user_config = SparkCMDParser.parse_cmd(argv=argv)

    # 查询或创建相应负载
    load = _create_or_update_load(exec_attr=exec_attr, config=user_config)
    # 获取待执行参数配置
    exec_config = _get_exec_config_from_load(load=load)

    # 根据配置和负载执行Spark任务
    global_logger.info("Going to execute Spark load ……")
    exam_record, output = spark_run(load=load, config=exec_config, wait_for_trace=False, exec_in_isolation=False)
    # 执行结果分析
    if exam_record.status == OA_CONF.ExecStatus.success:
        # 打印结果输出
        global_logger.info("Spark execute success, going to print Spark output.")
        print(output, end="", flush=True)
    # 若执行失败 则判断是否需要拉起安全机制
    else:
        if exec_config != user_config:
            raise RuntimeError("Spark execute failed, ready to activate security protection mechanism.")
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
