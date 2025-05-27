import argparse

from algo.expert.tuning import ExpertTuning
from algo.iterative.tuning import SmacAppendTuning
from algo.native.tuning import NativeTuning
from algo.transfer.tuning import TransferTuning
from common.constant import OA_CONF

from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.retest_service import retest
from omniadvisor.service.tuning_result.tuning_result import remove_tuning_result
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history, \
    get_other_tuning_result_history

from omniadvisor.utils.logger import global_logger


# 异常类暂时用于当前模块，没必要放到公共里
class NoOptimalConfigError(Exception):
    """仅用于调优模块的异常"""
    pass


def _parse_tuning_args():
    """
    解析用户直命令行输入的参数
    :return:
    """
    parser = argparse.ArgumentParser(
        description="Parse params that will frequently used. For other configs, change the config file.")
    parser.add_argument("-l", "--load-id", type=str, required=True, help='Id of the load.')

    parser.add_argument("-r", "--retest-way", type=str, choices=OA_CONF.RetestWay.all,
                        required=True,
                        help='If choose hijacking, a test will execute immediately')

    parser.add_argument("-t", "--tuning-method", type=str, required=False,
                        choices=[method for method in OA_CONF.TuningMethod.all if method != OA_CONF.TuningMethod.user],
                        help='Tuning methods used to give optimal config, for iterative, use BO; for expert ...')

    args = parser.parse_args()
    return args


def unified_tuning(load, retest_way: str, tuning_method: str):
    """
    后台调优
    :param load: 负载
    :param retest_way: 复测方法
    :param tuning_method: 算法的类型
    :return:
    """
    if not load:
        raise ValueError("Invalid load: configuration cannot be loaded")

    method_extend, next_config = _get_next_config(load, tuning_method)

    if not next_config:
        raise NoOptimalConfigError('The recommending config is empty, please try other tuning methods.')

    # 用户的default_config上叠加next_config叠加
    next_config = {**load.default_config, **next_config}

    TuningRecordRepository.create(load=load, config=next_config, method=tuning_method, method_extend=method_extend)

    # 复测
    if retest_way == OA_CONF.RetestWay.backend:
        try:
            retest(load, next_config)
        except Exception:
            # 如果之前发生错误，状态被置为running，此调优配置不可能再被复测，那么删除对应的exam_records、tuning_record
            global_logger.info('Retest process did not complete normally. Please check the detailed logs for more '
                               'information. Remove tuning results.')
            remove_tuning_result(load, next_config)
            raise
        tuning_result_history = get_tuning_result_history(load)
        # 更新最优配置
        if tuning_result_history.best_config and tuning_result_history.best_config != next_config:
            LoadRepository.update_best_config(load, tuning_result_history.best_config)
    else:
        # 更新待测试配置即可
        LoadRepository.update_test_config(load, next_config)


def _get_next_config(load, tuning_method):
    """
    获取下一个配置
    :param load: 负载
    :param tuning_method: 调优方法
    :return:
    """
    if tuning_method == OA_CONF.TuningMethod.iterative:
        tuning_result_history = get_tuning_result_history(load)
        next_config, method_extend = SmacAppendTuning.tune(tuning_result_history.tuning_history)
    elif tuning_method == OA_CONF.TuningMethod.expert:
        tuning_result_history = get_tuning_result_history(load)
        next_config, method_extend = ExpertTuning.tune(tuning_result_history.tuning_history)
    elif tuning_method == OA_CONF.TuningMethod.native:
        tuning_result_history = get_tuning_result_history(load)
        next_config, method_extend = NativeTuning.tune(tuning_result_history.tuning_history)
    elif tuning_method == OA_CONF.TuningMethod.transfer:
        tuning_result_history = get_tuning_result_history(load)
        other_history = get_other_tuning_result_history(load)
        next_config, method_extend = TransferTuning.tune(
            tuning_result_history.tuning_history, [other.tuning_history for other in other_history]
        )
    else:
        raise ValueError(f'Not supported tuning method: {tuning_method}')
    return method_extend, next_config


def _pre_check(load_id):
    """
    前置检查
    :param load_id:
    :return:
    """
    # 查询负载：
    loads = LoadRepository.query_by_id(load_id)
    if not loads:
        # 若查询结果为空，直接返回即可
        global_logger.info('Cannot find load id: %s in database, the tuning process exits.', load_id)
        return None
    load = loads.pop()
    if not load.tuning_needed:
        global_logger.info('The load dont need to tune, the tuning process exits.')
        return None
    return load


def main():
    """
    配置调优主函数
    """
    args = _parse_tuning_args()

    # 前台复测需要指定调优方法，否则
    if args.retest_way == OA_CONF.RetestWay.hijacking and not args.tuning_method:
        global_logger.info('When using retest way \'hijacking\', param --tuning-method is needed.')
        return

    # 检查load 是否存在以及是否需要调优
    load = _pre_check(args.load_id)
    if not load:
        return

    # 查询上一次调优的所推送的配置是否完成复测流程 如果满足以下所有条件，则提示无法调优并退出：
    # 1. 存在最近的调优结果
    # 2. 调优方法不是用户配置(用户配置无需复测)
    # 3. 调优结果状态仍在运行中(复测中)
    tuning_result_history = get_tuning_result_history(load)
    if (tuning_result_history.latest_tuning_result
            and tuning_result_history.latest_tuning_result.tuning_record.method != OA_CONF.TuningMethod.user
            and tuning_result_history.latest_tuning_result.status == OA_CONF.ExecStatus.running):
        global_logger.info('The retest status of latest tuning result is still running, please wait.')
        return

    if args.tuning_method or args.retest_way == OA_CONF.RetestWay.hijacking:
        unified_tuning(load, args.retest_way, args.tuning_method)
        return

    # 当且仅当后台复测，且未指定调优方法时，会连续拉起调优
    for tuning_strategy in OA_CONF.tuning_strategies:
        tuning_method = tuning_strategy[0]
        tuning_times = tuning_strategy[1]
        for _ in range(tuning_times):
            try:
                unified_tuning(load, args.retest_way, tuning_method)
            except NoOptimalConfigError:
                global_logger.info('The tuning method: %s cannot find optimal config, trying next.', tuning_method)
                break
