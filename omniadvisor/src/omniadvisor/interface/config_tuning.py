import argparse
import signal

from algo.expert.tuning import ExpertTuning
from algo.iterative.tuning import SmacAppendTuning
from algo.native.tuning import NativeTuning
from algo.transfer.tuning import TransferTuning
from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.retest_service import retest
from omniadvisor.service.tuning_result.tuning_result import (
    get_tuning_result,
    remove_tuning_result
)
from omniadvisor.service.tuning_result.tuning_result_history import (
    get_tuning_result_history,
    get_other_tuning_result_history
)
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import float_format


# 异常类暂时用于当前模块，没必要放到公共里
class NoOptimalConfigError(Exception):
    """仅用于调优模块的异常"""
    pass


def handler():
    """
    用于注册异常退出信号
    """
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
    signal.signal(signal.SIGTERM, handler)

    method_extend, next_config = _get_next_config(load, tuning_method)

    if not next_config:
        raise NoOptimalConfigError(f'The recommending config of method {tuning_method} is empty, please try other tuning methods.')

    # 用户的default_config上叠加next_config叠加
    global_logger.info("Load config tuning success, get new config to retest.")
    next_config = {**load.default_config, **next_config}

    # 判断是否有重复推荐配置
    if TuningRecordRepository.query_by_load_and_config(load=load, config=next_config):
        raise NoOptimalConfigError(f'The recommending config of method {tuning_method} already exists, please try other tuning methods.')

    TuningRecordRepository.create(load=load, config=next_config, method=tuning_method, method_extend=method_extend)

    # 复测
    if retest_way == OA_CONF.RetestWay.backend:
        global_logger.info("The way of retest is backend, going to retest the config from tuning……")
        try:
            retest(load, next_config)
        except (Exception, KeyboardInterrupt, SystemExit):
            # 如果之前发生错误，状态被置为running，此调优配置不可能再被复测，那么删除对应的exam_records、tuning_record
            global_logger.info('Retest process did not complete normally. Please check the detailed logs for more '
                               'information. Remove tuning results.')
            remove_tuning_result(load, next_config)
            raise
        tuning_result = get_tuning_result(load=load, config=next_config)
        tuning_result_history = get_tuning_result_history(load=load)
        # 更新最优配置
        tuning_result_history.refresh_best_config()
        global_logger.info(
            'Current tuning result: %s, best tuning result ever: %s, best tuning speedup: %s %%',
            float_format(tuning_result.runtime),
            float_format(tuning_result_history.best_tuning_result.runtime),
            float_format(tuning_result_history.boost_percentage)
        )

    else:
        # 更新待测试配置即可
        global_logger.info(f"The way of retest is hijacking, update the config from tuning to test config.")
        LoadRepository.update_test_config(load, next_config)


def _get_next_config(load: Load, tuning_method: str):
    """
    调优推荐下一个配置

    :param load: 负载
    :param tuning_method: 调优方法
    :return:
    """
    # 获取指定负载的调优历史记录
    tuning_result_history = get_tuning_result_history(load)

    # AI迭代调优
    if tuning_method == OA_CONF.TuningMethod.iterative:
        global_logger.info("Use AI iterative optimization method to tuning.")
        tuning = SmacAppendTuning(tuning_history=tuning_result_history.tuning_history)
    # 专家规则调优
    elif tuning_method == OA_CONF.TuningMethod.expert:
        global_logger.info("Use expert rule optimization method to tuning.")
        tuning = ExpertTuning(tuning_history=tuning_result_history.tuning_history)
    # Native特性使能
    elif tuning_method == OA_CONF.TuningMethod.native:
        global_logger.info("Use native operator acceleration feature to tuning.")
        tuning = NativeTuning(tuning_history=tuning_result_history.tuning_history)
    # 迁移泛化调优
    elif tuning_method == OA_CONF.TuningMethod.transfer:
        global_logger.info("Use migration generalization optimization method to tuning.")
        other_history = get_other_tuning_result_history(load)
        tuning = TransferTuning(
            tuning_history=tuning_result_history.tuning_history,
            other_histories=[other.tuning_history for other in other_history]
        )
    else:
        raise ValueError(f'Not supported tuning method: {tuning_method}')

    # 调优
    next_config, method_extend = tuning.tune()
    global_logger.debug(f"The method_extend is: {method_extend}, config from tuning is: {next_config}")
    return method_extend, next_config


def _query_and_check_load(load_id):
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

    # 检查load 是否存在以及是否需要调优
    load = _query_and_check_load(args.load_id)
    if not load:
        return

    # 前台复测
    # 1. 必须指定调优方法
    # 2. 若负载存在test_config，则说明有配置在前台调优流程中，退出
    if args.retest_way == OA_CONF.RetestWay.hijacking:
        if not args.tuning_method:
            global_logger.info('When using retest way \'hijacking\', param --tuning-method is needed.')
            return
        if load.test_config:
            global_logger.info('There is a config in hijacking retest, please try another load.')
            return
        unified_tuning(load=load, retest_way=args.retest_way, tuning_method=args.tuning_method)

    # 后台复测
    # 1. 若指定调优方法，则执行一轮调优；若不指定，则进入连续调优策略
    # 2. 不做调优冲突判断，若推荐相同配置，则退出
    elif args.retest_way == OA_CONF.RetestWay.backend:
        if args.tuning_method:
            unified_tuning(load=load, retest_way=args.retest_way, tuning_method=args.tuning_method)
        else:
            for tuning_strategy in OA_CONF.tuning_strategies:
                tuning_method = tuning_strategy[0]
                tuning_times = tuning_strategy[1]
                for _ in range(tuning_times):
                    try:
                        unified_tuning(load, args.retest_way, tuning_method)
                    except NoOptimalConfigError as e:
                        global_logger.warning(e)
                        global_logger.info('No config found in the loop of tuning strategy, trying next method.')
                        break
