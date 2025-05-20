import argparse

from algo.expert.tuning import ExpertTuning
from algo.iterative.tuning import SmacAppendTuning
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.retest_service import retest
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history, \
    get_next_tuning_method
from omniadvisor.utils.logger import global_logger

from common.constant import OA_CONF


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
                        choices=OA_CONF.TuningMethod.all,
                        help='Support three tune method, for iterative, use BO; for expert ...')

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

    # 推荐下一个配置
    if tuning_method == OA_CONF.TuningMethod.iterative:
        perf_history = get_tuning_result_history(load)
        next_config = SmacAppendTuning.tune(perf_history.tuning_history)
    elif tuning_method == OA_CONF.TuneMethod.expert:
        perf_history = get_tuning_result_history(load)
        next_config = ExpertTuning.tune(perf_history.tuning_history)
        TuningRecordRepository.create(load=load, config=next_config, method=OA_CONF.TuningMethod.expert)
    else:
        raise ValueError(f'Not supported tuning method: {tuning_method}')

    # 用户的default_config上叠加next_config叠加
    if load:
        next_config = {**load.default_config, **next_config}
    else:
        raise ValueError("Invalid load: configuration cannot be loaded")

    TuningRecordRepository.create(load=load, config=next_config, method=tuning_method)

    # 复测
    if retest_way == OA_CONF.RetestWay.backend:
        retest(load, next_config)
        perf_history = get_tuning_result_history(load)
        # 更新最优配置
        if perf_history.best_config:
            LoadRepository.update_best_config(load, perf_history.best_config)
    else:
        # 更新待测试配置即可
        LoadRepository.update_test_config(load, next_config)


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
    # 检查load 是否存在以及是否需要调优
    load = _pre_check(args.load_id)
    if not load:
        return

    # 查询上一次调优的结果是否测试完毕：
    tuning_result_history = get_tuning_result_history(load)
    # 如果上一次的调优结果所使用的配置是用户默认配置 则无需调优 直接将状态设置为success
    if tuning_result_history.latest_tuning_result.config == load.default_config:
        tuning_result_history.latest_tuning_result.status = OA_CONF.ExecStatu.success
    if (tuning_result_history.latest_tuning_result and tuning_result_history.latest_tuning_result.status ==
            OA_CONF.ExecStatus.running):
        # 有上一次调优，且调优的结果仍在复测中，提示无法调优并退出
        global_logger.info('The retest status of latest tuning result is still running, please wait.')
        return

    if args.tuning_method:
        # 用户输入了强制调优的请求，以用户为准
        unified_tuning(load, args.retest_way, args.tuning_method)
    else:
        tuning_method = get_next_tuning_method(args.load_id)
        if not tuning_method:
            global_logger.info(
                'The tuning times reaches default settings, if you want to append tuning times, add \'-t\' in your '
                'command'
            )
            return
        unified_tuning(args.load_id, args.retest_way, tuning_method)
