import argparse

from common.constant import OA_CONF
from omniadvisor.utils.logger import logger
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history
from omniadvisor.service.retest_service import retest
from omniadvisor.utils.utils import load_config_space


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

    parser.add_argument("-t", "--tuner-type", type=str, required=True,
                        choices=OA_CONF.TunerMethod.all,
                        help='Support three tune type, for iterative, use BO; for expert ...')

    args = parser.parse_args()
    return args


def unified_tuning(load_id: str, retest_way: str, tuner_type: str):
    """
    后台调优
    :param load_id: 负载id
    :param retest_way: 复测方法
    :param tuner_type: 算法的类型
    :return:
    """
    # 加载参数空间
    config_space = load_config_space()
    # 创建负载：
    loads = LoadRepository.query_by_id(load_id)
    if not loads:
        # 若查询结果为空，直接返回即可
        logger.info('Cannot find load id: %s in database.', load_id)
        return
    load = loads[0]

    # TODO 这里先用参数空间采样代替，后期替换为bin目录下的具体算法
    configuration = config_space.sample_configuration(1)
    next_config = dict(configuration)
    # 推荐下一个配置
    # 复测
    if retest_way == OA_CONF.RetestWay.backend:
        retest(load, next_config)
        perf_history = get_tuning_result_history(load)
        # 更新最优配置
        LoadRepository.update_best_config(load, perf_history.best_config)
    else:
        # 更新待测试配置即可
        LoadRepository.update_test_config(load, next_config)


def main():
    args = _parse_tuning_args()
    unified_tuning(args.load_id, args.retest_way, args.tuner_type)


