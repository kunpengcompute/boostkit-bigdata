import argparse

import django
from django.conf import settings

from django_server.engine.settings import DATABASES
from omniadvisor.utils.constant import TunerType, RetestWay
from omniadvisor.utils.logger import logger

# 没有配置时，才启用正式的数据库；测试绕过此通路
if not settings.configured:
    settings.configure(
        INSTALLED_APPS=['django_server'],
        DATABASES=DATABASES
    )
    django.setup()
from omniadvisor.service.tuner import TunerFactory
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.task_repository import TaskRepository

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

    parser.add_argument("-r", "--retest-way", type=str, choices=RetestWay.all,
                        required=True,
                        help='If choose hijacking, a test will execute immediately')

    parser.add_argument("-rt", "--retest-times", type=int, required=False, default=3,
                        help='Effective when retest way is backend.')

    parser.add_argument("-t", "--tuner-type", type=str, required=True,
                        choices=TunerType.all,
                        help='Support three tune type, for iterative, use BO; for expert ...')

    args = parser.parse_args()
    return args


def unified_tuning(load_id: str, retest_times: int, retest_way: str, tuner_type: str):
    """
    后台调优
    :param load_id: 负载id
    :param retest_times: 复测次数
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

    # 每一种算法，只创建一次
    factory = TunerFactory(tuner_type, load, config_space)
    # 创建不同的调优算法类
    tunner = factory.make()
    # 一些算法需要初始化
    tunner.initialize()
    # 推荐下一个配置
    next_config = tunner.tune()
    # 复测
    if retest_way == RetestWay.backend:
        retest(retest_times, load, next_config)
        perf_history = TaskRepository.query_perf_history_from_load(load)
        # 更新最优配置
        LoadRepository.update_best_config(load, perf_history.best_config)
    else:
        # 更新待测试配置即可
        LoadRepository.update_test_config(load, next_config)


def main():
    args = _parse_tuning_args()
    unified_tuning(args.load_id, args.retest_times, args.retest_way, args.tuner_type)


if __name__ == "__main__":
    main()
