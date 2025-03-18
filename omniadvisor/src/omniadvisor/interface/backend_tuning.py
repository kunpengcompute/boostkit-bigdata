import argparse
import os.path

import django
from django.conf import settings

from django_server.engine.settings import DATABASES
from omniadvisor.utils.logger import logger

# 没有配置时，才启用正式的数据库；测试绕过此通路
if not settings.configured:
    settings.configure(
        INSTALLED_APPS=['django_server'],
        DATABASES=DATABASES
    )
    django.setup()
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.service.config_recommendation import ConfigRecommendationFactory
from omniadvisor.service.retest_service import retest
from omniadvisor.utils.global_config import project_dir, retest_times
from omniadvisor.utils.utils import read_json_file


def _load_user_config() -> dict:
    """
    从文件中加载用户配置，默认在
    @return:
    """
    user_config_path = os.path.join(project_dir, 'config', 'user_config.json')
    return read_json_file(user_config_path)


def _update_to_args(args, user_config):
    """
    更新 user_config 至 args，以args作为统一传参
    @param args:
    @param user_config:
    @return:
    """
    algo_list = []
    algos = user_config.get('algo')
    for algo_name, tuning_times in algos.items():
        algo_list.append((algo_name, tuning_times))

    args.if_backend = args.retest_way == 'backend'
    args.algo_list = algo_list
    args.max_failed_configs = user_config.get('max_failed_configs')
    args.retest_times = user_config.get('retest_times')


def main():
    args = _parse_backend_tuning_args()
    user_config = _load_user_config()
    _update_to_args(args, user_config)
    unified_tuning(args)


def _parse_backend_tuning_args():
    """
    解析用户直命令行输入的参数
    @return:
    """
    parser = argparse.ArgumentParser(
        description="Parse params that will frequently used. For other configs, change the config file.")
    parser.add_argument("-l", "--load-id", type=str, required=True, help='Id of the load')
    parser.add_argument("-r", "--retest-way", type=str, choices=['hijacking', 'backend'], required=True,
                        help='If choose hijacking, a test will execute immediately')

    args = parser.parse_args()
    return args


def unified_tuning(args):
    """
    统一的调优入口
    @param args: 整合后的用户入参
    @return: None
    """
    repo_path = os.path.join(project_dir, 'config', 'spark_repo.json')
    knob_space = read_json_file(repo_path)
    if args.if_backend:
        # 进入后台测试
        backend_tuning(args, knob_space)
    else:
        # 前台测试
        hijacking_tuning(args, knob_space)


def hijacking_tuning(args, knob_space):
    """
    劫持调优
    @param args:
    @param knob_space:
    @return:
    """
    algorithm_type = args.algo_list[0][0]
    # 获取调优方法
    factory = ConfigRecommendationFactory(algorithm_type, args.load, knob_space)
    config_recommendation = factory.make()
    next_config = config_recommendation.recommend()
    LoadRepository.update_test_config(args.load, next_config)


def backend_tuning(args, knob_space):
    """
    后台调优
    @param args:
    @param knob_space:
    @return:
    """
    failed_times = args.max_failed_configs
    # 创建负载：
    load = LoadRepository.query_by_id(args.load_id)
    for algorithm_type, tuning_round in args.algo_list:
        # 最大失败次数
        if failed_times <= 0:
            break
        # 每一种算法，只创建一次
        factory = ConfigRecommendationFactory(algorithm_type, load, knob_space)
        # 创建不同的调优算法类
        config_recommendation = factory.make()
        # 一些算法需要初始化
        config_recommendation.initialize()
        # 每一个tuning_round，都推荐一个配置
        for _ in range(tuning_round):
            next_config = config_recommendation.recommend()
            LoadRepository.update_test_config(load, next_config)
            # 复测
            try:
                average_performance = retest(retest_times, args.load, next_config)
            # TODO 完善这里的异常
            except Exception:
                failed_times -= 1
                continue
            config_recommendation.feedback(average_performance)


if __name__ == "__main__":
    main()
    logger.info('测试完成')
