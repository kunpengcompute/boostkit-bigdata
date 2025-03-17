import argparse
import json
import os.path
from typing import Tuple, List

from omniadvisor.database.repository import CommonRepository
from omniadvisor.service.config_recommendation import ConfigRecommendationFactory
from omniadvisor.service.retest_service import retest
from omniadvisor.utils.global_config import project_dir, max_failed_configs, retest_times
from omniadvisor.utils.logger import logger


def read_json_file(file_path: str):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            # 将 JSON 文件内容解析为 Python 对象
            data = json.load(file)
    except FileNotFoundError:
        logger.error(f"Error: The file '{file_path}' does not exist.")
    except json.JSONDecodeError:
        logger.error(f"Error: The file '{file_path}' is not a valid JSON file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    else:
        logger.info(data)
    return data


def load_user_config() -> dict:
    user_config_path = os.path.join(project_dir, 'config', 'user_config.json')
    return read_json_file(user_config_path)


def convert_params(args, user_config):
    # 要返回的参数：load, tuning_algorithm, silent
    algo_list = []
    algos = user_config.get('algo')
    for algo_name, tuning_times in algos.items():
        algo_list.append((algo_name, tuning_times))
    return args.load, algo_list, args.retest_way == 'backend'


def main():
    args = parse_backend_tuning_args()
    # # TODO 这里的负载，我认为是一个文件名？根据负载以及参数空间文件 -> 找到所有配置及其性能记录
    user_config = load_user_config()
    load, tuning_algorithm, if_backend = convert_params(args, user_config)
    unified_tuning(load, tuning_algorithm, if_backend)


def parse_backend_tuning_args():
    parser = argparse.ArgumentParser(
        description="Parse params that will frequently used. For other configs, change the config file.")
    parser.add_argument("-l", "--load", type=str, help='Name of the task')
    parser.add_argument("-r", "--retest-way", type=str, choices=['hijacking', 'backend'],
                        help='If choose hijacking, a test will execute immediately')

    args = parser.parse_args()
    return args


def unified_tuning(load, tuning_algorithm: List[Tuple[str, int]], if_backend: bool):
    """
    统一的后台调优入口
    """
    repo_path = os.path.join(project_dir, 'config', 'spark_repo.json')
    knob_space = read_json_file(repo_path)
    if if_backend:
        # 如果是后台测试，进入循环
        failed_times = max_failed_configs
        for algorithm_type, tuning_round in tuning_algorithm:
            # 最大失败次数
            if failed_times <= 0:
                break
            # 原始参数空间
            for _ in range(tuning_round):
                factory = ConfigRecommendationFactory(algorithm_type, load, knob_space)
                # 不同的调优算法
                config_recommendation = factory.make()
                next_config = config_recommendation.recommend()
                CommonRepository.update_untest_load(load, next_config)
                # 复测
                try:
                    retest(retest_times, load, next_config)
                except Exception:
                    failed_times -= 1
                    continue
    else:
        # TODO 这段代码可能有冗余
        # 前台测试
        algorithm_type = tuning_algorithm[0][0]
        # 获取调优方法
        factory = ConfigRecommendationFactory(algorithm_type, load, knob_space)
        config_recommendation = factory.make()
        next_config = config_recommendation.recommend()
        CommonRepository.update_untest_load(load, next_config)


if __name__ == "__main__":
    main()
