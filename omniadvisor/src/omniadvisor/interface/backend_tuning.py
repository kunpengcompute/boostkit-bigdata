import argparse
import json
import os.path
from typing import Tuple, List

from omniadvisor.database.repository import CommonRepository
from omniadvisor.service import spark_service
from omniadvisor.service.config_recommendation import ConfigRecommendationFactory
from omniadvisor.service.retest_service import retest
from omniadvisor.utils.global_config import project_dir, max_failed_configs, retest_times
from omniadvisor.utils.logger import logger


def read_knob_space():
    repo_path = os.path.join(project_dir, 'config', 'spark_repo.json')
    try:
        with open(repo_path, "r", encoding="utf-8") as file:
            # 将 JSON 文件内容解析为 Python 对象
            data = json.load(file)
    except FileNotFoundError:
        logger.error(f"Error: The file '{repo_path}' does not exist.")
    except json.JSONDecodeError:
        logger.error(f"Error: The file '{repo_path}' is not a valid JSON file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    else:
        logger.info(data)
    return data


def calculate_id(load_content) -> int:
    # TODO 这里先用hash粗略计算
    return hash(load_content)


def main():
    # args = parse_backend_tuning_args()
    # # TODO 这里的负载，我认为是一个文件名？根据负载以及参数空间文件 -> 找到所有配置及其性能记录
    unified_tuning('test_20250101_task', [('iterative', 3)], silent=True)


# def parse_backend_tuning_args():
#     parser = argparse.ArgumentParser(description="self define")
#     parser.add_argument("-w", "--workload", type=str, help="负载")
#     parser.add_argument("-a", "--algorithm", type=str, choices=['iterative', 'expert', 'transfer'], help="算法")
#     # parser.add_argument("-c", "--count", type=int, help="调优次数")
#     # parser.add_argument("-u", "--up", action="store_true", help="调优方向")
#     # parser.add_argument("-k", "--knob-space", type=str, help="参数空间")
#     parser.add_argument("-s", "--silent-retest", action="store_true", help="静默复测")
#     # 解析命令行参数
#     args = parser.parse_args()
#     return args


def unified_tuning(load: Load, tuning_algorithm: List[Tuple[str, int]], silent: bool):
    """
    统一的后台调优入口
    TODO load 里面应该包含用户配置？
    """
    knob_space = read_knob_space()
    if silent:
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
