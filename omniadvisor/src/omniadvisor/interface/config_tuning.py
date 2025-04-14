import argparse

import django
from django.conf import settings

from common.constant import OA_CONF
from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from server.engine.settings import DATABASES
from omniadvisor.utils.logger import logger

# 没有配置时，才启用正式的数据库；测试绕过此通路
if not settings.configured:
    settings.configure(
        INSTALLED_APPS=['server'],
        DATABASES=DATABASES
    )
    django.setup()
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history

from omniadvisor.service.retest_service import retest
from algo.iterative.tuning import SmacAppendTuning

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

    parser.add_argument("-rt", "--retest-times", type=int, required=False, default=3,
                        help='Effective when retest way is backend.')

    parser.add_argument("-t", "--tuner-type", type=str, required=True,
                        choices=OA_CONF.TunerMethod.all,
                        help='Support three tune type, for iterative, use BO; for expert ...')

    args = parser.parse_args()
    return args


def unified_tuning(load_id: str, retest_times: int, retest_way: str, tuner_method: str):
    """
    后台调优
    :param load_id: 负载id
    :param retest_times: 复测次数
    :param retest_way: 复测方法
    :param tuner_method: 算法的类型
    :return:
    """
    # 加载参数空间
    # 创建负载：
    loads = LoadRepository.query_by_id(load_id)
    if not loads:
        # 若查询结果为空，直接返回即可
        logger.info('Cannot find load id: %s in database.', load_id)
        return
    load = loads[0]

    if tuner_method == OA_CONF.TunerMethod.iterative:
        smac_tuning = SmacAppendTuning()
        perf_history = get_tuning_result_history(load)
        next_config = smac_tuning.tune(perf_history.config_perf_pairs)
    else:
        raise ValueError(f'Not supported tuning method: {tuner_method}')
    # 推荐下一个配置
    # 复测
    if retest_way == OA_CONF.RetestWay.backend:
        retest(retest_times, load, next_config)
        perf_history = get_tuning_result_history(load)
        # 更新最优配置
        LoadRepository.update_best_config(load, perf_history.best_config)
    else:
        # 更新待测试配置即可
        LoadRepository.update_test_config(load, next_config)


def main():
    args = _parse_tuning_args()
    unified_tuning(args.load_id, args.retest_times, args.retest_way, args.tuner_type)


