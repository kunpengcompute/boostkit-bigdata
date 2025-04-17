from typing import List

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.tuning_result.tuning_result import TuningResult
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from common.constant import OA_CONF


def get_tuning_result_history(load: Load):
    """
    指定负载，获取调优结果所有历史

    :param load: 负载实例
    :return: 调优结果历史实例
    """
    tuning_records = TuningRecordRepository.query_by_load(load=load)

    tuning_results = list()
    for tuning_record in tuning_records:
        tuning_result = get_tuning_result(
            load=load,
            config=tuning_record.config
        )
        tuning_results.append(tuning_result)

    return TuningResultHistory(
        load=load,
        tuning_results=tuning_results
    )


class TuningResultHistory:
    """
    调优结果历史，汇总相同负载下所有配置对应的调优结果
    """
    def __init__(self, load: Load, tuning_results: List[TuningResult]):
        """
        初始化，负载对应下的所有性能历史数据

        :param load: 负载实例
        :param tuning_results: 调优结果实例列表
        """
        self._load = load
        self._tuning_results = tuning_results

    @property
    def config_perf_pairs(self):
        return [
            (tuning_result.config, tuning_result.runtime)
            for tuning_result in self._tuning_results if tuning_result.status == OA_CONF.ExecStatus.success
        ]

    @property
    def best_config(self):
        if self.config_perf_pairs:
            return min(self.config_perf_pairs, key=lambda x: x[1])[0]
        else:
            return {}
