from itertools import chain
from typing import List

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.tuning_result.tuning_result import TuningResult
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result

from common.constant import OA_CONF


def get_current_tuning_method(load):
    """
    根据 load，获取调优历史记录，并根据OA_CONF中的tuning_strategies决定本轮的调优方法
    :param load
    :return: 当前的调优方法
    """
    tuning_result_history = get_tuning_result_history(load)
    tuning_strategy_list = list(chain(*[[strategy[0]] * strategy[1] for strategy in OA_CONF.tuning_strategies]))
    tuning_times = len(tuning_result_history.tuning_results)
    tuning_method = tuning_strategy_list[tuning_times]
    return tuning_method


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
    def tuning_history(self):
        return list(map(lambda tuning_result: tuning_result.to_tuning(), self._tuning_results))

    @property
    def best_config(self):
        if self.tuning_history:
            return min(self.tuning_history, key=lambda x: x.runtime).config
        else:
            return {}

    @property
    def latest_tuning_record(self):
        if self._tuning_results:
            return max(self._tuning_results, key=lambda tuning_result: tuning_result.rounds)
        else:
            return None

    @property
    def tuning_results(self):
        return self._tuning_results
