from typing import List

from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from omniadvisor.repository.load_prefetch_repository import LoadPrefetchRepository
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.service.tuning_result.tuning_result import TuningResult
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result


def get_other_tuning_result_history(exclude_load: Load):
    """
    查询 exclude_load 以外的所有 tuning result history
    :param exclude_load
    :return:
    """
    # 预取相关TuningRecord和ExamRecord
    result = []
    load_tuning_records_pairs = LoadPrefetchRepository.exclude_query_load_with_tuning_records(exclude_load)
    for load, tuning_records in load_tuning_records_pairs:
        # 获取该Load下的所有TuningRecord
        tuning_results = []
        for tuning_record in tuning_records:
            # 查询匹配的ExamRecord
            exam_records = ExamRecordRepository.query_by_load_and_config(load, tuning_record.config)
            tuning_results.append(
                TuningResult(
                    tuning_record=tuning_record,
                    exam_records=exam_records
                )
            )

        result.append(TuningResultHistory(
            load=load,
            tuning_results=tuning_results
        ))

    return result


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
    def latest_tuning_result(self):
        """
        获取最近的一次调优记录
        :return:
        """
        if self._tuning_results:
            return max(self._tuning_results, key=lambda tuning_result: tuning_result.rounds)
        else:
            return None

    @property
    def collect_method_calls(self) -> dict:
        """
        收集历史数据中 调优方法-次数 信息
        :return:
        """
        data = {}
        for tuning_result in self._tuning_results:
            if tuning_result.method in data:
                data[tuning_result.method] = data.get(tuning_result.method) + 1
            else:
                data[tuning_result.method] = 1
        return data
