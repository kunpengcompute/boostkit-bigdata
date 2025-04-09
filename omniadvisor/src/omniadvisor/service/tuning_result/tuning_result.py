from typing import List

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from common.constant import OA_CONF


def get_tuning_result(load: Load, config: dict):
    """
    指定负载、参数配置，获取调优结果

    :param load: 负载实例
    :param config: 参数配置
    :return: 调优记录实例
    """
    # 查询获得调优记录
    tuning_records = TuningRecordRepository.query_by_load_and_config(
        load=load,
        config=config
    )
    tuning_record = tuning_records.pop()

    # 查询获得测试记录
    exam_records = ExamRecordRepository.query_by_load_and_config(
        load=load,
        config=config
    )

    return TuningResult(
        tuning_record=tuning_record,
        exam_records=exam_records
    )


class TuningResult:
    """
    调优结果，汇总相同负载和配置下的调优记录和测试记录
    """
    def __init__(
        self,
        tuning_record: TuningRecord,
        exam_records: List[ExamRecord]
    ):
        """
        初始化，负载对应下的所有性能历史数据

        :param tuning_record: 调优记录
        :param exam_records: 测试记录
        """
        self._tuning_record = tuning_record
        self._exam_records = exam_records

    @property
    def config(self):
        return self._tuning_record.config

    @property
    def method(self):
        return self._tuning_record.method

    @property
    def method_extend(self):
        return self._tuning_record.method_extend

    @property
    def rounds(self):
        return self._tuning_record.rounds

    @property
    def exam_times(self):
        return len(self._exam_records)

    @property
    def status(self):
        # 若配置未复测过，则默认该配置暂时为失败
        if not self._exam_records:
            return OA_CONF.ExecStatus.fail

        failed_times = sum([exam_record.status==OA_CONF.ExecStatus.fail for exam_record in self._exam_records])
        # 如果该配置执行失败过，则认为配置不可行
        if failed_times >= OA_CONF.config_fail_threshold:
            return OA_CONF.ExecStatus.fail
        else:
            return OA_CONF.ExecStatus.success

    @property
    def runtime(self):
        # 若配置未复测过，则runtime为失败返回值
        if not self._exam_records:
            return OA_CONF.exec_fail_return_value

        # 计算执行状态为success的平均runtime
        success_runtimes = [
            exam_record.runtime
            for exam_record in self._exam_records if exam_record.status == OA_CONF.ExecStatus.success
        ]
        return sum(success_runtimes) / len(success_runtimes)

    @property
    def trace(self):
        # 若配置未复测过，则trace为失败返回值
        if not self._exam_records:
            return OA_CONF.exec_fail_return_trace

        success_exam_results = [
            exam_record
            for exam_record in self._exam_records if exam_record.status == OA_CONF.ExecStatus.success
        ]
        best_exam_record = min(success_exam_results, key=lambda exam_record: exam_record.runtime)
        return best_exam_record.trace
