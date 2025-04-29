from typing import List

from algo.common.model import Tuning
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
        self._exam_records = exam_records if exam_records is not None else []

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
        """
        对于tuning result 定义三种状态
        成功：达到指定复测次数，并且失败次数没有超过阈值，这种情况下，对这个配置的复测流程已结束
        失败：失败的次数超过阈值，就失败了，对这个配置的复测流程已结束
        复测中：复测次数小于指定复测次数，且失败次数没有超过阈值，复测流程尚在进行中
        :return:
        """
        failed_times = sum([exam_record.status == OA_CONF.ExecStatus.fail for exam_record in self._exam_records])
        if failed_times >= OA_CONF.config_fail_threshold:
            overall_status = OA_CONF.ExecStatus.fail
        elif len(self._exam_records) == OA_CONF.tuning_retest_times:
            overall_status = OA_CONF.ExecStatus.success
        else:
            overall_status = OA_CONF.ExecStatus.running
        return overall_status

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

    def to_tuning(self) -> Tuning:
        return Tuning(
            round=self.rounds,
            config=self.config,
            method=self.method,
            rule='',
            status=self.status,
            runtime=self.runtime,
            trace=self.trace
        )
