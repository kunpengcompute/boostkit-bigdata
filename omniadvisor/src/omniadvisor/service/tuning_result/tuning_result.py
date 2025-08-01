from dataclasses import dataclass
from typing import List

from algo.common.model import TuningData, Trace
from django.contrib.messages import success

from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.repository.exam_record_repository import ExamRecordRepository


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


def remove_tuning_result(load: Load, config: dict):
    """
    删除给定负载和配置的执行记录
    """
    tuning_result = get_tuning_result(load, config)
    # 清测试记录
    for exam_record in tuning_result.exam_records:
        ExamRecordRepository.delete(exam_record)

    # 清调优记录
    TuningRecordRepository.delete(tuning_result.tuning_record)


@dataclass
class _ExamCount:
    total: int
    completed: int
    success: int
    failed: int


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

        self._exam_count = _ExamCount(
            total=len(self._exam_records),
            completed=sum([exam_record.status != OA_CONF.ExecStatus.running for exam_record in self._exam_records]),
            success=sum([exam_record.status == OA_CONF.ExecStatus.success for exam_record in self._exam_records]),
            failed=sum([exam_record.status == OA_CONF.ExecStatus.fail for exam_record in self._exam_records])
        )

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
    def exam_count(self):
        return self._exam_count

    @property
    def status(self):
        """
        对于 tuning result 定义四种状态
        成功：达到指定复测次数，并且失败次数没有超过阈值，这种情况下，对这个配置的复测流程已结束
        失败：失败的次数超过阈值，就失败了，对这个配置的复测流程已结束
        复测中：复测次数小于指定复测次数，且失败次数没有超过阈值，复测流程尚在进行中
        空置: 复测次数为0，即还未完成一次复测
        """
        if self.exam_count.failed >= OA_CONF.config_fail_threshold:
            return OA_CONF.TuningResultStatus.fail

        if self.exam_count.completed == 0:
            return OA_CONF.TuningResultStatus.empty
        elif self.exam_count.completed < OA_CONF.tuning_retest_times:
            return OA_CONF.TuningResultStatus.running
        else:
            return OA_CONF.TuningResultStatus.success

    @property
    def runtime(self):
        if self.status == OA_CONF.TuningResultStatus.fail:
            return OA_CONF.exec_fail_return_runtime

        # 计算执行状态为success的平均runtime
        success_runtimes = [
            exam_record.runtime
            for exam_record in self._exam_records if (
                    exam_record.status == OA_CONF.ExecStatus.success
                    and exam_record.runtime != OA_CONF.exec_fail_return_runtime
            )
        ]

        # 若配置成功复测的次数为0，则runtime为失败返回值
        if not success_runtimes:
            return OA_CONF.exec_fail_return_runtime

        return sum(success_runtimes) / len(success_runtimes)

    @property
    def trace(self):
        if self.status == OA_CONF.TuningResultStatus.fail:
            return OA_CONF.exec_fail_return_trace

        success_exam_results = [
            exam_record
            for exam_record in self._exam_records if exam_record.status == OA_CONF.ExecStatus.success
        ]

        # 若配置成功复测的次数为0，则trac为失败返回值
        if not success_exam_results:
            return OA_CONF.exec_fail_return_trace

        best_exam_record = min(success_exam_results, key=lambda exam_record: exam_record.runtime)
        return best_exam_record.trace

    @property
    def tuning_record(self):
        return self._tuning_record

    @property
    def exam_records(self):
        return self._exam_records

    def to_tuning_data(self) -> TuningData:
        """
        将TuningResult转为TuningData类型，用于调优
        :return:
        """
        if self.trace is not None:
            trace = Trace(stages_with_summaries=self.trace.get('stages'), sql=self.trace.get('sql'))
        else:
            trace = Trace(stages_with_summaries=None, sql=None)

        return TuningData(
            round=self.rounds,
            config=self.config,
            method=self.method,
            rule=self.method_extend,
            status=self.status,
            runtime=self.runtime,
            trace=trace
        )
