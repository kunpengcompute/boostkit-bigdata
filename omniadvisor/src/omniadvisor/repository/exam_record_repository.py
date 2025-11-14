from datetime import datetime
from typing import List, Tuple
from common.constant import OA_CONF
from server.models import (
    DatabaseTuningRecord,
    DatabaseExamRecord
)
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.repository import Repository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository


class ExamRecordRepository(Repository):
    """
    测试记录模型与数据库交互类
    """
    # 相关模型
    _model_class = DatabaseExamRecord
    # 用于存储不允许修改的参数
    _frozen_fields = [
        ExamRecord.FieldName.id,
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        ExamRecord.FieldName.tuning_record,
    ]
    # 用于存储字段格式
    _fields_format = {
        ExamRecord.FieldName.tuning_record: ([DatabaseTuningRecord], [''], []),
        ExamRecord.FieldName.start_time: ([datetime], [''], []),
        ExamRecord.FieldName.end_time: ([datetime], [''], []),
        ExamRecord.FieldName.status: ([str], [], OA_CONF.ExecStatus.all),
        ExamRecord.FieldName.runtime: ([float], [], []),
        ExamRecord.FieldName.application_id: ([str], [], []),
        ExamRecord.FieldName.sql_trace: ([str], [], []),
        ExamRecord.FieldName.stage_trace: ([str], [], []),
    }

    @classmethod
    def create(cls, load: Load, config: dict, status: str, runtime: float,
               start_time: datetime, end_time: datetime, application_id: str) -> ExamRecord:
        """
        指定exam_record属性，新增测试记录

        :param load: 负载
        :param config: 参数配置
        :param status: 执行状态
        :param runtime: 执行用时
        :param start_time: 任务开始时间
        :param end_time: 任务结束时间
        :param application_id: Spark任务application_id
        :return: 测试记录实例
        """
        tuning_records = TuningRecordRepository.query_by_load_and_config(load=load, config=config)
        if not tuning_records:
            raise RuntimeError('Create exam_record failed because of not finding tuning_record.')

        tuning_record = tuning_records.pop()
        model_attr = {
            ExamRecord.FieldName.tuning_record: tuning_record.database_model,
            ExamRecord.FieldName.status: status,
            ExamRecord.FieldName.runtime: runtime,
            ExamRecord.FieldName.start_time: start_time,
            ExamRecord.FieldName.end_time: end_time,
            ExamRecord.FieldName.application_id: application_id
        }
        database_record = cls._create(model_attr=model_attr)
        return ExamRecord(database_model=database_record)

    @classmethod
    def query_by_tuning_record(cls, tuning_record: TuningRecord) -> List[ExamRecord]:
        """
        指定调优记录，查询与该调优记录所关联的全部测试记录，并以列表的形式返回

        :param tuning_record: 调优记录实例
        :return: 测试记录实例列表
        """
        model_attr = {
            ExamRecord.FieldName.tuning_record: tuning_record.database_model,
        }
        database_records = cls._query(model_attr=model_attr)
        return [
            ExamRecord(database_model=database_record) for database_record in database_records
        ]

    @classmethod
    def query_by_load_and_config(cls, load: Load, config: dict) -> List[ExamRecord]:
        """
        指定(负载,配置)，查询与该(负载,配置)关联的全部测试记录，并以列表形式返回

        :param load: 负载
        :param config: 参数配置
        :return: 测试记录实例列表
        """
        tuning_records = TuningRecordRepository.query_by_load_and_config(load=load, config=config)
        if not tuning_records:
            return list()

        tuning_record = tuning_records.pop()
        return cls.query_by_tuning_record(tuning_record=tuning_record)

    @classmethod
    def update_trace(cls, exam_record: ExamRecord, sql_trace: str, stage_trace: str) -> ExamRecord:
        """
        更新测试记录中的trace信息

        :param exam_record: 测试记录实例
        :param sql_trace: 执行SQL Trace
        :param stage_trace: 执行Stage Trace
        :return: 更新完后的测试记录
        """
        model_attr = {
            ExamRecord.FieldName.sql_trace: sql_trace,
            ExamRecord.FieldName.stage_trace: stage_trace
        }
        database_task = cls._update(
            database_model=exam_record.database_model,
            model_attr=model_attr
        )

        return ExamRecord(database_model=database_task)

    @classmethod
    def update_runtime(cls, exam_record: ExamRecord, runtime: float) -> ExamRecord:
        """
        更新从jobs中获取的runtime信息

        :param exam_record: 测试记录实例
        :param runtime: 从Jobs信息中计算出的runtime值
        :return: 更新完后的测试记录
        """
        model_attr = {
            ExamRecord.FieldName.runtime: runtime
        }
        database_task = cls._update(
            database_model=exam_record.database_model,
            model_attr=model_attr
        )

        return ExamRecord(database_model=database_task)

    @classmethod
    def delete(cls, exam_record: ExamRecord) -> Tuple[int, dict]:
        """
        从数据库删除指定的测试记录

        :param exam_record: 测试记录实例
        :return:
        """
        return exam_record.database_model.delete()
