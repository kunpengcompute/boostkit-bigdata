from datetime import datetime

from django.utils import timezone

from server.app.models import DatabaseLoad, DatabaseExamRecord
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.repository import Repository
from common.constant import OA_CONF


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
        ExamRecord.FieldName.load,
        ExamRecord.FieldName.config,
    ]
    # 用于存储字段格式
    _fields_format = {
        ExamRecord.FieldName.load: ([DatabaseLoad], [''], []),
        ExamRecord.FieldName.config: ([dict], [''], []),
        ExamRecord.FieldName.start_time: ([datetime], [''], []),
        ExamRecord.FieldName.end_time: ([datetime], [''], []),
        ExamRecord.FieldName.status: ([str], [], OA_CONF.ExecStatus.all),
        ExamRecord.FieldName.runtime: ([float], [], []),
        ExamRecord.FieldName.trace: ([dict], [], []),
    }

    @classmethod
    def create(cls, load: Load, config: dict):
        """
        指定负载、配置，新增测试记录

        :param load: 负载
        :param config: 参数配置
        :return: 测试记录实例
        """
        model_attr = {
            ExamRecord.FieldName.load: load.database_model,
            ExamRecord.FieldName.config: config,
        }
        database_record = cls._create(model_attr=model_attr)
        return ExamRecord(database_model=database_record)

    @classmethod
    def query_by_load_and_config(cls, load: Load, config: dict):
        """
        指定负载、配置，查询测试记录
        (暂定以这两个信息进行查询，具体视现网情况而定)

        :param load: 负载
        :param config: 参数配置
        :return: 测试记录实例列表
        """
        model_attr = {
            ExamRecord.FieldName.load: load.database_model,
            ExamRecord.FieldName.config: config,
        }
        database_records = cls._query(model_attr=model_attr)
        return [
            ExamRecord(database_model=database_record) for database_record in database_records
        ]

    @classmethod
    def update_start_time(cls, exam_record: ExamRecord):
        """
        更新测试记录中的开始时间为此刻

        :param exam_record: 测试记录实例
        :return: 更新完后的测试记录
        """
        model_attr = {
            ExamRecord.FieldName.start_time: timezone.now()
        }
        database_task = cls._update(
            database_model=exam_record.database_model,
            model_attr=model_attr
        )
        return ExamRecord(database_model=database_task)

    @classmethod
    def update_end_time(cls, exam_record: ExamRecord):
        """
        更新测试记录中的结束时间为此刻

        :param exam_record: 测试记录实例
        :return: 更新完后的测试记录
        """
        model_attr = {
            ExamRecord.FieldName.end_time: timezone.now()
        }
        database_task = cls._update(
            database_model=exam_record.database_model,
            model_attr=model_attr
        )
        return ExamRecord(database_model=database_task)

    @classmethod
    def update_exam_result(cls, exam_record: ExamRecord, status: str = None, runtime: float = None,
                           trace: dict = None, output: str = None):
        """
        更新测试记录中的状态

        :param exam_record: 测试记录实例
        :param status: 执行状态
        :param runtime: 执行用时
        :param trace: 执行Trace
        :param output: 完整输出
        :return: 更新完后的测试记录
        """
        # 对于入参为None的参数 使用exam_record本身自带的值
        if not status:
            status = exam_record.status
        if not runtime:
            runtime = exam_record.runtime
        if not trace:
            trace = exam_record.trace
        if not output:
            output = exam_record.output

        model_attr = {
            ExamRecord.FieldName.status: status,
            ExamRecord.FieldName.runtime: runtime,
            ExamRecord.FieldName.trace: trace
        }
        database_task = cls._update(
            database_model=exam_record.database_model,
            model_attr=model_attr
        )
        return ExamRecord(database_model=database_task, output=output)
