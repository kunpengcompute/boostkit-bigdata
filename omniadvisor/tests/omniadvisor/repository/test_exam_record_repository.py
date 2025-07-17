import pytest
from datetime import datetime

from .common import create_table, delete_table
from common.constant import OA_CONF
from server.app.models import (
    DatabaseLoad,
    DatabaseTuningRecord,
    DatabaseExamRecord
)
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository
from omniadvisor.repository.exam_record_repository import ExamRecordRepository


class TestExamRecordRepository:
    """
    ExamRecordRepository测试类
    """

    @staticmethod
    def setup_class():
        """
        测试类开始前构建
        """
        # 创建表
        create_table(DatabaseLoad)
        create_table(DatabaseTuningRecord)
        create_table(DatabaseExamRecord)

    @staticmethod
    def teardown_class():
        """
        测试类结束后清理
        """
        # 删除表
        delete_table(DatabaseExamRecord)
        delete_table(DatabaseTuningRecord)
        delete_table(DatabaseLoad)

    def setup_method(self):
        """
        测试方法开始前构建
        """
        # 清空表数据
        # 由于外键的存在，因此删除顺序如下
        DatabaseExamRecord.objects.all().delete()
        DatabaseTuningRecord.objects.all().delete()
        DatabaseLoad.objects.all().delete()

        # 构建测试数据
        self.load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={'param1': 'value2'}
        )
        self.config = {'param1': 'value2'}
        self.tuning_record = TuningRecordRepository.create(
            load=self.load,
            config=self.config,
            method=OA_CONF.TuningMethod.user
        )
        self.status = OA_CONF.ExecStatus.success
        self.runtime = 20.4
        self.start_time = datetime(2023, 10, 5, 14, 30, 15)
        self.end_time = datetime(2024, 10, 5, 14, 30, 15)
        self.app_id = 'application_123456'
        # 创建task
        self.exam_record = ExamRecordRepository.create(
            load=self.load,
            config=self.config,
            status=self.status,
            runtime=self.runtime,
            start_time=self.start_time,
            end_time=self.end_time,
            app_id=self.app_id
        )

    def test_create(self):
        """
        测试创建exam_record
        """
        assert isinstance(self.exam_record, ExamRecord)
        # 由于外层的Load没有透传到最内层，而Task返回的load为新生成的Load
        # 因此导致这里两边的load不一致，但是属性是一致的
        assert self.exam_record.load.name == self.load.name
        assert self.exam_record.config == self.config
        assert self.exam_record.status == self.status
        assert self.exam_record.runtime == self.runtime
        assert self.exam_record.start_time == self.start_time
        assert self.exam_record.end_time == self.end_time
        assert self.exam_record.app_id == self.app_id

        # 测试没有tuning_record场景
        with pytest.raises(RuntimeError):
            self.exam_record = ExamRecordRepository.create(
                load=self.load,
                config={"param2": "value2"},
                status=self.status,
                runtime=self.runtime,
                start_time=self.start_time,
                end_time=self.end_time,
                app_id=self.app_id
            )

    def test_query_by_tuning_record(self):
        """
        测试 query_by_tuning_record 方法
        """
        # 查询任务
        exam_records = ExamRecordRepository.query_by_tuning_record(
            tuning_record=self.tuning_record,
        )
        assert len(exam_records) == 1
        assert exam_records[0].tuning_record.load.name == self.load.name
        assert exam_records[0].tuning_record.config == self.config

    def test_query_by_load_and_config(self):
        """
        测试 query_by_load_and_config 方法
        """
        # 查询任务
        exam_records = ExamRecordRepository.query_by_load_and_config(
            load=self.load,
            config=self.config
        )
        assert len(exam_records) == 1
        assert exam_records[0].load.name == self.load.name
        assert exam_records[0].config == self.config

        # 查询失败
        exam_records = ExamRecordRepository.query_by_load_and_config(
            load=self.load,
            config={"param2": "value2"}
        )
        assert len(exam_records) == 0

    def test_update_trace(self):
        """
        测试 update_trace 方法
        """
        # 更新任务执行结果
        updated_task = ExamRecordRepository.update_trace(
            exam_record=self.exam_record,
            trace={'step1': 1}
        )
        assert updated_task.database_model.trace == {'step1': 1}
