from datetime import datetime

from .common import create_table, delete_table
from common.constant import OA_CONF
from server.app.models import DatabaseLoad, DatabaseExamRecord
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.load_repository import LoadRepository
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
        create_table(DatabaseExamRecord)

    @staticmethod
    def teardown_class():
        """
        测试类结束后清理
        """
        # 删除表
        delete_table(DatabaseExamRecord)
        delete_table(DatabaseLoad)

    def setup_method(self):
        """
        测试方法开始前构建
        """
        # 清空表数据
        # 由于外键的存在，因此先删除Task
        DatabaseExamRecord.objects.all().delete()
        DatabaseLoad.objects.all().delete()

        # 构建测试数据
        self.load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={'param1': 'value2'}
        )
        self.config = {'param1': 'value2'}
        self.status = OA_CONF.ExecStatus.success
        self.runtime = 20.4
        self.start_time = datetime(2023, 10, 5, 14, 30, 15)
        self.end_time = datetime(2024, 10, 5, 14, 30, 15)
        # 创建task
        self.exam_record = ExamRecordRepository.create(
            load=self.load,
            config=self.config,
            status=self.status,
            runtime=self.runtime,
            start_time=self.start_time,
            end_time=self.end_time
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

    def test_query(self):
        """
        测试查询exam_record
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

    def test_update_start_time(self):
        """
        测试更新exam_record的任务开始时间
        """
        # 更新任务开始时间
        updated_task = ExamRecordRepository.update_start_time(exam_record=self.exam_record)
        assert updated_task.start_time != self.start_time

    def test_update_end_time(self):
        """
        测试更新exam_record的任务结束时间
        """
        # 更新任务开始时间
        updated_task = ExamRecordRepository.update_end_time(exam_record=self.exam_record)
        assert updated_task.end_time != self.end_time

    def test_update_task_result(self):
        """
        测试更新exam_record的任务历史结果
        """
        # 更新任务执行结果
        updated_task = ExamRecordRepository.update_exam_result(
            exam_record=self.exam_record,
            status=OA_CONF.ExecStatus.fail,
            runtime=10.0,
            trace={'step1': 1}
        )
        assert updated_task.database_model.status == OA_CONF.ExecStatus.fail
        assert updated_task.database_model.runtime == 10.0
        assert updated_task.database_model.trace == {'step1': 1}
