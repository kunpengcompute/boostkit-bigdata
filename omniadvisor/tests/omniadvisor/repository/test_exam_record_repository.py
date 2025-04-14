from .common import create_table, delete_table
from common.constant import OA_CONF
from server.app.models import DatabaseLoad, DatabaseExamRecord
from omniadvisor.repository.model.exam_record import ExamRecord
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.exam_record_repository import ExamRecordRepository


class TestTaskRepository:
    """
    TaskRepository测试类
    """

    @staticmethod
    def setup_class():
        # 创建表
        create_table(DatabaseLoad)
        create_table(DatabaseExamRecord)

    @staticmethod
    def teardown_class():
        # 删除表
        delete_table(DatabaseExamRecord)
        delete_table(DatabaseLoad)

    @staticmethod
    def setup_method():
        # 清空表数据
        # 由于外键的存在，因此先删除Task
        DatabaseExamRecord.objects.all().delete()
        DatabaseLoad.objects.all().delete()

    def test_create(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 创建task
        task = ExamRecordRepository.create(
            load=load,
            config={"param1": "value2"}
        )
        assert isinstance(task, ExamRecord)
        # 由于外层的Load没有透传到最内层，而Task返回的load为新生成的Load
        # 因此导致这里两边的load不一致，但是属性是一致的
        assert task.load.name == load.name
        assert task.config == {"param1": "value2"}

    def test_query(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = ExamRecordRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 查询任务
        tasks = ExamRecordRepository.query_by_load_and_config(
            load=load,
            config={"param1": "value2"}
        )
        assert len(tasks) == 1
        assert tasks[0].load.name == load.name
        assert tasks[0].config == {"param1": "value2"}

        # 查询失败
        tasks = ExamRecordRepository.query_by_load_and_config(
            load=load,
            config={"param2": "value2"}
        )
        assert len(tasks) == 0

    def test_update_start_time(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = ExamRecordRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务开始时间
        updated_task = ExamRecordRepository.update_start_time(exam_record=task)
        assert updated_task.start_time is not None

    def test_update_end_time(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = ExamRecordRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务开始时间
        updated_task = ExamRecordRepository.update_end_time(exam_record=task)
        assert updated_task.end_time is not None

    def test_update_task_result(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = ExamRecordRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务执行结果
        updated_task = ExamRecordRepository.update_exam_result(
            exam_record=task,
            status=OA_CONF.ExecStatus.success,
            runtime=10.0,
            trace={'step1': 1}
        )
        assert updated_task.database_model.status == OA_CONF.ExecStatus.success
        assert updated_task.database_model.runtime == 10.0
        assert updated_task.database_model.trace == {'step1': 1}
