import pytest

from .common import create_table
from .common import delete_table
from omniadvisor.utils.constant import OA_CONF
from server.app.models import DatabaseLoad
from server.app.models import DatabaseTask
from omniadvisor.repository.model.task import Task
from omniadvisor.repository.model.perf_history import PerfHistory
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.task_repository import TaskRepository


class TestTaskRepository:
    """
    TaskRepository测试类
    """

    @staticmethod
    def setup_class():
        # 创建表
        create_table(DatabaseLoad)
        create_table(DatabaseTask)

    @staticmethod
    def teardown_class():
        # 删除表
        delete_table(DatabaseTask)
        delete_table(DatabaseLoad)

    @staticmethod
    def setup_method():
        # 清空表数据
        # 由于外键的存在，因此先删除Task
        DatabaseTask.objects.all().delete()
        DatabaseLoad.objects.all().delete()

    def test_create(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 创建task
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )
        assert isinstance(task, Task)
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
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 查询任务
        tasks = TaskRepository.query(
            load=load,
            config={"param1": "value2"}
        )
        assert len(tasks) == 1
        assert tasks[0].load.name == load.name
        assert tasks[0].config == {"param1": "value2"}

        # 查询失败
        tasks = TaskRepository.query(
            load=load,
            config={"param2": "value2"}
        )
        assert len(tasks) == 0

    def test_query_perf_history_from_load(self):
        """
        测试 TaskRepository.query_perf_history_from_load 方法
        """
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )
        task.database_model.status = OA_CONF.TaskStatus.success
        task.database_model.runtime = 10.0
        task.database_model.trace = {}
        task.database_model.save()

        # 查询性能历史
        perf_history = TaskRepository.query_perf_history_from_load(load=load)
        assert isinstance(perf_history, PerfHistory)
        assert len(perf_history.config_perf_pairs) == 1

    def test_update_start_time(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务开始时间
        updated_task = TaskRepository.update_start_time(task=task)
        assert updated_task.start_time is not None

    def test_update_end_time(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务开始时间
        updated_task = TaskRepository.update_end_time(task=task)
        assert updated_task.end_time is not None

    def test_update_task_result(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        task = TaskRepository.create(
            load=load,
            config={"param1": "value2"}
        )

        # 更新任务执行结果
        updated_task = TaskRepository.update_task_result(
            task=task,
            status=OA_CONF.TaskStatus.success,
            runtime=10.0,
            trace={'step1': 1}
        )
        assert updated_task.database_model.status == OA_CONF.TaskStatus.success
        assert updated_task.database_model.runtime == 10.0
        assert updated_task.database_model.trace == {'step1': 1}
