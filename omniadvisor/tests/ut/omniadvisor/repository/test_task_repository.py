import pytest

from .common import create_test_table
from .common import delete_test_table
from omniadvisor.repository.task_repository import Task
from omniadvisor.repository.task_repository import Load
from omniadvisor.repository.task_repository import TaskAttrFiled
from omniadvisor.repository.task_repository import TaskRepository

class TestLoadRepository:
    """
    LoadRepository测试类
    """
    def setup_method(self):
        """
        测试前准备，创建表
        """
        create_test_table(Load)
        create_test_table(Task)

        load = Load(name='test_load',
                    exec_master='yarn',
                    exec_queue='root.default',
                    exec_load='./test.sql',
                    default_config={'executor_num': 1, 'executor_cores': 2})
        load.save()

        self.task_attr = {
            TaskAttrFiled.load: load,
            TaskAttrFiled.config: {'executor_num': 1, 'executor_cores': 2}
        }

    @staticmethod
    def cleanup_method():
        """
        测试后清理，删除表
        """
        delete_test_table(Task)
        delete_test_table(Load)


    @pytest.mark.django_db(transaction=True)
    def test_base_method(self):
        """
        测试类中基本方法，add/search/update
        """
        # TODO: 需补充完善测试，增加异常测试
        # 测试add
        created_task_1 = TaskRepository.add(model_attr=self.task_attr)
        assert created_task_1.load == self.task_attr[TaskAttrFiled.load]
        assert created_task_1.config == self.task_attr[TaskAttrFiled.config]

        # 测试search
        search_task_attr = {TaskAttrFiled.config: self.task_attr[TaskAttrFiled.config]}
        searched_tasks_1 = TaskRepository.search(search_task_attr)
        assert len(searched_tasks_1) == 1

        # 测试search
        created_task_2 = TaskRepository.add(model_attr=self.task_attr)
        searched_tasks_2 = TaskRepository.search(search_task_attr)
        assert len(searched_tasks_2) == 2
        for searched_task in searched_tasks_2.all():
            assert searched_task.load == self.task_attr[TaskAttrFiled.load]

        # 测试update
        update_task_attr = {TaskAttrFiled.config: {'executor_num': 2, 'executor_cores': 2}}
        updated_task = TaskRepository.update(created_task_2, update_task_attr)
        assert updated_task.config == {'executor_num': 2, 'executor_cores': 2}
        searched_tasks_3 = TaskRepository.search(update_task_attr)
        assert len(searched_tasks_3) == 1
        assert searched_tasks_3.first().config == {'executor_num': 2, 'executor_cores': 2}
