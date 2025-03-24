from datetime import datetime

from django.utils import timezone

from server.app.models import DatabaseLoad
from server.app.models import DatabaseTask
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.task import Task
from omniadvisor.repository.model.perf_history import PerfHistory
from omniadvisor.repository.repository import Repository
from omniadvisor.utils.constant import OA_CONF


class TaskRepository(Repository):
    """
    任务模型与数据库交互类
    """
    # 相关模型
    _model_class = DatabaseTask
    # 用于存储不允许修改的参数
    _frozen_fields = [
        Task.FieldName.id,
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        Task.FieldName.load,
        Task.FieldName.config,
    ]
    # 用于存储字段格式
    _fields_format = {
        Task.FieldName.load: ([DatabaseLoad], [''], []),
        Task.FieldName.config: ([dict], [''], []),
        Task.FieldName.start_time: ([datetime], [''], []),
        Task.FieldName.end_time: ([datetime], [''], []),
        Task.FieldName.status: ([str], [], OA_CONF.TaskStatus.all),
        Task.FieldName.runtime: ([float], [], []),
        Task.FieldName.trace: ([dict], [], []),
    }

    @classmethod
    def create(cls, load: Load, config: dict):
        """
        指定负载、配置，新增任务

        :param load: 负载
        :param config: 参数配置
        :return: 任务实例
        """
        model_attr = {
            Task.FieldName.load: load.database_model,
            Task.FieldName.config: config,
        }
        database_task = cls._create(model_attr=model_attr)
        return Task(database_model=database_task)

    @classmethod
    def query(cls, load: Load, config: dict):
        """
        指定负载、配置，查询负载
        (暂定以这两个信息进行查询，具体视现网情况而定)

        :param load: 负载
        :param config: 参数配置
        :return: 任务实例列表
        """
        model_attr = {
            Task.FieldName.load: load.database_model,
            Task.FieldName.config: config,
        }
        database_tasks = cls._query(model_attr=model_attr)
        return [
            Task(database_model=database_task) for database_task in database_tasks
        ]

    @classmethod
    def query_perf_history_from_load(cls, load: Load):
        """
        查询某个负载中，每个配置的执行结果

        :param load: 负载实例
        :return: 该负载下所有历史数据，PerfHistory
        """
        model_attr = {
            Task.FieldName.load: load.database_model
        }
        database_tasks = cls._query(model_attr=model_attr)

        # 初始化性能历史
        perf_history = PerfHistory(load=load)

        # 遍历 QuerySet，将数据添加入perf_history
        for database_task in database_tasks:
            perf_history.add(
                config=database_task.config,
                status=database_task.status,
                runtime=database_task.runtime,
                trace=database_task.trace
            )

        return perf_history

    @classmethod
    def update_start_time(cls, task: Task):
        """
        更新任务中的开始时间为此刻

        :param task: 任务实例
        :return: 更新完后的任务
        """
        model_attr = {
            Task.FieldName.start_time: timezone.now()
        }
        database_task = cls._update(
            database_model=task.database_model,
            model_attr=model_attr
        )
        return Task(database_model=database_task)

    @classmethod
    def update_end_time(cls, task: Task):
        """
        更新任务中的结束时间为此刻

        :param task: 任务实例
        :return: 更新完后的任务
        """
        model_attr = {
            Task.FieldName.end_time: timezone.now()
        }
        database_task = cls._update(
            database_model=task.database_model,
            model_attr=model_attr
        )
        return Task(database_model=database_task)

    @classmethod
    def update_task_result(cls, task: Task, status: str, runtime: float, trace: dict):
        """
        更新任务中的状态

        :param task: 任务实例
        :param status: 执行状态
        :param runtime: 执行用时
        :param trace: 执行Trace
        :return: 更新完后的任务
        """
        model_attr = {
            Task.FieldName.status: status,
            Task.FieldName.runtime: runtime,
            Task.FieldName.trace: trace
        }
        database_task = cls._update(
            database_model=task.database_model,
            model_attr=model_attr
        )
        return Task(database_model=database_task)
