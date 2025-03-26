from server.app.models import DatabaseTask
from omniadvisor.repository.model.load import Load


class Task:
    """
    任务
    """
    def __init__(self, database_model: DatabaseTask):
        """
        初始化，接收任务在数据库中的模型

        :param database_model: 数据库任务实例
        """
        self._database_model = database_model

    @property
    def id(self):
        return self._database_model.id

    @property
    def load(self):
        return Load(database_model=self._database_model.load)

    @property
    def config(self):
        return self._database_model.config

    @property
    def start_time(self):
        return self._database_model.start_time

    @property
    def end_time(self):
        return self._database_model.end_time

    @property
    def status(self):
        return self._database_model.status

    @property
    def runtime(self):
        return self._database_model.runtime

    @property
    def trace(self):
        return self._database_model.trace

    @property
    def database_model(self):
        return self._database_model

    class FieldName:
        id = 'id'
        load = 'load'
        config = 'config'
        start_time = 'start_time'
        end_time = 'end_time'
        status = 'status'
        runtime = 'runtime'
        trace = 'trace'
