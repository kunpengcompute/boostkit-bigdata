from server.app.models import DatabaseExamRecord
from omniadvisor.repository.model.load import Load


class ExamRecord:
    """
    测试记录
    """

    def __init__(self, database_model: DatabaseExamRecord, output=""):
        """
        初始化，接收测试记录在数据库中的模型

        :param database_model: 数据库测试记录实例
        """
        self._database_model = database_model
        self._output = output

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

    @property
    def output(self):
        return self._output

    class FieldName:
        id = 'id'
        load = 'load'
        config = 'config'
        start_time = 'start_time'
        end_time = 'end_time'
        status = 'status'
        runtime = 'runtime'
        trace = 'trace'
