from server.app.models import DatabaseTuningRecord
from omniadvisor.repository.load_repository import Load


class TuningRecord:
    """
    调优记录
    """
    def __init__(self, database_model: DatabaseTuningRecord):
        """
        初始化，接收调优记录在数据库中的模型

        :param database_model: 数据库调优记录实例
        """
        self._database_model = database_model
        self._load = Load(database_model=self._database_model.load)

    @property
    def id(self):
        return self._database_model.id

    @property
    def load(self):
        return self._load

    @property
    def config(self):
        return self._database_model.config

    @property
    def method(self):
        return self._database_model.method

    @property
    def method_extend(self):
        return self._database_model.method_extend

    @property
    def rounds(self):
        return self._database_model.rounds

    @property
    def database_model(self):
        return self._database_model

    class FieldName:
        id = 'id'
        load = 'load'
        config = 'config'
        method = 'method'
        method_extend = 'method_extend'
        rounds = 'rounds'