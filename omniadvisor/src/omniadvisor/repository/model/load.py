from django_server.app.models import DatabaseLoad


class Load:
    """
    负载
    """
    def __init__(self, database_model: DatabaseLoad):
        """
        初始化，接收负载在数据库中的模型

        :param database_model: 数据库负载实例
        """
        self._database_model = database_model

    @property
    def id(self):
        return self._database_model.id

    @property
    def name(self):
        return self._database_model.name

    @property
    def exec_attr(self):
        return self._database_model.exec_attr

    @property
    def default_config(self):
        return self._database_model.default_config

    @property
    def best_config(self):
        return self._database_model.best_config

    @property
    def test_config(self):
        return self._database_model.test_config

    @property
    def create_time(self):
        return self._database_model.create_time

    @property
    def is_need_tunning(self):
        return self._database_model.is_need_tunning

    @property
    def database_model(self):
        return self._database_model

    class FieldName:
        id = 'id'
        name = 'name'
        exec_attr = 'exec_attr'
        default_config = 'default_config'
        best_config = 'best_config'
        test_config = 'test_config'
        create_time = 'create_time'
        is_need_tunning = 'is_need_tunning'
