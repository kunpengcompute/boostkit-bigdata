from django_server.app.models import Load
from omniadvisor.repository.repository import Repository


class LoadAttrFiled:
    id = 'id'
    name = 'name'
    exec_load = 'exec_load'
    default_config = 'default_config'
    best_config = 'best_config'
    test_config = 'test_config'
    create_time = 'create_time'


class LoadRepository(Repository):
    """
    负载模型与数据库交互类
    """
    # 相关模型
    _model_class = Load
    # 用于存储不允许修改的参数
    _frozen_fields = [
        LoadAttrFiled.id,
        LoadAttrFiled.create_time
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        LoadAttrFiled.name,
        LoadAttrFiled.exec_load,
        LoadAttrFiled.default_config
    ]
    # 用于存储字段格式
    _fields_format = {
        LoadAttrFiled.name: ([str], [''], []),
        LoadAttrFiled.exec_load: ([dict], [dict()], []),
        LoadAttrFiled.default_config: ([dict], [], []),
        LoadAttrFiled.best_config: ([dict], [], []),
        LoadAttrFiled.test_config: ([dict], [], []),
    }
