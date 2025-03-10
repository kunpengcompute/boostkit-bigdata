from django_server.app.models import Task
from django_server.app.models import Load
from omniadvisor.repository.repository import Repository


class TaskAttrFiled:
    id = 'id'
    load = 'load'
    config = 'config'
    start_time = 'start_time'
    end_time = 'end_time'
    exec_status = 'exec_status'
    exec_runtime = 'exec_runtime'
    exec_trace = 'exec_trace'


class TaskRepository(Repository):
    """
    任务模型与数据库交互类
    """
    # 相关模型
    _model_class = Task
    # 用于存储不允许修改的参数
    _frozen_fields = [
        TaskAttrFiled.id,
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        TaskAttrFiled.load,
        TaskAttrFiled.config,
    ]
    # 用于存储字段格式
    _fields_format = {
        TaskAttrFiled.load: ([Load], [''], []),
        TaskAttrFiled.config: ([dict], [''], []),
        TaskAttrFiled.start_time: ([str], [''], []),
        TaskAttrFiled.end_time: ([str], [''], []),
        TaskAttrFiled.exec_status: ([str], [], ['fail', 'success']),
        TaskAttrFiled.exec_runtime: ([float], [], []),
        TaskAttrFiled.exec_trace: ([dict], [], []),
    }
