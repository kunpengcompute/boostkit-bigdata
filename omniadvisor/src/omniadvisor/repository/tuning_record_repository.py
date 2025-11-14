from typing import List
from common.constant import OA_CONF
from server.models import DatabaseLoad, DatabaseTuningRecord

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.repository import Repository


class TuningRecordRepository(Repository):
    """
    调优记录模型与数据库交互类
    """
    # 相关模型
    _model_class = DatabaseTuningRecord
    # 用于存储不允许修改的参数
    _frozen_fields = [
        TuningRecord.FieldName.id,
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        TuningRecord.FieldName.load,
        TuningRecord.FieldName.method,
        TuningRecord.FieldName.rounds
    ]
    # 用于存储字段格式
    _fields_format = {
        TuningRecord.FieldName.load: ([DatabaseLoad], [], []),
        TuningRecord.FieldName.method: ([str], [], OA_CONF.TuningMethod.all),
    }

    @classmethod
    def create(cls, load: Load, config: dict, method: str, method_extend: str = '') -> TuningRecord:
        """
        指定负载、参数配置和调优方法，新增调优记录

        :param load: 负载实例
        :param config: 参数配置
        :param method: 调优方法
        :return: 调优记录实例
        """
        # 判断是否有相同load和config的TuningRecord存在
        model_attr = {
            TuningRecord.FieldName.load: load.database_model,
            TuningRecord.FieldName.config: config,
        }
        database_records = cls._query(model_attr=model_attr)
        # 强制要求load和config组合是唯一的
        if len(database_records) != 0:
            raise RuntimeError(f'Create {cls._model_class.__name__} fail because combination of load {load.id} and '
                               f'config {config} exist.')

        # 获取调优伦次
        model_attr = {
            TuningRecord.FieldName.load: load.database_model
        }
        database_records = cls._query(model_attr=model_attr)
        tuning_rounds = len(database_records)

        # 新建调优记录
        model_attr = {
            TuningRecord.FieldName.load: load.database_model,
            TuningRecord.FieldName.config: config,
            TuningRecord.FieldName.method: method,
            TuningRecord.FieldName.rounds: tuning_rounds,
            TuningRecord.FieldName.method_extend: method_extend
        }
        database_record = cls._create(model_attr=model_attr)
        return TuningRecord(database_model=database_record)

    @classmethod
    def query_by_load(cls, load: Load) -> List[TuningRecord]:
        """
        指定负载，查询与该负载关联的全部调优记录

        :param load: 负载实例
        :return: 调优记录实例列表
        """
        model_attr = {
            TuningRecord.FieldName.load: load.database_model,
        }
        database_records = cls._query(model_attr=model_attr)
        return [
            TuningRecord(database_model=database_record) for database_record in database_records
        ]

    @classmethod
    def query_by_load_and_config(cls, load: Load, config: dict) -> List[TuningRecord]:
        """
        指定负载、参数配置，查询唯一匹配的调优记录

        :param load: 负载实例
        :param config: 参数配置
        :return: 调优记录实例列表(实际上只有一个实例，返回列表是为了维护接口的数据格式统一）
        """
        model_attr = {
            TuningRecord.FieldName.load: load.database_model,
            TuningRecord.FieldName.config: config,
        }
        database_records = cls._query(model_attr=model_attr)

        # 强制要求load和config组合是唯一的
        if len(database_records) > 1:
            raise RuntimeError(f'Find {cls._model_class.__name__} combination of load {Load.id} and config {config} '
                               f'is not unique.')

        return [
            TuningRecord(database_model=database_record) for database_record in database_records
        ]

    @classmethod
    def delete(cls, tuning_record: TuningRecord) -> None:
        """
        删除 tuning_record 记录
        :param tuning_record:调优记录实例
        :return:
        """
        return tuning_record.database_model.delete()
