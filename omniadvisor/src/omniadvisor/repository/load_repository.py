from django.db import IntegrityError

from common.exceptions import DuplicateEntryError
from server.app.models import DatabaseLoad
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.repository import Repository


class LoadRepository(Repository):
    """
    负载模型与数据库交互类
    """
    # 相关模型
    _model_class = DatabaseLoad
    # 用于存储不允许修改的参数
    _frozen_fields = [
        Load.FieldName.id,
        Load.FieldName.create_time
    ]
    # 用于存储必须要填写的参数
    _required_fields = [
        Load.FieldName.name,
        Load.FieldName.exec_attr,
        Load.FieldName.default_config
    ]
    # 用于存储字段格式
    _fields_format = {
        Load.FieldName.name: ([str], [], []),
        Load.FieldName.exec_attr: ([dict], [dict()], []),
        Load.FieldName.default_config: ([dict], [], []),
        Load.FieldName.best_config: ([dict], [], []),
        Load.FieldName.test_config: ([dict], [], []),
    }

    @classmethod
    def create(cls, name: str, exec_attr: dict, default_config: dict, hash_value: str, backend_retest_forbidden: bool):
        """
        指定名称、执行属性和默认配置，新增负载

        :param name: 负载名称
        :param exec_attr: 负载执行属性
        :param default_config: 负载默认配置
        :param hash_value: hash值
        :param backend_retest_forbidden: 是否禁用后台复测
        :return: 负载实例
        """
        model_attr = {
            Load.FieldName.name: name,
            Load.FieldName.exec_attr: exec_attr,
            Load.FieldName.default_config: default_config,
            Load.FieldName.hash_value: hash_value,
            Load.FieldName.backend_retest_forbidden: backend_retest_forbidden
        }
        try:
            database_load = cls._create(model_attr=model_attr)
        except IntegrityError as e:
            raise DuplicateEntryError(
                f'Create {cls._model_class.__name__} fail because hash_value {hash_value} exist.'
            ) from e
        return Load(database_model=database_load)

    @classmethod
    def query_by_id(cls, load_id: str) -> list:
        """
        根据 load_id 查询 load
        :param load_id: 负载id
        :return: 负载列表
        """
        model_attr = {
            Load.FieldName.id: load_id,
        }
        database_loads = cls._query(model_attr=model_attr)

        return [
            Load(database_model=database_load) for database_load in database_loads
        ]

    @classmethod
    def query_by_hash_value(cls, hash_value: str):
        """
        根据hash值，查找负载实例
        :param hash_value: 负载实例
        :return: 负载实例
        """
        model_attr = {
            Load.FieldName.hash_value: hash_value
        }
        database_loads = cls._query(model_attr=model_attr)
        return [
            Load(database_model=database_load) for database_load in database_loads
        ]

    @classmethod
    def update_name_and_exec_attr(cls, load: Load, name: str, exec_attr: dict):
        """
        更新和时间相关的信息
        :param load: 负载
        :param name: 负载名称
        :param exec_attr: 负载执行属性
        :return: 负载实例
        """
        model_attr = {
            Load.FieldName.name: name,
            Load.FieldName.exec_attr: exec_attr
        }
        database_load = cls._update(load.database_model, model_attr=model_attr)
        return Load(database_model=database_load)

    @classmethod
    def update_best_config(cls, load: Load, best_config: dict):
        """
        更新负载的最优配置

        :param load: 负载实例
        :param best_config: 最优配置
        :return: 更新完后的负载实例
        """
        model_attr = {
            Load.FieldName.best_config: best_config
        }
        database_load = cls._update(
            database_model=load.database_model,
            model_attr=model_attr
        )
        return Load(database_model=database_load)

    @classmethod
    def update_test_config(cls, load: Load, test_config: dict):
        """
        更新负载的测试配置

        :param load: 负载实例
        :param test_config: 测试配置
        :return: 更新完后的负载实例
        """
        model_attr = {
            Load.FieldName.test_config: test_config
        }
        database_load = cls._update(
            database_model=load.database_model,
            model_attr=model_attr
        )
        return Load(database_model=database_load)

    @classmethod
    def update_tuning_needed(cls, load: Load, tuning_needed: bool):
        """
        更新当前负载是否需要调优这一信息
        :param load: 负载实例
        :param tuning_needed: 是否需要调优 布尔型变量
        :return: 更新完之后的负载实例
        """
        model_attr = {
            Load.FieldName.tuning_needed: tuning_needed
        }
        database_load = cls._update(
            database_model=load.database_model,
            model_attr=model_attr
        )
        return Load(database_model=database_load)
