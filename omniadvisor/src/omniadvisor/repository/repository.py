from abc import ABC
from abc import abstractmethod

from django.db.models import Model


class Repository(ABC):
    """
    数据库交互类基类
    """
    # 对应模型
    @property
    @abstractmethod
    def _model_class(self):
        """
        相关模型
        """
        pass

    @property
    @abstractmethod
    def _frozen_fields(self):
        """
        不允许修改的字段，形式：[字段]
        """
        pass

    @property
    @abstractmethod
    def _required_fields(self):
        """
        必须要填写的字段，形式：[字段]
        """
        pass

    @property
    @abstractmethod
    def _fields_format(self):
        """
        字段格式，形式：{字段：([类型], [禁止的值], [允许的值])}
        """
        pass

    @classmethod
    def _check_frozen_field(cls, model_attr: dict):
        """
        检查是否存在不允许修改的字段

        :param model_attr: Model属性字典
        """
        for field in cls._frozen_fields:
            if field in model_attr.keys():
                raise ValueError(f'The {cls._model_class.__name__} field {field} can not be changed.')

    @classmethod
    def _check_required_field(cls, model_attr: dict):
        """
        检查是否缺少必须填写的字段

        :param model_attr: Model属性字典
        """
        for field in cls._required_fields:
            if field not in model_attr.keys():
                raise ValueError(f'The {cls._model_class.__name__} field {field} is required.')

    @classmethod
    def _check_field_format(cls, model_attr: dict):
        """
        检查字段格式是否合理

        :param model_attr: Model属性字典
        """
        for field, field_format in cls._fields_format.items():
            if field not in model_attr.keys():
                continue

            field_types, field_forbidden, field_allow = field_format
            if not isinstance(model_attr[field], tuple(field_types)):
                raise TypeError(f'Type of {cls._model_class.__name__} field {field} must in {field_types}.')
            if model_attr[field] in field_forbidden:
                raise ValueError(f'Value of {cls._model_class.__name__} field {field} cannot be in {field_forbidden}.')
            if field_allow and model_attr[field] not in field_allow:
                raise ValueError(f'Value of {cls._model_class.__name__} field {field} must be in {field_allow}.')

    @classmethod
    def _create(cls, model_attr: dict):
        """
        添加Model模型至数据库

        :param model_attr: Model属性字典
        :return: Model实例
        """
        cls._check_frozen_field(model_attr)
        cls._check_required_field(model_attr)
        cls._check_field_format(model_attr)

        database_model = cls._model_class.objects.create(**model_attr)
        return database_model

    @classmethod
    def _query(cls, model_attr: dict):
        """
        根据Model属性字典，搜索符合要求的实例

        :param model_attr: Model属性字典
        :return: 多个Model实例组成的QuerySet
        """
        cls._check_field_format(model_attr)

        database_models = cls._model_class.objects.filter(**model_attr)
        return database_models

    @classmethod
    def _update(cls, database_model: Model, model_attr: dict):
        """
        更新Model模型至数据库

        :param database_model: Model实例
        :param model_attr: Model属性字典
        :return: Model实例
        """
        cls._check_frozen_field(model_attr)
        cls._check_field_format(model_attr)

        for key, value in model_attr.items():
            setattr(database_model, key, value)

        database_model.save()
        return database_model
