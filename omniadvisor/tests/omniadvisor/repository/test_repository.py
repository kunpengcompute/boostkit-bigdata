import pytest
from django.db import models

from .common import create_table
from .common import delete_table
from omniadvisor.repository.repository import Repository


# 创建一个测试用的 Django 模型
class TestModel(models.Model):
    name = models.CharField(max_length=255)
    age = models.IntegerField()
    is_active = models.BooleanField(default=True)

    class Meta:
        app_label = 'test'


# 创建一个具体的 Repository 子类
class TestRepository(Repository):
    _model_class = TestModel
    _frozen_fields = ['is_active']
    _required_fields = ['name', 'age']
    _fields_format = {
        'name': ([str], ['Tony'], []),
        'age': ([int], [], [25]),
        'is_active': ([bool], [], [])
    }


class TestRepositoryClass:
    """
    Repository测试类
    """

    @staticmethod
    def setup_class():
        # 创建表
        create_table(TestModel)

    @staticmethod
    def teardown_class():
        # 删除表
        delete_table(TestModel)

    @staticmethod
    def setup_method():
        # 清空表数据
        TestModel.objects.all().delete()

    def test_abstract_methods(self):
        with pytest.raises(TypeError):
            Repository()

    def test_check_frozen_field(self):
        with pytest.raises(ValueError):
            TestRepository._check_frozen_field({'is_active': True})

    def test_check_required_field(self):
        with pytest.raises(ValueError):
            TestRepository._check_required_field({'age': 25})

    def test_check_field_format(self):
        with pytest.raises(TypeError):
            TestRepository._check_field_format({'name': 123})
        with pytest.raises(ValueError):
            TestRepository._check_field_format({'name': 'Tony'})
        with pytest.raises(ValueError):
            TestRepository._check_field_format({'age': 20})

    def test_create(self):
        model_attr = {'name': 'John', 'age': 25}
        instance = TestRepository._create(model_attr)
        assert isinstance(instance, TestModel)
        assert instance.name == 'John'
        assert instance.age == 25

    def test_query(self):
        TestModel.objects.create(name='John', age=25)
        model_attr = {'name': 'John'}
        instances = TestRepository._query(model_attr)
        assert len(instances) == 1
        assert instances[0].name == 'John'

    def test_update(self):
        instance = TestModel.objects.create(name='John', age=25)
        model_attr = {'name': 'Mike'}
        updated_instance = TestRepository._update(instance, model_attr)
        assert updated_instance.name == 'Mike'
        assert updated_instance.is_active is True
