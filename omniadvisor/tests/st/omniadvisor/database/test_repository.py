import pytest
from django.db import connection, transaction

from omniadvisor.database.repository import ConfigRepository
from django_server.app.models import Config


@pytest.fixture(scope="function")
def test_config_data():
    return {
        "id": "test_123",
        "param1": 100,
        "param2": 1.5,
        "param3": 2.5
    }


@pytest.fixture(scope="function")
def repo():
    return ConfigRepository()


def create_table_if_not_exists(model):
    """
    动态创建一个表
    :param model: Django 模型类
    """
    meta_data = getattr(model, '_meta')
    if meta_data.db_table not in connection.introspection.table_names():
        # 使用 schema_editor 创建表
        with connection.schema_editor() as schema_editor:
            schema_editor.create_model(model)


def delete_table_if_exists(model):
    """
    删除指定表名的表。
    :param model: Django 模型类
    """
    meta_data = getattr(model, '_meta')
    if meta_data.db_table in connection.introspection.table_names():
        with connection.schema_editor() as schema_editor:
            schema_editor.delete_model(model)


class TestConfigRepository:

    @pytest.mark.django_db(transaction=True)
    def test_save_should_create_record(self, repo, test_config_data):
            # 执行保存
        create_table_if_not_exists(Config)
        repo.save(test_config_data)
        # 验证数据库记录
        record = Config.objects.get(id="test_123")
        assert record.param1 == 100
        assert record.param2 == 1.5
        assert record.param3 == 2.5
        delete_table_if_exists(Config)

    @pytest.mark.django_db(transaction=True)
    def test_find_by_id_should_return_object(self, repo, test_config_data):
        # 准备测试数据
        create_table_if_not_exists(Config)
        Config.objects.create(**test_config_data)

        # 执行查询
        result = repo.find_by_id("test_123")

        assert result is not None
        assert result.id == "test_123"
        delete_table_if_exists(Config)

    @pytest.mark.django_db(transaction=True)
    def test_update_should_modify_fields(self, repo, test_config_data):
        # 创建初始数据
        create_table_if_not_exists(Config)
        Config.objects.create(**test_config_data)

        # 执行更新
        updated_data = {
            "id": "test_123",
            "param1": 200,
            "param2": 3.0,
            "param3": 4.0
        }
        repo.update(updated_data)

        # 验证更新结果
        record = Config.objects.get(id="test_123")
        assert record.param1 == 200
        assert record.param2 == 3.0
        delete_table_if_exists(Config)
