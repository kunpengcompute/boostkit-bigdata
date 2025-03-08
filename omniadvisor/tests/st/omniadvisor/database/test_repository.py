import django
import pytest
from django.conf import settings
from django.db import connection

# 第一重保障：启用内存数据库，确保不影响原来的数据库
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

settings.configure(
    DATABASES=DATABASES,
    DEBUG=False,
    INSTALLED_APPS=['django_server'],
)
django.setup()  # 初始化 Django 环境


# 第二重保障：启用测试表，测试表以test_开头
def load_test_env():
    from omniadvisor.database.repository import CommonRepository as config_repo_class, Config as config_class
    meta_class = getattr(config_class, '_meta')
    meta_class.db_table = f'test_{meta_class.db_table}'
    return config_repo_class, config_class


ConfigRepository, Config = load_test_env()


# 测试数据
@pytest.fixture(scope="function")
def config_data():
    return {
        "id": "test_123",
        "param1": 100,
        "param2": 1.5,
        "param3": 2.5
    }


@pytest.fixture(scope="function")
def table_operation():
    create_table_if_not_exists(Config)
    yield
    delete_table_if_exists(Config)


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
    def test_save_should_create_record(self, config_data, table_operation):
        # 执行保存
        ConfigRepository.save_config(config_data)
        # 验证数据库记录
        record = Config.objects.get(id="test_123")
        assert record.param1 == 100
        assert record.param2 == 1.5
        assert record.param3 == 2.5

    @pytest.mark.django_db(transaction=True)
    def test_find_by_id_should_return_object(self, config_data, table_operation):
        # 准备测试数据
        Config.objects.create(**config_data)

        # 执行查询
        result = ConfigRepository.find_by_id("test_123")

        assert result is not None
        assert result.id == "test_123"

    @pytest.mark.django_db(transaction=True)
    def test_update_should_modify_fields(self, config_data, table_operation):
        # 创建初始数据
        Config.objects.create(**config_data)

        # 执行更新
        updated_data = {
            "id": "test_123",
            "param1": 200,
            "param2": 3.0,
            "param3": 4.0
        }
        ConfigRepository.update(updated_data)

        # 验证更新结果
        record = Config.objects.get(id="test_123")
        assert record.param1 == 200
        assert record.param2 == 3.0
