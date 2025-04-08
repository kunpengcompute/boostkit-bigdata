import pytest

from .common import create_table
from .common import delete_table
from omniadvisor.repository.load_repository import DatabaseLoad
from omniadvisor.repository.load_repository import Load
from omniadvisor.repository.load_repository import LoadRepository


# 测试 LoadRepository 类
class TestLoadRepository:
    """
    LoadRepository测试类
    """

    @staticmethod
    def setup_class():
        # 创建表
        create_table(DatabaseLoad)

    @staticmethod
    def teardown_class():
        # 删除表
        delete_table(DatabaseLoad)

    @staticmethod
    def setup_method():
        # 清空表数据
        DatabaseLoad.objects.all().delete()

    def test_create(self):
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        assert isinstance(load, Load)
        assert load.database_model.name == 'test'
        assert load.database_model.exec_attr == {'cmd': 'spark-sql -f test.sql'}
        assert load.database_model.default_config == {"param1": "value2"}

        # 查询数据库中是否有新增数据
        database_loads = DatabaseLoad.objects.filter(name='test')
        assert len(database_loads) == 1

        # 输入无效数据
        with pytest.raises(ValueError):
            LoadRepository.create(name="", exec_attr={}, default_config={})

    def test_query(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 查询负载
        loads = LoadRepository.query(
            name='test',
            default_config={"param1": "value2"}
        )
        assert len(loads) == 1
        assert loads[0].exec_attr == {'cmd': 'spark-sql -f test.sql'}

        # 查询失败
        loads = LoadRepository.query(
            name='t',
            default_config={"param1": "value2"}
        )
        assert len(loads) == 0

    def test_update_best_config(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 更新最优配置
        updated_load = LoadRepository.update_best_config(
            load=load,
            best_config={"param2": "value2"}
        )
        assert updated_load.best_config == {"param2": "value2"}

    def test_update_test_config(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 更新测试配置
        updated_load = LoadRepository.update_test_config(
            load=load,
            test_config={"param2": "value2"}
        )
        assert updated_load.test_config == {"param2": "value2"}

    def test_update_tuning_needed(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 更新测试配置
        updated_load = LoadRepository.update_test_config(
            load=load,
            test_config={"is_need_tuning": True}
        )
        assert updated_load.test_config == {"is_need_tuning": True}
