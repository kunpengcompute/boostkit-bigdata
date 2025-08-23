import pytest

from omniadvisor.interface.hijack_recommend import _calculate_hash_value
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
            name='example_name',
            exec_attr={'name': 'example_name', 'cpu': 4},
            default_config={'param1': 'value2'},
            hash_value='0607',
            backend_retest_enable=False
        )
        assert isinstance(load, Load)
        assert load.database_model.name == 'example_name'
        assert load.database_model.exec_attr == {'name': 'example_name', 'cpu': 4}
        assert load.database_model.default_config == {'param1': 'value2'}
        assert load.database_model.hash_value == '0607'

        # 查询数据库中是否有新增数据
        database_loads = DatabaseLoad.objects.filter(name='example_name')
        assert len(database_loads) == 1

        # 输入无效数据
        with pytest.raises(ValueError):
            LoadRepository.create(name='', exec_attr={}, default_config={}, hash_value='', backend_retest_enable=True)

    def test_query_by_hash_value(self):
        # 创建测试数据
        hash_value = _calculate_hash_value(exec_attr={'name': 'example_name', 'cpu': 4}, config={'param1': 'value2'})

        LoadRepository.create(
            name='example_name',
            exec_attr={'name': 'example_name', 'cpu': 4},
            default_config={'param1': 'value2'},
            hash_value=hash_value,
            backend_retest_enable=True
        )

        # 查询负载
        loads = LoadRepository.query_by_hash_value(hash_value)
        assert len(loads) == 1
        assert loads[0].exec_attr == {'name': 'example_name', 'cpu': 4}

        # 查询失败
        wrong_hash_value = _calculate_hash_value(exec_attr={'name': 'wrong_name'}, config={'param1': 'value2'})
        loads = LoadRepository.query_by_hash_value(wrong_hash_value)
        assert len(loads) == 0

    def test_update_best_config(self):
        # 创建测试数据
        hash_value = _calculate_hash_value(exec_attr={'name': 'example_name', 'cpu': 4}, config={'param1': 'value2'})
        load = LoadRepository.create(
            name='example_name',
            exec_attr={'name': 'example_name', 'cpu': 4},
            default_config={'param1': 'value2'},
            hash_value=hash_value,
            backend_retest_enable=True
        )

        # 更新最优配置
        updated_load = LoadRepository.update_best_config(
            load=load,
            best_config={'param2': 'value2'}
        )
        assert updated_load.best_config == {'param2': 'value2'}

    def test_update_test_config(self):
        # 创建测试数据
        hash_value = _calculate_hash_value(exec_attr={'name': 'example_name', 'cpu': 4}, config={'param1': 'value2'})
        load = LoadRepository.create(
            name='example_name',
            exec_attr={'name': 'example_name', 'cpu': 4},
            default_config={'param1': 'value2'},
            hash_value=hash_value,
            backend_retest_enable=True
        )

        # 更新测试配置
        updated_load = LoadRepository.update_test_config(
            load=load,
            test_config={'param2': 'value2'}
        )
        assert updated_load.test_config == {'param2': 'value2'}

    def test_update_tuning_needed(self):
        # 创建测试数据
        hash_value = _calculate_hash_value(exec_attr={'name': 'example_name', 'cpu': 4}, config={'param1': 'value2'})
        load = LoadRepository.create(
            name='example_name',
            exec_attr={'name': 'example_name', 'cpu': 4},
            default_config={'param1': 'value2'},
            hash_value=hash_value,
            backend_retest_enable=True
        )

        # 更新测试配置
        updated_load = LoadRepository.update_test_config(
            load=load,
            test_config={'is_need_tuning': True}
        )
        assert updated_load.test_config == {'is_need_tuning': True}
