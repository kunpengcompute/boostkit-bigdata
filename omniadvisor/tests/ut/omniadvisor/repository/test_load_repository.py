import pytest

from .common import create_test_table
from .common import delete_test_table
from omniadvisor.repository.load_repository import Load
from omniadvisor.repository.load_repository import LoadAttrFiled
from omniadvisor.repository.load_repository import LoadRepository


class TestLoadRepository:
    """
    LoadRepository测试类
    """
    def setup_method(self):
        """
        测试前准备，创建表
        """
        create_test_table(Load)

        self.load_attr = {
            LoadAttrFiled.name: 'test',
            LoadAttrFiled.exec_load: {'cmd': 'spark-class --master yarn --queue {queue} -f test.sql',
                                      'queue': 'root.default'},
            LoadAttrFiled.default_config: {'executor_num': 1, 'executor_cores': 2}
        }

    @staticmethod
    def cleanup_method():
        """
        测试后清理，删除表
        """
        delete_test_table(Load)


    @pytest.mark.django_db(transaction=True)
    def test_base_method(self):
        """
        测试类中基本方法，add/search/update
        """
        # TODO: 需补充完善测试，增加异常测试
        # 测试add
        created_load_1 = LoadRepository.add(model_attr=self.load_attr)
        assert created_load_1.name == self.load_attr[LoadAttrFiled.name]
        assert created_load_1.exec_load == self.load_attr[LoadAttrFiled.exec_load]
        assert created_load_1.default_config == self.load_attr[LoadAttrFiled.default_config]

        # 测试search
        search_load_attr = {LoadAttrFiled.name: self.load_attr[LoadAttrFiled.name]}
        searched_loads_1 = LoadRepository.search(search_load_attr)
        assert len(searched_loads_1) == 1

        # 测试search
        created_load_2 = LoadRepository.add(model_attr=self.load_attr)
        searched_loads_2 = LoadRepository.search(search_load_attr)
        assert len(searched_loads_2) == 2
        for searched_load in searched_loads_2.all():
            assert searched_load.name == self.load_attr[LoadAttrFiled.name]

        # 测试update
        update_load_attr = {LoadAttrFiled.name: 'test2'}
        updated_load = LoadRepository.update(created_load_2, update_load_attr)
        assert updated_load.name == 'test2'
        searched_loads_3 = LoadRepository.search(update_load_attr)
        assert len(searched_loads_3) == 1
        assert searched_loads_3.first().name == 'test2'

