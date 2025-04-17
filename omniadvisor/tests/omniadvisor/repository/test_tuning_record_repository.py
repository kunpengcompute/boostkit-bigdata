from .common import create_table, delete_table
from common.constant import OA_CONF
from server.app.models import DatabaseLoad, DatabaseTuningRecord
from omniadvisor.repository.model.tuning_record import TuningRecord
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.repository.tuning_record_repository import TuningRecordRepository


class TestTaskRepository:
    """
    TaskRepository测试类
    """

    @staticmethod
    def setup_class():
        # 创建表
        create_table(DatabaseLoad)
        create_table(DatabaseTuningRecord)

    @staticmethod
    def teardown_class():
        # 删除表
        delete_table(DatabaseTuningRecord)
        delete_table(DatabaseLoad)

    @staticmethod
    def setup_method():
        # 清空表数据
        # 由于外键的存在，因此先删除TuningRecord
        DatabaseTuningRecord.objects.all().delete()
        DatabaseLoad.objects.all().delete()

    def test_create(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )

        # 创建TuningRecord
        tuning_record = TuningRecordRepository.create(
            load=load,
            config={"param1": "value2"},
            method=OA_CONF.TuningMethod.user
        )
        assert isinstance(tuning_record, TuningRecord)
        # 由于外层的Load没有透传到最内层，而Task返回的load为新生成的Load
        # 因此导致这里两边的load不一致，但是属性是一致的
        assert tuning_record.load.name == load.name
        assert tuning_record.config == {"param1": "value2"}
        assert tuning_record.method == OA_CONF.TuningMethod.user
        assert tuning_record.rounds == 0

        # 第二次创建TuningRecord
        tuning_record = TuningRecordRepository.create(
            load=load,
            config={"param2": "value2"},
            method=OA_CONF.TuningMethod.expert
        )
        assert tuning_record.method == OA_CONF.TuningMethod.expert
        assert tuning_record.rounds == 1

    def test_query_by_load(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        tuning_record = TuningRecordRepository.create(
            load=load,
            config={"param1": "value2"},
            method=OA_CONF.TuningMethod.user
        )

        # 查询任务
        tuning_records = TuningRecordRepository.query_by_load(
            load=load
        )
        assert len(tuning_records) == 1
        assert tuning_records[0].load.name == load.name
        assert tuning_records[0].config == {"param1": "value2"}

    def test_query_by_load_and_config(self):
        # 创建测试数据
        load = LoadRepository.create(
            name='test',
            exec_attr={'cmd': 'spark-sql -f test.sql'},
            default_config={"param1": "value2"}
        )
        tuning_record = TuningRecordRepository.create(
            load=load,
            config={"param1": "value2"},
            method=OA_CONF.TuningMethod.user
        )

        # 查询任务
        tuning_records = TuningRecordRepository.query_by_load_and_config(
            load=load,
            config={"param1": "value2"},
        )
        assert len(tuning_records) == 1
        assert tuning_records[0].load.name == load.name
        assert tuning_records[0].config == {"param1": "value2"}

        # 查询失败
        tuning_records = TuningRecordRepository.query_by_load_and_config(
            load=load,
            config={"param2": "value2"},
        )
        assert len(tuning_records) == 0
