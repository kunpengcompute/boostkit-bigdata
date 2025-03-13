import pytest

from omniadvisor.utils.constant import OA_CONF
from django_server.app.models import DatabaseLoad
from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.perf_history import PerfRecord
from omniadvisor.repository.model.perf_history import PerfHistory


class TestPerfRecord:
    """
    PerfRecord测试类
    """
    def setup_method(self):
        self.config = {"param1": "value1", "param2": "value2"}
        self.perf_record = PerfRecord(config=self.config)

    def test_add(self):
        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={"step": "A"}
        )
        assert len(self.perf_record._status_list) == 1
        assert len(self.perf_record._runtime_list) == 1
        assert len(self.perf_record._trace_list) == 1

        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={"step": "A"}
        )
        assert len(self.perf_record._status_list) == 2
        assert len(self.perf_record._runtime_list) == 2
        assert len(self.perf_record._trace_list) == 2

    def test_status(self):
        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=1.0,
            trace={}
        )
        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=2.0,
            trace={}
        )
        assert self.perf_record.status == OA_CONF.TaskStatus.success

        # 若有一个执行失败的记录，则认为失败
        self.perf_record.add(
            status=OA_CONF.TaskStatus.fail,
            runtime=1.0,
            trace={}
        )
        assert self.perf_record.status == OA_CONF.TaskStatus.fail

    def test_runtime(self):
        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=1.0,
            trace={}
        )
        self.perf_record.add(
            status=OA_CONF.TaskStatus.success,
            runtime=2.0,
            trace={}
        )
        assert self.perf_record.runtime == 1.5

        # 执行失败的执行用时，不计入平均用时的计算
        self.perf_record.add(
            status=OA_CONF.TaskStatus.fail,
            runtime=2.0,
            trace={}
        )
        assert self.perf_record.runtime == 1.5


class TestPerfHistory:
    """
    PerfHistory测试类
    """
    def setup_method(self):
        self.load = Load(database_model=DatabaseLoad())  # 确保 Load 类的初始化是有效的
        self.perf_history = PerfHistory(load=self.load)

    def test_add_performance_record(self):
        config1 = {"param1": "value1"}
        self.perf_history.add(
            config=config1,
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={})

        assert len(self.perf_history._perf_records) == 1
        self.perf_history.add(
            config=config1,
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={}
        )
        assert len(self.perf_history._perf_records) == 1

        config2 = {"param1": "value2"}
        self.perf_history.add(
            config=config2,
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={}
        )
        assert len(self.perf_history._perf_records) == 2

    def test_config_perf_pairs(self):
        config1 = {"param1": "value1"}
        self.perf_history.add(
            config=config1,
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={}
        )
        pairs = self.perf_history.config_perf_pairs
        assert len(pairs) == 1
        assert pairs[0] == (config1, 1.5)

        config2 = {"param1": "value2"}
        self.perf_history.add(
            config=config2,
            status=OA_CONF.TaskStatus.success,
            runtime=1.5,
            trace={}
        )
        pairs = self.perf_history.config_perf_pairs
        assert len(pairs) == 2
        assert pairs[1] == (config2, 1.5)

    def test_best_config(self):
        config1 = {"param1": "value1"}
        config2 = {"param1": "value2"}
        self.perf_history.add(
            config=config1,
            status=OA_CONF.TaskStatus.success,
            runtime=1.0,
            trace={}
        )
        self.perf_history.add(
            config=config2,
            status=OA_CONF.TaskStatus.success,
            runtime=2.0,
            trace={}
        )
        assert self.perf_history.best_config == config1
