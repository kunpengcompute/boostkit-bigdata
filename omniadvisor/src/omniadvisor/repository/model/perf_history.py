import json

from omniadvisor.repository.model.load import Load
from omniadvisor.utils.constant import TaskStatus


class PerfHistory:
    """
    性能历史数据
    """
    def __init__(self, load: Load):
        """
        初始化，负载对应下的所有性能历史数据

        :param load: 负载实例
        """
        self._load = load
        self._perf_records = dict()

    def add(self, config: dict, status: str, runtime: float, trace: dict):
        """
        初始化，增加性能记录

        :param config: 参数配置
        :param status: 执行状态
        :param runtime: 执行用时
        :param trace: 执行Trace
        """
        # config 字段暂时为Dict类型，无法hash，因此将其转为json字符串
        # 同时将key进行排序，保持排序一致性
        config_hash = json.dumps(config, sort_keys=True)
        # 若没有相应config记录，则新增
        if config_hash not in self._perf_records:
            self._perf_records[config_hash] = PerfRecord(config=config)

        # 向perf_records增加数据记录
        self._perf_records[config_hash].add(status=status, runtime=runtime, trace=trace)

    @property
    def config_perf_pairs(self):
        return [
            (perf_record.config, perf_record.runtime)
            for perf_record in self._perf_records.values() if perf_record.status == TaskStatus.success
        ]

    @property
    def best_config(self):
        return min(self.config_perf_pairs, key=lambda x: x[1])[0]


class PerfRecord:
    """
    性能记录
    """
    def __init__(self, config: dict):
        """
        初始化，某配置下对应的性能记录

        :param config: 参数配置
        """
        self._config = config
        self._status_list = list()
        self._runtime_list = list()
        self._trace_list = list()

    def add(self, status: str, runtime: float, trace: dict):
        """
        初始化，增加性能数据

        :param status: 任务状态
        :param runtime: 任务用时
        :param trace: 任务Trace
        """
        self._status_list.append(status)
        self._runtime_list.append(runtime)
        self._trace_list.append(trace)

    @property
    def config(self):
        return self._config

    @property
    def status(self):
        # 如果该配置执行失败过，则认为配置不可行
        if TaskStatus.fail in self._status_list:
            return TaskStatus.fail
        else:
            return TaskStatus.success

    @property
    def runtime(self):
        # 计算执行状态为success的平均runtime
        success_runtimes = [
            runtime
            for status, runtime in zip(self._status_list, self._runtime_list) if status == TaskStatus.success
        ]

        return sum(success_runtimes) / len(success_runtimes)
