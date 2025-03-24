import random
from abc import ABC, abstractmethod

from ConfigSpace import Configuration, ConfigurationSpace
from smac import Scenario, HyperparameterOptimizationFacade

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.task_repository import TaskRepository
from omniadvisor.utils.constant import TunerType


class Tuner(ABC):
    def __init__(self, load, config_space: ConfigurationSpace):
        self.config_space = config_space
        self.load = load

    @abstractmethod
    def tune(self):
        """
        调优算法推荐一个配置
        :return: config
        """
        pass

    @abstractmethod
    def initialize(self):
        """
        算法如需初始化，实现之
        :return:
        """
        pass


class IterativeTuner(Tuner):
    """
    迭代调优类，目前采用贝叶斯方法
    """

    def __init__(self, load, config_space: ConfigurationSpace):
        super().__init__(load, config_space)
        self.smac = None
        # 下一个实验的配置
        self.trail_info = None

    def initialize(self):
        """
        初始化动作，加载参数空间文件等
        :return:
        """
        history = TaskRepository.query_perf_history_from_load(self.load)
        scenario = Scenario(self.config_space, deterministic=True, n_trials=len(history.config_perf_pairs) + 1)
        smac = HyperparameterOptimizationFacade(scenario, self.train)
        # 初始化历史数据到model
        for config_value, performance in history.config_perf_pairs:
            config_instance = Configuration(configuration_space=self.config_space, values=config_value)
            smac.runhistory.add(config=config_instance, cost=performance, seed=1)
        self.smac = smac

    def tune(self):
        trail_info = self.smac.ask()
        self.trail_info = trail_info
        return trail_info.config.get_dictionary()

    def train(self, config: Configuration, seed: int = 0) -> float:
        # TODO 这里待改为spark调用
        assert self is not None
        rand_num = random.randint(1, 200)
        return rand_num


class ExpertTuner(Tuner):
    """
    专家调优类
    """

    def initialize(self):
        pass

    def tune(self):
        pass


class TransferTuner(Tuner):
    """
    迁移调优类
    """

    def initialize(self):
        pass

    def tune(self):
        pass


class TunerFactory:
    TUNER_TYPES = {
        TunerType.ITERATIVE.value: IterativeTuner,
        TunerType.EXPERT.value: ExpertTuner,
        TunerType.TRANSFER.value: TransferTuner
    }

    def __init__(self, tuner_type: str, load: Load, config_space: ConfigurationSpace):
        self.tuner_type = self.TUNER_TYPES.get(tuner_type)
        self.config_space = config_space
        self.load = load

    def make(self) -> Tuner:
        return self.tuner_type(self.load, self.config_space)
