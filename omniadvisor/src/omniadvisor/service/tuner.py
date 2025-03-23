import random
from abc import ABC, abstractmethod

from ConfigSpace import ConfigurationSpace, Configuration, UniformIntegerHyperparameter, Constant, \
    CategoricalHyperparameter, UniformFloatHyperparameter
from smac import Scenario, HyperparameterOptimizationFacade
from smac.runhistory import TrialValue

from omniadvisor.repository.model.load import Load
from omniadvisor.repository.task_repository import TaskRepository


class Tuner(ABC):
    def __init__(self, load, knob_space: dict):
        self.knob_space = knob_space
        self.load = load

    @abstractmethod
    def tune(self):
        """
        调优算法推荐一个配置
        @return: config
        """
        pass

    @abstractmethod
    def feedback(self, performance):
        """
        有些调优算法，需要性能回馈，如 smac
        @param performance: 性能
        @return: None
        """
        pass

    @abstractmethod
    def initialize(self):
        """
        算法如需初始化，实现之
        @return:
        """
        pass


class IterativeTuner(Tuner):
    """
    迭代调优类，目前采用贝叶斯方法
    """

    def __init__(self, load, knob_space: dict):
        super().__init__(load, knob_space)
        self.smac = None
        # 下一个实验的配置
        self.trail_info = None

    def initialize(self):
        history = TaskRepository.query_perf_history_from_load(self.load)
        config_space = self.convert_to_config_space()
        scenario = Scenario(config_space, deterministic=True, n_trials=len(history.config_perf_pairs) + 1)
        smac = HyperparameterOptimizationFacade(scenario, self.train)
        # 初始化历史数据到model
        for config_value, performance in history.config_perf_pairs:
            config_instance = Configuration(configuration_space=config_space, values=config_value)
            smac.runhistory.add(config=config_instance, cost=performance, seed=1)
        self.smac = smac

    def tune(self):
        trail_info = self.smac.ask()
        self.trail_info = trail_info
        return trail_info.config.get_dictionary()

    def feedback(self, performance):
        value = TrialValue(cost=performance)
        self.smac.tell(self.trail_info, value)

    def convert_to_config_space(self):
        """
        将knob_space 转为 configuration_space
        """
        config_space = ConfigurationSpace()
        for knob_name, knob_value in self.knob_space.items():
            if not knob_value['enable']:
                continue
            knob_type = knob_value.get('type')

            if knob_type == 'uniform_int':
                hyperparameter = UniformIntegerHyperparameter(
                    name=knob_name,
                    lower=knob_value.get('lower'),
                    upper=knob_value.get('upper'),
                    default_value=knob_value.get('default'),
                )
            elif knob_type == 'constant':
                hyperparameter = Constant(
                    name=knob_name,
                    value=knob_value.get('default')
                )
            elif knob_type == 'categorical':
                hyperparameter = CategoricalHyperparameter(
                    name=knob_name,
                    choices=knob_value.get('choices'),
                    default_value=knob_value.get('default'),
                    weights=knob_value.get('weights'),
                )
            elif knob_type == 'uniform_float':
                hyperparameter = UniformFloatHyperparameter(
                    name=knob_name,
                    lower=knob_value.get('lower'),
                    upper=knob_value.get('upper'),
                    default_value=knob_value.get('default'),
                )
            else:
                raise RuntimeError(f'Not supported config type: {knob_type}')
            config_space.add(hyperparameter)
        return config_space

    def train(self, config: Configuration, seed: int = 0) -> float:
        assert self is not None
        rand_num = random.randint(1, 200)
        return rand_num


class ExpertTuner(Tuner):
    """
    专家调优类
    """

    def feedback(self, performance):
        pass

    def initialize(self):
        pass

    def tune(self):
        pass


class TransferTuner(Tuner):
    """
    迁移调优类
    """

    def feedback(self, performance):
        pass

    def initialize(self):
        pass

    def tune(self):
        pass


class TunerFactory:

    def __init__(self, tuner_type, load: Load, knob_space):
        self.recommendations = {
            'iterative': IterativeTuner,
            'expert': ExpertTuner,
            'transfer:': TransferTuner
        }
        self.tuner_type = tuner_type
        self.knob_space = knob_space
        self.load = load

    def make(self) -> Tuner:
        class_obj = self.recommendations.get(self.tuner_type)
        return class_obj(self.load, self.knob_space)
