import hashlib
import random
from abc import ABC, abstractmethod

from ConfigSpace import ConfigurationSpace, Configuration, UniformIntegerHyperparameter, Constant, \
    CategoricalHyperparameter, UniformFloatHyperparameter
from smac import Scenario, HyperparameterOptimizationFacade

from omniadvisor.database.repository import CommonRepository


class ConfigRecommendation(ABC):
    def __init__(self, load: Load, knob_space: dict):
        self.knob_space = knob_space
        self.load = load

    @abstractmethod
    def recommend(self):
        pass


class IterativeTuning(ConfigRecommendation):
    """
    迭代调优类，目前采用贝叶斯方法
    """

    def __init__(self, load: Load, knob_space: dict):
        super().__init__(load, knob_space)
        self.history = CommonRepository.find_all_configs(load, knob_space)

    def recommend(self):
        config_space = self.convert_to_config_space()
        scenario = Scenario(config_space, deterministic=True, n_trials=len(self.history) + 1)
        smac = HyperparameterOptimizationFacade(scenario, self.train)
        # 初始化历史数据到model
        for config_value, performance in self.history:
            config_instance = Configuration(configuration_space=config_space, values=config_value)
            smac.runhistory.add(config=config_instance, cost=performance.average_performance, seed=1)

        incumbent = smac.optimize()
        return incumbent

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


class ExpertTuning(ConfigRecommendation):
    """
    专家调优类
    """
    def recommend(self):
        pass


class TransferTuning(ConfigRecommendation):
    """
    迁移调优类
    """
    def recommend(self):
        pass


class ConfigRecommendationFactory:

    def __init__(self, recommendation_type, load: Load, knob_space):
        self.recommendations = {
            'iterative': IterativeTuning,
            'expert': ExpertTuning,
            'transfer:': TransferTuning
        }
        self.recommendation_type = recommendation_type
        self.knob_space = knob_space
        self.load = load

    def make(self) -> ConfigRecommendation:
        class_obj = self.recommendations.get(self.recommendation_type)
        return class_obj(self.load, self.knob_space)
