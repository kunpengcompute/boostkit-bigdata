import json
import uuid
from typing import Dict, List, Tuple

from django_server.app.models import Config, Performance


class CommonRepository:

    @staticmethod
    def find_by_id(config_id: str):
        config_obj = Config.objects.filter(id=config_id).first()
        return config_obj

    @staticmethod
    def update(config: Dict):
        Config.objects.filter(id=config.get('id')).update(param1=config.get('param1'), param2=config.get('param2'),
                                                          param3=config.get('param3'))

    @staticmethod
    def find_all_configs(load, knob_space: dict) -> List[Tuple[dict, Performance]]:
        """
        TODO 根据负载唯一标识和参数空间得到该负载下所有的具体配置和性能记录对
        """
        knob_key_str = json.dumps(knob_space, sort_keys=True)
        # TODO 缺一个数据库操作
        run_history = list()
        knob_instance = {
            "spark.driver.instances": 4,
            "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold": 50,
            "spark.sql.optimizer.enableCsvExpressionOptimization": "true"
        }
        performance = Performance(id=uuid.UUID, workload=None, config=None, has_succeed=True, average_performance=12.3,
                                  poorest_performance=0.5, best_performance=99.1)
        run_history.append((knob_instance, performance))
        return run_history

    @classmethod
    def save_config(cls, next_config):
        pass

    @classmethod
    def update_untest_load(cls, load, next_config):
        """
        更新待测试配置
        """
        pass

    @classmethod
    def update_best_config(cls, load):
        """
        更新最优配置
        """
        pass
