import os
import configparser


def load_common_config():
    """
    使用configparser库加载common_config
    :return: 返回common_config的内容
    """
    common_config = configparser.ConfigParser()
    config_path = f"{os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))}/" \
                  f"config/common_config.cfg"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")
    common_config.read(config_path, encoding='utf-8')
    return common_config


class OmniAdvisorConf:
    """
    OmniAdvisor常量
    """
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 获取当前脚本所在的目录

    # 任务执行状态
    class TaskStatus:
        success = 'Success'
        fail = 'Fail'
        running = 'Running'
        all = [success, fail, running]

    # 存几种调优算法，目前暂无赋值
    class TunerType:
        iterative = 'iterative'
        expert = 'expert'
        transfer = 'transfer'
        all = [iterative, expert, transfer]

    class RetestWay:
        hijacking = 'hijacking'
        backend = 'backend'
        all = [hijacking, backend]

    @staticmethod
    # 项目通用配置
    class CommonConfig:
        COMMON_CONFIG = load_common_config()
        SPARK_HISTORY_REST_URL = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")

    CODING_TYPE = "utf-8"
    SPARK_NON_EXEC_ATTR_LIST = {"name", "conf"}
    DATA_DIR = "".join([PROJECT_DIR, "/data"])
    SPARK_TRACE_DATA_DIR = "".join([DATA_DIR, "/spark_trace"])


OA_CONF = OmniAdvisorConf()
