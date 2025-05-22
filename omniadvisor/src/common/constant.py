import json
import os
import configparser

from server.engine.settings import BASE_DIR


def load_common_config(config_path: str):
    """
    使用configparser库加载common_config

    :param config_path: 配置文件路径
    :return: 返回common_config的内容
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")

    common_config = configparser.ConfigParser()
    common_config.read(config_path, encoding='utf-8')

    return common_config


class OmniAdvisorConf:
    """
    OmniAdvisor常量
    """
    # OmniAdvisor根目录
    project_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Django Server根目录
    server_base_dir = BASE_DIR
    # Log文件目录
    log_dir = f'{project_base_dir}/logs'
    log_file_path = f'{log_dir}/app.log'
    # 配置文件路径
    config_dir = f'{project_base_dir}/config'
    spark_repo_path = f'{config_dir}/spark_repo.json'
    common_config_path = f'{config_dir}/common_config.cfg'
    # 数据存储目录
    data_dir = f'{project_base_dir}/data'

    # 任务执行状态
    class ExecStatus:
        success = 'Success'
        fail = 'Fail'
        running = 'Running'
        all = [success, fail, running]

    exec_fail_return_runtime = float('inf')
    exec_fail_return_trace = dict()

    # 调优算法
    class TuningMethod:
        user = 'user'
        iterative = 'iterative'
        expert = 'expert'
        transfer = 'transfer'
        native = 'native'
        all = [user, iterative, expert, transfer, native]

    # 复测方式
    class RetestWay:
        hijacking = 'hijacking'
        backend = 'backend'
        all = [hijacking, backend]

    # 评估配置失效的阈值，当配置执行失败次数大于等于此值，则配置失效
    config_fail_threshold = 1

    # Spark从History Sever抓取Trace的超时时间
    spark_fetch_trace_timeout = 10

    # 输入配置解析
    _common_config = load_common_config(config_path=common_config_path)
    # 配置罗列
    tuning_retest_times = _common_config.getint('common', 'tuning.retest.times')
    spark_history_rest_url = _common_config.get('spark', 'spark.history.rest.url')
    timeout_ratio = _common_config.getfloat('spark', 'spark.sql.timeout.ratio')

    # 规定每种调优方式的轮次 瞎拍的
    tuning_strategies = json.loads(_common_config.get('common', 'tuning.strategy'))


OA_CONF = OmniAdvisorConf()
