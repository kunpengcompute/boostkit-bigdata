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


def check_oa_conf():
    """
    校验OA_CONF中参数是否正确
    """
    if OA_CONF.tuning_retest_times <= 0:
        raise ValueError('The tuning retest times must > 0, please check common configuration.')
    if OA_CONF.config_fail_threshold <= 0:
        raise ValueError('The config fail threshold must > 0, please check common configuration.')
    if OA_CONF.spark_fetch_trace_timeout <= 0:
        raise ValueError('The spark fetch trace timeout must > 0, please check common configuration.')
    if OA_CONF.spark_fetch_trace_interval <= 0:
        raise ValueError('The spark fetch trace interval must > 0, please check common configuration.')
    if OA_CONF.spark_exec_timeout_ratio <= 0:
        raise ValueError('The spark exec timeout ratio must > 0, please check common configuration.')

    if not (
            (OA_CONF.spark_history_username and OA_CONF.spark_history_password) or
            (not OA_CONF.spark_history_username and not OA_CONF.spark_history_password)
    ):
        raise ValueError('The spark history username and password should be provided, or leave both blank, please check'
                         ' common configuration.')


class OmniAdvisorConf:
    """
    OmniAdvisor常量
    """
    # OmniAdvisor版本信息
    product_name = "Kunpeng BoostKit"
    product_version = "25.2.RC1"
    component_name = "BoostKit-OmniAdvisor"
    component_version = "2.0.0"

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

    # 调优结果状态
    class TuningResultStatus:
        empty = 'Empty'
        success = 'Success'
        fail = 'Fail'
        running = 'Running'
        all = [empty, success, fail, running]

    empty_config = dict()
    exec_fail_return_runtime = float('inf')
    exec_fail_return_app_id = ''
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

    # 输入配置解析
    _common_config = load_common_config(config_path=common_config_path)
    # 配置罗列
    tuning_retest_times = _common_config.getint('common', 'tuning.retest.times')
    config_fail_threshold = _common_config.getint('common', 'config.fail.threshold')
    spark_history_rest_url = _common_config.get('spark', 'spark.history.rest.url')
    spark_history_username = _common_config.get('spark', 'spark.history.username', fallback='')
    spark_history_password = _common_config.get('spark', 'spark.history.password', fallback='')
    spark_fetch_trace_timeout = _common_config.getint('spark', 'spark.fetch.trace.timeout')
    spark_fetch_trace_interval = _common_config.getint('spark', 'spark.fetch.trace.interval')
    spark_exec_timeout_ratio = _common_config.getfloat('spark', 'spark.exec.timeout.ratio')
    tuning_strategies = json.loads(_common_config.get('common', 'tuning.strategy'))

    # 保留小数位数
    decimal_digits = 3

    # spark 的 name、sql中日期匹配格式，最高精确度是小时
    date_patterns = [
        # YYYY-MM-DD 或 YYYY-MM-DD HH
        r'\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(?:\s+(0\d|1\d|2[0-3]))?',
        # YYYY/MM/DD 或 YYYY/MM/DD HH
        r'\d{4}/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])(?:\s+(0\d|1\d|2[0-3]))?',
        # YYYY.MM.DD 或 YYYY.MM.DD HH
        r'\d{4}\.(0[1-9]|1[0-2])\.(0[1-9]|[12]\d|3[01])(?:\s+(0\d|1\d|2[0-3]))?',
        # 无分隔：YYYYMMDD 或 YYYYMMDDHH
        r'\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])((0\d|1\d|2[0-3]))?'
    ]

    # spark-sql命令所提交的类名
    SparkSQLCLIDriver = 'org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver'

    # 所有涉及到数据库变动的sql关键字
    modify_keywords = ['create', 'insert', 'delete', 'truncate', 'drop', 'update', 'merge']


OA_CONF = OmniAdvisorConf()
