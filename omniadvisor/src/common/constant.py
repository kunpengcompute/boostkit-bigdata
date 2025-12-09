import json
import os
from common.crypto_tool import CryptoTool
from common.config_parser import user_configs


def check_oa_conf() -> None:
    """
    校验OA_CONF中参数是否符合要求，在参数值不符合规范时抛出异常

    :raises ValueError: OA_CONF中的值不符合要求时抛出ValueError
    """
    if OA_CONF.tuning_retest_times <= 0:
        raise ValueError('The tuning retest times must > 0, please check common configuration.')
    if OA_CONF.config_fail_threshold <= 0:
        raise ValueError('The config fail threshold must > 0, please check common configuration.')
    if not isinstance(OA_CONF.tuning_strategies, list):
        raise ValueError('The type of tuning strategies must be list, please check common configuration.')

    if not OA_CONF.postgresql_database_name:
        raise ValueError('The postgresql database name can not be empty, please check common configuration.')
    if not OA_CONF.postgresql_database_user:
        raise ValueError('The postgresql database user can not be empty, please check common configuration.')
    if not OA_CONF.postgresql_database_password:
        raise ValueError('The postgresql database password can not be empty, please use \'crypto.pyc\' to set password.')
    if not OA_CONF.postgresql_database_host:
        raise ValueError('The postgresql database host can not be empty, please check common configuration.')
    if not OA_CONF.postgresql_database_port:
        raise ValueError('The postgresql database port can not be empty, please check common configuration.')
    if not OA_CONF.postgresql_database_sslmode:
        raise ValueError('The postgresql database sslmode can not be empty, please check common configuration.')
    if OA_CONF.postgresql_database_sslmode == 'verify-ca' and not OA_CONF.postgresql_database_sslrootcert:
        raise ValueError('When the postgresql database sslmode is verify-ca, postgresql_database_sslrootcert can not '
                         'be empty, please check common configuration.')
    if OA_CONF.postgresql_database_sslmode == 'verify-full':
        if (not OA_CONF.postgresql_database_sslrootcert or not OA_CONF.postgresql_database_sslcert
                or not OA_CONF.postgresql_database_sslkey):
            raise ValueError(
                'When the postgresql database sslmode is verify-full. postgresql_database_sslrootcert、'
                'postgresql_database_sslcert、postgresql_database_sslkey can not '
                'be empty, please check common configuration.')

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
                         ' common configuration or use \'crypto.pyc\' to set password.')
    if not isinstance(OA_CONF.spark_history_sslverify, bool):
        raise ValueError('The spark history sslverify should be provided and must be boolean type,'
                         ' please check common configuration.')
    # 当前spark history ssl校验开关被打开的时候，才要求sslrootca,sslcrt,sslkey必须被配置
    if OA_CONF.spark_history_sslverify and not all([
        OA_CONF.spark_history_sslrootca,
        OA_CONF.spark_history_sslcrt,
        OA_CONF.spark_history_sslkey
    ]):
        raise ValueError("SSL verification requires all certificate files: "
                         "spark_history_sslrootca, spark_history_sslcrt, spark_history_sslkey")


class OmniAdvisorConf:
    """
    OmniAdvisor 常量配置类。

    存放 OmniAdvisor 系统的全局常量，包括 API 路径、默认参数等。
    """
    # OmniAdvisor版本信息
    product_name = "Kunpeng BoostKit"
    product_version = "25.3.0"
    component_name = "BoostKit-OmniAdvisor"
    component_version = "2.0.0"

    # OmniAdvisor根目录
    project_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    # Log文件目录
    log_dir = f'{project_base_dir}/logs'
    log_file_path = f'{log_dir}/app.log'
    # 配置文件路径
    config_dir = f'{project_base_dir}/config'
    spark_repo_path = f'{config_dir}/spark_repo.json'
    common_config_path = f'{config_dir}/common_config.ini'
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
    exec_fail_return_application_id = ''
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

    # common页
    tuning_retest_times = user_configs.getint('common', 'tuning.retest.times')
    config_fail_threshold = user_configs.getint('common', 'config.fail.threshold')
    tuning_strategies = json.loads(user_configs.get('common', 'tuning.strategy'))
    backend_retest_queue = user_configs.get('common', 'backend.retest.queue')
    # database页
    postgresql_database_name = user_configs.get('database', 'postgresql.database.name')
    postgresql_database_user = user_configs.get('database', 'postgresql.database.user')
    postgresql_database_password = CryptoTool.get_database_password_password()
    postgresql_database_host = user_configs.get('database', 'postgresql.database.host')
    postgresql_database_port = user_configs.get('database', 'postgresql.database.port')
    postgresql_database_sslmode = user_configs.get('database', 'postgresql.database.sslmode')
    postgresql_database_sslrootcert = user_configs.get('database', 'postgresql.database.sslrootcert')
    postgresql_database_sslcert = user_configs.get('database', 'postgresql.database.sslcert')
    postgresql_database_sslkey = user_configs.get('database', 'postgresql.database.sslkey')
    # spark页
    spark_history_rest_url = user_configs.get('spark', 'spark.history.rest.url')
    spark_history_username = user_configs.get('spark', 'spark.history.username')
    spark_history_password = CryptoTool.get_history_server_password()
    spark_history_sslverify = user_configs.getboolean('spark', 'spark.history.sslverify')
    spark_history_sslrootca = user_configs.get('spark', 'spark.history.sslrootca')
    spark_history_sslcrt = user_configs.get('spark', 'spark.history.sslcrt')
    spark_history_sslkey = user_configs.get('spark', 'spark.history.sslkey')
    spark_fetch_trace_timeout = user_configs.getint('spark', 'spark.fetch.trace.timeout')
    spark_fetch_trace_interval = user_configs.getint('spark', 'spark.fetch.trace.interval')
    spark_exec_timeout_ratio = user_configs.getfloat('spark', 'spark.exec.timeout.ratio')
    spark_output_merge_switch = user_configs.getboolean('spark', 'spark.output.merge.switch')
    # 调优策略
    strategies_tuples = [tuple(item) for item in json.loads(user_configs.get('common', 'tuning.strategy'))]

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

    # 所有日期-小时被替换后的变量：
    date_replacement_value = '{date}'

    # spark-sql命令所提交的类名
    spark_sql_cli_driver = 'org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver'

    # 所有涉及到数据库变动的sql关键字
    modify_keywords = [
        'create', 'alter', 'insert', 'truncate', 'drop', 'repair', 'load',
        # 以下这些关键字，在非 hadoop表中可能是合法的
        'update', 'merge', 'delete', 'rewrite', 'upsert', 'replace'
    ]


OA_CONF = OmniAdvisorConf()
