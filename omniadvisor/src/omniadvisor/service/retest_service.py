from omniadvisor.database.repository import CommonRepository
from omniadvisor.service.spark_service import run_task


def format_config_to_str(load_config):
    return ''


def retest(retest_times, load, config):
    """
    根据复测结果，更新最优配置
    """
    for _ in retest_times:
        load_config = load.config
        config_str_in_cmd = format_config_to_str(load_config)
        load.exec_load.format(config=config_str_in_cmd)
        result = run_task(load, config)

    CommonRepository.update_best_config(load)

