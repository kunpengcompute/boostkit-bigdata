from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.service.spark_service import run_task


def format_config_to_str(load_config):
    return ''


def retest(retest_times, load, config):
    """
    根据复测结果，更新最优配置
    """
    # TODO 待调用spark执行接口
    for _ in retest_times:
        load_config = load.config
        config_str_in_cmd = format_config_to_str(load_config)
        load.exec_load.format(config=config_str_in_cmd)
        result = run_task(load, config)
        LoadRepository.update_best_config(load, config)
