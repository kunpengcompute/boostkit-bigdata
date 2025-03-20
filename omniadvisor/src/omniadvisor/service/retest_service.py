from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.perf_history import PerfHistory
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.utils.logger import logger


def retest(retest_times: int, load: Load, config: dict):
    """
    根据复测结果，更新最优配置
    """
    perf_history = PerfHistory(load=load)
    for _ in range(retest_times):
        try:
            task_info = spark_run(load, config)
        # TODO 完善这里的异常
        except Exception as e:
            logger.info('复测第：%d 轮失败，原因：%s', e)
            logger.debug('异常详细原因：', exc_info=True)
            return float('inf')
        logger.info('复测第：%d 轮成功！')
        perf_history.add(
            config=task_info.config,
            status=task_info.status,
            runtime=task_info.runtime,
            trace=task_info.trace
        )
    return perf_history.best_config
