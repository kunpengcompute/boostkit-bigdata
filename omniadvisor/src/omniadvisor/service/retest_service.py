from omniadvisor.repository.model.load import Load
from omniadvisor.repository.model.perf_history import PerfHistory
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.utils.logger import logger


def retest(retest_times: int, load: Load, config: dict):
    """
    根据复测结果，更新最优配置
    """
    for _ in range(retest_times):
        try:
            spark_run(load, config)
        # TODO 完善这里的异常
        except Exception as e:
            logger.info('复测第：%d 轮失败，原因：%s', e)
            return float('inf')
        logger.info('复测第：%d 轮成功！')

