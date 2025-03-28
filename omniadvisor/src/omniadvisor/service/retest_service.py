from omniadvisor.repository.model.load import Load
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.utils.logger import logger


def retest(retest_times: int, load: Load, config: dict):
    """
    根据复测结果，更新最优配置
    :param retest_times: 复测次数
    :param load: 负载
    :param config: 配置
    :return:
    """
    for _ in range(retest_times):
        try:
            spark_run(load, config)
        # TODO 完善这里的异常
        except Exception as e:
            logger.info('复测第：%d 轮失败，原因：%s', e)
            return float('inf')
        logger.info('复测第：%d 轮成功！')
