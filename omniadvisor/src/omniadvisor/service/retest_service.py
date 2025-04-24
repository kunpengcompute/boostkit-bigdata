from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.utils.logger import logger


def retest(load: Load, config: dict):
    """
    根据复测结果，更新最优配置

    :param load: 负载
    :param config: 配置
    :return:
    """
    logger.debug('Starting retest config...')
    for i in range(1, OA_CONF.tuning_retest_times + 1):
        try:
            exam_record, spark_output = spark_run(load, config)
        except Exception as e:
            logger.error('复测第 %d 轮失败，异常来源：非spark运行异常，详情：%s', i, e)
            # 目前采取的策略是：抛异常，认为是其他原因导致的失败，可以肯定不是配置的原因
            raise

        if exam_record.status == OA_CONF.ExecStatus.success:
            logger.info('复测第 %d 轮成功，性能结果：%.3f', i, exam_record.runtime)
        else:
            # 若是出现异常配置，也要退出
            raise RuntimeError('复测第 %d 轮失败，异常来源：spark运行异常：%s', i, spark_output)
