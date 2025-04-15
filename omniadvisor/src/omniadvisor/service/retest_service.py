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
    logger.debug('开始复测配置...')
    for i in range(1, OA_CONF.tuning_retest_times + 1):
        try:
            exam_record, spark_output = spark_run(load, config)
        except Exception as e:
            logger.error('复测第 %d 轮失败，原因：%s', i, e)
            return

        if exam_record.status == OA_CONF.ExecStatus.success:
            logger.info('复测第 %d 轮成功，性能结果：%.3f', i, exam_record.runtime)
        else:
            logger.error('复测第 %d 轮失败，spark执行异常：%s', i, spark_output)
