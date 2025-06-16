from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import num_format


def retest(load: Load, config: dict):
    """
    根据复测结果，更新最优配置

    :param load: 负载
    :param config: 配置
    :return:
    """
    global_logger.debug('Starting retest config...')
    for i in range(1, OA_CONF.tuning_retest_times + 1):
        try:
            exam_record, spark_output = spark_run(load, config)
        except Exception as e:
            global_logger.error(
                'Retest failed in round %d. Exception source: Non-Spark exception. Details: %s.', i, e
            )
            # 目前采取的策略是：抛异常，认为是其他原因导致的失败，可以肯定不是配置的原因
            raise

        if exam_record.status == OA_CONF.ExecStatus.success:
            global_logger.info(
                'Retest succeeded in round %d. Performance result: %s.', i,
                num_format(exam_record.runtime)
            )
        else:
            # 若是出现异常配置，也要退出
            global_logger.warning('Retest failed in round %d. Exception source: Spark exception: %s.', i,
                                  spark_output)
            tuning_result = get_tuning_result(load, config)
            if tuning_result.failed_times >= OA_CONF.config_fail_threshold:
                global_logger.info('The number of retest failures has reached the failure threshold.')
                return
