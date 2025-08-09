from typing import Any
from common.constant import OA_CONF
from omniadvisor.repository.model.load import Load
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result
from omniadvisor.utils.logger import global_logger
from omniadvisor.utils.utils import float_format


def retest(load: Load, config: dict[str, Any]) -> None:
    """
    根据复测结果更新最优配置。

    该方法会在同一配置下执行多轮 Spark 任务复测（次数由 OA_CONF.tuning_retest_times控制），
    用于验证配置的稳定性与性能表现：
    - 如果某轮任务成功执行（状态为 OA_CONF.ExecStatus.success），会记录该轮性能结果。
    - 如果出现非 Spark 异常，会立即抛出并中止复测。
    - 如果出现 Spark 异常且调优结果状态为 OA_CONF.TuningResultStatus.fail，则提前结束复测。

    :param load: 待复测的负载实例。
    :param config: Spark 执行配置（JSON 风格字典）。
    :raises Exception: 当任务执行过程中出现非 Spark 异常时抛出。
    """
    global_logger.debug('Starting retest config...')
    for i in range(1, OA_CONF.tuning_retest_times + 1):
        try:
            exam_record, spark_output = spark_run(load=load, config=config, wait_for_trace=True)
        except Exception as e:
            global_logger.error(
                'Retest failed in round %d. Exception source: Non-Spark exception. Details: %s.', i, e
            )
            # 目前采取的策略是：抛异常，认为是其他原因导致的失败，可以肯定不是配置的原因
            raise

        if exam_record.status == OA_CONF.ExecStatus.success:
            global_logger.info(
                'Retest succeeded in round %d. Performance result: %s.',
                i,
                float_format(exam_record.runtime)
            )
        else:
            # 若是出现异常配置，也要退出
            global_logger.warning('Retest failed in round %d. Exception source: Spark exception: %s.', i, spark_output)
            tuning_result = get_tuning_result(load, config)
            if tuning_result.status == OA_CONF.TuningResultStatus.fail:
                global_logger.info('The number of retest failures has reached the failure threshold.')
                return
