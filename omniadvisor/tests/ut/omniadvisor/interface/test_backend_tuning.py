from omniadvisor.interface.tuning import unified_tuning
from omniadvisor.utils.constant import OA_CONF


class TestTuning:

    def test_unified_tuning_when_retest_backend(self):
        unified_tuning(load_id='2', retest_times=3, retest_way=OA_CONF.RetestWay.backend,
                       tuner_type=OA_CONF.TunerType.iterative)
        # TODO 验证最终最优配置是否被更新
        # mock 这个 spark_executor.submit_spark_task(submit_cmd)
        # mock 这个 spark_run中的 trace_data_saver
        # assert best_config 被更新

    def test_unified_tuning_when_retest_hijacking(self):
        unified_tuning(load_id='2', retest_times=3, retest_way=OA_CONF.RetestWay.hijacking,
                       tuner_type=OA_CONF.TunerType.iterative)
        # TODO 验证最终的待测试配置是否被更新
        # mock 这个 spark_executor.submit_spark_task(submit_cmd)
        # mock 这个 spark_run中的 trace_data_saver
        # assert best_config 被更新

    def test_unified_tuning_when_load_id_not_exist(self):
        # TODO 当load id 不存在时，是否触发了正确的行为
        pass

    def test_unified_tuning_when_retest_failed(self):
        # TODO 当复测过程中出现失败，，复测是否终止，即使最优，结果不应被更新
        pass

