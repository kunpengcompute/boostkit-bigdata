from unittest.mock import patch, MagicMock

from common.constant import OA_CONF
from omniadvisor.interface.config_tuning import unified_tuning


class TestTuning:

    def setup_class(self):
        # 创建表
        self.load_id = '2'
        self.retest_times = 3
        self.tune_method = OA_CONF.TuneMethod.iterative

    def test_unified_tuning_when_retest_backend(self):
        """
        后台复测顺利执行
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
            patch('omniadvisor.service.retest_service.spark_run'), \
            patch('omniadvisor.interface.config_tuning.get_tuning_result_history') as mocked_query_perf, \
            patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config') as mock_update_best:

            mock_obj = MagicMock()
            mocked_query_perf.return_value = mock_obj
            unified_tuning(load_id=self.load_id, retest_way=OA_CONF.RetestWay.backend, tune_method=self.tune_method)
            mock_update_best.assert_called_once()

    def test_unified_tuning_when_load_id_not_exist(self):
        """
        当 load id 不存在时
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id') as mock_query_by_id, \
            patch('omniadvisor.utils.logger.logger.info') as mock_info:

            mock_query_by_id.return_value = None
            unified_tuning(load_id=self.load_id, retest_way=OA_CONF.RetestWay.backend, tune_method=self.tune_method)
            mock_info.assert_called_with('Cannot find load id: %s in database.', '2')

    def test_unified_tuning_when_retest_failed(self):
        """
        前台复测
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
            patch('omniadvisor.interface.config_tuning.get_tuning_result_history') as mocked_query_perf, \
            patch('omniadvisor.repository.load_repository.LoadRepository.update_test_config') as mock_update_test:

            unified_tuning(load_id=self.load_id, retest_way=OA_CONF.RetestWay.hijacking, tune_method=self.tune_method)
            mocked_query_perf.assert_not_called()
            mock_update_test.assert_called_once()
