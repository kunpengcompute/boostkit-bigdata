import logging
import sys
from unittest.mock import patch, MagicMock

import pytest
from omniadvisor.interface.config_tuning import unified_tuning, main

from common.constant import OA_CONF


class TestTuning:

    def setup_class(self):
        # 创建表
        self.load = MagicMock()
        self.load.default_config = {"test_config": "1"}
        self.retest_times = 3
        self.tuning_method = OA_CONF.TuningMethod.iterative
        self.empty_str = ''
        self.tune_return_val = ({'key': 'value'}, '')

    def test_unified_tuning_when_retest_backend(self):
        """
        后台复测顺利执行
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
                patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create'), \
                patch('omniadvisor.service.retest_service.spark_run') as mock_spark_run, \
                patch('omniadvisor.interface.config_tuning.get_tuning_result_history'), \
                patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config') as mock_update_best, \
                patch('algo.iterative.tuning.SmacAppendTuning.tune') as mock_smac_tuning:
            mock_exam_record = MagicMock()
            mock_smac_tuning.return_value = self.tune_return_val
            mock_exam_record.status = OA_CONF.ExecStatus.success
            spark_output = self.empty_str
            mock_spark_run.return_value = mock_exam_record, spark_output
            unified_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)
            mock_update_best.assert_called_once()
            mock_smac_tuning.assert_called_once()
            assert mock_spark_run.call_count == OA_CONF.tuning_retest_times

    def test_unified_tuning_when_spark_execute_failed(self):
        """
        后台复测 spark命令执行异常
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
                patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create'), \
                patch('omniadvisor.service.retest_service.spark_run') as mock_spark_run, \
                patch('omniadvisor.interface.config_tuning.get_tuning_result_history') as mock_tuning_result_history, \
                patch('omniadvisor.service.retest_service.get_tuning_result') as mock_get_tuning_result, \
                patch('omniadvisor.interface.config_tuning.remove_tuning_result') as mock_remove_tuning_result, \
                patch('algo.iterative.tuning.SmacAppendTuning.tune') as mock_smac_tuning, \
                patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config') as mock_update_best:
            mock_exam_record = MagicMock()
            mock_exam_record.status = OA_CONF.ExecStatus.fail
            spark_output = self.empty_str
            mock_spark_run.return_value = mock_exam_record, spark_output
            mock_smac_tuning.return_value = self.tune_return_val
            mock_tuning_result = MagicMock()
            mock_tuning_result.failed_times = OA_CONF.config_fail_threshold
            mock_get_tuning_result.return_value = mock_tuning_result
            tuning_result_history = MagicMock()
            tuning_result_history.best_config = {**self.load.default_config, **self.tune_return_val[0]}
            mock_tuning_result_history.return_value = tuning_result_history
            unified_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)

            mock_smac_tuning.assert_called_once()
            mock_spark_run.assert_called_once()
            mock_update_best.assert_not_called()
            mock_remove_tuning_result.assert_not_called()

    def test_unified_tuning_when_other_exception(self):
        """
        后台复测 发生其他异常
        :return:
        """

        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
                patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create'), \
                patch('omniadvisor.service.retest_service.spark_run', side_effect=RuntimeError) as mock_spark_run, \
                patch('omniadvisor.interface.config_tuning.get_tuning_result_history'), \
                patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config'), \
                patch('algo.iterative.tuning.SmacAppendTuning.tune') as mock_smac_tuning, \
                patch('omniadvisor.interface.config_tuning.remove_tuning_result') as mock_remove_tuning_result:
            mock_exam_record = MagicMock()
            mock_exam_record.status = OA_CONF.ExecStatus.success
            mock_smac_tuning.return_value = self.tune_return_val
            with pytest.raises(RuntimeError):
                unified_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)

            mock_smac_tuning.assert_called_once()
            mock_spark_run.assert_called_once()
            mock_remove_tuning_result.assert_called_once()

    def test_main_when_load_id_not_exist(self):
        """
        当 load id 不存在时
        :return:
        """
        sys.argv = ['config_tuning.py',
                    '--load-id', None,
                    '--retest-way', OA_CONF.RetestWay.backend,
                    '--tuning-method', self.tuning_method]

        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id') as mock_query_by_id, \
                patch('omniadvisor.utils.logger.global_logger.info') as mock_info, \
                patch('omniadvisor.interface.config_tuning.get_tuning_result_history'), \
                patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create'):
            mock_query_by_id.return_value = None
            main()
            mock_info.assert_called_with('Cannot find load id: %s in database, the tuning process exits.', 'None')

    def test_unified_tuning_when_retest_failed(self):
        """
        前台复测
        :return:
        """
        with patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id'), \
                patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create'), \
                patch('omniadvisor.interface.config_tuning.get_tuning_result_history') as mocked_query_perf, \
                patch('omniadvisor.repository.load_repository.LoadRepository.update_test_config') as mock_update_test, \
                patch('algo.iterative.tuning.SmacAppendTuning.tune') as mock_smac_tuning:
            mock_smac_tuning.return_value = self.tune_return_val
            unified_tuning(load=self.load, retest_way=OA_CONF.RetestWay.hijacking,
                           tuning_method=self.tuning_method)
            mocked_query_perf.assert_called_once()
            mock_update_test.assert_called_once()
            mock_smac_tuning.assert_called_once()
