import sys
from unittest.mock import patch, MagicMock

import pytest

from common.exceptions import TuningPreconditionError
from omniadvisor.interface.config_tuning import _single_tuning, main

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

    @patch('omniadvisor.service.retest_service.float_format')
    @patch('omniadvisor.interface.config_tuning.float_format')
    @patch('algo.iterative.tuning.IterativeTunner.tune')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result_history')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result')
    @patch('omniadvisor.service.retest_service.spark_run')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id')
    def test_unified_tuning_when_retest_backend(
            self,
            mock_query_by_id,
            mock_query_tuning_record,
            mock_create_tuning_record,
            mock_spark_run,
            mock_get_tuning_result,
            mock_get_history,
            mock_smac_tuning,
            mock_float_format_config,
            mock_float_format_retest
    ):
        """
        后台复测顺利执行
        :return:
        """
        mock_query_tuning_record.return_value = list()
        mock_exam_record = MagicMock()
        mock_smac_tuning.return_value = self.tune_return_val
        mock_exam_record.status = OA_CONF.ExecStatus.success
        spark_output = self.empty_str
        mock_spark_run.return_value = mock_exam_record, spark_output
        mock_tuning_history = MagicMock()
        mock_get_history.return_value = mock_tuning_history
        _single_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)
        mock_tuning_history.refresh_best_config.assert_called_once()
        mock_smac_tuning.assert_called_once()
        assert mock_spark_run.call_count == OA_CONF.tuning_retest_times

    @patch('omniadvisor.service.retest_service.float_format')
    @patch('omniadvisor.interface.config_tuning.float_format')
    @patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config')
    @patch('algo.iterative.tuning.IterativeTunner.tune')
    @patch('omniadvisor.interface.config_tuning.remove_tuning_result')
    @patch('omniadvisor.service.retest_service.get_tuning_result')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result_history')
    @patch('omniadvisor.service.retest_service.spark_run')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id')
    def test_unified_tuning_when_spark_execute_failed(
            self,
            mock_query_by_id,
            mock_query_tuning_record,
            mock_create_tuning_record,
            mock_get_tuning_result_config,
            mock_spark_run,
            mock_tuning_result_history,
            mock_get_tuning_result_service,
            mock_remove_tuning_result,
            mock_smac_tuning,
            mock_update_best,
            mock_float_format_config,
            mock_float_format_service
    ):
        """
        后台复测 spark命令执行异常
        :return:
        """

        mock_query_tuning_record.return_value = list()
        mock_exam_record = MagicMock()
        mock_exam_record.status = OA_CONF.ExecStatus.fail
        spark_output = self.empty_str
        mock_spark_run.return_value = mock_exam_record, spark_output
        mock_smac_tuning.return_value = self.tune_return_val
        mock_tuning_result = MagicMock()
        mock_tuning_result.failed_count = OA_CONF.config_fail_threshold
        mock_get_tuning_result_service.return_value = mock_tuning_result
        tuning_result_history = MagicMock()
        tuning_result_history.best_config = {**self.load.default_config, **self.tune_return_val[0]}
        mock_tuning_result_history.return_value = tuning_result_history
        _single_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)

        mock_smac_tuning.assert_called_once()
        mock_spark_run.assert_called_once()
        mock_update_best.assert_not_called()
        mock_remove_tuning_result.assert_not_called()

    @patch('omniadvisor.interface.config_tuning.remove_tuning_result')
    @patch('algo.iterative.tuning.IterativeTunner.tune')
    @patch('omniadvisor.repository.load_repository.LoadRepository.update_best_config')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result_history')
    @patch('omniadvisor.service.retest_service.spark_run', side_effect=RuntimeError)
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id')
    def test_unified_tuning_when_other_exception(
            self,
            mock_query_by_id,
            mock_query_tuning_record,
            mock_create_tuning_record,
            mock_spark_run,
            mock_get_tuning_result_history,
            mock_update_best_config,
            mock_smac_tuning,
            mock_remove_tuning_result
    ):
        """
        后台复测 发生其他异常
        :return:
        """
        mock_query_tuning_record.return_value = list()
        mock_exam_record = MagicMock()
        mock_exam_record.status = OA_CONF.ExecStatus.success
        mock_smac_tuning.return_value = self.tune_return_val
        with pytest.raises(RuntimeError):
            _single_tuning(load=self.load, retest_way=OA_CONF.RetestWay.backend, tuning_method=self.tuning_method)

        mock_smac_tuning.assert_called_once()
        mock_spark_run.assert_called_once()
        mock_remove_tuning_result.assert_called_once()

    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result_history')
    @patch('omniadvisor.utils.logger.global_logger.info')
    @patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id')
    def test_main_when_load_id_not_exist(
            self,
            mock_query_by_id,
            mock_info,
            mock_get_tuning_result_history,
            mock_create_tuning_record
    ):
        """
        当 load id 不存在时
        :return:
        """
        sys.argv = ['config_tuning.py',
                    '--load-id', None,
                    '--retest-way', OA_CONF.RetestWay.backend,
                    '--tuning-method', self.tuning_method]

        mock_query_by_id.return_value = None
        with pytest.raises(TuningPreconditionError, match='Cannot find load id'):
            main()

    @patch('algo.iterative.tuning.IterativeTunner.tune')
    @patch('omniadvisor.repository.load_repository.LoadRepository.update_test_config')
    @patch('omniadvisor.interface.config_tuning.get_tuning_result_history')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create')
    @patch('omniadvisor.repository.tuning_record_repository.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.repository.load_repository.LoadRepository.query_by_id')
    def test_unified_tuning_when_retest_failed(
            self,
            mock_query_by_id,
            mock_query_tuning_record,
            mock_create_tuning_record,
            mocked_query_perf,
            mock_update_test,
            mock_smac_tuning
    ):
        """
        前台复测
        :return:
        """
        mock_query_tuning_record.return_value = list()
        mock_smac_tuning.return_value = self.tune_return_val
        _single_tuning(load=self.load, retest_way=OA_CONF.RetestWay.hijacking,
                       tuning_method=self.tuning_method)
        mocked_query_perf.assert_called_once()
        mock_update_test.assert_called_once()
        mock_smac_tuning.assert_called_once()
