import pytest
from unittest.mock import patch, MagicMock
from omniadvisor.repository.model.load import Load
from omniadvisor.service.tuning_result.tuning_result import TuningResult
from omniadvisor.service.tuning_result.tuning_result_history import get_tuning_result_history, TuningResultHistory
from common.constant import OA_CONF


class TestTuningResultHistory:
    """
    TuningHistoryResult测试类
    """
    @pytest.fixture
    def setup_mocks(self):
        load = MagicMock(spec=Load)

        # Mocking tuning records and tuning results
        tuning_record_mock = MagicMock()
        tuning_record_mock.config = {"param1": "value1"}

        tuning_result_mock = MagicMock(spec=TuningResult)
        tuning_result_mock.config = tuning_record_mock.config
        tuning_result_mock.runtime = 10
        tuning_result_mock.status = OA_CONF.ExecStatus.success

        return load, tuning_record_mock, tuning_result_mock

    @patch('omniadvisor.service.tuning_result.tuning_result_history.TuningRecordRepository.query_by_load')
    @patch('omniadvisor.service.tuning_result.tuning_result_history.get_tuning_result')
    def test_get_tuning_result_history(self, mock_get_tuning_result, mock_query_by_load, setup_mocks):
        load, tuning_record_mock, tuning_result_mock = setup_mocks

        # Set return values for mocks
        mock_query_by_load.return_value = [tuning_record_mock]
        mock_get_tuning_result.return_value = tuning_result_mock

        result = get_tuning_result_history(load)

        assert isinstance(result, TuningResultHistory)
        assert result._load == load
        assert len(result._tuning_results) == 1
        assert result._tuning_results[0] == tuning_result_mock

    def test_tuning_result_history_config_perf_pairs(self, setup_mocks):
        load, tuning_record_mock, tuning_result_mock = setup_mocks

        tuning_result_mock2 = MagicMock(spec=TuningResult)
        tuning_result_mock2.config = {"param2": "value2"}
        tuning_result_mock2.runtime = 5
        tuning_result_mock2.status = OA_CONF.ExecStatus.success

        tuning_result_history = TuningResultHistory(load, [tuning_result_mock, tuning_result_mock2])


        assert tuning_result_history.config_perf_pairs == [
            (tuning_record_mock.config, tuning_result_mock.runtime),
            ({"param2": "value2"}, 5)
        ]
        assert tuning_result_history.best_config == {"param2": "value2"}
