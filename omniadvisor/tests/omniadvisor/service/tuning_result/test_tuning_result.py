import pytest
from unittest.mock import patch, MagicMock

from omniadvisor.repository.model.load import Load
from common.constant import OA_CONF
from omniadvisor.service.tuning_result.tuning_result import get_tuning_result, TuningResult


class TestTuningResult:
    """
    TuningResult测试类
    """
    @pytest.fixture
    def setup_mocks(self):
        load = MagicMock(spec=Load)
        config = {"param1": "value1", "param2": "value2"}

        # Mocking TuningRecord and ExamRecord
        tuning_record_mock = MagicMock()
        task_mock = MagicMock()

        # Set up mock attributes for tuning record
        tuning_record_mock.config = {"param1": "value1", "param2": "value2"}
        tuning_record_mock.method = "method1"
        tuning_record_mock.method_extend = "method_extend1"
        tuning_record_mock.rounds = 1

        # Set up mock attributes for exam record
        task_mock.status = OA_CONF.ExecStatus.success
        task_mock.runtime = 10
        task_mock.trace = "trace_info"

        return load, config, tuning_record_mock, task_mock

    @patch('omniadvisor.service.tuning_result.tuning_result.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.service.tuning_result.tuning_result.ExamRecordRepository.query_by_load_and_config')
    def test_get_tuning_result(self, mock_task_query, mock_tuning_record_query, setup_mocks):
        load, config, tuning_record_mock, task_mock = setup_mocks

        # Set return values for mocks
        mock_tuning_record_query.return_value = [tuning_record_mock]
        mock_task_query.return_value = [task_mock]

        result = get_tuning_result(load, config)

        assert isinstance(result, TuningResult)
        assert result.config == {"param1": "value1", "param2": "value2"}
        assert result.method == "method1"
        assert result.method_extend == "method_extend1"
        assert result.rounds == 1
        assert result.runtime == 10

    def test_tuning_result_property(self, setup_mocks):
        _, _, tuning_record_mock, _ = setup_mocks

        # 准备测试数据
        task_mock1 = MagicMock()
        task_mock1.status = OA_CONF.ExecStatus.success
        task_mock1.runtime = 10
        task_mock1.trace = "trace_info1"
        task_mock2 = MagicMock()
        task_mock2.status = OA_CONF.ExecStatus.success
        task_mock2.runtime = 15
        task_mock2.trace = "trace_info2"
        task_mock3 = MagicMock()
        task_mock3.status = OA_CONF.ExecStatus.fail
        task_mock3.runtime = 10
        task_mock3.trace = "trace_info3"
        # 强制设定复测次数为3，用于测试
        OA_CONF.tuning_retest_times = 3

        tasks = [task_mock1, task_mock2]
        tuning_result = TuningResult(
            tuning_record=tuning_record_mock,
            exam_records=tasks
        )

        assert tuning_result.config == {"param1": "value1", "param2": "value2"}
        assert tuning_result.method == 'method1'
        assert tuning_result.method_extend == "method_extend1"
        assert tuning_result.rounds == 1
        assert tuning_result.status == OA_CONF.ExecStatus.running
        assert tuning_result.runtime == 12.5
        assert tuning_result.trace == "trace_info1"

        tasks = [task_mock1, task_mock2, task_mock3]
        tuning_result = TuningResult(
            tuning_record=tuning_record_mock,
            exam_records=tasks
        )
        assert tuning_result.status == OA_CONF.ExecStatus.fail
        assert tuning_result.runtime == 12.5
        assert tuning_result.trace == "trace_info1"

