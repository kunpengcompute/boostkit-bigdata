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
        exam_record_mock_1 = MagicMock()
        exam_record_mock_2 = MagicMock()
        exam_record_mock_3 = MagicMock()

        # Set up mock attributes for tuning record
        tuning_record_mock.config = {"param1": "value1", "param2": "value2"}
        tuning_record_mock.method = "method1"
        tuning_record_mock.method_extend = "method_extend1"
        tuning_record_mock.rounds = 1

        # Set up mock attributes for exam record
        exam_record_mock_1.status = OA_CONF.ExecStatus.success
        exam_record_mock_1.runtime = 10
        exam_record_mock_1.trace = "trace_info1"
        exam_record_mock_2.status = OA_CONF.ExecStatus.success
        exam_record_mock_2.runtime = 8
        exam_record_mock_2.trace = "trace_info2"
        exam_record_mock_3.status = OA_CONF.ExecStatus.success
        exam_record_mock_3.runtime = 12
        exam_record_mock_3.trace = "trace_info3"

        return load, config, tuning_record_mock, [exam_record_mock_1, exam_record_mock_2, exam_record_mock_3]

    @patch('omniadvisor.service.tuning_result.tuning_result.TuningRecordRepository.query_by_load_and_config')
    @patch('omniadvisor.service.tuning_result.tuning_result.ExamRecordRepository.query_by_load_and_config')
    def test_get_tuning_result(self, mock_task_query, mock_tuning_record_query, setup_mocks):
        # 强制设定复测次数为3，用于测试
        OA_CONF.tuning_retest_times = 3

        # 准备测试数据
        load, config, tuning_record_mock, exam_record_mock_list = setup_mocks
        mock_tuning_record_query.return_value = [tuning_record_mock]
        mock_task_query.return_value = exam_record_mock_list

        result = get_tuning_result(load, config)

        assert isinstance(result, TuningResult)
        assert result.config == {"param1": "value1", "param2": "value2"}
        assert result.method == "method1"
        assert result.method_extend == "method_extend1"
        assert result.rounds == 1
        assert result.runtime == 10

    def test_tuning_result_property(self, setup_mocks):
        # 强制设定复测次数为3，用于测试
        OA_CONF.tuning_retest_times = 3

        # 准备测试数据
        _, _, tuning_record_mock, exam_record_mock_list = setup_mocks
        exam_record_mock_list[-1].status = OA_CONF.ExecStatus.running

        tuning_result = TuningResult(
            tuning_record=tuning_record_mock,
            exam_records=exam_record_mock_list
        )

        assert tuning_result.config == {"param1": "value1", "param2": "value2"}
        assert tuning_result.method == 'method1'
        assert tuning_result.method_extend == "method_extend1"
        assert tuning_result.rounds == 1
        assert tuning_result.status == OA_CONF.ExecStatus.running
        assert tuning_result.runtime == 9
        assert tuning_result.trace == "trace_info2"

        # 准备测试数据
        exam_record_mock_list[-1].status = OA_CONF.ExecStatus.fail
        tuning_result = TuningResult(
            tuning_record=tuning_record_mock,
            exam_records=exam_record_mock_list
        )
        assert tuning_result.status == OA_CONF.ExecStatus.fail
        assert tuning_result.runtime == OA_CONF.exec_fail_return_runtime
        assert tuning_result.trace == OA_CONF.exec_fail_return_trace

