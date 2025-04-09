import json
from unittest.mock import patch, MagicMock, Mock
# 根据实际情况调整导入路径
from common.constant import OA_CONF
from omniadvisor.service.spark_service.spark_run import spark_run

MOCK_GET_RESPONSE = [
    {"id": "app-1", "name": "App 1"},
    {"id": "app-2", "name": "App 2"}
]


conf = {"another_key": "another_value"}


class TestSparkRun:
    @patch('omniadvisor.service.spark_service.spark_run.spark_command_reconstruct')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.parser_spark_output')
    @patch('omniadvisor.service.spark_service.spark_run.trace_data_saver')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.update_exam_result')
    @patch('requests.get')
    def test_spark_run_success(self, mock_get, mock_update_exam_result, mock_trace_data_saver, mock_parser_spark_output,
                               mock_submit_spark_task, mock_create_exam_record, mock_spark_command_reconstruct):

        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 配置mock对象的返回值
        mock_spark_command_reconstruct.return_value = "spark-submit --master local[*]"
        exam_record_mock = MagicMock()
        mock_create_exam_record.return_value = exam_record_mock
        task_update_mock = MagicMock()
        mock_update_exam_result.return_value = task_update_mock
        mock_submit_spark_task.return_value = (0, "success output\napplication_id:app_12345\ntime_taken: 10")
        mock_parser_spark_output.return_value = ["spark-submit --master local[*]", "app_12345", 10]
        mock_trace_data_saver.return_value = f"{OA_CONF.data_dir}/testfile"
        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }

        result_exam_record = spark_run(load_mock, conf)

        assert result_exam_record == task_update_mock
        mock_update_exam_result.assert_called_once_with(exam_record_mock, OA_CONF.ExecStatus.success, 10, {
            'sql': f'{OA_CONF.data_dir}/testfile',
            'stages': f'{OA_CONF.data_dir}/testfile',
            'executor': f'{OA_CONF.data_dir}/testfile'
        }, "success output\napplication_id:app_12345\ntime_taken: 10")
        mock_update_exam_result.assert_called()

    @patch('omniadvisor.service.spark_service.spark_run.spark_command_reconstruct')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.trace_data_saver')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.update_exam_result')
    @patch('requests.get')
    def test_spark_run_success(self, mock_get, mock_update_exam_result, mock_trace_data_saver,
                               mock_submit_spark_task, mock_create_exam_record, mock_spark_command_reconstruct):
        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 配置mock返回值
        mock_spark_command_reconstruct.return_value = "spark-submit --master local[*]"
        exam_record_mock = MagicMock()
        mock_create_exam_record.return_value = exam_record_mock
        task_update_mock = MagicMock()
        mock_update_exam_result.return_value = task_update_mock
        mock_submit_spark_task.return_value = (1, "failure output")
        mock_trace_data_saver.return_value = f"{OA_CONF.data_dir}/testfile"

        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }
        result_exam_record = spark_run(load_mock, conf)

        assert result_exam_record == task_update_mock
        mock_update_exam_result.assert_called_once_with(
            exam_record=exam_record_mock,
            status=OA_CONF.ExecStatus.fail,
            runtime=OA_CONF.exec_fail_return_runtime,
            trace=OA_CONF.exec_fail_return_trace,
            output="failure output"
        )