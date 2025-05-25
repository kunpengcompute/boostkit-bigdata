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
    @patch('omniadvisor.service.spark_service.spark_run.SparkCMDParser.reconstruct_cmd')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.parser_spark_output')
    @patch('omniadvisor.service.spark_service.spark_run.save_trace_data')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.update_exam_result')
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch('requests.get')
    def test_spark_run_success(self, mock_get, mock_process, mock_update_exam_result, mock_save_trace_data, mock_parser_spark_output,
                               mock_submit_spark_task, mock_create_exam_record, mock_reconstruct_cmd):

        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response
        mock_instance = MagicMock()
        mock_process.return_value = mock_instance

        # 配置mock对象的返回值
        mock_reconstruct_cmd.return_value = "spark-submit --master local[*]"
        exam_record_mock = MagicMock()
        mock_create_exam_record.return_value = exam_record_mock
        task_update_mock = MagicMock()
        mock_update_exam_result.return_value = task_update_mock
        mock_submit_spark_task.return_value = (0, "success output\napplication_id:app_12345\ntime_taken: 10")
        mock_parser_spark_output.return_value = ["spark-submit --master local[*]", "app_12345", 10]
        mock_save_trace_data.return_value = f"{OA_CONF.data_dir}/testfile"
        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }
        load_mock.default_config = {"another_key": "another_value"}

        result_exam_record, spark_out_put = spark_run(load_mock, conf)

        assert result_exam_record == task_update_mock
        mock_update_exam_result.assert_called()

        # 断言Process被调用
        mock_process.assert_called_once()
        mock_instance.start.assert_called_once()



    @patch('omniadvisor.service.spark_service.spark_run.SparkCMDParser.reconstruct_cmd')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.save_trace_data')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.update_exam_result')
    @patch('requests.get')
    def test_spark_run_failed(self, mock_get, mock_update_exam_result, mock_save_trace_data,
                               mock_submit_spark_task, mock_create_exam_record, mock_reconstruct_cmd):
        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 配置mock返回值
        mock_reconstruct_cmd.return_value = "spark-submit --master local[*]"
        exam_record_mock = MagicMock()
        mock_create_exam_record.return_value = exam_record_mock
        task_update_mock = MagicMock()
        mock_update_exam_result.return_value = task_update_mock
        mock_submit_spark_task.return_value = (1, "failure output")
        mock_save_trace_data.return_value = f"{OA_CONF.data_dir}/testfile"

        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }
        load_mock.default_config = {"another_key": "another_value"}
        result_exam_record, spark_out_put = spark_run(load_mock, conf)

        assert result_exam_record == task_update_mock
        mock_update_exam_result.assert_called_once_with(
            exam_record=exam_record_mock,
            status=OA_CONF.ExecStatus.fail,
            runtime=OA_CONF.exec_fail_return_runtime,
            trace=OA_CONF.exec_fail_return_trace,
        )