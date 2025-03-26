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
    @patch('omniadvisor.service.spark_service.spark_run.TaskRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.trace_data_saver')
    @patch('omniadvisor.service.spark_service.spark_run.TaskRepository.update_task_result')
    @patch('requests.get')
    def test_spark_run_success(self, mock_get, mock_update_task_result, mock_trace_data_saver,
                               mock_submit_spark_task, mock_create_task, mock_spark_command_reconstruct):

        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 配置mock对象的返回值
        mock_spark_command_reconstruct.return_value = "spark-submit --master local[*]"
        task_mock = MagicMock()
        mock_create_task.return_value = task_mock
        mock_submit_spark_task.return_value = (0, "success output", {"application_id": "app_12345", "time_taken": 10})
        mock_trace_data_saver.return_value = f"{OA_CONF.DATA_DIR}/testfile"
        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }

        result_task = spark_run(load_mock, conf)

        assert result_task == task_mock
        mock_update_task_result.assert_called_once_with(task_mock, OA_CONF.TaskStatus.success, 10, {
            'sql': f'{OA_CONF.DATA_DIR}/testfile',
            'stages': f'{OA_CONF.DATA_DIR}/testfile',
            'executor': f'{OA_CONF.DATA_DIR}/testfile'
        })
        mock_update_task_result.assert_called()

    @patch('omniadvisor.service.spark_service.spark_run.spark_command_reconstruct')
    @patch('omniadvisor.service.spark_service.spark_run.TaskRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.trace_data_saver')
    @patch('omniadvisor.service.spark_service.spark_run.TaskRepository.update_task_result')
    @patch('requests.get')
    def test_spark_run_failure(self, mock_get, mock_update_task_result, mock_trace_data_saver,
                               mock_submit_spark_task, mock_create_task, mock_spark_command_reconstruct):
        # 配置mock对象返回值
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_GET_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 配置mock返回值
        mock_spark_command_reconstruct.return_value = "spark-submit --master local[*]"
        task_mock = MagicMock()
        mock_create_task.return_value = task_mock
        mock_submit_spark_task.return_value = (1, "failure output", {})
        mock_trace_data_saver.return_value = f"{OA_CONF.DATA_DIR}/testfile"

        load_mock = MagicMock()
        load_mock.name = "test_name"
        load_mock.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }
        result_task = spark_run(load_mock, conf)

        assert result_task == task_mock
        mock_update_task_result.assert_called_once_with(task_mock, OA_CONF.TaskStatus.fail, None, {
            'sql': f'{OA_CONF.DATA_DIR}/testfile',
            'stages': f'{OA_CONF.DATA_DIR}/testfile',
            'executor': f'{OA_CONF.DATA_DIR}/testfile'
        })