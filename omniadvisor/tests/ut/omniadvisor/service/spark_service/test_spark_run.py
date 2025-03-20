import json
from unittest.mock import patch, MagicMock, Mock
# 根据实际情况调整导入路径
from omniadvisor.utils.constant import OA_CONF
from omniadvisor.service.spark_service.spark_run import spark_run
from ut.omniadvisor.repository.common import create_test_table, delete_test_table
from ut.omniadvisor.repository.test_repository import TestModel

MOCK_GET_RESPONSE = [
    {"id": "app-1", "name": "App 1"},
    {"id": "app-2", "name": "App 2"}
]

MOCK_SQL_RESPONSE = [
    {"id": "sql-1", "description": "SQL Query 1"},
    {"id": "sql-2", "description": "SQL Query 2"}
]

MOCK_STAGES_RESPONSE = [
    {"stageId": 1, "status": "ACTIVE"},
    {"stageId": 2, "status": "COMPLETED"}
]

MOCK_EXECUTORS_RESPONSE = [
    {"executorId": "exec-1", "memoryUsed": 1024},
    {"executorId": "exec-2", "memoryUsed": 2048}
]


conf = {"another_key": "another_value"}


class TestSparkRun:
    @staticmethod
    def setup_class():
        # 创建表
        create_test_table(TestModel)

    @staticmethod
    def cleanup_class():
        # 删除表
        delete_test_table(TestModel)

    @patch('omniadvisor.service.spark_service.spark_command_reconstruct.spark_command_reconstruct')
    @patch('omniadvisor.repository.task_repository.TaskRepository.create')
    @patch('omniadvisor.service.spark_service.spark_executor.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.utils.utils.trace_data_saver')
    @patch('omniadvisor.repository.task_repository.TaskRepository.update_task_result')
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

        self.assertEqual(result_task, task_mock)
        mock_update_task_result.assert_called_once_with(task_mock, OA_CONF.TaskStatus.success, 10, {
            'sql': f'{OA_CONF.DATA_DIR}/sql_info',
            'stages': f'{OA_CONF.DATA_DIR}/stages_info',
            'executor': f'{OA_CONF.DATA_DIR}/executor_info'
        })

    @patch('omniadvisor.service.spark_service.spark_command_reconstruct.spark_command_reconstruct')
    @patch('omniadvisor.repository.task_repository.TaskRepository.create')
    @patch('omniadvisor.service.spark_service.spark_executor.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.repository.task_repository.TaskRepository.update_task_result')
    def test_spark_run_failure(self, mock_update_task_result, mock_submit_spark_task, mock_create_task,
                               mock_spark_command_reconstruct):
        # 配置mock返回值
        mock_spark_command_reconstruct.return_value = "spark-submit --master local[*]"
        task_mock = MagicMock()
        mock_create_task.return_value = task_mock
        mock_submit_spark_task.return_value = (1, "failure output", {})


        result_task = spark_run(load, conf)

        self.assertEqual(result_task, task_mock)
        mock_update_task_result.assert_called_once_with(task_mock, OA_CONF.TaskStatus.fail, None, {})

    def assertEqual(self, result_task, task_mock):
        pass