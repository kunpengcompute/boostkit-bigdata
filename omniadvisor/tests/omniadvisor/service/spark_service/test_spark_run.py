import json
from unittest.mock import (
    patch,
    MagicMock,
    Mock
)
from datetime import datetime

from common.constant import OA_CONF
from omniadvisor.service.spark_service.spark_run import spark_run


class TestSparkRun:
    """
    Spark Run 测试类
    """

    def setup_method(self):
        """
        测试方法开始前构建
        """
        # 构建测试数据
        self.conf = {"another_key": "another_value"}
        # 构建load mock对象
        self.mock_load = MagicMock()
        self.mock_load.name = "test_name"
        self.mock_load.exec_attr = {
            "attr1": "value1",
            "attr2": "value2"
        }
        self.mock_load.default_config = {"another_key": "another_value"}
        # 构建exec_result mock对象
        self.mock_exec_result = MagicMock
        self.mock_exec_result.exitcode = 0
        self.mock_exec_result.app_id = 'application_123456'
        self.mock_exec_result.duration = 24.2
        self.mock_exec_result.start_time = datetime(2023, 10, 5, 14, 30, 15)
        self.mock_exec_result.end_time = datetime(2023, 10, 5, 14, 30, 15)
        self.mock_exec_result.output = 'output'
        # 构建response mock对象
        self.mock_response = Mock()
        self.mock_response.status_code = 200
        response = [
            {"id": "app-1", "name": "App 1"},
            {"id": "app-2", "name": "App 2"}
        ]
        self.mock_response.text = json.dumps(response)

    @patch('omniadvisor.service.spark_service.spark_run.SparkFetcher.get_spark_runtime_by_app')
    @patch('omniadvisor.service.spark_service.spark_run.SparkCMDParser.reconstruct_cmd')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.save_trace_data')
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch('requests.get')
    def test_spark_run_success(self, mock_requests_get, mock_multiprocess, mock_save_trace_data, mock_submit_spark_task,
                               mock_create_exam_record, mock_reconstruct_cmd, mock_get_spark_runtime):
        """
        测试 spark_run 方法
        """
        # 配置requests mock对象
        mock_requests_get.return_value = self.mock_response
        # 配置multiprocessing mock对象
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process
        # 配置reconstruct_cmd mock对象
        mock_reconstruct_cmd.return_value = ['spark-submit', '--class', 'MyClass', 'my_app.jar']
        # 配置create_exam_record mock对象
        mock_exam_record = MagicMock()
        mock_create_exam_record.return_value = mock_exam_record
        # 配置submit_spark_task mock对象
        mock_submit_spark_task.return_value = self.mock_exec_result
        # 配置save_trace_data mock对象
        mock_save_trace_data.return_value = f'{OA_CONF.data_dir}/testfile'
        # 配置get_spark_runtime_by_app
        mock_get_spark_runtime.return_value = 20.4

        # 方法调用
        result_exam_record, spark_output = spark_run(self.mock_load, self.conf, wait_for_trace=False, exec_in_isolation=False)
        # 结果验证
        assert result_exam_record == mock_exam_record
        assert spark_output == self.mock_exec_result.output
        mock_submit_spark_task.assert_called_once()
        assert spark_output == self.mock_exec_result.output
        mock_create_exam_record.assert_called_once_with(
            load=self.mock_load,
            config=self.conf,
            status=OA_CONF.ExecStatus.success,
            runtime=self.mock_exec_result.duration,
            start_time=self.mock_exec_result.start_time,
            end_time=self.mock_exec_result.end_time,
            app_id=self.mock_exec_result.app_id
        )
        mock_multiprocess.assert_called_once()
        mock_process.start.assert_called_once()

    @patch('omniadvisor.service.spark_service.spark_run.SparkCMDParser.reconstruct_cmd')
    @patch('omniadvisor.service.spark_service.spark_run.ExamRecordRepository.create')
    @patch('omniadvisor.service.spark_service.spark_run.SparkExecutor.submit_spark_task')
    @patch('omniadvisor.service.spark_service.spark_run.save_trace_data')
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch('requests.get')
    def test_spark_run_failed(self, mock_requests_get, mock_multiprocess, mock_save_trace_data, mock_submit_spark_task,
                              mock_create_exam_record, mock_reconstruct_cmd):
        # 配置requests mock对象
        mock_requests_get.return_value = self.mock_response
        # 配置reconstruct_cmd mock对象
        mock_reconstruct_cmd.return_value = ['spark-submit', '--class', 'MyClass', 'my_app.jar']
        # 配置create_exam_record mock对象
        mock_exam_record = MagicMock()
        mock_create_exam_record.return_value = mock_exam_record
        # 配置submit_spark_task mock对象
        self.mock_exec_result.exitcode = 1
        self.mock_exec_result.duration = OA_CONF.exec_fail_return_runtime
        mock_submit_spark_task.return_value = self.mock_exec_result
        # 配置save_trace_data mock对象
        mock_save_trace_data.return_value = f'{OA_CONF.data_dir}/testfile'

        result_exam_record, spark_output = spark_run(self.mock_load, self.conf)

        assert result_exam_record == mock_exam_record
        assert spark_output == self.mock_exec_result.output
        mock_create_exam_record.assert_called_once_with(
            load=self.mock_load,
            config=self.conf,
            status=OA_CONF.ExecStatus.fail,
            runtime=OA_CONF.exec_fail_return_runtime,
            start_time=self.mock_exec_result.start_time,
            end_time=self.mock_exec_result.end_time,
            app_id=OA_CONF.exec_fail_return_app_id
        )
        mock_multiprocess.assert_not_called()
