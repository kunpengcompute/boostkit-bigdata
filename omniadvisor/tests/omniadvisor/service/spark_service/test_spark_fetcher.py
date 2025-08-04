import json
import pytest
import requests
import unittest
from unittest import mock
from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from common.constant import OA_CONF
from datetime import datetime


history_server_url = OA_CONF.spark_history_rest_url
spark_fetcher = SparkFetcher(history_server_url)

# 示例数据
MOCK_APPS_RESPONSE = [
    {"id": "app-1", "name": "App 1"},
    {"id": "app-2", "name": "App 2"}
]

MOCK_APP_DETAILS_RESPONSE = {
    "id": "app-1",
    "name": "App 1",
    "state": "RUNNING"
}

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

MOCK_JOBS_RESPONSE = [
    {'submissionTime': '2025-07-28T08:30:32.374GMT', 'completionTime': '2025-07-28T08:31:32.374GMT'},
    {'submissionTime': '2025-07-28T08:32:32.374GMT', 'completionTime': '2025-07-28T08:33:32.374GMT'}
]


class TestSparkFetcher(unittest.TestCase):
    @mock.patch('requests.get')
    def test_get_spark_apps(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_APPS_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        apps = spark_fetcher.get_spark_apps()
        assert apps == MOCK_APPS_RESPONSE

    @mock.patch('requests.get')
    def test_get_application_details(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_APP_DETAILS_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        app_details = spark_fetcher.get_application_details("app-1")
        assert app_details == MOCK_APP_DETAILS_RESPONSE

    @mock.patch('requests.get')
    def test_get_spark_sql_by_app(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_SQL_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        sql_info = spark_fetcher.get_spark_sql_by_app("app-1")
        assert sql_info == MOCK_SQL_RESPONSE

    @mock.patch('requests.get')
    def test_get_spark_stages_by_app(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_STAGES_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        stages_info = spark_fetcher.get_spark_stages_by_app("app-1")
        assert stages_info == MOCK_STAGES_RESPONSE

    @mock.patch('requests.get')
    def test_get_spark_executor_by_app(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_EXECUTORS_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        executors_info = spark_fetcher.get_spark_executor_by_app("app-1")
        assert executors_info == MOCK_EXECUTORS_RESPONSE

    @mock.patch('requests.get')
    def test_get_spark_jobs_by_app(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(MOCK_JOBS_RESPONSE)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法并断言结果
        executors_info = spark_fetcher.get_spark_jobs_by_app("app-1")
        assert executors_info == MOCK_JOBS_RESPONSE

    @mock.patch('requests.get')
    def test_get_spark_runtime_by_app_normal_case(self, mock_get):
        """测试正常情况：偶数个时间戳，格式正确"""
        # 模拟返回的 jobs 数据
        mock_jobs = [
            {'submissionTime': '2025-07-28T08:30:32.374GMT', 'completionTime': '2025-07-28T08:31:32.374GMT'},
            {'submissionTime': '2025-07-28T08:32:32.374GMT', 'completionTime': '2025-07-28T08:33:32.374GMT'}
        ]

        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_jobs)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 调用方法
        runtime = spark_fetcher.get_spark_runtime_by_app("app-1")

        # 验证结果
        expected_runtime = (datetime.strptime('2025-07-28T08:33:32.374', "%Y-%m-%dT%H:%M:%S.%f") -
                            datetime.strptime('2025-07-28T08:30:32.374', "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()
        assert runtime == expected_runtime

    @mock.patch('requests.get')
    def test_get_spark_runtime_by_app_no_jobs(self, mock_get):
        """测试异常情况：无 jobs 数据（空列表）"""
        # 模拟返回的 jobs 数据（空列表）
        mock_jobs = []

        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_jobs)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 验证是否抛出异常
        with self.assertRaises(ValueError) as context:
            spark_fetcher.get_spark_runtime_by_app("app-1")
        self.assertIn("No job information found for app_id=app-1", str(context.exception))

    @mock.patch('requests.get')
    def test_get_spark_runtime_by_app_incomplete_jobs(self, mock_get):
        """测试异常情况：jobs 数据中的提交与完成时间信息不完整"""
        # 模拟返回的 jobs 数据（空列表）
        mock_jobs = [
            {'submissionTime': '2025-07-28T08:30:32.374GMT'},
        ]

        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_jobs)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 验证是否抛出异常
        with self.assertRaises(KeyError) as context:
            spark_fetcher.get_spark_runtime_by_app("app-1")
        self.assertIn("missing 'completionTime' or 'submissionTime'", str(context.exception))

    @mock.patch('requests.get')
    def test_http_error_handling(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 404

        mock_response.text = json.dumps(MOCK_APPS_RESPONSE)

        # 配置raise_for_status_code方法 使其根据状态码抛出HTTPError
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP error occurred",
                                                                                   response=mock_response)

        # 配置mock对象的返回值
        mock_get.return_value = mock_response

        # 断言HTTPError被抛出
        with pytest.raises(requests.exceptions.HTTPError):
            spark_fetcher.get_spark_apps()

        with pytest.raises(requests.exceptions.HTTPError):
            spark_fetcher.get_application_details("app-1")

        with pytest.raises(requests.exceptions.HTTPError):
            spark_fetcher.get_spark_sql_by_app("app-1")

        with pytest.raises(requests.exceptions.HTTPError):
            spark_fetcher.get_spark_stages_by_app("app-1")

        # get_spark_executor_by_app 方法抛出的是 Exception
        with pytest.raises(requests.exceptions.HTTPError):
            spark_fetcher.get_spark_executor_by_app("app-1")



