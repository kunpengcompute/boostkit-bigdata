import json
import pytest
import requests
from unittest import mock
from omniadvisor.service.spark_fetcher import SparkFetcher
from constants import COMMON_CONFIG


history_server_url = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")
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


class TestSparkFetcher:
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
    def test_http_error_handling(self, mock_get):
        # 配置mock对象返回值
        mock_response = mock.Mock()
        mock_response.status_code = 404

        mock_response.text = json.dumps(MOCK_APPS_RESPONSE)

        # 配置raise_for_status_code方法 使其根据状态码抛出HTTPError
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP error occurred", response=mock_response)

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



