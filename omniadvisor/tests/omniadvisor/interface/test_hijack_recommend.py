import unittest
import pytest
from unittest.mock import MagicMock, patch
from omniadvisor.interface.hijack_recommend import hijack_recommend


# 测试代码
class TestHijackRecommend(unittest.TestCase):

    # 场景 1: 正常流程，Spark 执行成功
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_successful_execution(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "test_job"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = user_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_query_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status="success"), "job output")

        hijack_recommend(argv)
        mock_spark_run.assert_called_once_with(load, exec_config)
        mock_process_config.assert_not_called()

    # 场景 2: 执行失败 + 进入安全机制
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_fallback_to_safe_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job_name"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = {"spark.executor.memory": "8g"}

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_query_load.return_value = load
        mock_get_config.return_value = exec_config

        mock_spark_run.side_effect = [
            (MagicMock(status="fail"), "fail output"),
            (MagicMock(status="success"), "safe output")
        ]

        hijack_recommend(argv)
        assert mock_spark_run.call_count == 2
        mock_process_config.assert_not_called()

    # 场景 3: 执行失败 + 不进入安全机制
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_failed_user_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job_name"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = user_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_query_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status="fail"), "fail output")

        hijack_recommend(argv)
        # 不进入安全机制的情况call_count的值为1
        assert mock_spark_run.call_count == 1
        mock_process_config.assert_not_called()

    # 场景 4: 缺少任务名称
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_missing_task_name(self, mock_parse_cmd):
        argv = ["--conf", "spark.executor.memory=4g"]
        mock_parse_cmd.return_value = ({}, {"spark.executor.memory": "4g"})

        with pytest.raises(ValueError, match="Task name not in Spark submit cmd"):
            hijack_recommend(argv)

    # 场景 5: 执行的是 test_config，触发 _process_load_config
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_process_test_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        user_config = {"spark.executor.memory": "4g"}
        test_config = {"spark.executor.memory": "6g"}
        load = MagicMock(test_config=test_config, default_config=user_config)
        exec_config = test_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_query_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status="success"), "job output")

        hijack_recommend(argv)
        mock_process_config.assert_called_once_with(load=load)

    # 场景 6: 命令解析异常
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_parse_cmd_raises(self, mock_parse_cmd):
        mock_parse_cmd.side_effect = Exception("parse error")
        with pytest.raises(Exception, match="parse error"):
            hijack_recommend(["--conf", "x"])

    # 场景 7: 创建 Load 异常
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_query_create_load_raises(self, mock_parse_cmd, mock_query_load):
        mock_parse_cmd.return_value = ({"name": "job"}, {})
        mock_query_load.side_effect = Exception("db error")

        with pytest.raises(Exception, match="db error"):
            hijack_recommend(["--conf", "x"])

    # 场景 8: spark_run 抛异常
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_spark_run_raises(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=config)

        mock_parse_cmd.return_value = (exec_attr, config)
        mock_query_load.return_value = load
        mock_get_config.return_value = config
        mock_spark_run.side_effect = RuntimeError("spark failed")

        with pytest.raises(RuntimeError, match="spark failed"):
            hijack_recommend(argv)

    # 场景 9: _process_load_config 内部依赖出错
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_process_load_config_raises(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config=config, default_config=config)

        mock_parse_cmd.return_value = (exec_attr, config)
        mock_query_load.return_value = load
        mock_get_config.return_value = config
        mock_spark_run.return_value = (MagicMock(status="success"), "output")
        mock_process_config.side_effect = RuntimeError("process failed")

        with pytest.raises(RuntimeError, match="process failed"):
            hijack_recommend(argv)
