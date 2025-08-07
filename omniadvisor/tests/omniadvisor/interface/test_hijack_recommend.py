import unittest
import pytest
from unittest.mock import MagicMock, patch

from common.constant import OA_CONF
from omniadvisor.interface.hijack_recommend import hijack_recommend, _process_load_config


# 测试代码
class TestHijackRecommend(unittest.TestCase):

    # 场景 1: 正常流程，Spark 执行成功
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_successful_execution(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run,
                                  mock_process_config):
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
        mock_spark_run.assert_called_once_with(load=load, config=exec_config, wait_for_trace=False)
        mock_process_config.assert_not_called()

    # 场景 2: 执行失败 + 进入安全机制
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_fallback_to_safe_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run,
                                     mock_process_config, mock_multiprocess):
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
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process

        with pytest.raises(RuntimeError,
                           match="Spark execute failed, ready to activate security protection mechanism."):
            hijack_recommend(argv)
        assert mock_spark_run.call_count == 1

    # 场景 3: 执行失败 + 不进入安全机制
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_failed_user_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run,
                                mock_process_config):
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
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._query_or_create_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_process_test_config(self, mock_parse_cmd, mock_query_load, mock_get_config, mock_spark_run,
                                 mock_process_config, mock_multiprocess):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        user_config = {"spark.executor.memory": "4g"}
        test_config = {"spark.executor.memory": "6g"}
        load = MagicMock(test_config=test_config, default_config=user_config)
        exec_config = test_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_query_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status=OA_CONF.ExecStatus.success), "job output")
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process

        hijack_recommend(argv)

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

    # 场景9：测试process_load_config函数
    @patch("omniadvisor.interface.hijack_recommend.float_format")
    @patch("omniadvisor.interface.hijack_recommend.LoadRepository.update_test_config")
    @patch("omniadvisor.interface.hijack_recommend.get_tuning_result_history")
    @patch("omniadvisor.interface.hijack_recommend.get_tuning_result")
    def test_process_load_config(self, mock_get_tuning_result, mock_get_tuning_result_history, mock_update_test_config,
                                 mock_float_format):
        best_config = {'param1': 'value1'}
        test_config = {'param2': 'value2'}

        load = MagicMock()
        load.best_config = best_config
        load.test_config = test_config
        mock_tuning_result = MagicMock()
        mock_get_tuning_result.return_value = mock_tuning_result
        mock_tuning_result_history = MagicMock()
        mock_get_tuning_result_history.return_value = mock_tuning_result_history

        # best_config执行失败场景
        mock_tuning_result.status = OA_CONF.TuningResultStatus.fail
        _process_load_config(load=load, config=best_config)
        mock_update_test_config.assert_not_called()
        mock_tuning_result_history.refresh_best_config.assert_called_once()

        # best_config执行成功场景
        mock_update_test_config.call_count = 0
        mock_tuning_result_history.refresh_best_config.call_count = 0
        mock_tuning_result.status = OA_CONF.TuningResultStatus.success
        _process_load_config(load=load, config=best_config)
        mock_update_test_config.assert_not_called()
        mock_tuning_result_history.refresh_best_config.assert_not_called()

        # test_config复测失败场景
        mock_update_test_config.call_count = 0
        mock_tuning_result_history.refresh_best_config.call_count = 0
        mock_tuning_result.status = OA_CONF.TuningResultStatus.fail
        _process_load_config(load=load, config=test_config)
        mock_update_test_config.assert_called_once()
        mock_tuning_result_history.refresh_best_config.assert_not_called()

        # test_config复测成功场景
        mock_update_test_config.call_count = 0
        mock_tuning_result_history.refresh_best_config.call_count = 0
        mock_tuning_result.status = OA_CONF.TuningResultStatus.success
        _process_load_config(load=load, config=test_config)
        mock_update_test_config.assert_called_once()
        mock_tuning_result_history.refresh_best_config.assert_called_once()

        # test_config复测未完毕场景
        mock_update_test_config.call_count = 0
        mock_tuning_result_history.refresh_best_config.call_count = 0
        mock_tuning_result.status = OA_CONF.TuningResultStatus.running
        _process_load_config(load=load, config=test_config)
        mock_update_test_config.assert_not_called()
        mock_tuning_result_history.refresh_best_config.assert_not_called()
