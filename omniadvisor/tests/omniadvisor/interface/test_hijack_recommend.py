from unittest.mock import MagicMock, patch

import pytest

from common.constant import OA_CONF
from common.exceptions import DuplicateEntryError
from omniadvisor.interface.hijack_recommend import hijack_recommend, _process_load_config, _create_or_update_load, \
    _remove_time, _calculate_hash_value


# 测试代码
class TestHijackRecommend:

    # 场景 1: 正常流程，Spark 执行成功
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_successful_execution(self, mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run,
                                  mock_process_config):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "test_job"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = user_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_create_or_update_load.return_value = load
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
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_fallback_to_safe_config(self, mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run,
                                     mock_process_config, mock_multiprocess):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job_name"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = {"spark.executor.memory": "8g"}

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_create_or_update_load.return_value = load
        mock_get_config.return_value = exec_config

        mock_spark_run.side_effect = [
            (MagicMock(status="fail"), "fail output"),
            (MagicMock(status="success"), "safe output")
        ]
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process

        hijack_recommend(argv)
        assert mock_spark_run.call_count == 2
        mock_process.start.assert_called_once()

    # 场景 3: 执行失败 + 不进入安全机制
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_failed_user_config(self, mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run,
                                mock_process_config):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job_name"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = user_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_create_or_update_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status="fail"), "fail output")

        hijack_recommend(argv)
        # 不进入安全机制的情况call_count的值为1
        assert mock_spark_run.call_count == 1
        mock_process_config.assert_not_called()

    # 场景 5: 执行的是 test_config，触发 _process_load_config
    @patch('omniadvisor.service.spark_service.spark_run.multiprocessing.Process')
    @patch("omniadvisor.interface.hijack_recommend._process_load_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_process_test_config(self, mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run,
                                 mock_process_config, mock_multiprocess):
        argv = ["--class", "Job", "--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        user_config = {"spark.executor.memory": "4g"}
        test_config = {"spark.executor.memory": "6g"}
        load = MagicMock(test_config=test_config, default_config=user_config)
        exec_config = test_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_create_or_update_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_run.return_value = (MagicMock(status="success"), "job output")
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process

        hijack_recommend(argv)
        mock_process.start.assert_called_once()

    # 场景 6: 命令解析异常
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_parse_cmd_raises(self, mock_parse_cmd):
        mock_parse_cmd.side_effect = Exception("parse error")
        with pytest.raises(Exception, match="parse error"):
            hijack_recommend(["--conf", "x"])

    # 场景 7: 创建 Load 异常
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_query_create_load_raises(self, mock_parse_cmd, mock_create_or_update_load):
        mock_parse_cmd.return_value = ({"name": "job"}, {})
        mock_create_or_update_load.side_effect = Exception("db error")

        with pytest.raises(Exception, match="db error"):
            hijack_recommend(["--conf", "x"])

    # 场景 8: spark_run 抛异常
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    def test_spark_run_raises(self, mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job"}
        config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=config)

        mock_parse_cmd.return_value = (exec_attr, config)
        mock_create_or_update_load.return_value = load
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

    @patch("omniadvisor.repository.load_repository.LoadRepository.query_by_hash_value")
    @patch("omniadvisor.repository.tuning_record_repository.TuningRecordRepository.create")
    @patch("omniadvisor.repository.load_repository.LoadRepository.create")
    def test_create_or_update_load_when_create_success(self, mock_load_create, mock_tuning_record_create,
                                                       mock_query_by_hash_value):
        name = "test_exec"
        exec_attr = {"name": name, "cmd": "run"}
        default_config = {"param": 123}
        hash_value = _calculate_hash_value(exec_attr, default_config)
        mock_load_create.return_value = "created_object"
        mock_query_by_hash_value.return_value = []
        result = _create_or_update_load(exec_attr, default_config)
        mock_load_create.assert_called_once_with(name, exec_attr, default_config, hash_value)
        mock_tuning_record_create.assert_called_once()
        assert result == "created_object"

    # 测试：创建抛出异常 -> 更新路径
    @patch("omniadvisor.repository.load_repository.LoadRepository.update_name_and_exec_attr")
    @patch("omniadvisor.repository.load_repository.LoadRepository.query_by_hash_value")
    @patch("omniadvisor.repository.load_repository.LoadRepository.create", side_effect=DuplicateEntryError)
    def test_create_or_update_load_when_create_failed(self, mock_create, mock_query_by_hash_value, mock_update):
        name = "test_exec"
        exec_attr = {"name": name, "cmd": "run"}
        default_config = {"param": 123}
        hash_value = _calculate_hash_value(exec_attr, default_config)
        mock_load = MagicMock()

        mock_query_by_hash_value.return_value = [mock_load]
        mock_update.return_value = "updated_object"

        result = _create_or_update_load(exec_attr, default_config)

        mock_query_by_hash_value.assert_called_once_with(hash_value)
        mock_update.assert_called_once_with(mock_load, name, exec_attr)
        assert result == "updated_object"

    def test_remove_date_yyyy_mm_dd(self):
        input_text = "任务截止时间是 2025-08-01"
        expected = "任务截止时间是 {date}"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_yyyy_mm_dd_hour(self):
        input_text = "请在 2025-08-01 14 前完成"
        expected = "请在 {date} 前完成"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_yyyy_slash_mm_slash_dd(self):
        input_text = "报告日期为2025/08/01，请查阅"
        expected = "报告日期为{date}，请查阅"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_yyyy_slash_mm_slash_dd_hour(self):
        input_text = "开始于2025/08/01 09，持续3小时"
        expected = "开始于{date}，持续3小时"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_yyyy_dot_mm_dot_dd(self):
        input_text = "会议安排：2025.08.01 开始"
        expected = "会议安排：{date} 开始"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_yyyy_dot_mm_dot_dd_hour(self):
        input_text = "演讲时间：2025.08.01 17"
        expected = "演讲时间：{date}"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_compact_format(self):
        input_text = "任务编号：2025080114，已完成"
        expected = "任务编号：{date}，已完成"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_multiple_dates(self):
        input_text = "事件发生在2025-08-01 09，与2025/08/01 10无关"
        expected = "事件发生在{date}，与{date}无关"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_when_no_date_present(self):
        input_text = "这是一段没有时间信息的文本"
        expected = input_text
        assert _remove_time(input_text) == expected.strip()

    def test_remove_from_empty_string(self):
        input_text = ""
        expected = ""
        assert _remove_time(input_text) == expected.strip()

    def test_remove_from_spaces_only(self):
        input_text = "   "
        expected = ""
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_at_start(self):
        input_text = "2025-08-01 是个重要日子"
        expected = "{date} 是个重要日子"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_at_end(self):
        input_text = "截止日期是 2025/08/01"
        expected = "截止日期是 {date}"
        assert _remove_time(input_text) == expected.strip()

    def test_remove_date_embedded_in_filename(self):
        input_text = "文件名：report_2025080112_final.pdf"
        expected = "文件名：report_{date}_final.pdf"
        assert _remove_time(input_text) == expected.strip()
