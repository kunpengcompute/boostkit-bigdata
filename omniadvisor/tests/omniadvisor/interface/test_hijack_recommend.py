from unittest.mock import MagicMock, patch

import pytest

from common.constant import OA_CONF
from common.exceptions import DuplicateEntryError
from omniadvisor.interface.hijack_recommend import hijack_recommend, _process_load_config, _create_or_update_load, \
    _remove_time, _calculate_hash_value, _any_modify_keywords_in_sql


# 测试代码
class TestHijackRecommend:

    def setup_class(self):
        # 为了避免抓取trace子进程的等待
        OA_CONF.spark_fetch_trace_timeout = 0

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
        mock_spark_run.return_value = (MagicMock(status="success"), MagicMock(stdout="job output", stderr=None))

        hijack_recommend(argv)
        mock_spark_run.assert_called_once_with(load=load, config=exec_config, wait_for_trace=False, exec_in_isolation=False)
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
            (MagicMock(status="fail"), MagicMock(stdout=None, stderr="fail output")),
            (MagicMock(status="success"), MagicMock(stdout="Safe output", stderr=None))
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
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.validate_submit_arguments")
    def test_failed_user_config(self, mock_validate_submit_arguments, mock_parse_cmd, mock_create_or_update_load,
                                mock_get_config, mock_spark_run, mock_process_config):
        argv = ["--conf", "spark.executor.memory=4g"]
        exec_attr = {"name": "job_name"}
        user_config = {"spark.executor.memory": "4g"}
        load = MagicMock(test_config={}, default_config=user_config)
        exec_config = user_config

        mock_parse_cmd.return_value = (exec_attr, user_config)
        mock_create_or_update_load.return_value = load
        mock_get_config.return_value = exec_config
        mock_spark_output = MagicMock(stdout="job output", stderr=None)
        mock_spark_run.return_value = (MagicMock(status="fail"), mock_spark_output)

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
        mock_spark_run.return_value = (MagicMock(status=OA_CONF.ExecStatus.success),
                                       MagicMock(stdout="job output", stderr=None))
        mock_process = MagicMock()
        mock_multiprocess.return_value = mock_process

        hijack_recommend(argv)

    # 场景 6: 命令解析异常
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.validate_submit_arguments")
    def test_parse_cmd_raises(self, mock_validate_submit_arguments, mock_parse_cmd):
        mock_parse_cmd.side_effect = Exception("parse error")
        with pytest.raises(Exception, match="parse error"):
            hijack_recommend(["--conf", "x"])

    # 场景 7: 创建 Load 异常
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.validate_submit_arguments")
    def test_query_create_load_raises(self, mock_validate_submit_arguments, mock_parse_cmd, mock_create_or_update_load):
        mock_parse_cmd.return_value = ({"name": "job"}, {})
        mock_create_or_update_load.side_effect = Exception("db error")

        with pytest.raises(Exception, match="db error"):
            hijack_recommend(["--conf", "x"])

    # 场景 8: spark_run 抛异常
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    @patch("omniadvisor.interface.hijack_recommend._get_exec_config_from_load")
    @patch("omniadvisor.interface.hijack_recommend._create_or_update_load")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.parse_cmd")
    @patch("omniadvisor.interface.hijack_recommend.SparkCMDParser.validate_submit_arguments")
    def test_spark_run_raises(self, mock_validate_submit_arguments,
                              mock_parse_cmd, mock_create_or_update_load, mock_get_config, mock_spark_run):
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
        mock_load_create.assert_called_once_with(name, exec_attr, default_config, hash_value, False)
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

    def test_create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS users (id INT, name STRING)"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_create_view(self):
        sql = "CREATE OR REPLACE TEMP VIEW v_users AS SELECT * FROM users"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_insert_into(self):
        sql = "INSERT INTO users SELECT * FROM new_users"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_insert_overwrite(self):
        sql = "INSERT OVERWRITE TABLE warehouse SELECT * FROM staging"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_delete_statement(self):
        sql = "DELETE FROM logs WHERE level = 'DEBUG'"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_update_statement(self):
        sql = "UPDATE customers SET status = 'active' WHERE last_login > NOW()"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_merge_into(self):
        sql = """
        MERGE INTO customers USING updates
        ON customers.id = updates.id
        WHEN MATCHED THEN UPDATE SET customers.name = updates.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (updates.id, updates.name)
        """
        assert _any_modify_keywords_in_sql(sql) is True

    def test_drop_table(self):
        sql = "DROP TABLE IF EXISTS temp_table"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_truncate_table(self):
        sql = "TRUNCATE TABLE big_table"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_multiple_statements(self):
        sql = """
        CREATE TABLE tmp AS SELECT * FROM raw;
        INSERT INTO final SELECT * FROM tmp;
        """
        assert _any_modify_keywords_in_sql(sql) is True

    def test_create_database(self):
        sql = "CREATE DATABASE IF NOT EXISTS my_db"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_comment_with_real_sql(self):
        sql = """
        -- Let's drop the table
        DROP TABLE users
        """
        assert _any_modify_keywords_in_sql(sql) is True  # 注释中是“注释”但 SQL 在有效区域

    def test_select_only(self):
        sql = "SELECT * FROM customers WHERE region = 'US'"
        assert _any_modify_keywords_in_sql(sql) is False

    def test_with_cte_select(self):
        sql = """
        WITH recent_orders AS (
            SELECT * FROM orders WHERE order_date > '2025-01-01'
        )
        SELECT * FROM recent_orders
        """
        assert _any_modify_keywords_in_sql(sql) is False

    def test_keywords_in_string(self):
        """
        注意，这里我们会选择粗暴的认为这是危险操作
        """
        sql = "SELECT 'drop table users' AS msg"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_keywords_as_column_names(self):
        """
        注意，这里我们会选择粗暴的认为这是危险操作
        """
        sql = "SELECT create, insert, delete FROM keyword_table"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_keywords_as_aliases(self):
        """
        注意，这里我们会选择粗暴的认为这是危险操作
        """
        sql = "SELECT user_id AS update, name AS delete FROM users"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_function_names_overlap_keywords(self):
        """
        注意，这里 'update_' 'merge_'与并非有效修改、合并操作
        """
        sql = "SELECT update_time(), merge_fields(name, age) FROM meta"
        assert _any_modify_keywords_in_sql(sql) is False

    def test_sql_with_comments_only_keywords(self):
        """
        注意，这里我们会选择粗暴的认为这是危险操作
        """
        sql = """
        -- DELETE FROM customers
        -- DROP TABLE logs
        SELECT * FROM users
        """
        assert _any_modify_keywords_in_sql(sql) is True

    def test_empty_sql(self):
        sql = ""
        assert _any_modify_keywords_in_sql(sql) is False

    def test_whitespace_sql(self):
        sql = "     \n\t  "
        assert _any_modify_keywords_in_sql(sql) is False

    def test_sql_with_semicolon_and_comment(self):
        """
        注意，这里我们会选择粗暴的认为这是危险操作
        """
        sql = "SELECT * FROM table; -- insert into table"
        assert _any_modify_keywords_in_sql(sql) is True

    def test_multiple_statement_sql_file(self):
        """
        测试多语句的情况
        """
        sql = """
        -- 创建表
        CREATE TABLE IF NOT EXISTS tmp AS SELECT * FROM raw_data;

        -- 插入
        INSERT INTO final SELECT * FROM tmp;

        -- 查询结果
        SELECT COUNT(*) FROM final;

        -- 删除临时表
        DROP TABLE tmp;
        """
        assert _any_modify_keywords_in_sql(sql) is True
