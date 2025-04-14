import unittest
from unittest.mock import MagicMock, patch
from common.constant import OA_CONF
from omniadvisor.interface.hijack_recommend import hijack_recommend


# 测试代码
class TestHijackRecommend(unittest.TestCase):

    @patch("omniadvisor.interface.hijack_recommend.SparkParameterParser")
    @patch("omniadvisor.interface.hijack_recommend.LoadRepository.query_by_name_and_default_config")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    def test_hijack_recommend_success(self, mock_spark_run, mock_load_repo_query, mock_parser):
        """
        测试 hijack_recommend 在任务成功时的行为。
        """
        # 模拟输入的 Spark SQL 命令
        spark_sql_cmd = "SELECT * FROM example_table"

        # 模拟 SparkParameterParser 的行为
        mock_parser_instance = MagicMock()
        mock_parser_instance.process_parameter.return_value = MagicMock()
        mock_parser_instance.process_parameter.return_value.configure_mock(
            name="example_name",
            exec_attr={"cpu": 4},
            conf_params={"timeout": 30}
        )
        mock_parser.return_value = mock_parser_instance

        # 模拟 LoadRepository.query 返回一个有效的负载对象
        mock_load = MagicMock()
        mock_load.tuning_needed = False
        mock_load.default_config = {"timeout": 30}
        mock_load_repo_query.return_value = [mock_load]

        # 模拟 spark_run 的行为
        mock_spark_run.return_value = (MagicMock(status=OA_CONF.ExecStatus.fail), 'spark_output')

        # 调用被测函数
        hijack_recommend(spark_sql_cmd)

        # 验证 LoadRepository.query 被正确调用
        mock_load_repo_query.assert_called_once_with(name="example_name", default_config={"timeout": 30})

        # 验证 spark_run 被正确调用
        mock_spark_run.assert_called_once_with(mock_load, {"timeout": 30})

    @patch("omniadvisor.interface.hijack_recommend.SparkParameterParser")
    @patch("omniadvisor.interface.hijack_recommend.LoadRepository")
    @patch("omniadvisor.interface.hijack_recommend.spark_run")
    def test_hijack_recommend_failure_with_fallback(self, mock_spark_run, mock_load_repo, mock_parser):
        """
        测试 hijack_recommend 在任务失败且需要回退到用户默认配置时的行为。
        """
        # 模拟输入的 Spark SQL 命令
        spark_sql_cmd = "SELECT * FROM example_table"

        # 模拟 SparkParameterParser 的行为
        mock_parser_instance = MagicMock()
        mock_parser_instance.process_parameter.return_value = MagicMock()
        mock_parser_instance.process_parameter.return_value.configure_mock(
            name="example_name",
            exec_attr={"cpu": 4},
            conf_params={"timeout": 30}
        )
        mock_parser.return_value = mock_parser_instance

        # 模拟 LoadRepository.query 返回一个有效的负载对象
        mock_load = MagicMock()
        mock_load.tuning_needed = True
        mock_load.test_config = {"timeout": 60}  # 使用待测试配置
        mock_load_repo.query_by_name_and_default_config.return_value = [mock_load]

        # 模拟 spark_run 的行为
        mock_spark_run.side_effect = [
            (MagicMock(status=OA_CONF.ExecStatus.fail), 'spark_output1'),  # 第一次任务失败
            (MagicMock(status=OA_CONF.ExecStatus.success), 'spark_output2')   # 回退到默认配置后成功
        ]

        # 调用被测函数
        hijack_recommend(spark_sql_cmd)

        # 验证 LoadRepository.query 被正确调用
        mock_load_repo.query_by_name_and_default_config.assert_called_once_with(name="example_name", default_config={"timeout": 30})

        # 验证 spark_run 被调用了两次（第一次使用待测试配置，第二次回退到默认配置）
        assert mock_spark_run.call_count == 2
        mock_spark_run.assert_any_call(mock_load, {"timeout": 60})  # 第一次调用
        mock_spark_run.assert_any_call(mock_load, {"timeout": 30})  # 第二次调用

    # TODO 端到端的用例构造里要验证一下对 --num-executors 转换为 --conf num-executors后是否能正常执行