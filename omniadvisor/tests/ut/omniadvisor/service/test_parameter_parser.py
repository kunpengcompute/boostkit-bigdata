import pytest
from unittest.mock import patch

from omniadvisor.service.spark_service.spark_parameter_parser import SparkParameterParser


class TestParameterParser:
    def test_process_parameter(self):
        # 准备测试数据
        submit_cmd = "spark-submit --name test_sql --master local[*] --deploy-mode client " \
                     "--executor-memory 2g --conf spark.memory.fraction=0.6"

        # 创建 SparkParameterParser 实例
        parser = SparkParameterParser(submit_cmd)

        # 调用 process_parameter 方法
        params = parser.process_parameter()

        # 验证返回的字典包含预期的键值对
        assert 'name' in params and params['name'] == "test_sql"
        assert 'exec_attr' in params and isinstance(params['exec_attr'], dict)
        assert 'conf_params' in params and isinstance(params['conf_params'], dict)

    def test_get_base_params_with_empty_submit_cmd(self):
        # 测试 _get_base_params 在提交命令为空时抛出 ValueError
        parser = SparkParameterParser("")
        with pytest.raises(ValueError):
            parser._get_base_params()

    @patch('shlex.split')
    def test_get_base_params(self, mock_shlex_split):
        # 使用 mock 来控制 shlex.split 的返回值
        mock_shlex_split.return_value = ["--master", "local[*]", "--deploy-mode", "client"]

        # 创建 SparkParameterParser 实例
        parser = SparkParameterParser("any command")

        # 调用 _get_base_params 方法
        params = parser._get_base_params()

        # 验证返回的字典包含预期的键值对
        assert 'master' in params['exec_attr'] and params['exec_attr']['master'] == 'local[*]'
        assert 'deploy_mode' in params['exec_attr'] and params['exec_attr']['deploy_mode'] == 'client'
