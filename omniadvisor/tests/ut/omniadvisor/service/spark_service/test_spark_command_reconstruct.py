import pytest
from unittest.mock import MagicMock, patch
from omniadvisor.service.spark_service.spark_command_reconstruct import spark_command_reconstruct


# 假设 spark_command_reconstruct 函数所在的模块名为 spark_utils


class TestSparkCommandReconstruct:
    def test_basic_command_reconstruction(self):
        # 创建 mock load 对象
        mock_load = MagicMock()
        mock_load.name = "TestApp"
        mock_load.exec_attr = [("driver-memory", "8G"), ("executor-memory", "16G")]

        # 定义 conf 参数
        conf = [("spark.sql.shuffle.partitions", "3200"), ("spark.executor.cores", "4")]

        expected_cmd = "spark-sql --name TestApp" \
                       " --driver-memory 8G" \
                       " --executor-memory 16G" \
                       " --conf spark.sql.shuffle.partitions=3200" \
                       " --conf spark.executor.cores=4"
        result = spark_command_reconstruct(mock_load, conf)

        assert result == expected_cmd

    def test_i_param_handling(self):
        # 创建 mock load 对象
        mock_load = MagicMock()
        mock_load.name = "InitScriptApp"
        mock_load.exec_attr = [("i", "init.script.path")]

        conf = []

        expected_cmd = "spark-sql --name InitScriptApp -i init.script.path"

        result = spark_command_reconstruct(mock_load, conf)

        assert result == expected_cmd
