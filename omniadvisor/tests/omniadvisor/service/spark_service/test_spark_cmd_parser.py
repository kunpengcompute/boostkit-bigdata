import pytest
from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser


class TestSparkCMDParser:
    """
    测试SparkCMDParser类的功能
    """
    def test_validate_submit_argument(self):
        pass

    # 测试命令解析
    # 场景 1: 标准参数 + conf 参数
    def test_parse_cmd_standard(self):
        argv = [
            '--omniadvisor-cmd-prefix', 'spark-submit',
            '--master', 'yarn',
            '--name', 'test_app',
            '-e', 'SELECT * FROM table',
            '--executor-memory', '4g',
            '--conf', 'spark.executor.cores=8',
            '--conf', 'spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UseG1GC',
            '--unknown1', 'test',
            '-uk', 'test',
        ]

        exec_attr, conf_params = SparkCMDParser.parse_cmd(argv)

        assert exec_attr['omniadvisor-cmd-prefix'] == 'spark-submit'
        assert exec_attr['master'] == 'yarn'
        assert exec_attr['name'] == 'test_app'
        assert exec_attr['e'] == 'SELECT * FROM table'
        assert exec_attr['unknown'] == ['--unknown1', 'test', '-uk', 'test']
        assert conf_params['spark.executor.memory'] == '4g'
        assert conf_params['spark.driver.extraJavaOptions'] == '-XX:+UseG1GC -XX:+UseG1GC'

    # 场景 2: 包含 supplement 参数
    def test_parse_cmd_with_supplement(self):
        argv = [
            "--num-executors", "2",
            "--executor-cores", "4"
        ]
        exec_attr, conf_params = SparkCMDParser.parse_cmd(argv)

        assert "spark.executor.instances" in conf_params
        assert conf_params["spark.executor.instances"] == "2"
        assert conf_params["spark.executor.cores"] == "4"

    # 场景 3: 包含布尔类型的值
    def test_parse_cmd_with_boolean_flags(self):
        argv = ["--verbose", "--supervise", "--kill", "--status"]
        exec_attr, _ = SparkCMDParser.parse_cmd(argv)

        assert exec_attr["verbose"] is True
        assert exec_attr["supervise"] is True
        assert exec_attr["kill"] is True
        assert exec_attr["status"] is True

    # 场景 4: 解析包含未知参数
    def test_parse_cmd_with_unknown(self):
        argv = [
            "--name", "app",
            "--foo", "bar", "--bar", "baz", "--kills"
        ]
        exec_attr, conf_params = SparkCMDParser.parse_cmd(argv)

        assert "unknown" in exec_attr
        assert "--foo" in exec_attr["unknown"]
        assert "bar" in exec_attr["unknown"]

    # 场景 5: 空 argv 抛出异常
    def test_parse_cmd_empty_argv(self):
        with pytest.raises(ValueError, match="Submit command cannot be null"):
            SparkCMDParser.parse_cmd([])

    # 测试命令重建
    # 场景 1: 正常重建命令行
    def test_reconstruct_cmd_normal(self):
        """
        测试基本命令重组
        """
        exec_attr = {
            'omniadvisor-cmd-prefix': 'spark-submit',
            'master': 'yarn',
            'name': 'test_app',
            'e': 'SELECT * FROM table',
            'unknown': ['--unknown1', 'test', '-uk', 'test']
        }
        conf_params = {
            'spark.executor.memory': '4g',
            'spark.driver.extraJavaOptions': '-XX:+UseG1GC -XX:+UseG1GC',
        }

        cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert cmd.startswith(exec_attr['omniadvisor-cmd-prefix'])
        assert "--master 'yarn'" in cmd
        assert "--name 'test_app'" in cmd
        assert "-e 'SELECT * FROM table'" in cmd
        assert "'--unknown1' 'test'" in cmd
        assert "'-uk' 'test'" in cmd
        assert "--conf spark.executor.memory='4g'" in cmd
        assert "--conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:+UseG1GC'" in cmd

    # 场景 2: 缺少前缀字段抛异常
    def test_reconstruct_cmd_missing_prefix(self):
        exec_attr = {
            "name": "job"
        }
        conf_params = {}

        with pytest.raises(ValueError, match="Cmd prefix key is not in exec_attr"):
            SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

    # 场景 3: 包含 unknown 参数
    def test_reconstruct_cmd_with_unknown(self):
        exec_attr = {
            "omniadvisor-cmd-prefix": "spark-submit",
            "name": "job",
            "unknown": ["--extra", "arg"]
        }
        conf_params = {
            "spark.executor.instances": "4"
        }
        cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert "--extra" in cmd
        assert "arg" in cmd
        assert "--conf spark.executor.instances='4'" in cmd

    # 场景 3: 包含 boolean 参数
    def test_reconstruct_cmd_with_boolean(self):
        exec_attr = {
            "omniadvisor-cmd-prefix": "spark-submit",
            "name": "job",
            "unknown": ["--extra", "arg"],
            "verbose": True,
            "supervise": True,
            "kill": True,
            "status": True
        }
        conf_params = {
            "spark.executor.instances": "4"
        }
        cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert "--extra" in cmd
        assert "arg" in cmd
        assert "--conf spark.executor.instances='4'" in cmd
        assert "verbose" in cmd
        assert "supervise" in cmd
        assert "kill" in cmd
        assert "status" in cmd

