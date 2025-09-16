import shlex
import pytest
from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser, _CMD_PREFIX_KEY, _CMD_PRIMARY_RESOURCE


class TestSparkCMDParser:
    """
    测试SparkCMDParser类的功能
    """
    # Spark任务类型判断测试
    def test_validate_submit_arguments_valid(self):
        args = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit",
            "--class", "com.example.Main",
            "app.jar"
        ]
        assert SparkCMDParser.validate_submit_arguments(args) is None

    def test_validate_submit_arguments_missing_class_and_jar(self):
        args = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit"
        ]
        with pytest.raises(TypeError):
            SparkCMDParser.validate_submit_arguments(args)


    def test_validate_submit_arguments_empty_raises(self):
        with pytest.raises(ValueError):
            SparkCMDParser.validate_submit_arguments([])

    # 测试命令解析
    # 场景 1: 标准参数 + conf 参数
    def test_parse_cmd_basic_conf_and_exec_args(self):
        argv = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit",
            "--class", "com.example.Main",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.executor.cores=2",
            "--num-executors", "5",
            "--name", "test-app",
            "example.jar"
        ]

        exec_attr, conf_params = SparkCMDParser.parse_cmd(argv)

        assert exec_attr["class"] == "com.example.Main"
        assert exec_attr["name"] == "test-app"
        assert conf_params["spark.executor.memory"] == "4g"
        assert conf_params["spark.executor.cores"] == "2"
        assert conf_params["spark.executor.instances"] == "5"
        assert _CMD_PRIMARY_RESOURCE in exec_attr
        assert exec_attr[_CMD_PRIMARY_RESOURCE] == ["example.jar"]

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
    def test_parse_cmd_boolean_flags(self):
        argv = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit",
            "--verbose",
            "--version",
            "--supervise",
            "--usage-error",
            "--help",
            "example.jar"
        ]
        exec_attr, _ = SparkCMDParser.parse_cmd(argv)

        for flag in ["verbose", "version", "supervise", "usage-error", "help"]:
            assert exec_attr.get(flag) is True

        assert exec_attr[_CMD_PRIMARY_RESOURCE] == ["example.jar"]

    # 场景 4: 解析单短横杠的命令
    def test_parse_cmd_single_dash_args(self):
        argv = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit",
            "-e", "SELECT 1",
            "-f", "script.sql",
            "-i", "init.sql",
            "-d", "testdb",
            "example.jar"
        ]
        exec_attr, _ = SparkCMDParser.parse_cmd(argv)

        assert exec_attr["e"] == "SELECT 1"
        assert exec_attr["f"] == "script.sql"
        assert exec_attr["i"] == "init.sql"
        assert exec_attr["define"] == "testdb"
        assert exec_attr[_CMD_PRIMARY_RESOURCE] == ["example.jar"]

    # 场景 5: 空 argv 抛出异常
    def test_parse_cmd_empty_argv(self):
        with pytest.raises(ValueError, match="Submit command cannot be null"):
            SparkCMDParser.parse_cmd([])

    # 测试命令重建
    # 场景 1: 正常重建命令行
    def test_reconstruct_cmd_round_trip(self):
        original_argv = [
            f"--{_CMD_PREFIX_KEY}", "spark-submit",
            "--class", "com.example.Main",
            "--conf", "spark.executor.memory=4g",
            "--executor-cores", "2",
            "--driver-memory", "2g",
            "--verbose",
            '-e', 'SELECT * FROM table',
            "app.jar"
        ]
        exec_attr, conf_params = SparkCMDParser.parse_cmd(original_argv)
        reconstructed = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert "spark-submit" in reconstructed
        assert "--class" in reconstructed
        assert "com.example.Main" in reconstructed
        assert "--conf" in reconstructed
        assert "spark.executor.memory=4g" in reconstructed
        assert "spark.executor.cores=2" in reconstructed
        assert "spark.driver.memory=2g" in reconstructed
        assert "--verbose" in reconstructed
        assert "SELECT * FROM table" in reconstructed
        assert "app.jar" in reconstructed

    # 场景 2: 缺少前缀字段抛异常
    def test_reconstruct_cmd_missing_prefix(self):
        exec_attr = {
            "name": "job"
        }
        conf_params = {}

        with pytest.raises(ValueError, match="Cmd prefix key is not in exec_attr"):
            SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

    # 场景 3: primary_resource中包含 unknown 参数
    def test_reconstruct_cmd_with_unknown(self):
        exec_attr = {
            "omniadvisor-cmd-prefix": "spark-submit",
            "name": "job",
            "primary_resource": ["--extra", "arg"]
        }
        conf_params = {
            "spark.executor.instances": "4"
        }
        cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert "--extra" in cmd
        assert "arg" in cmd
        assert "spark.executor.instances=4" in cmd

    # 场景 3: 包含 boolean 参数的命令构建
    def test_reconstruct_cmd_with_boolean(self):
        exec_attr = {
            "omniadvisor-cmd-prefix": "spark-submit",
            "name": "job",
            "primary_resource": ["--extra", "arg"],
            "verbose": True,
            "supervise": False,
        }
        conf_params = {
            "spark.executor.instances": "4"
        }
        cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_params)

        assert "--extra" in cmd
        assert "arg" in cmd
        assert "spark.executor.instances=4" in cmd
        assert "--verbose" in cmd
        assert "--supervise" not in cmd

    # 全量参数测试
    def test_parse_and_reconstruct_all_params_from_cmd_string(self):
        cmd_string = """
        --omniadvisor-cmd-prefix '$SPARK_HOME/bin/spark-class org.apache.spark.deploy.SparkSubmit'
        --class com.example.MainClass
        --name MySparkApp
        --master yarn
        --deploy-mode cluster
        --driver-memory 2g
        --driver-cores 2
        --executor-memory 4g
        --executor-cores 4
        --num-executors 20
        --queue root.default
        --conf spark.executor.memoryOverhead=512
        --conf spark.yarn.maxAppAttempts=1
        --jars lib1.jar,lib2.jar
        --files config.yaml
        --archives env.zip#env
        --verbose
        --supervise
        --usage-error
        --help
        -i init_script.sql
        --database my_database
        --e "SELECT * FROM table"
        -f query.sql
        my_app.jar
        --custom-arg custom_value
        --principal principal1
        --version
        """

        # 使用 shlex.split 来模拟 shell 行为
        argv = shlex.split(cmd_string)

        # Step 1: Parse
        exec_attr, conf_attr = SparkCMDParser.parse_cmd(argv)

        # Step 2: Reconstruct
        reconstructed_cmd = SparkCMDParser.reconstruct_cmd(exec_attr, conf_attr)

        # Step 3: 验证 reconstruct 是否保留了所有参数
        expected = [
            "--class", "com.example.MainClass",
            "--name", "MySparkApp",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--conf", "spark.driver.memory=2g",
            "--conf", "spark.driver.cores=2",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.executor.cores=4",
            "--conf", "spark.executor.instances=20",
            "--queue", "root.default",
            "--conf", "spark.executor.memoryOverhead=512",
            "--conf", "spark.yarn.maxAppAttempts=1",
            "--jars", "lib1.jar,lib2.jar",
            "--files", "config.yaml",
            "--archives", "env.zip#env",
            "--verbose",
            "--supervise",
            "--usage-error",
            "--help",
            "-i", "init_script.sql",
            "--database", "my_database",
            "-e", "SELECT * FROM table",
            "-f", "query.sql",
            "my_app.jar",
            "--custom-arg", "custom_value",
            "--principal", "principal1",
            "--version"
        ]

        for item in expected:
            assert item in reconstructed_cmd, f"Missing '{item}' in reconstructed command"

        # Step 4: 验证 parse_cmd 是否解析了所有 key
        expected_keys = {
            "class", "name", "master", "deploy-mode",
            "queue", "jars", "files", "archives",
            "verbose", "version", "supervise", "usage-error", "help",
            "i", "database", "e", "f",
            _CMD_PRIMARY_RESOURCE
        }

        assert set(exec_attr.keys()).issuperset(expected_keys), "Missing expected keys in parsed exec_attr"

        # Step 5 验证primary_resource是否符合预期
        expected_primary_resource = ["my_app.jar", "--custom-arg", "custom_value",
                                     "--principal", "principal1", "--version"]
        assert exec_attr[_CMD_PRIMARY_RESOURCE] == expected_primary_resource

        # Step 6 验证exec_attr是否不包含"--custom-arg", "--principal", "--version"
        assert "custom-arg" not in exec_attr
        assert "principal" not in exec_attr
        assert exec_attr["version"] is False

