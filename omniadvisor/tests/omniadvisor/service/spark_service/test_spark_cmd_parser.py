from omniadvisor.service.spark_service.spark_cmd_parser import SparkCMDParser


class TestSparkCMDParser:
    """
    测试SparkCMDParser类的功能
    """
    def test_parse_cmd(self):
        """
        测试基本命令解析
        """
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

    def test_reconstruct_cmd(self):
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
        assert '--master yarn' in cmd
        assert '--name test_app' in cmd
        assert '-e "SELECT * FROM table"' in cmd
        assert '--unknown1 test' in cmd
        assert '-uk test' in cmd
        assert '--conf spark.executor.memory=4g' in cmd
        assert '--conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+UseG1GC"' in cmd
