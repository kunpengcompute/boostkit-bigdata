import re
from typing import Tuple
from dataclasses import dataclass
from datetime import datetime

from django.utils import timezone

from common.constant import OA_CONF
from omniadvisor.utils.utils import run_cmd
from omniadvisor.utils.logger import global_logger


@dataclass
class SparkExecResult:
    exitcode: int
    output: str
    application_id: str
    duration: float
    start_time: datetime
    end_time: datetime


class SparkExecutor:
    """
    Spark执行与结果解析
    """

    @classmethod
    def submit_spark_task(cls, cmd_fields: list, timeout: int) -> SparkExecResult:
        """
        在shell终端提交Spark任务，在Spark任务执行正常退出时解析结果中的数据，并使用SparkExecResult类返回解析后的结果

        :param cmd_fields: Spark的提交命令
        :param timeout: 超时时间
        :return: SparkExecResult
        """
        if timeout:
            # 命令列表需全为str类型
            cmd_fields = ['timeout', str(timeout)] + cmd_fields

        start_time = timezone.now()
        try:
            exitcode, output = run_cmd(cmd_fields=cmd_fields)
        except Exception:
            global_logger.error('During Spark command submission, unknown error occurred.')
            raise
        end_time = timezone.now()

        if exitcode == 0:
            application_id, total_time_taken = cls._parser_spark_output(spark_output=output)
            return SparkExecResult(
                exitcode=exitcode,
                output=output,
                application_id=application_id,
                duration=total_time_taken,
                start_time=start_time,
                end_time=end_time
            )
        else:
            return SparkExecResult(
                exitcode=exitcode,
                output=output,
                application_id='',
                duration=OA_CONF.exec_fail_return_runtime,
                start_time=start_time,
                end_time=end_time
            )

    @classmethod
    def _parser_spark_output(cls, spark_output: str) -> Tuple[str, float]:
        """
        解析Spark输出，获得spark提交命令、Application ID和Time taken(若存在)
        (注: Time taken后续不再作为性能指标被使用 目前仅作为性能参考获取系其数值)

        :param spark_output: Spark执行输出
        :raises RuntimeError: 当无法从输出中匹配到 Application ID 时抛出。
        :return: application_id, total_time_taken
        """
        # 解析Application id:
        # 该条匹配模式的文本来自Spark3.3.1源码中yarn/Client.scala文件在Line224所打印的日志
        # 若Spark版本迭代时对该日志内容做出修改 则存在application id匹配失败的风险
        pattern = r'Client: Submitting application (application_\d+_\d+) to ResourceManager|Application Id: (.*)\n'
        search_obj = re.search(pattern, spark_output)
        if search_obj:
            application_id = search_obj.group(1) or search_obj.group(2)
            if search_obj.group(1):
                global_logger.debug(f"Match group 1, application id is {application_id}")
            if search_obj.group(2):
                global_logger.debug(f"Match group 2, application id is {application_id}")
        else:
            raise RuntimeError('Can not find application id message in spark output.')

        # 解析Time Taken，只解析行首匹配字符串，避免Spark debug log中重复匹配
        pattern = r'^Time taken: (.*) seconds'
        match_objs = re.findall(pattern, spark_output, re.MULTILINE)
        if match_objs:
            time_taken_list = [float(obj) for obj in match_objs]
            total_time_taken = sum(time_taken_list)
            global_logger.debug(f'Total time taken is {total_time_taken} seconds')
        else:
            global_logger.info('This spark task do not have Time taken message.')
        # OA_CONF.exec_fail_return_runtime仅作为占位用变量
        # 维持spark_executor的接口不变 实际的执行时间需要从jobs中获取
        total_time_taken = OA_CONF.exec_fail_return_runtime
        return application_id, total_time_taken
