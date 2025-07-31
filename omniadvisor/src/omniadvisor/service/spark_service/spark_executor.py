import re
from dataclasses import dataclass
from datetime import datetime

from common.constant import OA_CONF
from omniadvisor.utils.utils import run_cmd
from omniadvisor.utils.logger import global_logger


@dataclass
class SparkExecResult:
    exitcode: int
    output: str
    app_id: str
    duration: float
    start_time: datetime
    end_time: datetime


class SparkExecutor:
    """
    Spark执行与结果解析
    """

    @classmethod
    def submit_spark_task(cls, cmd_fields: list, timeout: int):
        """
        在shell终端提交spark命令，并在命令成功执行并返回时解析结果中的数据

        :param cmd_fields: Spark的提交命令
        :param timeout: 超时时间
        :return:
        """
        if timeout:
            # 命令列表需全为str类型
            cmd_fields = ['timeout', str(timeout)] + cmd_fields

        start_time = datetime.now()
        try:
            exitcode, output = run_cmd(cmd_fields=cmd_fields)
        except Exception:
            global_logger.error('During Spark command submission, unknown error occurred.')
            raise
        end_time = datetime.now()

        if exitcode == 0:
            application_id, total_time_taken = cls._parser_spark_output(spark_output=output)
            return SparkExecResult(
                exitcode=exitcode,
                output=output,
                app_id=application_id,
                duration=total_time_taken,
                start_time=start_time,
                end_time=end_time
            )
        else:
            return SparkExecResult(
                exitcode=exitcode,
                output=output,
                app_id='',
                duration=OA_CONF.exec_fail_return_runtime,
                start_time=start_time,
                end_time=end_time
            )

    @classmethod
    def _parser_spark_output(cls, spark_output: str):
        """
        解析Spark输出，获得spark提交命令、Application ID和Time taken
        (注: Time taken后续不再作为性能指标被使用 目前仅作为性能参考获取系其数值)

        :param spark_output: Spark执行输出
        :return:
        """
        # 解析Application id:
        search_obj = re.search(r'Application Id: (.*)\n', spark_output)
        if search_obj:
            application_id = search_obj.group(1)
        else:
            raise RuntimeError('Can not find application id message in spark output.')

        # 解析Time Taken，只解析行首匹配字符串，避免Spark debug log中重复匹配
        match_objs = re.findall(r'^Time taken: (.*) seconds', spark_output, re.MULTILINE)
        if match_objs:
            time_taken_list = [float(obj) for obj in match_objs]
            total_time_taken = sum(time_taken_list)
            global_logger.debug(f'Total time taken is {total_time_taken} seconds')
        else:
            # 没有捕获到Time Taken时使用OA_CONF.exec_complete_return_runtime作为返回值，后续需要从trace中获取实际运行时间
            total_time_taken = OA_CONF.exec_complete_return_runtime
            global_logger.info('This spark task do not have Time taken message.')

        return application_id, total_time_taken
