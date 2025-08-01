import re
import json
import requests
from datetime import datetime


class SparkFetcher:
    def __init__(self, history_server_url, ):
        """
        初始化SparkFetcher类

        :param history_server_url: Spark History Server的URL地址
        """
        self.history_server_url = history_server_url.rstrip('/')  # 确保URL末尾没有斜杠

    def _make_request(self, endpoint):
        """
        发送GET请求并处理响应。

        :param endpoint: API端点路径
        :return: 解析后的JSON响应数据
        :raises: requests.exceptions.HTTPError 如果HTTP请求返回了错误状态码
        """
        endpoint = endpoint.strip("/")
        url = f"{self.history_server_url}/{endpoint}"
        response = requests.get(url)
        # 若status.code != 200~209则抛出异常
        response.raise_for_status()
        # 将获取到的数据转为json返回
        try:
            json_data = json.loads(response.text)
        except Exception as e:
            raise ValueError('Something wrong in trace fetched, can not decode into Json data.') from e
        return json_data

    def get_spark_apps(self):
        """
        获取所有Spark应用的信息。

        :return: 返回包含所有应用信息的列表
        """
        return self._make_request("api/v1/applications")

    def get_application_details(self, app_id):
        """
        根据应用ID获取指定Spark应用的详细信息。

        :param app_id: 应用ID
        :return: 返回指定应用的详细信息
        """
        return self._make_request(f"api/v1/applications/{app_id}")

    def get_spark_sql_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的SQL信息。

        :param app_id: 应用ID
        :return: 返回指定应用的SQL信息
        """
        return self._make_request(f"api/v1/applications/{app_id}/sql")

    def get_spark_stages_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的阶段信息。

        :param app_id: 应用ID
        :return: 返回指定应用的阶段信息
        """
        return self._make_request(f"api/v1/applications/{app_id}/stages?withSummaries=true")

    def get_spark_executor_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的执行器信息。

        :param app_id: 应用ID
        :return: 返回指定应用的执行器信息
        """
        return self._make_request(f"api/v1/applications/{app_id}/executors")

    def get_spark_jobs_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的Jobs信息

        :param app_id: 应用ID
        :return: 返回指定应用的Jobs信息
        """
        return self._make_request(f"api/v1/applications/{app_id}/jobs")

    def get_spark_runtime_by_app(self, app_id) -> float:
        """
        通过从HistoryServer上获取的jobs信息，计算任务的执行耗时
        runtime的计算的耗时为 最早的Job提交的时间 与 最晚的Job完成时间 之间的差值（注：最晚完成的Job并不一定是最后提交的Job）

        :return: runtime (seconds)
        """
        jobs_detail = self.get_spark_jobs_by_app(app_id)

        # 获取全部的时间戳信息
        time_str_list = []
        for job_detail in jobs_detail:
            time_str_list.append(job_detail['completionTime'])
            time_str_list.append(job_detail['submissionTime'])

        # 检查时间戳信息完整性 要求时间戳数目大于0且为偶数个
        if len(time_str_list) > 0:
            if len(time_str_list) % 2 == 0:
                pass
            else:
                raise ValueError(f"Timestamps must always be submitted in even numbers "
                                 f"({len(time_str_list)} timestamps are not allowed (because of single/unpaired)).")
        else:
            raise ValueError("No timestamp in jobs info. Need at least 2 timestamps")

        # 从 2025-07-28T08:30:32.374GMT 中匹配出 2025-07-28T08:30:32.37
        reg_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}'
        time_format = "%Y-%m-%dT%H:%M:%S.%f"
        timestamp_list = []
        for time_str in time_str_list:
            match = re.search(reg_pattern, time_str)
            if match:
                timestamp_list.append(datetime.strptime(match.group(), time_format))
            else:
                raise ValueError(f"The format of timestamp '{time_str}' does not match expected pattern.")

        # 计算时间差
        time_diff = max(timestamp_list) - min(timestamp_list)
        return time_diff.total_seconds()
