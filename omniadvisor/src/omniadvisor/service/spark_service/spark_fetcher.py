import re
import json
import requests
from dateutil import parser
from typing import Any

from omniadvisor.utils.logger import global_logger


class SparkFetcher:
    def __init__(self, history_server_url: str, username: str, password: str):
        """
        初始化SparkFetcher类

        :param history_server_url: Spark History Server的URL地址
        """
        self.history_server_url = history_server_url.rstrip('/')  # 确保URL末尾没有斜杠
        self.username = username
        self.password = password

    def _make_request(self, endpoint: str) -> dict[str, Any]:
        """
        发送 GET 请求到 History Server，并返回解析后的 JSON 数据。

        该方法会根据配置决定是否使用用户名和密码进行 HTTP 基本认证。
        如果请求返回错误状态码（非 2xx），会直接抛出异常。
        响应数据会解析为 JSON 格式返回，如果解析失败，则抛出 ValueError

        :param endpoint: API端点路径
        :return: 解析后的JSON响应数据
        :raises requests.exceptions.HTTPError:当 HTTP 响应状态码不是 200~209 时抛出。
        :raises ValueError:当响应内容无法解析为 JSON 时抛出。
        """
        endpoint = endpoint.strip("/")
        url = f"{self.history_server_url}/{endpoint}"
        if self.username:
            # 若用户配置了用户名和密码，那么应当走验证通道
            response = requests.get(url, auth=(self.username, self.password), verify=False, allow_redirects=False)
        else:
            response = requests.get(url)
        # 若status.code != 200~209则抛出异常
        response.raise_for_status()
        # 将获取到的数据转为json返回
        try:
            json_data = json.loads(response.text)
        except Exception as e:
            raise ValueError('Something wrong in trace fetched, can not decode into Json data.') from e
        return json_data

    def get_spark_apps(self) -> dict[str, Any]:
        """
        获取所有Spark应用的信息。

        :return: 返回包含所有应用信息的列表
        """
        return self._make_request("api/v1/applications")

    def get_application_details(self, application_id: str) -> dict[str, Any]:
        """
        根据应用ID获取指定Spark应用的详细信息。

        :param application_id: 应用ID
        :return: 返回指定应用的详细信息
        """
        return self._make_request(f"api/v1/applications/{application_id}")

    def get_spark_sql_by_app(self, application_id: str) -> dict[str, Any]:
        """
        根据应用ID获取指定Spark应用的SQL信息。

        :param application_id: 应用ID
        :return: 返回指定应用的SQL信息
        """
        return self._make_request(f"api/v1/applications/{application_id}/sql")

    def get_spark_stages_by_app(self, application_id: str) -> dict[str, Any]:
        """
        根据应用ID获取指定Spark应用的阶段信息。

        :param application_id: 应用ID
        :return: 返回指定应用的阶段信息
        """
        return self._make_request(f"api/v1/applications/{application_id}/stages?withSummaries=true")

    def get_spark_executor_by_app(self, application_id: str) -> dict[str, Any]:
        """
        根据应用ID获取指定Spark应用的执行器信息。

        :param application_id: 应用ID
        :return: 返回指定应用的执行器信息
        """
        return self._make_request(f"api/v1/applications/{application_id}/executors")

    def get_spark_jobs_by_app(self, application_id: str) -> dict[str, Any]:
        """
        根据应用ID获取指定Spark应用的Jobs信息

        :param application_id: 应用ID
        :return: 返回指定应用的Jobs信息
        """
        return self._make_request(f"api/v1/applications/{application_id}/jobs")

    def get_spark_runtime_by_app(self, application_id) -> float:
        """
        通过从HistoryServer上获取的jobs信息，计算任务的执行耗时
        runtime的计算的耗时为 最早的Job提交的时间 与 最晚的Job完成时间 之间的差值（注：最晚完成的Job并不一定是最后提交的Job）

        :return: runtime (seconds)
        """
        jobs_detail = self.get_spark_jobs_by_app(application_id)
        if not jobs_detail:
            global_logger.error(f"No job info returned for application_id={application_id}")
            raise ValueError(f"No job information found for application_id={application_id}")

        # 获取全部的时间戳信息
        timestamp_list = []
        for idx, job_detail in enumerate(jobs_detail):
            if 'completionTime' not in job_detail or 'submissionTime' not in job_detail:
                raise KeyError(f"Job at index {idx} is missing 'completionTime' or 'submissionTime'")
            timestamp_list.append(parser.parse(job_detail['completionTime']).timestamp())
            timestamp_list.append(parser.parse(job_detail['submissionTime']).timestamp())

        # 计算时间差
        time_diff = max(timestamp_list) - min(timestamp_list)
        global_logger.debug(f"Total time diff calculate by jobs is {time_diff} seconds")
        return time_diff
