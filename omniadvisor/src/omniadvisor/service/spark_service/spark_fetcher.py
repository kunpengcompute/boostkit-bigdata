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

    def get_spark_runtime_by_app(self) -> float:
        """
        通过从HistoryServer上获取的jobs信息，计算任务的执行耗时
        runtime的计算的耗时为 最早的Job提交的时间 与 最晚的Job完成时间 之间的差值（注：最晚完成的Job并不一定是最后提交的Job）

        :return: runtime (seconds)
        """
        jobs_detail = self.get_spark_jobs_by_app()

        time_format = "%Y-%m-%dT%H:%M:%S.%f"

        # 获取最早的Job提交时间
        first_submit_time = jobs_detail[-1]["submissionTime"]
        first_submittion_timestamp = datetime.strptime(first_submit_time.replace("GMT", ""), time_format)

        # 获取最后结束的Job的完成时间
        completiontime_list = []
        sortedcompletiontime_list = []
        for job_detail in jobs_detail:
            completiontime_str = job_detail['completionTime']
            completion_timestamp = datetime.strptime(completiontime_str.replace("GMT", ""), time_format)
            completiontime_list.append(completion_timestamp)
            sortedcompletiontime_list = sorted(completiontime_list)

        if sortedcompletiontime_list:
            last_completion_timestamp = sortedcompletiontime_list[-1]
        else:
            return float('inf')

        # 计算时间差
        time_diff = last_completion_timestamp - first_submittion_timestamp
        return time_diff.total_seconds()
