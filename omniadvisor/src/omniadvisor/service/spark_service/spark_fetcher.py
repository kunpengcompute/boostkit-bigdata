import json
import requests


class SparkFetcher:
    def __init__(self, history_server_url, username, password):
        """
        初始化SparkFetcher类

        :param history_server_url: Spark History Server的URL地址
        """
        self.history_server_url = history_server_url.rstrip('/')  # 确保URL末尾没有斜杠
        self.username = username
        self.password = password
        # 若用户配置了用户名和密码，那么应当使用 with_auth 方法
        self._make_request = self._make_request_with_auth if self.username else self._make_request_without_auth

    def _make_request_without_auth(self, endpoint):
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

    def _make_request_with_auth(self, endpoint):
        """
        发送带 ssl 认证的 http 请求
        :param endpoint: API端点路径
        :return: 解析后的JSON响应数据
        :raises: requests.exceptions.HTTPError 如果HTTP请求返回了错误状态码
        """
        endpoint = endpoint.strip("/")
        url = f"{self.history_server_url}/{endpoint}"
        # 使用 self.username 和 self.password 进行 HTTP Basic 认证 & 忽略 SSL 证书验证 & 禁止自动跳转
        response = requests.get(url, auth=(self.username, self.password), verify=False, allow_redirects=False)
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
