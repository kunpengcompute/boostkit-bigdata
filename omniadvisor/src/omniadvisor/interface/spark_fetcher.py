import requests
import json


class SparkAppInspector:
    def __init__(self, history_server_url):
        """
        初始化SparkAppInspector类。

        :param history_server_url: Spark History Server的URL地址
        """
        self.history_server_url = history_server_url.rstrip('/')  # 确保URL末尾没有斜杠

    def get_spark_apps(self):
        """
        获取所有Spark应用的信息。

        :return: 返回包含所有应用信息的列表
        """
        response = requests.get(f"{self.history_server_url}/api/v1/applications")
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to fetch applications: {response.status_code}")

    def get_application_details(self, app_id):
        """
        根据应用ID获取指定Spark应用的详细信息。

        :param app_id: 应用ID
        :return: 返回指定应用的详细信息
        """
        response = requests.get(f"{self.history_server_url}/api/v1/applications/{app_id}")
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to fetch application details for {app_id}: {response.status_code}")

    def get_spark_sql_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的SQL信息。

        :param app_id: 应用ID
        :return: 返回指定应用的SQL信息
        """
        response = requests.get(f"{self.history_server_url}/api/v1/applications/{app_id}/sql")
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to fetch SQL information for {app_id}: {response.status_code}")

    def get_spark_stages_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的阶段信息。

        :param app_id: 应用ID
        :return: 返回指定应用的阶段信息
        """
        response = requests.get(f"{self.history_server_url}/api/v1/applications/{app_id}/stages")
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to fetch stages information for {app_id}: {response.status_code}")

    def get_spark_executor_by_app(self, app_id):
        """
        根据应用ID获取指定Spark应用的执行器信息。

        :param app_id: 应用ID
        :return: 返回指定应用的执行器信息
        """
        response = requests.get(f"{self.history_server_url}/api/v1/applications/{app_id}/executors")
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to fetch executors information for {app_id}: {response.status_code}")


