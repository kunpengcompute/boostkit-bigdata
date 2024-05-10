import ipaddress
import re

import yaml

import utils


def expand_ip_range(ip_range):
    start_ip, end_ip = ip_range.split('-')
    dot_idx = start_ip.rfind(".")
    end_ip = start_ip[0:dot_idx + 1] + end_ip
    start_ip = ipaddress.IPv4Address(start_ip.strip())
    end_ip = ipaddress.IPv4Address(end_ip.strip())
    ip_list = [str(ipaddress.IPv4Address(ip)) for ip in range(int(start_ip), int(end_ip) + 1)]
    return ip_list


class OmniRuntimeConf(object):
    # 192.168.1.2单个ip的模式
    IPV4_PATTERN = re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b')
    # 192.168.1.2-210网段的模式
    IPV4_RANGE_PATTERN = re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}-[0-9]{1,3}\b')

    def __init__(self):
        self.env = OmniRuntimeConf.Env()
        self.omnioperator = OmniRuntimeConf.OmniOperator()

    # 解析yaml配置文件
    def parse(self, conf_file):
        with open(conf_file, 'r', encoding="UTF-8") as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
        # 解析env环境信息
        self.env.username = conf['env']['username']
        self.env.password = conf['env']['password']
        self.env.sudo_password = conf['env']['sudo_password']
        self.env.port = conf['env']['port']
        self.env.jumper_host = conf['env']['jumper_host']
        if conf['env']['server_hosts'] is not None:
            for host in conf['env']['server_hosts']:
                if OmniRuntimeConf.IPV4_RANGE_PATTERN.match(host):
                    ips = expand_ip_range(host)
                    for ip in ips:
                        self.env.server_hosts.append(ip)
                else:
                    self.env.server_hosts.append(host)
        if conf['env']['agent_hosts'] is not None:
            for host in conf['env']['agent_hosts']:
                if OmniRuntimeConf.IPV4_RANGE_PATTERN.match(host):
                    ips = expand_ip_range(host)
                    for ip in ips:
                        self.env.agent_hosts.append(ip)
                else:
                    self.env.agent_hosts.append(host)
        # 解析omnioperator相关信息
        self.omnioperator.omni_version = conf['omnioperator']['omni_version']
        self.omnioperator.spark_version = conf['omnioperator']['spark_version']
        self.omnioperator.os_type = conf['omnioperator']['os_type']
        self.omnioperator.omni_home = utils.trim_dir(conf['omnioperator']['omni_home'])
        self.omnioperator.deploy_home = utils.trim_dir(conf['omnioperator']['deploy_home'])
        self.omnioperator.is_need_sudo = conf['omnioperator']['is_need_sudo']

    # 将ip网段解析成ip列表

    class Env(object):
        def __init__(self):
            self.username = ''
            self.password = ''
            self.sudo_password = ''
            self.port = 0
            self.jumper_host = ''
            self.server_hosts = []
            self.agent_hosts = []

    class OmniOperator(object):
        def __init__(self):
            self.omni_version = ''
            self.spark_version = ''
            self.os_type = ''
            self.omni_home = ''
            self.deploy_home = ''
            self.is_need_sudo = False
