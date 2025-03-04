import configparser
import os
import yaml


def load_common_config():
    """
    使用configparser库加载common_config
    :return: 返回common_config的内容
    """
    cur_dir_path = os.path.dirname(os.path.abspath(__file__))
    common_config = configparser.ConfigParser()
    config_path = f"{os.path.dirname(os.path.dirname(os.path.dirname(cur_dir_path)))}/" \
                  f"config/common_config.cfg"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"{config_path} does not exist")
    common_config.read(config_path, encoding='utf-8')
    return common_config


