import argparse
import getpass
import re

from common.crypto_tool import CryptoTool
from omniadvisor.utils.logger import global_logger


def _parse_user_args() -> argparse.Namespace:
    """
    解析用户入参

    :return: 解析后参数的命名空间
    """
    parser = argparse.ArgumentParser(description='')
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-i', '--initialize', action='store_true', help='Initialize passwords.')
    group.add_argument(
        '-d', '--update-database-password', action='store_true', help='Update password of database.'
    )
    group.add_argument(
        '-s', '--update-history-server-password', action='store_true', help='Update password of history server.'
    )

    user_args = parser.parse_args()
    return user_args


def validate_password(password: str) -> bool:
    """
    校验口令是否满足：
    1. 长度 ≥ 8
    2. 至少包含「小写字母、大写字母、数字、特殊字符」中的两种
    特殊字符集合：`~!@#$%^&*()-_=+\\|[{}];:'\",<.>/? 和空格
    返回 True 表示通过，False 表示不通过。

    :param password: 密码
    :returns: 通过则true
    """
    if len(password) < 8:
        return False

    # 定义四类字符的正则
    patterns = {
        'lower': r'[a-z]',
        'upper': r'[A-Z]',
        'digit': r'[0-9]',
        'special': r'[`~!@#$%^&*()\-_=+\\|[{}];:\'",<.>/? ]'
    }

    # 统计满足的类别数
    cnt = 0
    for kind, pattern in patterns.items():
        if re.search(pattern, password):
            cnt += 1
            if cnt >= 2:  # 提前满足即可返回
                return True
    return False


def _initialize():
    """
    初始化数据库密码和history server密码
    """
    _update_database_password_interactively()
    _update_history_server_password_interactively()


def _has_initialized():
    """
    检测是否已经初始化过
    """
    return CryptoTool.get_history_server_password() or CryptoTool.get_database_password_password()


def _update_database_password_interactively():
    """
    交互式刷新数据库密码
    """
    password = getpass.getpass("Please enter your database password: ")
    password2 = getpass.getpass("Please enter your database password again: ")
    if password != password2:
        raise ValueError("Error: Your database passwords didn't match.")
    if not validate_password(password):
        response = input(
            "Bypass password validation and use password anyway? [y/N]: "
        )
        if response.lower() != "y":
            global_logger.warning('Database password not set yet !')
            return
    CryptoTool.store_database_password_password(password)


def _update_history_server_password_interactively():
    """
    交互式刷新 history server密码
    """
    password = getpass.getpass("Please enter your history server password (If none, skip.) : ")
    password2 = getpass.getpass("Please enter your history server password again:")
    if password != password2:
        raise ValueError("Error: Your history server passwords didn't match.")
    if not validate_password(password):
        response = input(
            "Bypass password validation and use password anyway? [y/N]: "
        )
        if response.lower() != "y":
            global_logger.warning('History server password not set yet !')
            return
    CryptoTool.store_history_server_password(password)


def main():
    user_args = _parse_user_args()
    if user_args.initialize:
        # 初始化情况
        if _has_initialized():
            global_logger.warning('Already initialized before, please check your option.')
        else:
            _initialize()
            global_logger.info('Successfully initialized.')

    elif user_args.update_database_password:
        # 改数据库密码
        _update_database_password_interactively()
        global_logger.info('Successfully update database password.')

    else:
        # 改 history server密码
        _update_history_server_password_interactively()
        global_logger.info('Successfully update history server password.')


if __name__ == '__main__':
    try:
        main()
    except ValueError as e:
        global_logger.error('An error occurred when setting password: %s', e)
