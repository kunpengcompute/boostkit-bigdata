import argparse
import getpass
import re

from common.constant import OA_CONF
from common.crypto_tool import CryptoTool
from common.exceptions import KmcError
from common.kmc_toolkit.log import enable_kmc_logger
from omniadvisor.utils.logger import global_logger


class PasswordType:
    database = 'database'
    history_server = 'history_server'


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


def _validate_password(password: str, password_type: str) -> bool:
    """
    校验口令是否满足：
    1. 长度 ≥ 8
    2. 至少包含「小写字母、大写字母、数字、特殊字符」中的两种
    特殊字符集合：`~!@#$%^&*()-_=+\\|[{}];:'\",<.>/? 和空格
    返回 True 表示通过，False 表示不通过。

    :param password: 密码
    :param password_type: 密码类型
    :returns: 通过则true
    """
    if len(password) < 8:
        global_logger.warning("This password is too short. It must contain at least 8 characters.")
        return False

    if password_type == PasswordType.database and password == OA_CONF.postgresql_database_user:
        global_logger.warning("This password is the same as the username.")
        return False

    if password_type == PasswordType.history_server and password == OA_CONF.spark_history_username:
        global_logger.warning("This password is the same as the username.")
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
    for _, pattern in patterns.items():
        if re.search(pattern, password):
            cnt += 1
            if cnt >= 2:  # 提前满足即可返回
                return True
    global_logger.warning("This password is too common.")
    global_logger.warning("For better security, include any two of these: uppercase (A–Z), lowercase (a–z), "
                          "digits (0–9), or special symbols (e.g., !@#$%).")
    return False


def _initialize():
    """
    初始化数据库密码和history server密码
    """
    _password_interactively(PasswordType.database)
    _password_interactively(PasswordType.history_server)


def _has_initialized():
    """
    检测是否已经初始化过
    """
    return CryptoTool.get_history_server_password() or CryptoTool.get_database_password_password()


def _password_interactively(password_type: str):
    if password_type == PasswordType.database:
        content = 'database'
        store_func = CryptoTool.store_database_password_password
    else:
        content = 'history server'
        store_func = CryptoTool.store_history_server_password

    while True:
        password = getpass.getpass(f"Please enter your {content} password (If none, skip.) : ")
        password2 = getpass.getpass(f"Please enter your {content} password again:")
        if password != password2:
            global_logger.error("Error: Your %s passwords didn't match, please try again.", content)
            continue
        break
    if not _validate_password(password, password_type):
        response = input(
            "Bypass password validation and use password anyway? [y/N]: "
        )
        if response.lower() != "y":
            global_logger.warning('The %s password not changed!', content)
            return
    store_func(password)


def main():
    global_logger.info('Welcome to the crypto module. You could press Ctrl+C to exit if needed.')
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
        _password_interactively(PasswordType.database)
        global_logger.info('Successfully update database password.')

    else:
        # 改 history server密码
        _password_interactively(PasswordType.history_server)
        global_logger.info('Successfully update history server password.')


if __name__ == '__main__':
    enable_kmc_logger()
    try:
        main()
    except KeyboardInterrupt:
        global_logger.info('Bye!')
    except KmcError as e:
        global_logger.error(e)
