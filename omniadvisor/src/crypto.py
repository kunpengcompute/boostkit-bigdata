import argparse

from common.crypto_tool import (
    store_history_server_password,
    store_database_password_password,
    get_history_server_password,
    get_database_password_password
)
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
    return get_history_server_password() or get_database_password_password()


def _update_database_password_interactively():
    """
    交互式刷新数据库密码
    """
    database_password = input("Please enter your database password: ")
    store_database_password_password(database_password)


def _update_history_server_password_interactively():
    """
    交互式刷新 history server密码
    """
    history_server_password = input("Please enter your history server password (If none, just press Enter.) : ")
    store_history_server_password(history_server_password)


if __name__ == '__main__':
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
