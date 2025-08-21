import keyring
from keyrings.alt.file import PlaintextKeyring

keyring.set_keyring(PlaintextKeyring())

SERVICE_NAME = "omniadvisor"

django_secretkey_str = 'django_secretkey'
history_server_password_str = 'history_server'
database_password_str = 'database_password'


def _store_data(key: str, value: str):
    """
    向keyring中存储 key-value信息

    :param key: 要存储的键
    :param value: 要存储的值
    :returns: None
    """
    keyring.set_password(SERVICE_NAME, key, value)


def _get_data(key: str):
    """
    从keyring中获取 key对应的value值

    :param key: 要获取的值的键
    :returns: 要获取的值
    """
    value = keyring.get_password(SERVICE_NAME, key)
    if value:
        return value
    else:
        return None


def store_django_secretkey(secret_key: str):
    """
    向keyring中存储 django的 secret key 值

    :param secret_key: django 密钥
    :returns: None
    """
    _store_data(django_secretkey_str, secret_key)


def store_history_server_password(password: str):
    """
    向 keyring中存储 history server 的密码

    :param password: 密码
    :returns: None
    """
    _store_data(history_server_password_str, password)


def store_database_password_password(password: str):
    """
    向 keyring中存储 database 的密码

    :param password: 密码
    :returns: None
    """
    _store_data(database_password_str, password)


def get_django_secretkey() -> str:
    """
    从keyring中获取 django的 secret key

    """
    return _get_data(django_secretkey_str)


def get_history_server_password() -> str:
    """
    从keyring中获取 history server 的密码
    """
    return _get_data(history_server_password_str)


def get_database_password_password() -> str:
    """
    从keyring中获取 database 的密码
    """
    return _get_data(database_password_str)