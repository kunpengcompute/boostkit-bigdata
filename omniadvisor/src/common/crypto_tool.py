import keyring
from keyrings.alt.file import PlaintextKeyring

keyring.set_keyring(PlaintextKeyring())

_SERVICE_NAME = "omniadvisor"
_DJANGO_SECRETKEY_STR = 'django_secretkey'
_HISTORY_SERVER_PASSWORD_STR = 'history_server'
_DATABASE_PASSWORD_STR = 'database_password'


class CryptoTool:

    @classmethod
    def _store_data(cls, key: str, value: str):
        """
        向keyring中存储 key-value信息

        :param key: 要存储的键
        :param value: 要存储的值
        :returns: None
        """
        keyring.set_password(_SERVICE_NAME, key, value)

    @classmethod
    def _get_data(cls, key: str):
        """
        从keyring中获取 key对应的value值

        :param key: 要获取的值的键
        :returns: 要获取的值
        """
        value = keyring.get_password(_SERVICE_NAME, key)
        if value:
            return value
        else:
            return None

    @classmethod
    def store_django_secretkey(cls, secret_key: str):
        """
        向keyring中存储 django的 secret key 值

        :param secret_key: django 密钥
        :returns: None
        """
        cls._store_data(_DJANGO_SECRETKEY_STR, secret_key)

    @classmethod
    def store_history_server_password(cls, password: str):
        """
        向 keyring中存储 history server 的密码

        :param password: 密码
        :returns: None
        """
        cls._store_data(_HISTORY_SERVER_PASSWORD_STR, password)

    @classmethod
    def store_database_password_password(cls, password: str):
        """
        向 keyring中存储 database 的密码

        :param password: 密码
        :returns: None
        """
        cls._store_data(_DATABASE_PASSWORD_STR, password)

    @classmethod
    def get_django_secretkey(cls) -> str:
        """
        从keyring中获取 django的 secret key

        """
        return cls._get_data(_DJANGO_SECRETKEY_STR)

    @classmethod
    def get_history_server_password(cls) -> str:
        """
        从keyring中获取 history server 的密码
        """
        return cls._get_data(_HISTORY_SERVER_PASSWORD_STR)

    @classmethod
    def get_database_password_password(cls) -> str:
        """
        从keyring中获取 database 的密码
        """
        return cls._get_data(_DATABASE_PASSWORD_STR)
