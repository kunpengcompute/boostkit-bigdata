import django
from django.conf import settings

from common.constant import OA_CONF


if not settings.configured:
    # 配置Django数据库信息
    settings.configure(
        DEBUG=False,
        INSTALLED_APPS=[
            'server'
        ],
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.postgresql',
                'NAME': OA_CONF.postgresql_database_name,
                'USER': OA_CONF.postgresql_database_user,
                'PASSWORD': OA_CONF.postgresql_database_password,
                'HOST': OA_CONF.postgresql_database_host,
                'PORT': OA_CONF.postgresql_database_port,
            }
        },
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField',
        USE_TZ=True,
        TIME_ZONE=OA_CONF.admin_timezone
    )
    # 初始化 Django 环境
    django.setup()
