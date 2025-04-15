import django
from django.conf import settings

from common.constant import OA_CONF


if not settings.configured:
    # 启用内存数据库，确保不影响原来的数据库
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': OA_CONF.server_base_dir / 'db.sqlite3',
        }
    }

    settings.configure(
        DATABASES=DATABASES,
        DEBUG=False,
        INSTALLED_APPS=['server'],
    )
    # 初始化 Django 环境
    django.setup()
