import django
from django.conf import settings

from server.engine.settings import DATABASES


if not settings.configured:
    # 配置Django数据库信息
    settings.configure(
        DATABASES=DATABASES,
        DEBUG=False,
        INSTALLED_APPS=['server'],
        USE_TZ=False
    )
    # 初始化 Django 环境
    django.setup()
