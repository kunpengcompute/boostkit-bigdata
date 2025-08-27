import django
from django.conf import settings

from common.constant import OA_CONF
from server.engine.settings import DATABASES, USE_TZ


if not settings.configured:
    # 配置Django数据库信息
    settings.configure(
        DATABASES=DATABASES,
        DEBUG=False,
        INSTALLED_APPS=['server'],
        USE_TZ=USE_TZ,
        TIME_ZONE=OA_CONF.admin_timezone
    )
    # 初始化 Django 环境
    django.setup()
