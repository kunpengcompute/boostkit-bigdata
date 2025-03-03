import django
from django.conf import settings
from django_server.engine.settings import DATABASES
# 没有配置时，才启用正式的数据库；测试绕过此通路
if not settings.configured:
    settings.configure(
        INSTALLED_APPS=['django_server'],  # 将当前文件视为一个“应用”
        DATABASES=DATABASES
    )
    django.setup()
