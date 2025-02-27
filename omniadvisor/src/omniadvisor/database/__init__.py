import django
from django.conf import settings
from django_server.engine.settings import DATABASES
if not settings.configured:
    settings.configure(
        INSTALLED_APPS=['django_server'],  # 将当前文件视为一个“应用”
        DATABASES=DATABASES
    )
    django.setup()
