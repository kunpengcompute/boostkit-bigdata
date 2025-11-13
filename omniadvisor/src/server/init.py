#!/usr/bin/env python
from django.core.management import call_command

def django_create_table():
    """
    初始化Django ORM，根据models在数据库中创建相应表
    """
    call_command('makemigrations', 'server', interactive=False)
    call_command('migrate', interactive=False)
