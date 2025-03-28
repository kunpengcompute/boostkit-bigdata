from django.db import connection


def create_table(model_class):
    """
    创建测试用表

    :param model_class: Model类
    """
    # 使用 schema_editor 创建表
    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(model_class)


def delete_table(model_class):
    """
    删除测试用表

    :param model_class: Model类
    """
    # 删除指定表名
    with connection.schema_editor() as schema_editor:
        schema_editor.delete_model(model_class)
