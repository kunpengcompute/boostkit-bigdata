from django.db import connection


def create_test_table(model_class):
    """
    创建测试用表

    :param model_class: Model类
    """
    meta_data = getattr(model_class, '_meta')
    meta_data.db_table = f'test_{meta_data.db_table}'
    if meta_data.db_table not in connection.introspection.table_names():
        # 使用 schema_editor 创建表
        with connection.schema_editor() as schema_editor:
            schema_editor.create_model(model_class)


def delete_test_table(model_class):
    """
    删除测试用表

    :param model_class: Model类
    """
    # 删除指定表名，使用test表
    meta_data = getattr(model_class, '_meta')
    meta_data.db_table = f'test_{meta_data.db_table}'
    if meta_data.db_table in connection.introspection.table_names():
        with connection.schema_editor() as schema_editor:
            schema_editor.delete_model(model_class)
