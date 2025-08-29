from django.contrib import admin

from .models import DatabaseLoad, DatabaseTuningRecord, DatabaseExamRecord

# Register your models here.
_LOAD_FIELDS = ('id', 'name', 'exec_attr', 'default_config', 'best_config', 'test_config', 'backend_retest_enable')
_TUNING_RECORD_FIELDS = ('id', 'load', 'config', 'method', 'method_extend', 'rounds')
_EXAM_RECORD_FIELDS = (
    'id', 'load', 'tuning_record', 'start_time', 'end_time', 'status', 'runtime_f', 'application_id', 'trace'
)


def remove_then_add(org_tuple: tuple, to_remove: tuple, to_add: tuple) -> tuple:
    """
    从原始元组中移除指定元素，然后添加新元素

    :param org_tuple: 原始元组
    :param to_remove: 需要移除的元素元组
    :param to_add: 需要添加的元素元组
    :return: 返回一个新的元组
    """
    # 将元组转换为列表以便修改
    temp_list = list(org_tuple)

    # 移除 to_remove 中的所有元素
    for item in to_remove:
        temp_list.remove(item)

    # 添加 to_add 中的所有元素
    temp_list.extend(to_add)

    # 将列表转换回元组并返回
    return tuple(temp_list)


class BaseAdmin(admin.ModelAdmin):
    # 每一页的最大数据量
    list_per_page = 50

    # 禁止编辑和保存
    def has_change_permission(self, request, obj=None):
        return False  # 返回 False，表示禁止编辑保存

    # 禁止删除
    def has_delete_permission(self, request, obj=None):
        return False  # 返回 False，表示禁止删除

    # 禁止添加数据
    def has_add_permission(self, request, obj=None):
        return False


# 定义内联模型：TuningRecord
class TuningRecordInline(admin.TabularInline):  # 或使用 StackedInline
    model = DatabaseTuningRecord
    extra = 1  # 默认添加一个空白的字段行
    fields = _TUNING_RECORD_FIELDS


# 定义内联模型：ExamRecord
class ExamRecordInline(admin.TabularInline):  # 或使用 StackedInline
    model = DatabaseExamRecord
    # 默认添加一个空白的字段行
    extra = 1
    # 这里去掉 load，加了 load会报错，原因暂时未知
    fields = remove_then_add(_EXAM_RECORD_FIELDS, ('load',), ())

    # 一定要有这个字段
    readonly_fields = ('runtime_f',)

    # tuning record 关联的 exam record 记录
    def runtime_f(self, model):
        return round(model.runtime, 3)

    runtime_f.short_description = 'Runtime'


@admin.register(DatabaseLoad)
class LoadAdmin(BaseAdmin):
    list_display = _LOAD_FIELDS
    list_filter = ()
    readonly_fields = _LOAD_FIELDS
    # search_fields 这个是页面上的搜索框，下同
    search_fields = remove_then_add(_LOAD_FIELDS, ('id',), ())
    # 内联模型，决定了
    inlines = [TuningRecordInline]


@admin.register(DatabaseTuningRecord)
class TuningRecordAdmin(BaseAdmin):
    list_display = _TUNING_RECORD_FIELDS
    list_filter = ()
    readonly_fields = _TUNING_RECORD_FIELDS
    search_fields = ('config', 'method')
    inlines = [ExamRecordInline]


@admin.register(DatabaseExamRecord)
class ExamRecordAdmin(BaseAdmin):
    list_display = _EXAM_RECORD_FIELDS
    list_filter = ()
    # 这里用 runtime_f 会报错，原因未知
    readonly_fields = _EXAM_RECORD_FIELDS
    search_fields = ('start_time', 'application_id')

    # 增加对Load的显示
    def load(self, model):
        return model.tuning_record.load
    load.short_description = 'Load'

    # 对Runtime的浮点数保留3位有效数字
    def runtime_f(self, model):
        return round(model.runtime, 3)
    runtime_f.short_description = 'Runtime'
