from django.contrib import admin

from .models import DatabaseLoad, DatabaseTuningRecord, DatabaseExamRecord


# Register your models here.

@admin.register(DatabaseLoad)
class LoadAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'exec_attr', 'default_config', 'best_config', 'test_config', 'backend_retest_forbidden'
    )
    list_filter = ()


@admin.register(DatabaseTuningRecord)
class TuningRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'load', 'config', 'method', 'method_extend', 'rounds')
    list_filter = ()


@admin.register(DatabaseExamRecord)
class ExamRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'load', 'tuning_record', 'start_time', 'end_time', 'status', 'runtime_f', 'application_id', 'trace')
    list_filter = ()

    # 增加对Load的显示
    def load(self, model):
        return model.tuning_record.load
    load.short_description = 'Load'

    # 对Runtime的浮点数保留3位有效数字
    def runtime_f(self, model):
        return round(model.runtime, 3)
    runtime_f.short_description = 'Runtime'
