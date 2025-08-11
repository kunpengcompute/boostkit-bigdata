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
    list_display = ('id', 'tuning_record', 'start_time', 'end_time', 'status', 'runtime', 'application_id', 'trace')
    list_filter = ()
