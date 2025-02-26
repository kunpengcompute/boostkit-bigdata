from django.contrib import admin

from .models import Config, Workload, Task, Performance


# Register your models here.


@admin.register(Config)
class ConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'param1', 'param2', 'param3')
    list_filter = ()


@admin.register(Workload)
class WorkloadAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'app_type', 'command', 'exec_queue', 'default_config', 'best_config', 'test_config')
    list_filter = ()


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'workload', 'config', 'start_time', 'end_time', 'execute_status', 'execute_performance', 'app_id')
    list_filter = ()


@admin.register(Performance)
class PerformanceAdmin(admin.ModelAdmin):
    list_display = ('id', 'workload', 'config', 'has_succeed', 'average_performance', 'poorest_performance', 'best_performance')
    list_filter = ()
