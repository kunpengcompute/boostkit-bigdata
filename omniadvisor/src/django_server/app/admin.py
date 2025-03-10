from django.contrib import admin

from .models import Load, Task


# Register your models here.

@admin.register(Load)
class LoadAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'exec_master', 'exec_queue', 'exec_load', 'default_config', 'best_config', 'test_config')
    list_filter = ()


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'load', 'config', 'start_time', 'end_time', 'exec_status', 'exec_runtime', 'exec_trace')
    list_filter = ()
