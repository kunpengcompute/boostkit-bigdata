from django.contrib import admin

from .models import DatabaseLoad, DatabaseTask


# Register your models here.

@admin.register(DatabaseLoad)
class LoadAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'exec_attr', 'default_config', 'best_config', 'test_config')
    list_filter = ()


@admin.register(DatabaseTask)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'load', 'config', 'start_time', 'end_time', 'status', 'runtime', 'trace')
    list_filter = ()
