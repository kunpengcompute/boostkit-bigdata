# Create your models here.
from django.db import models


class Config(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    param1 = models.IntegerField()
    param2 = models.FloatField()
    param3 = models.FloatField()

    class Meta:
        db_table = 'omniadvisor_config'  # 自定义表名


class Workload(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    name = models.CharField(max_length=100, help_text="script name of the workload")
    app_type = models.CharField(max_length=10, help_text="app type, spark, for example")
    command = models.CharField(max_length=100, help_text="command to execute")
    exec_queue = models.CharField(max_length=100)
    default_config = models.ForeignKey(Config, on_delete=models.DO_NOTHING, help_text="config that user provides", related_name='workload_default_config')
    best_config = models.ForeignKey(Config, on_delete=models.DO_NOTHING, related_name='workload_best_config')
    test_config = models.ForeignKey(Config, on_delete=models.DO_NOTHING, help_text="config need to be test", related_name='workload_test_config')

    class Meta:
        db_table = 'omniadvisor_workload'  # 自定义表名


class Task(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    workload = models.ForeignKey(Workload, on_delete=models.DO_NOTHING)
    config = models.ForeignKey(Config, on_delete=models.DO_NOTHING)
    start_time = models.TimeField(auto_now_add=True, help_text="start time of task")
    end_time = models.TimeField(auto_now_add=False, help_text="end time of task")
    execute_status = models.CharField(max_length=10, choices=[("fail", "fail"), ("success", "success")])
    execute_performance = models.FloatField()
    app_id = models.CharField(max_length=32, help_text="when task finish, app id will be given")

    class Meta:
        db_table = 'omniadvisor_task'  # 自定义表名


class Performance(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    workload = models.ForeignKey(Workload, on_delete=models.DO_NOTHING)
    config = models.ForeignKey(Config, on_delete=models.DO_NOTHING)
    has_succeed = models.BooleanField(default=True)
    average_performance = models.FloatField()
    poorest_performance = models.FloatField()
    best_performance = models.FloatField()

    class Meta:
        db_table = 'omniadvisor_performance'  # 自定义表名